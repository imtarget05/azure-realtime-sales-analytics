"""
Deploy to Online Endpoint — Triển khai model lên Azure ML Managed Endpoint.

Thay thế ML API Pod trên AKS bằng Azure ML Managed Online Endpoint:
  - Tích hợp trực tiếp với Model Registry
  - Auto-scaling, health probes, traffic splitting
  - Blue/green deployment (canary release)
  - Ghi prediction trực tiếp vào Azure SQL

Được gọi bởi:
  - GitHub Actions (sau khi training thành công)
  - Manual deployment
"""

import os
import sys
import json
import argparse
from datetime import datetime

try:
    from azure.ai.ml import MLClient
    from azure.ai.ml.entities import (
        ManagedOnlineEndpoint,
        ManagedOnlineDeployment,
        Model,
        Environment,
        CodeConfiguration,
        ProbeSettings,
    )
    from azure.identity import DefaultAzureCredential
except ImportError:
    print("[ERROR] Cần cài đặt: pip install azure-ai-ml azure-identity")
    sys.exit(1)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config.settings import AML_SUBSCRIPTION_ID, AML_RESOURCE_GROUP, AML_WORKSPACE_NAME
from mlops.model_registry import get_ml_client as get_registry_client, MODEL_NAME

ENDPOINT_NAME = "sales-forecast-endpoint"


def get_ml_client() -> MLClient:
    return MLClient(
        credential=DefaultAzureCredential(),
        subscription_id=AML_SUBSCRIPTION_ID,
        resource_group_name=AML_RESOURCE_GROUP,
        workspace_name=AML_WORKSPACE_NAME,
    )


def get_production_model(ml_client: MLClient) -> Model:
    """Lấy model version đang ở stage 'production'."""
    versions = list(ml_client.models.list(name=MODEL_NAME))

    for m in versions:
        if m.tags.get("stage") == "production":
            print(f"[DEPLOY] Production model found: v{m.version}")
            return m

    # Fallback: lấy version mới nhất
    if versions:
        versions.sort(key=lambda x: int(x.version), reverse=True)
        latest = versions[0]
        print(f"[DEPLOY] No production tag found, using latest: v{latest.version}")
        return latest

    raise ValueError("No model found in registry")


def get_latest_model(ml_client: MLClient) -> Model:
    """Lấy model version mới nhất."""
    versions = list(ml_client.models.list(name=MODEL_NAME))
    if not versions:
        raise ValueError("No model found in registry")
    versions.sort(key=lambda x: int(x.version), reverse=True)
    return versions[0]


def ensure_endpoint(ml_client: MLClient, endpoint_name: str) -> ManagedOnlineEndpoint:
    """Tạo endpoint nếu chưa tồn tại."""
    try:
        endpoint = ml_client.online_endpoints.get(endpoint_name)
        print(f"[DEPLOY] Endpoint exists: {endpoint_name}")
        return endpoint
    except Exception:
        print(f"[DEPLOY] Creating endpoint: {endpoint_name}...")
        endpoint = ManagedOnlineEndpoint(
            name=endpoint_name,
            description="Real-time Sales Forecasting — Managed Online Endpoint",
            auth_mode="key",
            tags={
                "project": "retail-sales-forecasting",
                "managed_by": "mlops_pipeline",
            },
        )
        ml_client.online_endpoints.begin_create_or_update(endpoint).result()
        print(f"[DEPLOY] Endpoint created: {endpoint_name}")
        return endpoint


def deploy_model(
    model: Model,
    endpoint_name: str = ENDPOINT_NAME,
    instance_type: str = "Standard_DS2_v2",
    instance_count: int = 1,
    traffic_pct: int = 100,
) -> ManagedOnlineDeployment:
    """
    Deploy model lên Online Endpoint với blue/green strategy.

    Nếu đã có deployment → tạo deployment mới (green) → chuyển traffic dần.
    """
    ml_client = get_ml_client()

    # Ensure endpoint
    ensure_endpoint(ml_client, endpoint_name)

    # Deployment name dựa trên model version
    deployment_name = f"v{model.version}-{datetime.utcnow().strftime('%Y%m%d')}"
    # Azure ML yêu cầu deployment name chỉ chứa lowercase, numbers, dashes
    deployment_name = deployment_name.lower().replace("_", "-")[:32]

    print(f"[DEPLOY] Creating deployment: {deployment_name}")
    print(f"  Model: {model.name} v{model.version}")
    print(f"  Instance: {instance_type} x{instance_count}")

    # Environment
    env = Environment(
        name="sales-forecast-scoring-env",
        conda_file=os.path.join(os.path.dirname(__file__), "..", "ml", "conda_env.yml"),
        image="mcr.microsoft.com/azureml/openmpi4.1.0-ubuntu20.04:latest",
    )

    # Deployment
    deployment = ManagedOnlineDeployment(
        name=deployment_name,
        endpoint_name=endpoint_name,
        model=model,
        environment=env,
        code_configuration=CodeConfiguration(
            code=os.path.join(os.path.dirname(__file__), "..", "ml"),
            scoring_script="score.py",
        ),
        instance_type=instance_type,
        instance_count=instance_count,
        liveness_probe=ProbeSettings(
            initial_delay=30,
            period=10,
            timeout=2,
            failure_threshold=30,
        ),
        readiness_probe=ProbeSettings(
            initial_delay=10,
            period=10,
            timeout=2,
            failure_threshold=30,
        ),
    )

    print("[DEPLOY] Deploying (this may take a few minutes)...")
    ml_client.online_deployments.begin_create_or_update(deployment).result()

    # Route traffic
    endpoint = ml_client.online_endpoints.get(endpoint_name)
    endpoint.traffic = {deployment_name: traffic_pct}

    # Nếu có deployment cũ, giảm traffic
    existing_deployments = list(ml_client.online_deployments.list(endpoint_name))
    for d in existing_deployments:
        if d.name != deployment_name:
            remaining = 100 - traffic_pct
            if remaining > 0:
                endpoint.traffic[d.name] = remaining
            else:
                endpoint.traffic[d.name] = 0

    ml_client.online_endpoints.begin_create_or_update(endpoint).result()

    # Get endpoint info
    endpoint = ml_client.online_endpoints.get(endpoint_name)
    keys = ml_client.online_endpoints.get_keys(endpoint_name)

    print(f"\n[DEPLOY] Deployment complete!")
    print(f"  Endpoint: {endpoint_name}")
    print(f"  Scoring URI: {endpoint.scoring_uri}")
    print(f"  Deployment: {deployment_name}")
    print(f"  Traffic: {endpoint.traffic}")
    print(f"  API Key: {keys.primary_key[:20]}...")

    return deployment


def cleanup_old_deployments(endpoint_name: str = ENDPOINT_NAME, keep_latest: int = 2):
    """Xoá deployments cũ, giữ lại N deployments mới nhất."""
    ml_client = get_ml_client()
    deployments = list(ml_client.online_deployments.list(endpoint_name))

    if len(deployments) <= keep_latest:
        print(f"[DEPLOY] Only {len(deployments)} deployments, nothing to clean up")
        return

    # Sort by creation time
    deployments.sort(key=lambda d: d.name, reverse=True)
    to_delete = deployments[keep_latest:]

    for d in to_delete:
        # Kiểm tra traffic trước khi xoá
        endpoint = ml_client.online_endpoints.get(endpoint_name)
        if endpoint.traffic.get(d.name, 0) > 0:
            print(f"[DEPLOY] Skipping {d.name} — still receiving traffic")
            continue

        print(f"[DEPLOY] Deleting old deployment: {d.name}")
        ml_client.online_deployments.begin_delete(
            name=d.name, endpoint_name=endpoint_name
        ).result()


def main():
    parser = argparse.ArgumentParser(description="Deploy model to Azure ML Online Endpoint")
    parser.add_argument("--use-latest", action="store_true", help="Deploy latest model version")
    parser.add_argument("--version", type=str, help="Deploy specific model version")
    parser.add_argument("--endpoint-name", default=ENDPOINT_NAME)
    parser.add_argument("--instance-type", default="Standard_DS2_v2")
    parser.add_argument("--instance-count", type=int, default=1)
    parser.add_argument("--traffic", type=int, default=100, help="Traffic percentage (0-100)")
    parser.add_argument("--cleanup", action="store_true", help="Clean up old deployments")
    args = parser.parse_args()

    ml_client = get_ml_client()

    if args.cleanup:
        cleanup_old_deployments(args.endpoint_name)
        return

    if args.version:
        model = ml_client.models.get(name=MODEL_NAME, version=args.version)
    elif args.use_latest:
        model = get_latest_model(ml_client)
    else:
        model = get_production_model(ml_client)

    deploy_model(
        model=model,
        endpoint_name=args.endpoint_name,
        instance_type=args.instance_type,
        instance_count=args.instance_count,
        traffic_pct=args.traffic,
    )

    if args.cleanup:
        cleanup_old_deployments(args.endpoint_name)


if __name__ == "__main__":
    main()
