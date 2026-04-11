"""
Script triển khai mô hình lên Azure Machine Learning
dưới dạng Real-time Endpoint (Online Endpoint).

NOTE: Phiên bản MLOps mới sử dụng mlops/deploy_to_endpoint.py
với tích hợp Model Registry đầy đủ. Script này giữ lại cho
backward compatibility và local development.
"""

import os
import sys
import json
import argparse

try:
    from azure.ai.ml import MLClient
    from azure.ai.ml.entities import (
        ManagedOnlineEndpoint,
        ManagedOnlineDeployment,
        Model,
        Environment,
        CodeConfiguration,
    )
    from azure.identity import DefaultAzureCredential
except ImportError:
    print("[ERROR] Cần cài đặt: pip install azure-ai-ml azure-identity")
    sys.exit(1)

sys.path.insert(0, ".")
from config.settings import AML_SUBSCRIPTION_ID, AML_RESOURCE_GROUP, AML_WORKSPACE_NAME


def deploy_model(model_dir: str, endpoint_name: str = "sales-forecast-endpoint"):
    """Triển khai mô hình lên Azure ML Online Endpoint."""

    # Kết nối Azure ML
    credential = DefaultAzureCredential()
    ml_client = MLClient(
        credential=credential,
        subscription_id=AML_SUBSCRIPTION_ID,
        resource_group_name=AML_RESOURCE_GROUP,
        workspace_name=AML_WORKSPACE_NAME,
    )
    print(f"[INFO] Đã kết nối Azure ML Workspace: {AML_WORKSPACE_NAME}")

    # 1. Đăng ký mô hình vào Model Registry
    print("[INFO] Đăng ký mô hình vào Model Registry...")
    metadata_path = os.path.join(model_dir, "model_metadata.json")
    model_tags = {"source": "local_deploy"}
    if os.path.exists(metadata_path):
        with open(metadata_path) as f:
            metadata = json.load(f)
        metrics = metadata.get("revenue_metrics", {})
        for k, v in metrics.items():
            model_tags[f"metric_{k}"] = str(v)

    model = Model(
        path=model_dir,
        name="sales-forecast-model",
        description="Mô hình dự đoán doanh thu và số lượng bán hàng",
        type="custom_model",
        tags=model_tags,
    )
    registered_model = ml_client.models.create_or_update(model)
    print(f"  Model: {registered_model.name} v{registered_model.version}")

    # 2. Tạo Endpoint
    print(f"[INFO] Tạo Endpoint: {endpoint_name}...")
    endpoint = ManagedOnlineEndpoint(
        name=endpoint_name,
        description="Real-time Sales Forecasting Endpoint",
        auth_mode="key",
    )
    ml_client.online_endpoints.begin_create_or_update(endpoint).result()
    print(f"  Endpoint URL: {endpoint.scoring_uri}")

    # 3. Tạo Environment
    env = Environment(
        name="sales-forecast-env",
        conda_file=os.path.join(os.path.dirname(__file__), "conda_env.yml"),
        image="mcr.microsoft.com/azureml/openmpi4.1.0-ubuntu20.04:latest",
    )

    # 4. Tạo Deployment
    print("[INFO] Tạo Deployment...")
    deployment = ManagedOnlineDeployment(
        name="sales-forecast-v1",
        endpoint_name=endpoint_name,
        model=registered_model,
        environment=env,
        code_configuration=CodeConfiguration(
            code=os.path.dirname(__file__),
            scoring_script="score.py",
        ),
        instance_type="Standard_DS2_v2",
        instance_count=1,
    )
    ml_client.online_deployments.begin_create_or_update(deployment).result()

    # 5. Đặt 100% traffic đến deployment mới
    endpoint.traffic = {"sales-forecast-v1": 100}
    ml_client.online_endpoints.begin_create_or_update(endpoint).result()

    print("\n[INFO] Triển khai hoàn tất!")
    print(f"  Endpoint: {endpoint_name}")
    print(f"  Scoring URI: {endpoint.scoring_uri}")

    # Lấy API key
    keys = ml_client.online_endpoints.get_keys(endpoint_name)
    print(f"  API Key: {keys.primary_key[:20]}...")

    return endpoint


def main():
    parser = argparse.ArgumentParser(description="Deploy Sales Forecast Model to Azure ML")
    parser.add_argument("--model-dir", type=str, default="ml/model_output", help="Model directory")
    parser.add_argument("--endpoint-name", type=str, default="sales-forecast-endpoint")
    args = parser.parse_args()

    deploy_model(args.model_dir, args.endpoint_name)


if __name__ == "__main__":
    main()
