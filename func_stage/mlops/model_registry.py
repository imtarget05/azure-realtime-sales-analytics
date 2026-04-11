"""
Model Registry — Quản lý version mô hình trên Azure ML.

Chức năng:
  - Đăng ký model mới (với metadata, metrics, tags)
  - Liệt kê tất cả versions
  - So sánh metrics giữa các versions
  - Rollback về version cũ
  - Promote model (staging → production)
"""

import os
import sys
import json
import argparse
from datetime import datetime

try:
    from azure.ai.ml import MLClient
    from azure.ai.ml.entities import Model
    from azure.ai.ml.constants import AssetTypes
    from azure.identity import DefaultAzureCredential
except ImportError:
    print("[ERROR] Cần cài đặt: pip install azure-ai-ml azure-identity")
    sys.exit(1)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config.settings import AML_SUBSCRIPTION_ID, AML_RESOURCE_GROUP, AML_WORKSPACE_NAME

MODEL_NAME = "sales-forecast-model"


def get_ml_client() -> MLClient:
    """Tạo MLClient kết nối Azure ML Workspace."""
    return MLClient(
        credential=DefaultAzureCredential(),
        subscription_id=AML_SUBSCRIPTION_ID,
        resource_group_name=AML_RESOURCE_GROUP,
        workspace_name=AML_WORKSPACE_NAME,
    )


def register_model(
    model_path: str,
    metrics: dict,
    description: str = "",
    tags: dict | None = None,
) -> Model:
    """
    Đăng ký model mới vào Registry với metadata đầy đủ.

    Args:
        model_path: Đường dẫn đến thư mục model (chứa .pkl files)
        metrics: Dict metrics (mae, rmse, r2_score, ...)
        description: Mô tả version
        tags: Custom tags
    Returns:
        Model đã đăng ký (với version number)
    """
    ml_client = get_ml_client()

    model_tags = {
        "registered_at": datetime.utcnow().isoformat(),
        "framework": "scikit-learn",
        "task": "regression",
    }
    # Thêm metrics vào tags để dễ query
    for key, value in metrics.items():
        model_tags[f"metric_{key}"] = str(value)

    if tags:
        model_tags.update(tags)

    model = Model(
        path=model_path,
        name=MODEL_NAME,
        description=description or f"Sales forecast model — {datetime.utcnow().strftime('%Y-%m-%d %H:%M')}",
        type=AssetTypes.CUSTOM_MODEL,
        tags=model_tags,
        properties={
            "metrics": json.dumps(metrics),
            "training_date": datetime.utcnow().isoformat(),
        },
    )

    registered = ml_client.models.create_or_update(model)
    print(f"[REGISTRY] Model registered: {registered.name} v{registered.version}")
    return registered


def list_versions(top_n: int = 10) -> list:
    """Liệt kê N version gần nhất."""
    ml_client = get_ml_client()
    versions = list(ml_client.models.list(name=MODEL_NAME))
    versions.sort(key=lambda m: int(m.version), reverse=True)

    print(f"\n{'Version':<10} {'R²':<10} {'MAE':<12} {'RMSE':<12} {'Registered At':<25}")
    print("─" * 70)
    for m in versions[:top_n]:
        r2 = m.tags.get("metric_r2_score", "N/A")
        mae = m.tags.get("metric_mae", "N/A")
        rmse = m.tags.get("metric_rmse", "N/A")
        registered = m.tags.get("registered_at", "N/A")[:19]
        print(f"v{m.version:<9} {r2:<10} {mae:<12} {rmse:<12} {registered:<25}")

    return versions[:top_n]


def compare_versions(version_a: str, version_b: str) -> dict:
    """So sánh metrics giữa 2 version model."""
    ml_client = get_ml_client()

    model_a = ml_client.models.get(name=MODEL_NAME, version=version_a)
    model_b = ml_client.models.get(name=MODEL_NAME, version=version_b)

    metrics_a = json.loads(model_a.properties.get("metrics", "{}"))
    metrics_b = json.loads(model_b.properties.get("metrics", "{}"))

    comparison = {}
    print(f"\n{'Metric':<20} {'v{version_a}':<15} {'v{version_b}':<15} {'Diff':<15} {'Winner':<10}")
    print("─" * 75)

    all_keys = set(list(metrics_a.keys()) + list(metrics_b.keys()))
    for key in sorted(all_keys):
        val_a = metrics_a.get(key, 0)
        val_b = metrics_b.get(key, 0)
        diff = val_b - val_a

        # R² higher is better, MAE/RMSE lower is better
        if "r2" in key:
            winner = version_b if diff > 0 else version_a
        else:
            winner = version_b if diff < 0 else version_a

        comparison[key] = {"v_a": val_a, "v_b": val_b, "diff": diff, "winner": winner}
        print(f"{key:<20} {val_a:<15.4f} {val_b:<15.4f} {diff:<+15.4f} v{winner}")

    return comparison


def get_best_version(metric: str = "r2_score") -> Model:
    """Tìm version có metric tốt nhất."""
    ml_client = get_ml_client()
    versions = list(ml_client.models.list(name=MODEL_NAME))

    best_model = None
    best_value = float("-inf") if "r2" in metric else float("inf")

    for m in versions:
        val_str = m.tags.get(f"metric_{metric}")
        if val_str is None:
            continue
        val = float(val_str)

        if "r2" in metric:
            if val > best_value:
                best_value = val
                best_model = m
        else:
            if val < best_value:
                best_value = val
                best_model = m

    if best_model:
        print(f"[REGISTRY] Best model (by {metric}): v{best_model.version} = {best_value:.4f}")
    return best_model


def promote_model(version: str, stage: str = "production") -> Model:
    """
    Promote model version bằng cách gắn tag stage.
    Azure ML dùng tags thay vì stages (khác với MLflow).
    """
    ml_client = get_ml_client()
    model = ml_client.models.get(name=MODEL_NAME, version=version)

    # Remove stage tag từ tất cả model khác
    all_versions = list(ml_client.models.list(name=MODEL_NAME))
    for m in all_versions:
        if m.tags.get("stage") == stage and m.version != version:
            m.tags["stage"] = "archived"
            ml_client.models.create_or_update(m)

    # Set stage cho version mới
    model.tags["stage"] = stage
    model.tags[f"promoted_to_{stage}_at"] = datetime.utcnow().isoformat()
    updated = ml_client.models.create_or_update(model)
    print(f"[REGISTRY] Model v{version} promoted to '{stage}'")
    return updated


def main():
    parser = argparse.ArgumentParser(description="Model Registry Management")
    sub = parser.add_subparsers(dest="command")

    # register
    reg = sub.add_parser("register", help="Register a new model version")
    reg.add_argument("--model-path", required=True)
    reg.add_argument("--metrics-file", required=True, help="JSON file with metrics")
    reg.add_argument("--description", default="")

    # list
    sub.add_parser("list", help="List model versions")

    # compare
    cmp = sub.add_parser("compare", help="Compare two versions")
    cmp.add_argument("--version-a", required=True)
    cmp.add_argument("--version-b", required=True)

    # best
    best = sub.add_parser("best", help="Find best model version")
    best.add_argument("--metric", default="r2_score")

    # promote
    prm = sub.add_parser("promote", help="Promote model to stage")
    prm.add_argument("--version", required=True)
    prm.add_argument("--stage", default="production")

    args = parser.parse_args()

    if args.command == "register":
        with open(args.metrics_file) as f:
            metrics = json.load(f)
        register_model(args.model_path, metrics, args.description)
    elif args.command == "list":
        list_versions()
    elif args.command == "compare":
        compare_versions(args.version_a, args.version_b)
    elif args.command == "best":
        get_best_version(args.metric)
    elif args.command == "promote":
        promote_model(args.version, args.stage)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
