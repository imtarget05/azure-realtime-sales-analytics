"""
Training Pipeline Trigger — Kích hoạt Azure ML Training Pipeline.

Được gọi bởi:
  - GitHub Actions CI/CD (khi code ml/ thay đổi)
  - Drift Detection workflow (khi phát hiện drift)
  - Manual trigger

Pipeline flow:
  1. Submit training job lên Azure ML compute cluster
  2. Đợi job hoàn thành
  3. Đăng ký model mới vào Registry
  4. So sánh với model production hiện tại
  5. Nếu model mới tốt hơn → promote lên production
"""

import os
import sys
import json
import argparse
import time
from datetime import datetime

try:
    from azure.ai.ml import MLClient, command, Input
    from azure.ai.ml.entities import Environment, BuildContext
    from azure.identity import DefaultAzureCredential
except ImportError:
    print("[ERROR] Cần cài đặt: pip install azure-ai-ml azure-identity")
    sys.exit(1)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config.settings import AML_SUBSCRIPTION_ID, AML_RESOURCE_GROUP, AML_WORKSPACE_NAME
from mlops.model_registry import register_model, get_best_version, promote_model


COMPUTE_CLUSTER = "training-cluster"
EXPERIMENT_NAME = "sales-forecast-training"


def get_ml_client() -> MLClient:
    return MLClient(
        credential=DefaultAzureCredential(),
        subscription_id=AML_SUBSCRIPTION_ID,
        resource_group_name=AML_RESOURCE_GROUP,
        workspace_name=AML_WORKSPACE_NAME,
    )


def submit_training_job(
    ml_client: MLClient,
    n_samples: int = 50000,
) -> str:
    """
    Submit training job lên Azure ML compute cluster.
    Returns: Job name (để theo dõi)
    """
    print(f"[TRAIN] Submitting training job to {COMPUTE_CLUSTER}...")

    # Environment
    env = Environment(
        name="sales-forecast-training-env",
        conda_file=os.path.join(os.path.dirname(__file__), "..", "ml", "conda_env.yml"),
        image="mcr.microsoft.com/azureml/openmpi4.1.0-ubuntu20.04:latest",
    )

    # Training command
    job = command(
        code=os.path.join(os.path.dirname(__file__), "..", "ml"),
        command=(
            "python train_model.py "
            "--output-dir ./outputs/model_output "
            f"--n-samples {n_samples}"
        ),
        environment=env,
        compute=COMPUTE_CLUSTER,
        experiment_name=EXPERIMENT_NAME,
        display_name=f"train-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}",
        description="Automated training triggered by MLOps pipeline",
        tags={
            "triggered_by": "mlops_pipeline",
            "trigger_time": datetime.utcnow().isoformat(),
        },
    )

    submitted_job = ml_client.jobs.create_or_update(job)
    print(f"[TRAIN] Job submitted: {submitted_job.name}")
    print(f"[TRAIN] Studio URL: {submitted_job.studio_url}")

    return submitted_job.name


def wait_for_job(ml_client: MLClient, job_name: str, timeout_minutes: int = 60) -> dict:
    """Đợi training job hoàn thành."""
    print(f"[TRAIN] Waiting for job {job_name}...")
    start_time = time.time()
    timeout_seconds = timeout_minutes * 60

    while True:
        job = ml_client.jobs.get(job_name)
        status = job.status

        elapsed = time.time() - start_time
        print(f"  [{elapsed:.0f}s] Status: {status}")

        if status in ("Completed", "Failed", "Canceled"):
            break

        if elapsed > timeout_seconds:
            print(f"[TRAIN] Timeout after {timeout_minutes} minutes")
            ml_client.jobs.cancel(job_name)
            return {"status": "Timeout", "job_name": job_name}

        time.sleep(30)

    if status == "Completed":
        print(f"[TRAIN] Job completed successfully in {elapsed:.0f}s")
        # Download outputs
        download_root = os.path.join(os.path.dirname(__file__), "..", "ml", "artifacts")
        ml_client.jobs.download(
            job_name,
            output_name="default",
            download_path=download_root,
        )
        return {
            "status": "Completed",
            "job_name": job_name,
            "model_output_dir": os.path.join(download_root, "outputs", "model_output"),
        }
    else:
        print(f"[TRAIN] Job ended with status: {status}")
        return {"status": status, "job_name": job_name}


def register_and_evaluate(model_output_dir: str) -> bool:
    """
    Đăng ký model mới và so sánh với production model.
    Returns: True nếu model mới tốt hơn và đã promote.
    """
    # Load metrics từ training output
    metadata_path = os.path.join(model_output_dir, "model_metadata.json")
    if not os.path.exists(metadata_path):
        print(f"[TRAIN] No metadata found at {metadata_path}")
        return False

    with open(metadata_path) as f:
        metadata = json.load(f)

    metrics = metadata.get("revenue_metrics", {})
    print(f"[TRAIN] New model metrics: {json.dumps(metrics, indent=2)}")

    # Đăng ký vào Registry
    new_model = register_model(
        model_path=model_output_dir,
        metrics=metrics,
        description=f"Training pipeline — {datetime.utcnow().strftime('%Y-%m-%d %H:%M')}",
        tags={"source": "training_pipeline"},
    )

    # So sánh với production model hiện tại
    current_best = get_best_version(metric="r2_score")

    if current_best and current_best.version != new_model.version:
        best_r2 = float(current_best.tags.get("metric_r2_score", "0"))
        new_r2 = metrics.get("r2_score", 0)

        if new_r2 >= best_r2:
            print(f"[TRAIN] New model (R²={new_r2:.4f}) >= current best (R²={best_r2:.4f})")
            print("[TRAIN] Promoting new model to production...")
            promote_model(new_model.version, stage="production")
            return True
        else:
            print(f"[TRAIN] New model (R²={new_r2:.4f}) < current best (R²={best_r2:.4f})")
            print("[TRAIN] Keeping current production model")
            return False
    else:
        # Đây là model đầu tiên → tự động promote
        print("[TRAIN] First model registered — promoting to production")
        promote_model(new_model.version, stage="production")
        return True


def run_pipeline(n_samples: int = 50000, timeout_minutes: int = 60) -> dict:
    """Chạy full training pipeline: train → register → evaluate → promote."""
    ml_client = get_ml_client()

    print("=" * 60)
    print("MLOPS TRAINING PIPELINE")
    print(f"Timestamp: {datetime.utcnow().isoformat()}")
    print("=" * 60)

    # Step 1: Submit training job
    job_name = submit_training_job(ml_client, n_samples)

    # Step 2: Wait for completion
    result = wait_for_job(ml_client, job_name, timeout_minutes)

    if result["status"] != "Completed":
        print(f"[PIPELINE] Training failed: {result['status']}")
        return result

    # Step 3: Register and evaluate
    model_output = result.get("model_output_dir") or os.path.join(
        os.path.dirname(__file__), "..", "ml", "outputs", "model_output"
    )
    promoted = register_and_evaluate(model_output)

    result["model_promoted"] = promoted
    print(f"\n[PIPELINE] Pipeline complete. Model promoted: {promoted}")
    return result


def main():
    parser = argparse.ArgumentParser(description="Trigger ML Training Pipeline")
    parser.add_argument("--n-samples", type=int, default=50000)
    parser.add_argument("--timeout", type=int, default=60, help="Timeout in minutes")
    args = parser.parse_args()

    run_pipeline(args.n_samples, args.timeout)


if __name__ == "__main__":
    main()
