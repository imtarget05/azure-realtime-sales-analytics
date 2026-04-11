"""Check failed AML job and submit new one with proper config."""
import os, sys, json
sys.path.insert(0, ".")

from azure.ai.ml import MLClient, command
from azure.ai.ml.entities import Environment
from azure.identity import DefaultAzureCredential

ml_client = MLClient(
    DefaultAzureCredential(),
    "34849ef9-3814-44df-ba32-a86ed9f2a69a",
    "rg-sales-analytics-dev",
    "aml-sales-analytics-d9bt2m2",
)

# 1. Check why last job failed
print("=== Last Job Details ===")
job = ml_client.jobs.get("helpful_brake_snkzrxbv7h")
print(f"  Name: {job.name}")
print(f"  Status: {job.status}")
print(f"  Error: {getattr(job, 'error', 'N/A')}")
if hasattr(job, 'outputs'):
    print(f"  Outputs: {job.outputs}")

# 2. Check compute state
cluster = ml_client.compute.get("training-cluster")
print(f"\n=== Compute ===")
print(f"  Name: {cluster.name}")
print(f"  Size: {cluster.size}")
print(f"  State: {getattr(cluster, 'state', 'N/A')}")
print(f"  Min instances: {cluster.min_instances}")
print(f"  Max instances: {cluster.max_instances}")

# 3. Submit new training job
print("\n=== Submitting New Training Job ===")
ml_dir = os.path.join(os.path.dirname(__file__), "..", "ml")

env = Environment(
    name="sales-forecast-env",
    conda_file=os.path.join(ml_dir, "conda_env.yml"),
    image="mcr.microsoft.com/azureml/openmpi4.1.0-ubuntu20.04:latest",
)

from datetime import datetime
job = command(
    code=ml_dir,
    command="python train_model.py --output-dir ./outputs/model_output --n-samples 5000",
    environment=env,
    compute="training-cluster",
    experiment_name="sales-forecast-training",
    display_name=f"train-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}",
    description="Automated training - fixed pipeline",
)

submitted = ml_client.jobs.create_or_update(job)
print(f"  Job Name: {submitted.name}")
print(f"  Status: {submitted.status}")
print(f"  Studio URL: {submitted.studio_url}")
print("\nJob submitted successfully!")
