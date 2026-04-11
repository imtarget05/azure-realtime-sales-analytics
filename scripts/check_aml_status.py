"""Check AML workspace computes and recent jobs."""
import sys
sys.path.insert(0, ".")
from azure.ai.ml import MLClient
from azure.identity import DefaultAzureCredential

ml_client = MLClient(
    DefaultAzureCredential(),
    "34849ef9-3814-44df-ba32-a86ed9f2a69a",
    "rg-sales-analytics-dev",
    "aml-sales-analytics-d9bt2m2",
)

print("=== Computes ===")
for c in ml_client.compute.list():
    sz = getattr(c, "size", "N/A")
    print(f"  {c.name}: type={c.type}, size={sz}")

print("\n=== Recent Jobs ===")
for j in list(ml_client.jobs.list(max_results=5)):
    print(f"  {j.name}: status={j.status}")
