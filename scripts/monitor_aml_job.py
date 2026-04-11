"""Monitor AML job status."""
import sys, time
sys.path.insert(0, ".")
from azure.ai.ml import MLClient
from azure.identity import DefaultAzureCredential

ml_client = MLClient(
    DefaultAzureCredential(),
    "34849ef9-3814-44df-ba32-a86ed9f2a69a",
    "rg-sales-analytics-dev",
    "aml-sales-analytics-d9bt2m2",
)

job_name = "mighty_bell_00lbnc9m91"
start = time.time()

while True:
    job = ml_client.jobs.get(job_name)
    elapsed = time.time() - start
    print(f"[{elapsed:.0f}s] Status: {job.status}")

    if job.status in ("Completed", "Failed", "Canceled", "CancelRequested"):
        if job.status == "Failed":
            print(f"Error: {getattr(job, 'error', 'N/A')}")
        break

    if elapsed > 1200:
        print("Timeout after 600s, stopping poll (job continues in cloud)")
        break

    time.sleep(30)
