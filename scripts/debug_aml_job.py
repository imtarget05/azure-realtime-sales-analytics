"""Get detailed AML job failure info."""
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

job_name = "teal_kite_t0vcx8qxdx"
job = ml_client.jobs.get(job_name)

print(f"Status: {job.status}")
print(f"Error: {job.error}")
print(f"Display Name: {job.display_name}")
print(f"Compute: {job.compute}")

# Get error details
if hasattr(job, 'error') and job.error:
    print(f"Error code: {getattr(job.error, 'code', 'N/A')}")
    print(f"Error message: {getattr(job.error, 'message', 'N/A')}")

# Try to get job logs
try:
    import tempfile, os
    dl_path = tempfile.mkdtemp()
    ml_client.jobs.download(job_name, download_path=dl_path, all=True)
    for root, dirs, files in os.walk(dl_path):
        for f in files:
            fp = os.path.join(root, f)
            if f.endswith(('.txt', '.log', '.json')):
                print(f"\n=== {f} ===")
                with open(fp, 'r', errors='replace') as fh:
                    content = fh.read()
                    print(content[-3000:] if len(content) > 3000 else content)
except Exception as e:
    print(f"Failed to download logs: {e}")
