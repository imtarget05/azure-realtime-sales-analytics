#!/usr/bin/env python3
"""Fix pipeline cluster node type to avoid CLOUD_PROVIDER_RESOURCE_STOCKOUT."""
import os, sys, json, urllib.request, urllib.error

sys.path.insert(0, ".")
from dotenv import load_dotenv; load_dotenv()

HOST = os.getenv("DATABRICKS_HOST", "").rstrip("/")
TOKEN = os.getenv("DATABRICKS_TOKEN", "")
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

def api_post(path, data):
    body = json.dumps(data).encode()
    req = urllib.request.Request(f"{HOST}{path}", data=body, headers=HEADERS, method="POST")
    try:
        return json.loads(urllib.request.urlopen(req, timeout=20).read())
    except urllib.error.HTTPError as e:
        print(f"HTTP {e.code}: {e.read().decode()[:300]}")
        return {}

def api_get(path):
    req = urllib.request.Request(f"{HOST}{path}", headers=HEADERS)
    return json.loads(urllib.request.urlopen(req, timeout=20).read())

JOB_ID = 274825266735713

# Cancel current stuck run
print("=== Cancelling stuck run 502241543535383 ===")
api_post("/api/2.1/jobs/runs/cancel", {"run_id": 502241543535383})
print("  Cancelled")

# Update job clusters to use Standard_E4ds_v5 (more available, same cost tier)
print("\n=== Updating cluster node types ===")
update = {
    "job_id": JOB_ID,
    "new_settings": {
        "job_clusters": [
            {
                "job_cluster_key": "etl_cluster",
                "new_cluster": {
                    "spark_version": "14.3.x-scala2.12",
                    "node_type_id": "Standard_D4ds_v5",
                    "autoscale": {"min_workers": 1, "max_workers": 2},
                    "spark_conf": {
                        "spark.databricks.delta.preview.enabled": "true"
                    },
                    "azure_attributes": {
                        "first_on_demand": 1,
                        "availability": "ON_DEMAND_AZURE",
                        "spot_bid_max_price": -1
                    }
                }
            },
            {
                "job_cluster_key": "ml_cluster",
                "new_cluster": {
                    "spark_version": "14.3.x-ml-scala2.12",
                    "node_type_id": "Standard_D4ds_v5",
                    "autoscale": {"min_workers": 1, "max_workers": 2},
                    "spark_conf": {
                        "spark.databricks.delta.preview.enabled": "true"
                    },
                    "azure_attributes": {
                        "first_on_demand": 1,
                        "availability": "ON_DEMAND_AZURE",
                        "spot_bid_max_price": -1
                    }
                }
            }
        ]
    }
}
result = api_post("/api/2.1/jobs/update", update)
print(f"  Update result: {result}")

# Verify
job = api_get(f"/api/2.1/jobs/get?job_id={JOB_ID}")
for jc in job.get("settings", {}).get("job_clusters", []):
    nc = jc.get("new_cluster", {})
    print(f"  {jc['job_cluster_key']}: {nc.get('node_type_id')} (autoscale {nc.get('autoscale',{}).get('min_workers',0)}-{nc.get('autoscale',{}).get('max_workers',0)})")

# Trigger fresh run
print("\n=== Triggering new run with updated node types ===")
run_result = api_post("/api/2.1/jobs/run-now", {"job_id": JOB_ID})
new_run_id = run_result.get("run_id")
print(f"  New run ID: {new_run_id}")
print(f"  Monitor: python scripts/run_pipeline_once.py")
