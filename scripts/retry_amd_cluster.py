#!/usr/bin/env python3
"""Switch to AMD node type and retry pipeline."""
import urllib.request, json, ssl

ctx = ssl.create_default_context()
import os
from dotenv import load_dotenv
load_dotenv()
HOST = os.getenv("DATABRICKS_HOST", "https://adb-7405607469187602.2.azuredatabricks.net")
TOKEN = os.getenv("DATABRICKS_TOKEN", "")
JOB_ID = 274825266735713

def api(path, data=None):
    body = json.dumps(data).encode() if data else None
    req = urllib.request.Request(
        HOST + path, data=body,
        headers={"Authorization": "Bearer " + TOKEN, "Content-Type": "application/json"},
    )
    return json.loads(urllib.request.urlopen(req, context=ctx, timeout=30).read())


# Get current job
job = api(f"/api/2.1/jobs/get?job_id={JOB_ID}")
settings = job.get("settings", {})
clusters = settings.get("job_clusters", [])

# Try AMD-based VM (better regional availability)
NEW_TYPE = "Standard_D4as_v5"
print(f"Switching clusters to {NEW_TYPE} (AMD, better availability)...")

for c in clusters:
    nc = c.get("new_cluster", {})
    old_type = nc.get("node_type_id", "?")
    nc["node_type_id"] = NEW_TYPE
    nc["driver_node_type_id"] = NEW_TYPE
    key = c.get("job_cluster_key", "?")
    print(f"  {key}: {old_type} -> {NEW_TYPE}")

# Update
api("/api/2.1/jobs/reset", {"job_id": JOB_ID, "new_settings": settings})
print("Job updated successfully")

# Trigger
result = api("/api/2.1/jobs/run-now", {"job_id": JOB_ID})
new_run = result.get("run_id")
print(f"New run triggered: {new_run}")
