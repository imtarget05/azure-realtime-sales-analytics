"""
Try very specific VM types that might work:
- Standard_DS12_v2: DSv2 family (quota=4), 4 cores, 28GB - different from DS3_v2
- Standard_L4s: LS family (quota=4), deprecated but might work
- Standard_D12_v2: Dv2 family (quota=4), 4 cores 28GB
"""
import os, requests, json, time
from dotenv import load_dotenv

load_dotenv()
HOST = os.getenv("DATABRICKS_HOST", "").rstrip("/")
TOKEN = os.getenv("DATABRICKS_TOKEN", "")
JOB_ID = "274825266735713"
H = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

VM_CANDIDATES = [
    "Standard_DS12_v2",   # DSv2 family, quota=4, 4c 28GB
    "Standard_D12_v2",    # Dv2 family, quota=4, 4c 28GB
    "Standard_L4s",       # LS family, quota=4, 4c 32GB (deprecated)
    "Standard_D3_v2",     # Dv2 family, quota=4, 4c 14GB
]

import sys
idx = int(sys.argv[1]) if len(sys.argv) > 1 else 0
VM = VM_CANDIDATES[idx]

print(f"=== Trying: {VM} (index {idx}) ===")

# Cancel all
print("Cancelling active runs...")
runs = requests.get(f"{HOST}/api/2.1/jobs/runs/list", headers=H,
                    params={"job_id": JOB_ID, "active_only": True}).json()
for r in runs.get("runs", []):
    requests.post(f"{HOST}/api/2.1/jobs/runs/cancel", headers=H, json={"run_id": r["run_id"]})
time.sleep(10)

# Terminate all clusters
clusters = requests.get(f"{HOST}/api/2.0/clusters/list", headers=H).json()
for c in clusters.get("clusters", []):
    if c.get("state") not in ("TERMINATED", "TERMINATING"):
        requests.post(f"{HOST}/api/2.0/clusters/delete", headers=H, json={"cluster_id": c["cluster_id"]})
time.sleep(30)

# Update job_clusters
job_info = requests.get(f"{HOST}/api/2.1/jobs/get", headers=H, params={"job_id": JOB_ID}).json()
settings = job_info["settings"]
for jc in settings.get("job_clusters", []):
    nc = jc.get("new_cluster", {})
    nc["node_type_id"] = VM
    nc["driver_node_type_id"] = VM
    nc["num_workers"] = 0
    sc = nc.get("spark_conf", {})
    sc["spark.databricks.cluster.profile"] = "singleNode"
    sc["spark.master"] = "local[*]"
    sc["pipeline.mode"] = "demo"
    nc["spark_conf"] = sc
    ct = nc.get("custom_tags", {})
    ct["ResourceClass"] = "SingleNode"
    nc["custom_tags"] = ct

resp = requests.post(f"{HOST}/api/2.1/jobs/reset", headers=H, json={
    "job_id": int(JOB_ID), "new_settings": settings
})
print(f"Job update: {resp.status_code}")

# Trigger
resp = requests.post(f"{HOST}/api/2.1/jobs/run-now", headers=H, json={"job_id": int(JOB_ID)})
run_id = resp.json().get("run_id")
print(f"Run ID: {run_id}")

# Monitor - check activity log after 2 minutes
for i in range(18):  # 3 min
    time.sleep(10)
    st = requests.get(f"{HOST}/api/2.1/jobs/runs/get", headers=H, params={"run_id": run_id}).json()
    tasks = st.get("tasks", [])
    
    for t in tasks:
        tstate = t.get("state", {}).get("life_cycle_state", "?")
        if tstate == "RUNNING":
            print(f"\n>>> {VM} WORKS! Task {t['task_key']} is RUNNING! <<<")
            print(f"Run ID: {run_id}")
            sys.exit(0)
        if tstate == "INTERNAL_ERROR":
            print(f"INTERNAL_ERROR on task {t['task_key']}")
            # Check if retrying
            
    elapsed = (i+1)*10
    if elapsed % 60 == 0:
        print(f"  [{elapsed}s] still pending...")

print(f"Timeout. Run ID: {run_id}")
print("Check activity log for failure details.")
