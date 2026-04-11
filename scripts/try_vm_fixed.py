"""
Fix: update job_clusters (not task-level new_cluster) and try a new VM type.
"""
import os, sys, json, requests, time
from dotenv import load_dotenv

load_dotenv()
HOST = os.getenv("DATABRICKS_HOST", "").rstrip("/")
TOKEN = os.getenv("DATABRICKS_TOKEN", "")
JOB_ID = "274825266735713"
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

VM_CANDIDATES = [
    "Standard_E4ds_v4",   # EDSv4 family, quota=4
    "Standard_D4as_v4",   # DASv4 family, quota=4
    "Standard_E4s_v3",    # ESv3 family, quota=4
    "Standard_D4s_v4",    # DSv4 family, quota=4
    "Standard_E4_v3",     # Ev3 family, quota=4
    "Standard_D4_v3",     # Dv3 family, quota=4
    "Standard_E4_v4",     # Ev4 family, quota=4
    "Standard_D4_v4",     # DDv4 family, quota=4
    "Standard_E4as_v4",   # EASv4 family, quota=4
]

idx = int(sys.argv[1]) if len(sys.argv) > 1 else 0
VM_TYPE = VM_CANDIDATES[idx]
print(f"=== Trying VM type: {VM_TYPE} (index {idx}/{len(VM_CANDIDATES)-1}) ===")

# 1) Cancel active runs
print("\n1) Cancelling active runs...")
runs = requests.get(f"{HOST}/api/2.1/jobs/runs/list",
                     headers=HEADERS, params={"job_id": JOB_ID, "active_only": True}).json()
for r in runs.get("runs", []):
    rid = r["run_id"]
    print(f"   Cancelling run {rid}...")
    requests.post(f"{HOST}/api/2.1/jobs/runs/cancel", headers=HEADERS, json={"run_id": rid})
    for _ in range(20):
        time.sleep(2)
        st = requests.get(f"{HOST}/api/2.1/jobs/runs/get", headers=HEADERS,
                          params={"run_id": rid}).json()
        if st.get("state", {}).get("life_cycle_state") in ("TERMINATED", "INTERNAL_ERROR"):
            print(f"   Cancelled.")
            break

# 2) Update job_clusters
print(f"\n2) Updating job_clusters to {VM_TYPE}...")
job_info = requests.get(f"{HOST}/api/2.1/jobs/get", headers=HEADERS, params={"job_id": JOB_ID}).json()
settings = job_info["settings"]

for jc in settings.get("job_clusters", []):
    nc = jc.get("new_cluster", {})
    old_type = nc.get("node_type_id", "?")
    nc["node_type_id"] = VM_TYPE
    nc["driver_node_type_id"] = VM_TYPE
    nc["num_workers"] = 0
    sc = nc.get("spark_conf", {})
    sc["spark.databricks.cluster.profile"] = "singleNode"
    sc["spark.master"] = "local[*]"
    sc["pipeline.mode"] = "demo"
    nc["spark_conf"] = sc
    ct = nc.get("custom_tags", {})
    ct["ResourceClass"] = "SingleNode"
    nc["custom_tags"] = ct
    print(f"   {jc['job_cluster_key']}: {old_type} -> {VM_TYPE}")

resp = requests.post(f"{HOST}/api/2.1/jobs/reset", headers=HEADERS, json={
    "job_id": int(JOB_ID),
    "new_settings": settings
})
if resp.status_code == 200:
    print(f"   Job updated successfully!")
else:
    print(f"   ERROR: {resp.status_code} {resp.text}")
    sys.exit(1)

# Verify
job_info2 = requests.get(f"{HOST}/api/2.1/jobs/get", headers=HEADERS, params={"job_id": JOB_ID}).json()
for jc in job_info2["settings"].get("job_clusters", []):
    nc = jc.get("new_cluster", {})
    print(f"   VERIFIED {jc['job_cluster_key']}: {nc.get('node_type_id')}, driver: {nc.get('driver_node_type_id')}")

# 3) Trigger run
print("\n3) Triggering new run...")
resp = requests.post(f"{HOST}/api/2.1/jobs/run-now", headers=HEADERS, json={"job_id": int(JOB_ID)})
data = resp.json()
run_id = data.get("run_id")
print(f"   Run ID: {run_id}")

# 4) Monitor for 4 minutes
print(f"\n4) Monitoring for 4 minutes...")
for i in range(24):
    time.sleep(10)
    st = requests.get(f"{HOST}/api/2.1/jobs/runs/get", headers=HEADERS, params={"run_id": run_id}).json()
    state = st.get("state", {})
    lcs = state.get("life_cycle_state", "?")
    
    tasks_info = []
    for t in st.get("tasks", []):
        ts = t.get("state", {}).get("life_cycle_state", "?")
        tasks_info.append(f"{t['task_key']}={ts}")
    
    print(f"   [{i*10+10:3d}s] {lcs} | {', '.join(tasks_info)}")
    
    if lcs in ("TERMINATED", "INTERNAL_ERROR"):
        print(f"\n   RESULT: {state.get('result_state', 'N/A')}")
        print(f"   MESSAGE: {state.get('state_message', '')}")
        break
    
    for t in st.get("tasks", []):
        if t.get("state", {}).get("life_cycle_state") == "RUNNING":
            print(f"\n   >>> CLUSTER STARTED! Task {t['task_key']} is RUNNING! <<<")
            print(f"   VM type {VM_TYPE} WORKS!")
            print(f"   Run ID: {run_id}")
            sys.exit(0)

print(f"\nRun ID: {run_id}")
