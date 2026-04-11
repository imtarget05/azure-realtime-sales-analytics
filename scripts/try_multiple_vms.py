"""
Cancel current run and try a new VM type.
Iterates through untried VM families with quota >= 4.
"""
import os, sys, json, requests, time
from dotenv import load_dotenv

load_dotenv()
HOST = os.getenv("DATABRICKS_HOST", "").rstrip("/")
TOKEN = os.getenv("DATABRICKS_TOKEN", "")
JOB_ID = "274825266735713"
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

# VM types to try - families with quota >= 4, not yet tried
# Already failed: DS3_v2, D4s_v3, D4ds_v4, E2ads_v6, F4s_v2, D4as_v5
VM_CANDIDATES = [
    "Standard_E4ds_v4",   # EDSv4 family, quota=4
    "Standard_D4as_v4",   # DASv4 family, quota=4
    "Standard_E4s_v3",    # ESv3 family, quota=4
    "Standard_D4s_v4",    # DSv4 family, quota=4  (NOT same as DDSv4!)
    "Standard_E4_v3",     # Ev3 family, quota=4
    "Standard_D4_v3",     # Dv3 family, quota=4
    "Standard_E4_v4",     # Ev4 family, quota=4
    "Standard_D4_v4",     # DDv4 family, quota=4
    "Standard_E4as_v4",   # EASv4 family, quota=4  
]

# Pick which one via CLI arg, default to first
idx = int(sys.argv[1]) if len(sys.argv) > 1 else 0
VM_TYPE = VM_CANDIDATES[idx]

print(f"=== Trying VM type: {VM_TYPE} (index {idx}/{len(VM_CANDIDATES)-1}) ===")
print(f"Candidates: {VM_CANDIDATES}")

# 1) Cancel all active runs
print("\n1) Cancelling active runs...")
runs = requests.get(f"{HOST}/api/2.1/jobs/runs/list",
                     headers=HEADERS, params={"job_id": JOB_ID, "active_only": True}).json()
for r in runs.get("runs", []):
    rid = r["run_id"]
    print(f"   Cancelling run {rid}...")
    requests.post(f"{HOST}/api/2.1/jobs/runs/cancel", headers=HEADERS, json={"run_id": rid})
    # Wait for cancellation
    for _ in range(30):
        time.sleep(2)
        st = requests.get(f"{HOST}/api/2.1/jobs/runs/get", headers=HEADERS,
                          params={"run_id": rid}).json()
        if st.get("state", {}).get("life_cycle_state") in ("TERMINATED", "INTERNAL_ERROR"):
            print(f"   Run {rid} terminated.")
            break
    else:
        print(f"   WARNING: Run {rid} not yet terminated, proceeding anyway...")

# 2) Update job with new VM type
print(f"\n2) Updating job to use {VM_TYPE}...")
job_info = requests.get(f"{HOST}/api/2.1/jobs/get", headers=HEADERS,
                        params={"job_id": JOB_ID}).json()

new_cluster = None
for tc in job_info.get("settings", {}).get("tasks", []):
    if "new_cluster" in tc:
        new_cluster = tc["new_cluster"]
        break

if new_cluster:
    print(f"   Current node_type_id: {new_cluster.get('node_type_id')}")
    print(f"   Current driver_node_type_id: {new_cluster.get('driver_node_type_id')}")

# Update all tasks
tasks = job_info["settings"]["tasks"]
for task in tasks:
    if "new_cluster" in task:
        task["new_cluster"]["node_type_id"] = VM_TYPE
        task["new_cluster"]["driver_node_type_id"] = VM_TYPE
        task["new_cluster"]["num_workers"] = 0
        sc = task["new_cluster"].get("spark_conf", {})
        sc["spark.databricks.cluster.profile"] = "singleNode"
        sc["spark.master"] = "local[*]"
        sc["pipeline.mode"] = "demo"
        task["new_cluster"]["spark_conf"] = sc
        ce = task["new_cluster"].get("custom_tags", {})
        ce["ResourceClass"] = "SingleNode"
        task["new_cluster"]["custom_tags"] = ce

resp = requests.post(f"{HOST}/api/2.1/jobs/reset", headers=HEADERS, json={
    "job_id": int(JOB_ID),
    "new_settings": job_info["settings"]
})
if resp.status_code == 200:
    print(f"   Job updated to {VM_TYPE}")
else:
    print(f"   ERROR updating job: {resp.text}")
    sys.exit(1)

# 3) Trigger new run
print("\n3) Triggering new run...")
resp = requests.post(f"{HOST}/api/2.1/jobs/run-now", headers=HEADERS,
                     json={"job_id": int(JOB_ID)})
data = resp.json()
run_id = data.get("run_id")
print(f"   New run ID: {run_id}")

# 4) Wait and check if cluster starts or fails
print(f"\n4) Monitoring run {run_id} for 3 minutes...")
for i in range(18):  # 18 * 10s = 3 minutes
    time.sleep(10)
    st = requests.get(f"{HOST}/api/2.1/jobs/runs/get", headers=HEADERS,
                      params={"run_id": run_id}).json()
    state = st.get("state", {})
    lcs = state.get("life_cycle_state", "?")
    
    tasks_info = []
    for t in st.get("tasks", []):
        ts = t.get("state", {})
        tasks_info.append(f"{t['task_key']}={ts.get('life_cycle_state','?')}")
    
    print(f"   [{i*10:3d}s] {lcs} | {', '.join(tasks_info)}")
    
    if lcs in ("TERMINATED", "INTERNAL_ERROR"):
        result = state.get("result_state", "N/A")
        msg = state.get("state_message", "")
        print(f"\n   RESULT: {result}")
        print(f"   MESSAGE: {msg}")
        break
    
    # Check if any task is RUNNING (cluster started!)
    for t in st.get("tasks", []):
        ts = t.get("state", {}).get("life_cycle_state", "")
        if ts == "RUNNING":
            print(f"\n   CLUSTER STARTED! Task {t['task_key']} is RUNNING!")
            print(f"   VM type {VM_TYPE} works!")
            print(f"   Run ID: {run_id}")
            sys.exit(0)

print(f"\nDone monitoring. Run ID: {run_id}")
print(f"Check with: python scripts/check_run_status.py (update RUN_ID to {run_id})")
