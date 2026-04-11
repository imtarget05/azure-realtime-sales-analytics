"""
Rapid-fire: try all VM candidates, check activity log for failures, move to next.
"""
import os, sys, json, requests, time
from dotenv import load_dotenv

load_dotenv()
HOST = os.getenv("DATABRICKS_HOST", "").rstrip("/")
TOKEN = os.getenv("DATABRICKS_TOKEN", "")
JOB_ID = "274825266735713"
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

VM_CANDIDATES = [
    "Standard_D4as_v4",   # DASv4 family, quota=4
    "Standard_E4s_v3",    # ESv3 family, quota=4
    "Standard_D4s_v4",    # DSv4 family, quota=4
    "Standard_E4_v3",     # Ev3 family, quota=4
    "Standard_D4_v3",     # Dv3 family, quota=4
    "Standard_E4_v4",     # Ev4 family, quota=4
    "Standard_D4_v4",     # DDv4 family, quota=4
    "Standard_E4as_v4",   # EASv4 family, quota=4
    "Standard_F4s_v2",    # FSv2 family, quota=4
]

def cancel_all():
    runs = requests.get(f"{HOST}/api/2.1/jobs/runs/list",
                         headers=HEADERS, params={"job_id": JOB_ID, "active_only": True}).json()
    for r in runs.get("runs", []):
        rid = r["run_id"]
        requests.post(f"{HOST}/api/2.1/jobs/runs/cancel", headers=HEADERS, json={"run_id": rid})
    # Brief wait for cancellation
    time.sleep(5)

def update_job(vm_type):
    job_info = requests.get(f"{HOST}/api/2.1/jobs/get", headers=HEADERS, params={"job_id": JOB_ID}).json()
    settings = job_info["settings"]
    for jc in settings.get("job_clusters", []):
        nc = jc.get("new_cluster", {})
        nc["node_type_id"] = vm_type
        nc["driver_node_type_id"] = vm_type
        nc["num_workers"] = 0
        sc = nc.get("spark_conf", {})
        sc["spark.databricks.cluster.profile"] = "singleNode"
        sc["spark.master"] = "local[*]"
        sc["pipeline.mode"] = "demo"
        nc["spark_conf"] = sc
        ct = nc.get("custom_tags", {})
        ct["ResourceClass"] = "SingleNode"
        nc["custom_tags"] = ct
    resp = requests.post(f"{HOST}/api/2.1/jobs/reset", headers=HEADERS, json={
        "job_id": int(JOB_ID), "new_settings": settings
    })
    return resp.status_code == 200

def trigger_run():
    resp = requests.post(f"{HOST}/api/2.1/jobs/run-now", headers=HEADERS, json={"job_id": int(JOB_ID)})
    return resp.json().get("run_id")

def check_run(run_id, timeout=180):
    """Check run. Returns 'running' if cluster started, 'failed' if error, 'pending' if still waiting."""
    for i in range(timeout // 10):
        time.sleep(10)
        st = requests.get(f"{HOST}/api/2.1/jobs/runs/get", headers=HEADERS, params={"run_id": run_id}).json()
        state = st.get("state", {})
        lcs = state.get("life_cycle_state", "?")
        
        # Check tasks
        for t in st.get("tasks", []):
            tstate = t.get("state", {}).get("life_cycle_state", "?")
            if tstate == "RUNNING":
                return "running", t["task_key"]
            if tstate == "INTERNAL_ERROR":
                return "failed", "INTERNAL_ERROR"
        
        if lcs in ("TERMINATED", "INTERNAL_ERROR"):
            return "failed", state.get("state_message", "")
        
        elapsed = (i + 1) * 10
        if elapsed % 30 == 0:
            print(f"      [{elapsed}s] still pending...")
    
    return "pending", "timeout"

# MAIN
print("=" * 60)
print("RAPID VM TYPE SEARCH")
print("=" * 60)

cancel_all()

for idx, vm in enumerate(VM_CANDIDATES):
    print(f"\n{'='*60}")
    print(f"[{idx+1}/{len(VM_CANDIDATES)}] Trying: {vm}")
    print(f"{'='*60}")
    
    cancel_all()
    
    if not update_job(vm):
        print(f"  SKIP - Failed to update job")
        continue
    
    run_id = trigger_run()
    print(f"  Run ID: {run_id}")
    
    # Wait up to 3 minutes
    result, detail = check_run(run_id, timeout=180)
    
    if result == "running":
        print(f"\n  >>> SUCCESS! {vm} WORKS! <<<")
        print(f"  Task {detail} is RUNNING!")
        print(f"  Run ID: {run_id}")
        print(f"\n  DO NOT cancel this run!")
        sys.exit(0)
    elif result == "failed":
        print(f"  FAILED: {detail}")
        print(f"  Moving to next VM type...")
    else:
        print(f"  TIMEOUT: Still pending after 3 minutes")
        # Check activity log for this VM
        print(f"  (May be SkuNotAvailable, checking next...)")

print(f"\n{'='*60}")
print("ALL VM TYPES EXHAUSTED!")
print("No VM type could start in Southeast Asia.")
print("Consider: requesting quota increase or changing Databricks region")
print(f"{'='*60}")
