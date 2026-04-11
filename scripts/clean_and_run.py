"""
1. Cancel ALL active runs
2. Terminate ALL clusters
3. Wait for Total Regional vCPUs to drop to 0
4. Try Standard_F4s_v2 (FSv2 family, quota=4, 4 cores)
"""
import os, requests, json, time
from dotenv import load_dotenv

load_dotenv()
HOST = os.getenv("DATABRICKS_HOST", "").rstrip("/")
TOKEN = os.getenv("DATABRICKS_TOKEN", "")
JOB_ID = "274825266735713"
H = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

# 1. Cancel ALL active runs for this job
print("1) Cancelling ALL active runs...")
runs = requests.get(f"{HOST}/api/2.1/jobs/runs/list", headers=H,
                    params={"job_id": JOB_ID, "active_only": True}).json()
count = 0
for r in runs.get("runs", []):
    rid = r["run_id"]
    requests.post(f"{HOST}/api/2.1/jobs/runs/cancel", headers=H, json={"run_id": rid})
    count += 1
print(f"   Cancelled {count} runs")

# Wait for cancellations
print("   Waiting 30s for cancellations to propagate...")
time.sleep(30)

# 2. Terminate ALL clusters (job clusters + any interactive)
print("\n2) Listing all clusters...")
clusters = requests.get(f"{HOST}/api/2.0/clusters/list", headers=H).json()
for c in clusters.get("clusters", []):
    cid = c["cluster_id"]
    state = c.get("state", "?")
    cname = c.get("cluster_name", "?")
    node = c.get("node_type_id", "?")
    print(f"   Cluster {cid} ({cname}): state={state}, node={node}")
    if state not in ("TERMINATED", "TERMINATING"):
        print(f"   -> Terminating...")
        resp = requests.post(f"{HOST}/api/2.0/clusters/delete", headers=H, json={"cluster_id": cid})
        print(f"   -> {resp.status_code}: {resp.text[:200]}")

# Wait for termination
print("\n   Waiting 60s for clusters to terminate...")
time.sleep(60)

# 3. Verify all clusters terminated
print("\n3) Verifying clusters are terminated...")
clusters = requests.get(f"{HOST}/api/2.0/clusters/list", headers=H).json()
all_terminated = True
for c in clusters.get("clusters", []):
    cid = c["cluster_id"]
    state = c.get("state", "?")
    print(f"   {cid}: {state}")
    if state not in ("TERMINATED",):
        all_terminated = False
        
if not all_terminated:
    print("   WARNING: Not all clusters terminated. Waiting 60 more seconds...")
    time.sleep(60)

# 4. Now try Standard_F4s_v2 (4 cores, FSv2 family, quota=4)
VM_TYPE = "Standard_F4s_v2"
print(f"\n4) Setting VM to {VM_TYPE} and triggering run...")

job_info = requests.get(f"{HOST}/api/2.1/jobs/get", headers=H, params={"job_id": JOB_ID}).json()
settings = job_info["settings"]
for jc in settings.get("job_clusters", []):
    nc = jc.get("new_cluster", {})
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

resp = requests.post(f"{HOST}/api/2.1/jobs/reset", headers=H, json={
    "job_id": int(JOB_ID), "new_settings": settings
})
print(f"   Job update: {resp.status_code}")

# Verify
job2 = requests.get(f"{HOST}/api/2.1/jobs/get", headers=H, params={"job_id": JOB_ID}).json()
for jc in job2["settings"].get("job_clusters", []):
    nc = jc.get("new_cluster", {})
    print(f"   {jc['job_cluster_key']}: {nc.get('node_type_id')}, driver: {nc.get('driver_node_type_id')}")

# Trigger
resp = requests.post(f"{HOST}/api/2.1/jobs/run-now", headers=H, json={"job_id": int(JOB_ID)})
run_id = resp.json().get("run_id")
print(f"\n   New run ID: {run_id}")

# 5. Monitor for 5 minutes
print(f"\n5) Monitoring run {run_id}...")
for i in range(30):  # 30 * 10 = 5 min
    time.sleep(10)
    st = requests.get(f"{HOST}/api/2.1/jobs/runs/get", headers=H, params={"run_id": run_id}).json()
    state = st.get("state", {})
    lcs = state.get("life_cycle_state", "?")
    
    tasks_info = []
    for t in st.get("tasks", []):
        ts = t.get("state", {}).get("life_cycle_state", "?")
        tasks_info.append(f"{t['task_key']}={ts}")
    
    elapsed = (i + 1) * 10
    print(f"   [{elapsed:3d}s] {lcs} | {', '.join(tasks_info)}")
    
    if lcs in ("TERMINATED", "INTERNAL_ERROR"):
        print(f"\n   RESULT: {state.get('result_state', 'N/A')}")
        print(f"   MESSAGE: {state.get('state_message', '')}")
        break
    
    for t in st.get("tasks", []):
        if t.get("state", {}).get("life_cycle_state") == "RUNNING":
            print(f"\n   >>> CLUSTER STARTED! {t['task_key']} is RUNNING! <<<")
            print(f"   VM: {VM_TYPE} WORKS!")
            print(f"   Run ID: {run_id}")
            
            # Now monitor until completion
            print(f"\n   Continuing to monitor until pipeline completes...")
            for j in range(180):  # 30 more minutes
                time.sleep(10)
                st2 = requests.get(f"{HOST}/api/2.1/jobs/runs/get", headers=H, params={"run_id": run_id}).json()
                state2 = st2.get("state", {})
                lcs2 = state2.get("life_cycle_state", "?")
                ti = []
                for t2 in st2.get("tasks", []):
                    ts2 = t2.get("state", {}).get("life_cycle_state", "?")
                    ti.append(f"{t2['task_key']}={ts2}")
                et = elapsed + (j + 1) * 10
                print(f"   [{et:4d}s] {lcs2} | {', '.join(ti)}")
                if lcs2 in ("TERMINATED", "INTERNAL_ERROR"):
                    print(f"\n   FINAL RESULT: {state2.get('result_state', 'N/A')}")
                    print(f"   MESSAGE: {state2.get('state_message', '')}")
                    for t2 in st2.get("tasks", []):
                        ts2 = t2.get("state", {})
                        print(f"   Task {t2['task_key']}: {ts2.get('result_state','?')} - {ts2.get('state_message','')[:100]}")
                    break
            break
    else:
        continue
    break

print(f"\nDone. Run ID: {run_id}")
