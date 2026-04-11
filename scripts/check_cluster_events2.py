import os, requests, json
from dotenv import load_dotenv
load_dotenv()
HOST = os.getenv("DATABRICKS_HOST","").rstrip("/")
TOKEN = os.getenv("DATABRICKS_TOKEN","")
H = {"Authorization": f"Bearer {TOKEN}"}

RUN_ID = "540987864627245"

r = requests.get(f"{HOST}/api/2.1/jobs/runs/get", headers=H, params={"run_id": RUN_ID}).json()
for t in r.get("tasks", []):
    ck = t.get("cluster_instance", {}).get("cluster_id", "")
    tk = t["task_key"]
    st = t.get("state", {}).get("life_cycle_state", "?")
    print(f"Task: {tk}, cluster_id: {ck}, state: {st}")
    if ck:
        ci = requests.get(f"{HOST}/api/2.0/clusters/get", headers=H, params={"cluster_id": ck}).json()
        print(f"  Cluster state: {ci.get('state', '?')}")
        print(f"  State message: {ci.get('state_message', '')}")
        print(f"  node_type_id: {ci.get('node_type_id', '?')}")
        print(f"  driver_node_type_id: {ci.get('driver_node_type_id', '?')}")
        
        ev = requests.post(f"{HOST}/api/2.0/clusters/events", headers=H, 
                          json={"cluster_id": ck, "limit": 10}).json()
        for e in ev.get("events", []):
            etype = e.get("type", "?")
            reason = e.get("details", {}).get("reason", {})
            code = reason.get("code", "")
            params = reason.get("parameters", {})
            azure_err = params.get("azure_error_message", "")[:300]
            print(f"  Event: {etype} - {code} {azure_err}")
