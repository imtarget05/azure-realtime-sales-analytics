import os, requests, json
from dotenv import load_dotenv
load_dotenv()
HOST = os.getenv("DATABRICKS_HOST","").rstrip("/")
TOKEN = os.getenv("DATABRICKS_TOKEN","")
H = {"Authorization": f"Bearer {TOKEN}"}
j = requests.get(f"{HOST}/api/2.1/jobs/get", headers=H, params={"job_id": "274825266735713"}).json()

# Check job clusters
for jc in j["settings"].get("job_clusters", []):
    print(f"Job cluster: {jc['job_cluster_key']}")
    nc = jc.get("new_cluster", {})
    print(f"  node_type_id: {nc.get('node_type_id','?')}")
    print(f"  driver_node_type_id: {nc.get('driver_node_type_id','?')}")
    print(f"  num_workers: {nc.get('num_workers','?')}")
    print(f"  spark_conf: {json.dumps(nc.get('spark_conf',{}), indent=2)}")

print()
# Check tasks
for t in j["settings"]["tasks"]:
    tk = t["task_key"]
    jck = t.get("job_cluster_key", "")
    nc = t.get("new_cluster")
    print(f"Task: {tk}, job_cluster_key: {jck}, has_new_cluster: {nc is not None}")
