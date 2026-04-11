"""Get detailed cluster info for a Databricks run."""
import os, json, urllib.request
from dotenv import load_dotenv
load_dotenv()

HOST = os.environ.get("DATABRICKS_HOST", "https://adb-7405607469187602.2.azuredatabricks.net")
TOKEN = os.environ.get("DATABRICKS_TOKEN", "")
RUN_ID = 977603001396338


def api_get(path):
    url = f"{HOST}{path}"
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {TOKEN}"})
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


run = api_get(f"/api/2.1/jobs/runs/get?run_id={RUN_ID}")
tasks = run.get("tasks", [])
for t in tasks:
    name = t.get("task_key", "?")
    cluster_instance = t.get("cluster_instance", {})
    state = t.get("state", {})
    print(f"Task: {name}")
    print(f"  State: {state}")
    print(f"  Cluster: {cluster_instance}")
    start = t.get("start_time", 0)
    if start:
        import datetime
        print(f"  Start time: {datetime.datetime.fromtimestamp(start/1000)}")
    print(f"  Setup duration ms: {t.get('setup_duration', 'N/A')}")
    print()

# Also check job clusters config
job = api_get(f"/api/2.1/jobs/get?job_id=274825266735713")
for c in job["settings"].get("job_clusters", []):
    nc = c.get("new_cluster", {})
    print(f"Cluster {c['job_cluster_key']}:")
    print(f"  node_type: {nc.get('node_type_id')}")
    print(f"  num_workers: {nc.get('num_workers')}")
    print(f"  spark_version: {nc.get('spark_version')}")
    print(f"  spark_conf: {nc.get('spark_conf')}")
    print()
