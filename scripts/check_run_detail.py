#!/usr/bin/env python3
"""Check exact pipeline run status and cluster details."""
import os, sys, json, urllib.request, urllib.error

sys.path.insert(0, ".")
from dotenv import load_dotenv; load_dotenv()

HOST = os.getenv("DATABRICKS_HOST", "").rstrip("/")
TOKEN = os.getenv("DATABRICKS_TOKEN", "")
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

def api(path):
    req = urllib.request.Request(f"{HOST}{path}", headers=HEADERS)
    try:
        return json.loads(urllib.request.urlopen(req, timeout=20).read())
    except urllib.error.HTTPError as e:
        print(f"HTTP {e.code}: {e.read().decode()[:300]}")
        return {}

RUN_ID = 502241543535383

# Get run detail
run = api(f"/api/2.1/jobs/runs/get?run_id={RUN_ID}")
state = run.get("state", {})
print(f"Run {RUN_ID}: {state.get('life_cycle_state')}/{state.get('result_state','?')}")
print(f"Message: {state.get('state_message','')[:300]}")

# Check cluster config
settings = run.get("job_parameters", {})
clusters = run.get("cluster_spec", {})
print(f"\nCluster spec: {json.dumps(run.get('cluster_spec', {}), indent=2)[:500]}")

# Check tasks
for t in run.get("tasks", []):
    tst = t.get("state", {})
    cid = t.get("cluster_instance", {}).get("cluster_id", "?")
    print(f"\nTask {t['task_key']}: {tst.get('life_cycle_state')}/{tst.get('result_state','?')}")
    print(f"  cluster_id: {cid}")
    if tst.get("state_message"):
        print(f"  message: {tst['state_message'][:200]}")

# Check job cluster config
job = api("/api/2.1/jobs/get?job_id=274825266735713")
jclusters = job.get("settings", {}).get("job_clusters", [])
print(f"\nJob cluster definitions ({len(jclusters)}):")
for jc in jclusters:
    print(f"  {jc.get('job_cluster_key')}:")
    nc = jc.get("new_cluster", {})
    print(f"    spark_version: {nc.get('spark_version')}")
    print(f"    node_type: {nc.get('node_type_id')}")
    print(f"    num_workers: {nc.get('num_workers')}")
    autoscale = nc.get("autoscale", {})
    if autoscale:
        print(f"    autoscale: {autoscale.get('min_workers')}-{autoscale.get('max_workers')}")
    print(f"    driver_node: {nc.get('driver_node_type_id', 'same as worker')}")
