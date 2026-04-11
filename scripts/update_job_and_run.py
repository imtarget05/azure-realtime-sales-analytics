"""Update Databricks job config for demo mode and trigger a new run."""
import os
import json
import urllib.request

# Load env
from dotenv import load_dotenv
load_dotenv()

DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST", "https://adb-7405607469187602.2.azuredatabricks.net")
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN", "")
JOB_ID = 274825266735713


def api_get(path):
    url = f"{DATABRICKS_HOST}{path}"
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {DATABRICKS_TOKEN}"})
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


def api_post(path, data):
    url = f"{DATABRICKS_HOST}{path}"
    body = json.dumps(data).encode()
    req = urllib.request.Request(
        url, data=body, method="POST",
        headers={"Authorization": f"Bearer {DATABRICKS_TOKEN}", "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


def main():
    # 1. Cancel any active runs
    print("=== Step 1: Cancel active runs ===")
    runs = api_get(f"/api/2.1/jobs/runs/list?job_id={JOB_ID}&active_only=true")
    active_runs = runs.get("runs", [])
    print(f"Active runs: {len(active_runs)}")
    for run in active_runs:
        run_id = run["run_id"]
        print(f"  Cancelling run {run_id}...")
        api_post("/api/2.1/jobs/runs/cancel", {"run_id": run_id})
        print(f"  Cancelled {run_id}")

    # 2. Update job config with pipeline.mode=demo
    print("\n=== Step 2: Update job config ===")
    job = api_get(f"/api/2.1/jobs/get?job_id={JOB_ID}")
    settings = job["settings"]

    for cluster in settings.get("job_clusters", []):
        key = cluster["job_cluster_key"]
        conf = cluster.get("new_cluster", {}).get("spark_conf", {})
        conf["pipeline.mode"] = "demo"
        cluster["new_cluster"]["spark_conf"] = conf
        node_type = cluster["new_cluster"].get("node_type_id", "?")
        print(f"  {key}: node={node_type}, spark_conf keys={list(conf.keys())}")

    api_post("/api/2.1/jobs/reset", {"job_id": JOB_ID, "new_settings": settings})
    print("  Job config updated successfully")

    # 3. Trigger new run
    print("\n=== Step 3: Trigger new run ===")
    result = api_post("/api/2.1/jobs/run-now", {"job_id": JOB_ID})
    run_id = result["run_id"]
    print(f"  New run triggered: run_id={run_id}")
    print(f"  Monitor at: {DATABRICKS_HOST}/#job/{JOB_ID}/run/{result.get('number_in_job', '?')}")

    return run_id


if __name__ == "__main__":
    main()
