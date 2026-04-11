"""Fix Databricks job: single-node clusters to fit 4 vCPU quota, then trigger."""
import os, json, urllib.request
from dotenv import load_dotenv
load_dotenv()

HOST = os.environ.get("DATABRICKS_HOST", "https://adb-7405607469187602.2.azuredatabricks.net")
TOKEN = os.environ.get("DATABRICKS_TOKEN", "")
JOB_ID = 274825266735713
CURRENT_RUN = 977603001396338


def api_get(path):
    url = f"{HOST}{path}"
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {TOKEN}"})
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


def api_post(path, data):
    url = f"{HOST}{path}"
    body = json.dumps(data).encode()
    req = urllib.request.Request(
        url, data=body, method="POST",
        headers={"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


def main():
    # 1. Cancel current run
    print("=== Step 1: Cancel current run ===")
    try:
        api_post("/api/2.1/jobs/runs/cancel", {"run_id": CURRENT_RUN})
        print(f"  Cancelled run {CURRENT_RUN}")
    except Exception as e:
        print(f"  Cancel failed (may already be done): {e}")

    # 2. Get job and fix clusters
    print("\n=== Step 2: Fix clusters to single-node ===")
    job = api_get(f"/api/2.1/jobs/get?job_id={JOB_ID}")
    settings = job["settings"]

    for cluster in settings.get("job_clusters", []):
        key = cluster["job_cluster_key"]
        nc = cluster["new_cluster"]

        # Set single-node: 0 workers
        nc["num_workers"] = 0

        # Use DS3_v2 for both (4 vCPUs, fits in 4 vCPU quota)
        nc["node_type_id"] = "Standard_DS3_v2"

        # Single-node spark config
        conf = nc.get("spark_conf", {})
        conf["spark.databricks.cluster.profile"] = "singleNode"
        conf["spark.master"] = "local[*, 4]"
        conf["pipeline.mode"] = "demo"
        nc["spark_conf"] = conf

        # Single-node custom tag
        tags = nc.get("custom_tags", {})
        tags["ResourceClass"] = "SingleNode"
        nc["custom_tags"] = tags

        # Use ML runtime for ml_cluster, standard for etl
        if key == "ml_cluster":
            nc["spark_version"] = "14.3.x-ml-scala2.12"
        else:
            nc["spark_version"] = "14.3.x-scala2.12"

        print(f"  {key}: Standard_DS3_v2, single-node (0 workers)")
        print(f"    spark_conf: {json.dumps(conf, indent=6)}")

    # 3. Reset job
    api_post("/api/2.1/jobs/reset", {"job_id": JOB_ID, "new_settings": settings})
    print("\n  Job config updated!")

    # 4. Trigger new run
    print("\n=== Step 3: Trigger new run ===")
    result = api_post("/api/2.1/jobs/run-now", {"job_id": JOB_ID})
    run_id = result["run_id"]
    print(f"  New run: {run_id}")
    print(f"  URL: {HOST}/#job/{JOB_ID}/run/{result.get('number_in_job', '?')}")

    return run_id


if __name__ == "__main__":
    main()
