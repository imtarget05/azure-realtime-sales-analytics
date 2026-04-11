"""Switch clusters to Standard_F4s_v2 (FSv2 family) and retry."""
import os, json, urllib.request
from dotenv import load_dotenv
load_dotenv()

HOST = os.environ.get("DATABRICKS_HOST", "https://adb-7405607469187602.2.azuredatabricks.net")
TOKEN = os.environ.get("DATABRICKS_TOKEN", "")
JOB_ID = 274825266735713
CURRENT_RUN = 1072703974389448


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
    print("=== Cancel current run ===")
    try:
        api_post("/api/2.1/jobs/runs/cancel", {"run_id": CURRENT_RUN})
        print(f"  Cancelled {CURRENT_RUN}")
    except Exception as e:
        print(f"  {e}")

    # 2. Try different VM families
    # FSv2: 4 vCPU quota, good compute
    # DSv3: 4 vCPU quota, balanced
    VM_TYPE = "Standard_F4s_v2"  # 4 cores, 8GB, FSv2 family

    print(f"\n=== Switch to {VM_TYPE} single-node ===")
    job = api_get(f"/api/2.1/jobs/get?job_id={JOB_ID}")
    settings = job["settings"]

    for cluster in settings.get("job_clusters", []):
        key = cluster["job_cluster_key"]
        nc = cluster["new_cluster"]
        nc["num_workers"] = 0
        nc["node_type_id"] = VM_TYPE

        conf = nc.get("spark_conf", {})
        conf["spark.databricks.cluster.profile"] = "singleNode"
        conf["spark.master"] = "local[*, 4]"
        conf["pipeline.mode"] = "demo"
        nc["spark_conf"] = conf

        tags = nc.get("custom_tags", {})
        tags["ResourceClass"] = "SingleNode"
        nc["custom_tags"] = tags

        if key == "ml_cluster":
            nc["spark_version"] = "14.3.x-ml-scala2.12"
        else:
            nc["spark_version"] = "14.3.x-scala2.12"

        print(f"  {key}: {VM_TYPE} single-node")

    api_post("/api/2.1/jobs/reset", {"job_id": JOB_ID, "new_settings": settings})
    print("  Job updated!")

    # 3. Trigger
    print("\n=== Trigger new run ===")
    result = api_post("/api/2.1/jobs/run-now", {"job_id": JOB_ID})
    run_id = result["run_id"]
    print(f"  Run ID: {run_id}")
    print(f"  URL: {HOST}/#job/{JOB_ID}/run/{result.get('number_in_job', '?')}")


if __name__ == "__main__":
    main()
