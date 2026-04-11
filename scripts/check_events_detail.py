"""Check cluster events in detail."""
import os, json, urllib.request, datetime
from dotenv import load_dotenv
load_dotenv()

HOST = os.environ.get("DATABRICKS_HOST", "https://adb-7405607469187602.2.azuredatabricks.net")
TOKEN = os.environ.get("DATABRICKS_TOKEN", "")


def api_get(path):
    url = HOST + path
    req = urllib.request.Request(url, headers={"Authorization": "Bearer " + TOKEN})
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


def api_post(path, data):
    url = HOST + path
    body = json.dumps(data).encode()
    req = urllib.request.Request(url, data=body, method="POST",
        headers={"Authorization": "Bearer " + TOKEN, "Content-Type": "application/json"})
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


# Find cluster from current run
run = api_get("/api/2.1/jobs/runs/get?run_id=281899568790093")
for t in run.get("tasks", []):
    ci = t.get("cluster_instance", {})
    cid = ci.get("cluster_id", "")
    if cid:
        cluster = api_get("/api/2.0/clusters/get?cluster_id=" + cid)
        print("Cluster:", cid)
        print("State:", cluster.get("state"))
        print("Message:", cluster.get("state_message", ""))
        print("Node:", cluster.get("node_type_id"))
        print("Workers:", cluster.get("num_workers"))

        # Full events
        events = api_post("/api/2.0/clusters/events", {
            "cluster_id": cid, "limit": 20, "order": "DESC"
        })
        print("\nEvents:")
        for ev in events.get("events", []):
            ts = ev.get("timestamp", 0)
            t_str = datetime.datetime.fromtimestamp(ts/1000).strftime("%H:%M:%S") if ts else "?"
            etype = ev.get("type", "?")
            details = ev.get("details", {})
            reason = details.get("reason", {})
            code = reason.get("code", "")
            params = reason.get("parameters", {})
            azure_msg = params.get("azure_error_message", "")
            print("  [{}] {}: {} {}".format(t_str, etype, code, azure_msg[:200]))
        break
