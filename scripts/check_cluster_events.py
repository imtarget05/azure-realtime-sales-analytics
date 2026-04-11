"""Check cluster events for startup issues."""
import os, json, urllib.request
from dotenv import load_dotenv
load_dotenv()

HOST = os.environ.get("DATABRICKS_HOST", "https://adb-7405607469187602.2.azuredatabricks.net")
TOKEN = os.environ.get("DATABRICKS_TOKEN", "")
CLUSTER_ID = ""  # Will auto-detect from run


def api_post(path, data):
    url = f"{HOST}{path}"
    body = json.dumps(data).encode()
    req = urllib.request.Request(
        url, data=body, method="POST",
        headers={"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


def api_get(path):
    url = f"{HOST}{path}"
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {TOKEN}"})
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


# Get cluster info
try:
    cluster = api_get(f"/api/2.0/clusters/get?cluster_id={CLUSTER_ID}")
    print(f"Cluster: {cluster.get('cluster_name', 'N/A')}")
    print(f"  State: {cluster.get('state', 'N/A')}")
    print(f"  State message: {cluster.get('state_message', 'N/A')}")
    print(f"  Node type: {cluster.get('node_type_id', 'N/A')}")
    reason = cluster.get("termination_reason", {})
    if reason:
        print(f"  Termination reason: {reason}")
except Exception as e:
    print(f"Error getting cluster: {e}")

# Get cluster events
print("\nRecent events:")
try:
    events = api_post("/api/2.0/clusters/events", {
        "cluster_id": CLUSTER_ID,
        "limit": 10,
        "order": "DESC",
    })
    for ev in events.get("events", []):
        etype = ev.get("type", "?")
        ts = ev.get("timestamp", 0)
        details = ev.get("details", {})
        reason = details.get("reason", {})
        import datetime
        time_str = datetime.datetime.fromtimestamp(ts / 1000).strftime("%H:%M:%S") if ts else "?"
        print(f"  [{time_str}] {etype}: {reason.get('code', '')} {reason.get('parameters', {}).get('azure_error_message', '')[:200]}")
except Exception as e:
    print(f"Error getting events: {e}")
