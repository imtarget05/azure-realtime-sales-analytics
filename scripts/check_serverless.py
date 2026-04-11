import os, requests, json
from dotenv import load_dotenv
load_dotenv()
HOST = os.getenv("DATABRICKS_HOST","").rstrip("/")
TOKEN = os.getenv("DATABRICKS_TOKEN","")
H = {"Authorization": f"Bearer {TOKEN}"}

# Check spark versions for serverless
resp = requests.get(f"{HOST}/api/2.0/clusters/spark-versions", headers=H)
versions = resp.json().get("versions", [])
serverless = [v for v in versions if "serverless" in v.get("name","").lower() or "serverless" in v.get("key","").lower()]
print(f"Serverless spark versions: {json.dumps(serverless, indent=2)}")

# Check if we can create a job with "environment" instead of new_cluster
# Try the Jobs API 2.2 features
print("\nTrying to check compute availability...")
resp2 = requests.get(f"{HOST}/api/2.0/clusters/list", headers=H)
clusters = resp2.json().get("clusters", [])
print(f"Existing clusters: {len(clusters)}")

# Check if any all-purpose clusters exist we could use
for c in clusters:
    state = c.get("state", "?")
    if state != "TERMINATED":
        print(f"  Active: {c.get('cluster_id')} ({c.get('cluster_name')}) - {state} - {c.get('node_type_id')}")
