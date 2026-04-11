import os, requests
from dotenv import load_dotenv
load_dotenv()
HOST = os.getenv("DATABRICKS_HOST","").rstrip("/")
TOKEN = os.getenv("DATABRICKS_TOKEN","")
H = {"Authorization": f"Bearer {TOKEN}"}
j = requests.get(f"{HOST}/api/2.1/jobs/get", headers=H, params={"job_id": "274825266735713"}).json()
for t in j["settings"]["tasks"]:
    nc = t.get("new_cluster", {})
    print(f"Task: {t['task_key']}, node_type: {nc.get('node_type_id','?')}, driver_type: {nc.get('driver_node_type_id','?')}")
