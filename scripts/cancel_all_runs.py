import os, requests
from dotenv import load_dotenv
load_dotenv()
HOST = os.getenv("DATABRICKS_HOST","").rstrip("/")
TOKEN = os.getenv("DATABRICKS_TOKEN","")
H = {"Authorization": f"Bearer {TOKEN}"}
runs = requests.get(f"{HOST}/api/2.1/jobs/runs/list", headers=H,
                    params={"job_id": "274825266735713", "active_only": True}).json()
for r in runs.get("runs", []):
    rid = r["run_id"]
    print(f"Cancelling {rid}...")
    requests.post(f"{HOST}/api/2.1/jobs/runs/cancel", headers=H, json={"run_id": rid})
print("Done.")
