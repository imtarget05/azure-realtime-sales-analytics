#!/usr/bin/env python3
"""Pause Databricks pipeline and cancel queued runs."""
import os, sys, json, urllib.request, urllib.error

sys.path.insert(0, ".")
from dotenv import load_dotenv; load_dotenv()

HOST = os.getenv("DATABRICKS_HOST", "").rstrip("/")
TOKEN = os.getenv("DATABRICKS_TOKEN", "")
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

def api_get(path):
    req = urllib.request.Request(f"{HOST}{path}", headers=HEADERS)
    return json.loads(urllib.request.urlopen(req, timeout=20).read())

def api_post(path, data=None):
    body = json.dumps(data).encode() if data else None
    req = urllib.request.Request(f"{HOST}{path}", data=body, headers=HEADERS, method="POST")
    try:
        return json.loads(urllib.request.urlopen(req, timeout=20).read())
    except urllib.error.HTTPError as e:
        print(f"  HTTP {e.code}: {e.read().decode()[:200]}")
        return {}

# 1. Find job
r = api_get("/api/2.1/jobs/list?name=Sales_Lakehouse_Pipeline")
jobs = r.get("jobs", [])
if not jobs:
    print("Job not found!")
    sys.exit(1)

jid = jobs[0]["job_id"]
print(f"Job: Sales_Lakehouse_Pipeline (ID={jid})")

# 2. Pause the schedule
settings = jobs[0]["settings"]
schedule = settings.get("schedule")
if schedule:
    print(f"Current schedule: {schedule}")
    # Update job to pause schedule
    update = {
        "job_id": jid,
        "new_settings": {
            "schedule": {
                "quartz_cron_expression": schedule.get("quartz_cron_expression", "0 */5 * * * ?"),
                "timezone_id": schedule.get("timezone_id", "Asia/Ho_Chi_Minh"),
                "pause_status": "PAUSED"
            }
        }
    }
    api_post("/api/2.1/jobs/update", update)
    print("  Schedule PAUSED!")
else:
    print("  No schedule found (already removed or manual only)")

# 3. Cancel all queued/running runs
runs_r = api_get(f"/api/2.1/jobs/runs/list?job_id={jid}&limit=20&active_only=true")
runs = runs_r.get("runs", [])
print(f"\nActive runs: {len(runs)}")
for run in runs:
    rid = run["run_id"]
    state = run.get("state", {}).get("life_cycle_state", "?")
    print(f"  Run {rid}: {state}", end="")
    if state in ("QUEUED", "PENDING"):
        api_post(f"/api/2.1/jobs/runs/cancel", {"run_id": rid})
        print(" -> CANCELLED")
    elif state == "RUNNING":
        api_post(f"/api/2.1/jobs/runs/cancel", {"run_id": rid})
        print(" -> CANCEL REQUESTED")
    else:
        print(" (skipping)")

# 4. Verify
print("\nVerifying...")
r2 = api_get(f"/api/2.1/jobs/get?job_id={jid}")
sched2 = r2["settings"].get("schedule", {})
print(f"Schedule pause_status: {sched2.get('pause_status', 'N/A')}")

runs2 = api_get(f"/api/2.1/jobs/runs/list?job_id={jid}&limit=5")
for run in runs2.get("runs", [])[:5]:
    st = run.get("state", {})
    print(f"  Run {run['run_id']}: {st.get('life_cycle_state')} / {st.get('result_state', '?')}")
