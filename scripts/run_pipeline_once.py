#!/usr/bin/env python3
"""Run the Databricks pipeline once and monitor until completion."""
import os, sys, json, urllib.request, urllib.error, time

sys.path.insert(0, ".")
from dotenv import load_dotenv; load_dotenv()

HOST = os.getenv("DATABRICKS_HOST", "").rstrip("/")
TOKEN = os.getenv("DATABRICKS_TOKEN", "")
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

def api_get(path):
    req = urllib.request.Request(f"{HOST}{path}", headers=HEADERS)
    return json.loads(urllib.request.urlopen(req, timeout=30).read())

def api_post(path, data=None):
    body = json.dumps(data).encode() if data else None
    req = urllib.request.Request(f"{HOST}{path}", data=body, headers=HEADERS, method="POST")
    resp = urllib.request.urlopen(req, timeout=30)
    return json.loads(resp.read())

JOB_ID = 274825266735713

# Trigger one run
print("Triggering pipeline run...")
result = api_post("/api/2.1/jobs/run-now", {"job_id": JOB_ID})
run_id = result.get("run_id")
print(f"Run ID: {run_id}")
print("Monitoring (check back with: python scripts/check_run_status.py)")

# Poll for status
while True:
    run = api_get(f"/api/2.1/jobs/runs/get?run_id={run_id}")
    state = run.get("state", {})
    lcs = state.get("life_cycle_state", "?")
    rs = state.get("result_state", "?")

    # Print task statuses
    tasks = run.get("tasks", [])
    task_str = " | ".join(
        f"{t['task_key']}={t.get('state',{}).get('life_cycle_state','?')}"
        for t in tasks
    )
    print(f"  [{lcs}/{rs}] {task_str}")

    if lcs in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
        print(f"\nFinal status: {lcs}/{rs}")
        if rs == "SUCCESS":
            print("Pipeline completed successfully!")
        else:
            msg = state.get("state_message", "")
            print(f"Error: {msg}")
            # Print task errors
            for t in tasks:
                tst = t.get("state", {})
                if tst.get("result_state") not in ("SUCCESS", None):
                    print(f"  Task {t['task_key']}: {tst.get('result_state')} - {tst.get('state_message','')[:200]}")
        break

    time.sleep(30)
