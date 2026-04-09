#!/usr/bin/env python3
"""Inspect raw Databricks job settings and current run status."""
import os, sys, json, urllib.request, urllib.error

sys.path.insert(0, ".")
from dotenv import load_dotenv
load_dotenv()

HOST = os.getenv("DATABRICKS_HOST", "").rstrip("/")
TOKEN = os.getenv("DATABRICKS_TOKEN", "")
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

def api(path):
    req = urllib.request.Request(f"{HOST}{path}", headers=HEADERS)
    try:
        resp = urllib.request.urlopen(req, timeout=20)
        return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        print(f"HTTP Error {e.code}: {body[:300]}")
        return {}

# Get job
r = api("/api/2.1/jobs/list?name=Sales_Lakehouse_Pipeline")
jobs = r.get("jobs", [])
if not jobs:
    print("Job not found")
    sys.exit(1)

jid = jobs[0]["job_id"]
print(f"Job ID: {jid}")

# Full GET with all details
full = api(f"/api/2.1/jobs/get?job_id={jid}")
settings = full.get("settings", {})

print(f"\nName: {settings.get('name')}")
print(f"git_source present: {'git_source' in settings}")

tasks = settings.get("tasks", [])
print(f"\nTasks count: {len(tasks)}")
for i, t in enumerate(tasks):
    print(f"\n--- Task {i}: ---")
    print(json.dumps(t, indent=2))

# Check latest run
print("\n\n=== Latest Run Details ===")
runs_r = api(f"/api/2.1/jobs/runs/list?job_id={jid}&limit=2")
runs = runs_r.get("runs", [])
if runs:
    latest = runs[0]
    rid = latest["run_id"]
    state = latest.get("state", {})
    print(f"Run {rid}: {state.get('life_cycle_state')} / {state.get('result_state','pending')}")
    if state.get("state_message"):
        print(f"Message: {state['state_message']}")

    # Get run tasks
    run_detail = api(f"/api/2.1/jobs/runs/get?run_id={rid}")
    for rt in run_detail.get("tasks", []):
        rst = rt.get("state", {})
        print(f"  Task {rt.get('task_key')}: {rst.get('life_cycle_state')} / {rst.get('result_state','?')}")
        if rst.get("state_message"):
            print(f"    Error: {rst['state_message'][:200]}")
