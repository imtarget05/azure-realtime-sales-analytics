#!/usr/bin/env python3
"""Check Databricks pipeline status via REST API (avoids local module conflict)."""
import os
import sys
import json
import urllib.request

# Must run from repo root: python scripts/dbx_status.py
sys.path.insert(0, ".")
from dotenv import load_dotenv
load_dotenv()

HOST = os.getenv("DATABRICKS_HOST", "").rstrip("/")
TOKEN = os.getenv("DATABRICKS_TOKEN", "")
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}


def api(path):
    req = urllib.request.Request(f"{HOST}{path}", headers=HEADERS)
    resp = urllib.request.urlopen(req, timeout=15)
    return json.loads(resp.read())


def main():
    # 1. Check identity
    me = api("/api/2.0/preview/scim/v2/Me")
    print(f"Connected as: {me.get('userName')}")

    # 2. Check uploaded notebooks
    print("\n=== Uploaded Notebooks ===")
    try:
        r = api("/api/2.0/workspace/list?path=/Shared/azure-realtime-sales-analytics/notebooks")
        objs = r.get("objects", [])
        print(f"Found {len(objs)} notebooks:")
        for o in objs:
            print(f"  {o['path']}")
    except Exception as e:
        print(f"  ERROR or directory not found: {e}")

    # 3. Check job
    print("\n=== Sales_Lakehouse_Pipeline Job ===")
    r = api("/api/2.1/jobs/list?name=Sales_Lakehouse_Pipeline")
    jobs = r.get("jobs", [])
    if not jobs:
        print("  Job NOT FOUND!")
        print("\n  All jobs:")
        all_jobs = api("/api/2.1/jobs/list")
        for j in all_jobs.get("jobs", []):
            print(f"    [{j['job_id']}] {j['settings']['name']}")
        return

    j = jobs[0]
    jid = j["job_id"]
    settings = j["settings"]
    print(f"Job: {settings['name']} (ID={jid})")

    # Print git_source if any
    git_source = settings.get("git_source")
    if git_source:
        print(f"  git_source: {git_source.get('git_url')} branch={git_source.get('git_branch')}")
    else:
        print("  git_source: NONE (using WORKSPACE paths)")

    # Print task sources
    print("\n  Task sources:")
    for task in settings.get("tasks", []):
        nb = task.get("notebook_task", {})
        src = nb.get("source", "?")
        path = nb.get("notebook_path", "?")
        print(f"    {task['task_key']:25s}: {src} -> {path}")

    # Recent runs
    runs_r = api(f"/api/2.1/jobs/runs/list?job_id={jid}&limit=5")
    runs = runs_r.get("runs", [])
    print(f"\n  Recent runs ({len(runs)}):")
    for run in runs:
        st = run.get("state", {})
        lcs = st.get("life_cycle_state", "?")
        rs = st.get("result_state", "(pending)")
        msg = st.get("state_message", "")
        print(f"    Run {run['run_id']}: {lcs} / {rs}")
        if msg:
            print(f"      Message: {msg[:100]}")


if __name__ == "__main__":
    main()
