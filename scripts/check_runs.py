#!/usr/bin/env python3
"""Check pipeline run history."""
import os, sys, json, urllib.request
sys.path.insert(0, ".")
from dotenv import load_dotenv; load_dotenv()
HOST = os.getenv("DATABRICKS_HOST", "").rstrip("/")
TOKEN = os.getenv("DATABRICKS_TOKEN", "")
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}
def api(path):
    req = urllib.request.Request(f"{HOST}{path}", headers=HEADERS)
    return json.loads(urllib.request.urlopen(req, timeout=20).read())

runs = api("/api/2.1/jobs/runs/list?job_id=274825266735713&limit=20")
succeeded = 0
failed = 0
cancelled = 0
for run in runs.get("runs", []):
    st = run.get("state", {})
    lcs = st.get("life_cycle_state", "?")
    rs = st.get("result_state", "?")
    trigger = run.get("trigger", "?")
    rid = run["run_id"]
    msg = st.get("state_message", "")[:80]
    symbol = "?" 
    if rs == "SUCCESS":
        symbol = "OK"
        succeeded += 1
    elif rs == "FAILED":
        symbol = "FAIL"
        failed += 1
    elif rs == "CANCELED":
        symbol = "SKIP"
        cancelled += 1
    print(f"  [{symbol:4s}] Run {rid}: {lcs}/{rs} ({trigger}) {msg}")

print(f"\nSummary: {succeeded} succeeded, {failed} failed, {cancelled} cancelled")
