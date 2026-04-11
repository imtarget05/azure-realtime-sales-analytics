#!/usr/bin/env python3
"""Quick check of pipeline run + cluster status."""
import urllib.request, json, ssl, time

ctx = ssl.create_default_context()
import os
from dotenv import load_dotenv
load_dotenv()
HOST = os.getenv("DATABRICKS_HOST", "https://adb-7405607469187602.2.azuredatabricks.net")
TOKEN = os.getenv("DATABRICKS_TOKEN", "")

def api(path):
    req = urllib.request.Request(
        HOST + path,
        headers={"Authorization": "Bearer " + TOKEN}
    )
    return json.loads(urllib.request.urlopen(req, context=ctx, timeout=20).read())

# Check run first to get cluster_id
run_id = 379611463363637
r = api(f"/api/2.1/jobs/runs/get?run_id={run_id}")
st = r.get("state", {})
print(f"Run {run_id}:")
print(f"  State: {st.get('life_cycle_state')} / {st.get('result_state', 'N/A')}")
msg = st.get("state_message", "")
if msg:
    print(f"  Msg: {msg[:300]}")

for t in r.get("tasks", []):
    ts = t.get("state", {})
    tk = t.get("task_key", "?")
    lcs = ts.get("life_cycle_state", "?")
    rs = ts.get("result_state", "-")
    tmsg = ts.get("state_message", "")[:120]
    cid = t.get("cluster_instance", {}).get("cluster_id", "")
    extra = f"  cluster={cid}" if cid else ""
    print(f"  Task {tk}: {lcs}/{rs}{extra}  {tmsg}")
    # Check cluster if available
    if cid:
        try:
            c = api("/api/2.0/clusters/get?cluster_id=" + cid)
            print(f"    Cluster state: {c.get('state')} - {c.get('state_message', '')[:100]}")
            tr = c.get("termination_reason", {})
            if tr and tr.get("code"):
                print(f"    Term: {tr.get('code')} / {tr.get('type')}")
        except:
            pass

elapsed = (time.time() * 1000 - r.get("start_time", 0)) / 60000
print(f"\nElapsed: {elapsed:.1f} minutes")
