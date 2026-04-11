"""Check Databricks pipeline run status."""
import os
import json
import urllib.request

from dotenv import load_dotenv
load_dotenv()

DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST", "https://adb-7405607469187602.2.azuredatabricks.net")
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN", "")
RUN_ID = 865988146815578


def api_get(path):
    url = f"{DATABRICKS_HOST}{path}"
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {DATABRICKS_TOKEN}"})
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


run = api_get(f"/api/2.1/jobs/runs/get?run_id={RUN_ID}")
state = run.get("state", {})
print(f"Run {RUN_ID}")
print(f"  Life cycle: {state.get('life_cycle_state')}")
print(f"  Result:     {state.get('result_state', 'N/A')}")
print(f"  Message:    {state.get('state_message', 'N/A')}")

tasks = run.get("tasks", [])
print(f"\nTasks ({len(tasks)}):")
for t in tasks:
    ts = t.get("state", {})
    name = t.get("task_key", "?")
    lifecycle = ts.get("life_cycle_state", "?")
    result = ts.get("result_state", "")
    msg = ts.get("state_message", "")
    print(f"  {name:25s} {lifecycle:15s} {result:10s} {msg[:80] if msg else ''}")
