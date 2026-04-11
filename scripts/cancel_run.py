#!/usr/bin/env python3
"""Cancel the stuck pipeline run"""
import urllib.request, urllib.error, json, os
from dotenv import load_dotenv
load_dotenv()
HOST = os.getenv("DATABRICKS_HOST", "https://adb-7405607469187602.2.azuredatabricks.net")
TOKEN = os.getenv("DATABRICKS_TOKEN", "")
RUN_ID = 379611463363637

req = urllib.request.Request(
    f"{HOST}/api/2.1/jobs/runs/cancel",
    data=json.dumps({"run_id": RUN_ID}).encode(),
    headers={"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"},
    method="POST"
)
try:
    resp = urllib.request.urlopen(req)
    print(f"Cancelled run {RUN_ID}: {resp.status}")
except urllib.error.HTTPError as e:
    print(f"Error: {e.code} {e.read().decode()}")
