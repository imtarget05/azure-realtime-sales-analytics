"""Check activity run details for ADF pipeline run."""
import os, sys, time, requests
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
os.environ["KEY_VAULT_URI"] = "DISABLED"
from azure.identity import DefaultAzureCredential

cred = DefaultAzureCredential()
tok = cred.get_token("https://management.azure.com/.default").token

SUB = "34849ef9-3814-44df-ba32-a86ed9f2a69a"
RG  = "rg-sales-analytics-dev"
ADF = "adf-sales-paivm"
run_id = "ba91121b-beff-45b0-923e-1dc1d4ae1f58"

BASE = f"https://management.azure.com/subscriptions/{SUB}/resourceGroups/{RG}/providers/Microsoft.DataFactory/factories/{ADF}"
hdrs_get = {"Authorization": f"Bearer {tok}"}
hdrs_post = {"Authorization": f"Bearer {tok}", "Content-Type": "application/json"}

# Pipeline run overall
r = requests.get(f"{BASE}/pipelineruns/{run_id}?api-version=2018-06-01", headers=hdrs_get, timeout=15)
info = r.json()
print(f"Pipeline run:  {info.get('status')} ({info.get('durationMs', '?')}ms)")

# Activity runs
now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
yes = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() - 86400))
r2 = requests.post(
    f"{BASE}/pipelineruns/{run_id}/queryActivityruns?api-version=2018-06-01",
    headers=hdrs_post,
    json={
        "lastUpdatedAfter": yes,
        "lastUpdatedBefore": now,
    },
    timeout=15,
)
acts = r2.json().get("value", [])
print(f"\nActivities ({len(acts)}):")
for a in sorted(acts, key=lambda x: x.get("activityRunStart", "")):
    name = a["activityName"]
    status = a["status"]
    atype = a.get("activityType", "")
    dur = a.get("durationMs", 0)
    err = a.get("error", {})
    icon = "ok" if status == "Succeeded" else ("FAIL" if status == "Failed" else "...")
    print(f"  [{icon}] {name} ({atype}): {status} - {dur}ms")
    if err and err.get("message"):
        print(f"       ERROR: {err.get('message', '')[:200]}")
