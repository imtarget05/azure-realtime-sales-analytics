"""Poll Kudu deployment until complete."""
import requests
import time
import sys

KUDU = "https://func-sales-validation-d9bt2m.scm.azurewebsites.net"
AUTH = ("$func-sales-validation-d9bt2m", "PRm9EqjnN0C40fwoYfbQLl2ZabkQ6xbBRgSpkLfwgeWyGH8ykmT71E2BhDfS")
POLL = KUDU + "/api/deployments/latest"
LOG  = KUDU + "/api/deployments"

STATUS_LABELS = {0: "Pending", 1: "Building", 2: "Deploying", 3: "Failed", 4: "Success"}

for i in range(60):
    time.sleep(15)
    r = requests.get(POLL, auth=AUTH, timeout=30)
    if not r.ok:
        print("poll error", r.status_code)
        break
    d = r.json()
    status = d.get("status", 0)
    label  = STATUS_LABELS.get(status, str(status))
    done   = d.get("complete", False)
    msg    = (d.get("message") or "")[:60]
    pct    = d.get("progress") or ""
    print(f"[{i+1:02d}] {label} complete={done} {pct} {msg}")
    sys.stdout.flush()
    if done:
        if status == 4:
            print("SUCCESS - deployment complete!")
        else:
            print(f"FAILED - final status: {label}")
            # Print last log entries
            logs_r = requests.get(LOG, auth=AUTH, timeout=30)
            if logs_r.ok:
                deploys = logs_r.json()
                if deploys:
                    log_url = deploys[0].get("log_url", "")
                    if log_url:
                        lr = requests.get(log_url, auth=AUTH, timeout=30)
                        if lr.ok:
                            for entry in lr.json()[-10:]:
                                print(" ", entry.get("log_time","")[:19], entry.get("message",""))
        break
else:
    print("Timed out after 15 min")
