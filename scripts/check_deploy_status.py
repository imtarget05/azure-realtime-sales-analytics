"""Check the latest deployment status from Kudu."""
import requests

KUDU = "https://func-sales-validation-d9bt2m.scm.azurewebsites.net"
UNAME = "$func-sales-validation-d9bt2m"
PWORD = "PRm9EqjnN0C40fwoYfbQLl2ZabkQ6xbBRgSpkLfwgeWyGH8ykmT71E2BhDfS"
AUTH = (UNAME, PWORD)

STATUS_LABELS = {0: "Pending", 1: "Building", 2: "Deploying", 3: "Failed", 4: "Success"}

r = requests.get(KUDU + "/api/deployments", auth=AUTH, timeout=30)
print("HTTP", r.status_code)
if r.ok:
    for d in r.json()[:4]:
        lbl = STATUS_LABELS.get(d["status"], str(d["status"]))
        print(f"  id={d['id'][:20]}  status={lbl}  complete={d['complete']}  active={d['active']}")
        if d["complete"] and d["status"] == 4:
            # Success - show it
            print("  -> DEPLOYED SUCCESSFULLY")
        if d["log_url"]:
            lr = requests.get(d["log_url"], auth=AUTH, timeout=30)
            if lr.ok:
                entries = lr.json()
                for e in entries[-5:]:
                    print(f"     {e.get('log_time','')[:19]} {e.get('message','')[:80]}")
                    if e.get("details_url"):
                        ddr = requests.get(e["details_url"], auth=AUTH, timeout=30)
                        if ddr.ok:
                            sub = ddr.json()
                            for se in sub[-3:]:
                                print(f"       >> {se.get('message','')[:100]}")
