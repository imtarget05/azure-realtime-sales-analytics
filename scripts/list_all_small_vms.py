"""
List ALL Databricks node types with <=4 cores, checking availability.
"""
import os, requests, json
from dotenv import load_dotenv

load_dotenv()
HOST = os.getenv("DATABRICKS_HOST", "").rstrip("/")
TOKEN = os.getenv("DATABRICKS_TOKEN", "")
HEADERS = {"Authorization": f"Bearer {TOKEN}"}

resp = requests.get(f"{HOST}/api/2.0/clusters/list-node-types", headers=HEADERS)
data = resp.json()

print("=== Node types with <= 4 vCPUs (available in region) ===")
types = []
for nt in data.get("node_types", []):
    ncpu = nt.get("num_cores", 0) 
    mem = nt.get("memory_mb", 0)
    name = nt.get("node_type_id", "")
    status = nt.get("node_type_status", {})
    is_dep = nt.get("is_deprecated", False)
    not_avail = status.get("not_available_in_region", False)
    not_enabled = status.get("not_enabled_on_subscription", False)
    
    if ncpu <= 4 and ncpu > 0 and not not_avail:
        types.append({
            "name": name,
            "cores": ncpu,
            "mem_gb": mem / 1024,
            "deprecated": is_dep,
            "not_enabled": not_enabled,
        })

types.sort(key=lambda x: (x["cores"], x["name"]))
for t in types:
    flags = []
    if t["deprecated"]: flags.append("DEP")
    if t["not_enabled"]: flags.append("NOT_EN")
    f = " [" + ",".join(flags) + "]" if flags else ""
    print(f"  {t['cores']}c {t['name']:35s} {t['mem_gb']:6.1f} GB{f}")

print(f"\nTotal: {len(types)}")
