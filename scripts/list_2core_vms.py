"""
List available Databricks node types with 2 vCPUs.
"""
import os, requests, json
from dotenv import load_dotenv

load_dotenv()
HOST = os.getenv("DATABRICKS_HOST", "").rstrip("/")
TOKEN = os.getenv("DATABRICKS_TOKEN", "")
HEADERS = {"Authorization": f"Bearer {TOKEN}"}

resp = requests.get(f"{HOST}/api/2.0/clusters/list-node-types", headers=HEADERS)
data = resp.json()

print("=== 2 vCPU node types ===")
two_core = []
for nt in data.get("node_types", []):
    ncpu = nt.get("num_cores", 0)
    mem = nt.get("memory_mb", 0)
    name = nt.get("node_type_id", "")
    status = nt.get("node_type_status", {})
    # Only show 2 core, non-deprecated, available
    if ncpu == 2 and not status.get("not_available_in_region", False):
        info = nt.get("node_info", {})
        avail = info.get("available_core_quota", -1)
        two_core.append({
            "name": name,
            "mem_gb": mem / 1024,
            "category": nt.get("category", "?"),
            "is_deprecated": nt.get("is_deprecated", False),
            "not_enabled": status.get("not_enabled_on_subscription", False),
        })

two_core.sort(key=lambda x: (x["is_deprecated"], x["not_enabled"], x["name"]))
for t in two_core:
    dep = " [DEPRECATED]" if t["is_deprecated"] else ""
    noten = " [NOT ENABLED]" if t["not_enabled"] else ""
    print(f"  {t['name']:35s} {t['mem_gb']:6.1f} GB  {t['category']:20s}{dep}{noten}")

print(f"\nTotal 2-core types: {len(two_core)}")
