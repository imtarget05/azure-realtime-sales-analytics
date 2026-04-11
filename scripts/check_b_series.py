import os, requests, json
from dotenv import load_dotenv
load_dotenv()
HOST = os.getenv("DATABRICKS_HOST","").rstrip("/")
TOKEN = os.getenv("DATABRICKS_TOKEN","")
H = {"Authorization": f"Bearer {TOKEN}"}

resp = requests.get(f"{HOST}/api/2.0/clusters/list-node-types", headers=H)
data = resp.json()

# Search for B-series, L-series, and specific types
search_terms = ["Standard_B", "Standard_L", "Standard_NC4", "Standard_NV4"]
for term in search_terms:
    matches = []
    for nt in data.get("node_types", []):
        name = nt.get("node_type_id", "")
        if name.startswith(term):
            status = nt.get("node_type_status", {})
            matches.append({
                "name": name,
                "cores": nt.get("num_cores", 0),
                "mem_gb": nt.get("memory_mb", 0) / 1024,
                "not_avail": status.get("not_available_in_region", False),
                "not_enabled": status.get("not_enabled_on_subscription", False),
            })
    if matches:
        print(f"\n=== {term}* ({len(matches)} found) ===")
        for m in sorted(matches, key=lambda x: x["cores"]):
            flags = []
            if m["not_avail"]: flags.append("NOT_IN_REGION")
            if m["not_enabled"]: flags.append("NOT_ENABLED")
            f = " [" + ",".join(flags) + "]" if flags else ""
            print(f"  {m['name']:35s} {m['cores']}c {m['mem_gb']:6.1f}GB{f}")
    else:
        print(f"\n=== {term}*: NONE found in Databricks ===")
