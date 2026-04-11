#!/usr/bin/env python3
"""
Fix Databricks cluster: đổi Standard_D4as_v5 → Standard_DS3_v2
để tránh AZURE_QUOTA_EXCEEDED_EXCEPTION.
"""
import os, sys, json, urllib.request, urllib.error

sys.path.insert(0, ".")
from dotenv import load_dotenv
load_dotenv()

HOST = os.getenv("DATABRICKS_HOST", "").rstrip("/")
TOKEN = os.getenv("DATABRICKS_TOKEN", "")
JOB_ID = 274825266735713

if not HOST or not TOKEN:
    print("ERROR: DATABRICKS_HOST hoặc DATABRICKS_TOKEN chưa được set")
    sys.exit(1)

print(f"Host: {HOST}")
print(f"Token set: {bool(TOKEN and len(TOKEN) > 10)}")

HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

def api_get(path):
    req = urllib.request.Request(f"{HOST}{path}", headers=HEADERS)
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())

def api_post(path, body):
    data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(f"{HOST}{path}", data=data, headers=HEADERS, method="POST")
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())

# 1. Lấy job settings hiện tại
print(f"\n--- Lấy job {JOB_ID} ---")
try:
    job = api_get(f"/api/2.1/jobs/get?job_id={JOB_ID}")
except urllib.error.HTTPError as e:
    print(f"HTTP Error {e.code}: {e.read().decode()}")
    sys.exit(1)

settings = job.get("settings", {})
print(f"Job name: {settings.get('name')}")

# 2. Hiện cluster config hiện tại
clusters = settings.get("job_clusters", [])
print("\n--- Cluster config HIỆN TẠI ---")
for c in clusters:
    key = c.get("job_cluster_key")
    nc = c.get("new_cluster", {})
    node_type = nc.get("node_type_id", "N/A")
    print(f"  {key}: {node_type}")

# 3. Đổi node_type sang không dùng DASv5
REPLACEMENT_MAP = {
    "etl_cluster": "Standard_DS3_v2",   # DSv2 family — có quota mặc định
    "ml_cluster":  "Standard_DS4_v2",   # DSv2 family — ML workload
}

changed = False
for c in clusters:
    key = c.get("job_cluster_key")
    nc = c.get("new_cluster", {})
    current = nc.get("node_type_id", "")
    if key in REPLACEMENT_MAP and current != REPLACEMENT_MAP[key]:
        print(f"\n  Đổi {key}: {current} → {REPLACEMENT_MAP[key]}")
        nc["node_type_id"] = REPLACEMENT_MAP[key]
        # Xóa autoscale nếu có để đơn giản (tránh quota issue)
        nc["num_workers"] = 1
        if "autoscale" in nc:
            del nc["autoscale"]
        changed = True
    elif key in REPLACEMENT_MAP:
        print(f"\n  {key}: already {current}, OK")

if not changed:
    print("\nKhông có gì thay đổi.")
    sys.exit(0)

# 4. Reset job với settings mới
print("\n--- Gửi update lên Databricks ---")
try:
    result = api_post("/api/2.1/jobs/reset", {
        "job_id": JOB_ID,
        "new_settings": settings
    })
    print("SUCCESS: Job updated.")
    print(result)
except urllib.error.HTTPError as e:
    body = e.read().decode()
    print(f"HTTP Error {e.code}: {body}")
    sys.exit(1)

# 5. Xác nhận lại
print("\n--- Verify cluster config SAU update ---")
job2 = api_get(f"/api/2.1/jobs/get?job_id={JOB_ID}")
for c in job2.get("settings", {}).get("job_clusters", []):
    key = c.get("job_cluster_key")
    nc = c.get("new_cluster", {})
    print(f"  {key}: {nc.get('node_type_id')} | workers: {nc.get('num_workers')}")

print("\nDone. Chạy lại job từ Databricks UI.")
