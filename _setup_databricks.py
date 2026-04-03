"""
Setup Databricks workspace: create SQL Warehouse, upload notebooks, create secret scope.
Uses Azure AD token for authentication.
"""
import json
import os
import subprocess
import time
import urllib.request
import urllib.error

# Get config
DATABRICKS_HOST = "https://adb-7405611397181783.3.azuredatabricks.net"

def get_token():
    """Get Azure AD token for Databricks."""
    result = subprocess.run(
        ['az', 'account', 'get-access-token', 
         '--resource', '2ff814a6-3304-4ab8-85cb-cd0e6f879c1d',
         '--query', 'accessToken', '-o', 'tsv'],
        capture_output=True, text=True, shell=True
    )
    return result.stdout.strip()

TOKEN = get_token()
print(f"Token acquired: {len(TOKEN)} chars")

def dbx_api(method, path, data=None):
    """Call Databricks REST API."""
    url = f"{DATABRICKS_HOST}/api/2.0/{path}"
    headers = {
        "Authorization": f"Bearer {TOKEN}",
        "Content-Type": "application/json"
    }
    body = json.dumps(data).encode() if data else None
    req = urllib.request.Request(url, body, headers, method=method)
    try:
        resp = urllib.request.urlopen(req)
        raw = resp.read().decode()
        return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        print(f"  API Error {e.code}: {body[:200]}")
        return {"error": body}

def dbx_api_get(path):
    url = f"{DATABRICKS_HOST}/api/2.0/{path}"
    headers = {"Authorization": f"Bearer {TOKEN}"}
    req = urllib.request.Request(url, headers=headers)
    try:
        resp = urllib.request.urlopen(req)
        return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        return {"error": body}

# ── Step 1: Create SQL Warehouse ──
print("\n" + "=" * 60)
print("  Step 1: Creating SQL Warehouse")
print("=" * 60)

# Check existing warehouses
existing = dbx_api_get("sql/warehouses")
warehouses = existing.get("warehouses", [])
wh_id = None
for wh in warehouses:
    if wh.get("name") == "sales-analytics-warehouse":
        wh_id = wh["id"]
        print(f"  SQL Warehouse already exists: {wh_id}")
        break

if not wh_id:
    wh_config = {
        "name": "sales-analytics-warehouse",
        "cluster_size": "2X-Small",
        "min_num_clusters": 1,
        "max_num_clusters": 1,
        "auto_stop_mins": 15,
        "warehouse_type": "PRO",
        "enable_serverless_compute": False,
        "tags": {
            "custom_tags": [
                {"key": "project", "value": "sales-analytics"},
                {"key": "env", "value": "dev"}
            ]
        }
    }
    result = dbx_api("POST", "sql/warehouses", wh_config)
    if "id" in result:
        wh_id = result["id"]
        print(f"  SQL Warehouse created: {wh_id}")
    else:
        print(f"  Failed to create warehouse: {result}")

# ── Step 2: Upload Notebooks ──
print("\n" + "=" * 60)
print("  Step 2: Uploading Notebooks")
print("=" * 60)

import base64

notebook_dir = os.path.join(os.path.dirname(__file__), "databricks", "notebooks")
remote_path = "/Workspace/sales-analytics"

# Create directory
dbx_api("POST", "workspace/mkdirs", {"path": remote_path})

notebooks = [
    "00_config.py",
    "01_bronze_ingestion.py",
    "02_silver_etl.py",
    "03_feature_engineering.py",
    "04_ml_prediction.py",
    "05_gold_aggregation.py",
]

for nb in notebooks:
    local_path = os.path.join(notebook_dir, nb)
    if not os.path.exists(local_path):
        print(f"  SKIP {nb}: file not found")
        continue
    
    with open(local_path, "r", encoding="utf-8") as f:
        content = f.read()
    
    # Base64 encode
    b64 = base64.b64encode(content.encode()).decode()
    
    remote_nb = f"{remote_path}/{nb.replace('.py', '')}"
    result = dbx_api("POST", "workspace/import", {
        "path": remote_nb,
        "format": "SOURCE",
        "language": "PYTHON",
        "content": b64,
        "overwrite": True
    })
    
    if "error" not in result:
        print(f"  Uploaded: {remote_nb}")
    else:
        print(f"  Error uploading {nb}: {result}")

# Also upload SQL scripts
sql_dir = os.path.join(os.path.dirname(__file__), "databricks", "sql")
remote_sql = "/Workspace/sales-analytics/sql"
dbx_api("POST", "workspace/mkdirs", {"path": remote_sql})

for sql_file in ["create_access_rights.sql", "create_security_mapping.sql"]:
    local_path = os.path.join(sql_dir, sql_file)
    if not os.path.exists(local_path):
        continue
    with open(local_path, "r", encoding="utf-8") as f:
        content = f.read()
    b64 = base64.b64encode(content.encode()).decode()
    remote_nb = f"{remote_sql}/{sql_file.replace('.sql', '')}"
    result = dbx_api("POST", "workspace/import", {
        "path": remote_nb,
        "format": "SOURCE",
        "language": "SQL",
        "content": b64,
        "overwrite": True
    })
    if "error" not in result:
        print(f"  Uploaded: {remote_nb}")
    else:
        print(f"  Error: {result}")

# ── Step 3: Get connection info for Power BI ──
print("\n" + "=" * 60)
print("  Step 3: Connection Info for Power BI")
print("=" * 60)

if wh_id:
    wh_info = dbx_api_get(f"sql/warehouses/{wh_id}")
    odbc_params = wh_info.get("odbc_params", {})
    print(f"  Workspace URL : adb-7405611397181783.3.azuredatabricks.net")
    print(f"  Warehouse ID  : {wh_id}")
    print(f"  HTTP Path     : /sql/1.0/warehouses/{wh_id}")
    print(f"  Port          : 443")
    print(f"  State         : {wh_info.get('state', 'unknown')}")

print("\n" + "=" * 60)
print("  DATABRICKS SETUP COMPLETE")
print("=" * 60)
print(f"""
Next Steps:
1. Open Databricks: {DATABRICKS_HOST}
2. Go to SQL Warehouses → Start 'sales-analytics-warehouse'
3. Open Power BI Desktop → Get Data → Azure Databricks
   - Server: adb-7405611397181783.3.azuredatabricks.net
   - HTTP Path: /sql/1.0/warehouses/{wh_id}
   - Auth: Azure Active Directory
4. Select tables from sales_analytics.gold.*
""")
