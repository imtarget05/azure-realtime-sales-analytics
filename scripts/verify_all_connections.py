"""End-to-end connection verification for all Azure services."""
import os, json, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from dotenv import load_dotenv
load_dotenv()

print("=" * 60)
print("  END-TO-END CONNECTION VERIFICATION")
print("=" * 60)

checks = []

# 1. Azure SQL
print("\n[1] Azure SQL Database...")
try:
    import pyodbc
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={os.getenv('SQL_SERVER')};DATABASE={os.getenv('SQL_DATABASE')};"
        f"UID={os.getenv('SQL_USERNAME')};PWD={os.getenv('SQL_PASSWORD')};"
        "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=10"
    )
    conn = pyodbc.connect(conn_str)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM dbo.SalesTransactions")
    cnt = cur.fetchone()[0]
    conn.close()
    print(f"  OK - SalesTransactions: {cnt:,} rows")
    checks.append(("Azure SQL", "OK", f"{cnt:,} rows"))
except Exception as e:
    print(f"  FAIL: {e}")
    checks.append(("Azure SQL", "FAIL", str(e)[:80]))

# 2. Azure ML Endpoint
print("\n[2] Azure ML Endpoint...")
try:
    import requests
    url = os.getenv("AML_ENDPOINT_URL")
    key = os.getenv("AML_API_KEY")
    payload = {"data": [{"hour": 14, "day_of_month": 15, "month": 3, "is_weekend": 0,
                         "store_id": "S01", "product_id": "COKE", "category": "Beverage",
                         "temperature": 28.0, "is_rainy": 0, "holiday": 0}]}
    resp = requests.post(url, headers={"Content-Type": "application/json",
                                       "Authorization": f"Bearer {key}"}, json=payload, timeout=30)
    print(f"  HTTP {resp.status_code}: {resp.text[:150]}")
    checks.append(("AML Endpoint", "OK", f"HTTP {resp.status_code}"))
except Exception as e:
    print(f"  FAIL: {e}")
    checks.append(("AML Endpoint", "FAIL", str(e)[:80]))

# 3. Web App
print("\n[3] Web App...")
try:
    import requests
    resp = requests.get("https://webapp-sales-analytics-d9bt2m.azurewebsites.net/api/health", timeout=30)
    data = resp.json()
    print(f"  HTTP {resp.status_code} - status: {data['status']}, ml_configured: {data['ml_endpoint_configured']}")
    checks.append(("Web App", "OK", f"Healthy, ML={data['ml_endpoint_configured']}"))
except Exception as e:
    print(f"  FAIL: {e}")
    checks.append(("Web App", "FAIL", str(e)[:80]))

# 4. Web App -> Predict API (full chain)
print("\n[4] Web App Predict API (full chain: Web -> AML -> Model)...")
try:
    import requests
    payload = {"hour": 14, "month": 6, "store_id": "S01", "product_id": "COKE"}
    resp = requests.post("https://webapp-sales-analytics-d9bt2m.azurewebsites.net/api/predict",
                         json=payload, timeout=60)
    data = resp.json()
    print(f"  HTTP {resp.status_code} - source: {data.get('source')}, "
          f"revenue: {data['prediction']['predicted_revenue']}")
    checks.append(("Web->AML Chain", "OK", f"source={data.get('source')}"))
except Exception as e:
    print(f"  FAIL: {e}")
    checks.append(("Web->AML Chain", "FAIL", str(e)[:80]))

# 5. Event Hub
print("\n[5] Event Hub...")
try:
    eh_conn = os.getenv("EVENT_HUB_CONNECTION_STRING", "")
    if "Endpoint=" in eh_conn:
        from azure.eventhub import EventHubProducerClient
        producer = EventHubProducerClient.from_connection_string(
            eh_conn, eventhub_name=os.getenv("EVENT_HUB_NAME", "sales-events"))
        info = producer.get_eventhub_properties()
        producer.close()
        print(f"  OK - partitions: {info['partition_count']}, created: {info['created_at']}")
        checks.append(("Event Hub", "OK", f"{info['partition_count']} partitions"))
    else:
        print("  SKIP - not configured")
        checks.append(("Event Hub", "SKIP", "not configured"))
except Exception as e:
    print(f"  FAIL: {e}")
    checks.append(("Event Hub", "FAIL", str(e)[:80]))

# 6. Blob Storage
print("\n[6] Blob Storage...")
try:
    blob_conn = os.getenv("BLOB_CONNECTION_STRING", "")
    if blob_conn and "DefaultEndpoints" in blob_conn:
        from azure.storage.blob import BlobServiceClient
        client = BlobServiceClient.from_connection_string(blob_conn)
        containers = [c.name for c in client.list_containers()]
        print(f"  OK - containers: {containers}")
        checks.append(("Blob Storage", "OK", f"{len(containers)} containers"))
    else:
        print("  SKIP - not configured")
        checks.append(("Blob Storage", "SKIP", "not configured"))
except Exception as e:
    print(f"  FAIL: {e}")
    checks.append(("Blob Storage", "FAIL", str(e)[:80]))

# 7. Local Models
print("\n[7] Local ML Models...")
try:
    import joblib
    rev = joblib.load("ml/model_output/revenue_model.pkl")
    qty = joblib.load("ml/model_output/quantity_model.pkl")
    with open("ml/model_output/model_metadata.json") as f:
        meta = json.load(f)
    print(f"  OK - version: {meta.get('model_version')}, "
          f"R2: {meta.get('revenue_metrics', {}).get('r2', '?')}")
    checks.append(("Local Models", "OK", f"v{meta.get('model_version')}"))
except Exception as e:
    print(f"  FAIL: {e}")
    checks.append(("Local Models", "FAIL", str(e)[:80]))

# 8. Benchmarks
print("\n[8] Benchmark Data...")
for name in ["benchmark_report", "benchmark_latency", "benchmark_read_write"]:
    p = f"benchmark_output/{name}.json"
    if os.path.exists(p):
        with open(p, encoding="utf-8") as f:
            d = json.load(f)
        sz = d.get("total_data_size_gb", d.get("metadata", {}).get("data_size", "?"))
        print(f"  OK - {name}: exists (size={sz})")
    else:
        print(f"  MISSING - {name}")

# 9. ML Comparison
print("\n[9] ML Model Comparison...")
p = "benchmark_output/ml_comparison/model_comparison_results.json"
if os.path.exists(p):
    with open(p) as f:
        d = json.load(f)
    print(f"  OK - best: {d['best_model']}, {len(d['ranking'])} models compared")
    checks.append(("ML Comparison", "OK", f"{len(d['ranking'])} models"))
else:
    print("  MISSING")
    checks.append(("ML Comparison", "MISSING", ""))

# 10. Charts
print("\n[10] Training Charts...")
chart_dir = "ml/model_output/charts"
if os.path.isdir(chart_dir):
    files = [f for f in os.listdir(chart_dir) if f.endswith(".png")]
    print(f"  OK - {len(files)} chart files")
else:
    print("  MISSING - charts directory")

print("\n" + "=" * 60)
print("  SUMMARY")
print("=" * 60)
for name, status, detail in checks:
    icon = "V" if status == "OK" else "X" if status in ("FAIL", "MISSING") else "-"
    print(f"  [{icon}] {name}: {status} - {detail}")
print()
