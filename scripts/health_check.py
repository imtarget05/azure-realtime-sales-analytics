#!/usr/bin/env python3
"""Full project health check for demo readiness."""
import os, sys, json
sys.path.insert(0, ".")
from dotenv import load_dotenv; load_dotenv()

PASS = "PASS"
FAIL = "FAIL"
WARN = "WARN"
results = []

def check(name, status, detail=""):
    results.append((name, status, detail))
    symbol = {"PASS": "[OK]", "FAIL": "[XX]", "WARN": "[!!]"}[status]
    print(f"  {symbol} {name}: {detail}")

print("=" * 60)
print("  PROJECT HEALTH CHECK - Demo Readiness")
print("=" * 60)

# ============================================================
# 1. SQL Server
# ============================================================
print("\n--- SQL Server ---")
try:
    import pyodbc
    srv = os.getenv("SQL_SERVER")
    uid = os.getenv("SQL_USERNAME")
    pwd = os.getenv("SQL_PASSWORD")
    cs = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={srv};DATABASE=SalesAnalyticsDB;"
        f"UID={uid};PWD={pwd};Encrypt=yes;TrustServerCertificate=no"
    )
    conn = pyodbc.connect(cs, timeout=10)
    cur = conn.cursor()
    check("SQL Connection", PASS, srv)

    # Check all tables
    cur.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE' ORDER BY TABLE_NAME")
    tables = [r[0] for r in cur.fetchall()]
    required = ["SalesTransactions", "HourlySalesSummary", "SalesForecast", "SalesAlerts", "ModelRegistry", "StoreRegions"]
    for t in required:
        if t in tables:
            cur.execute(f"SELECT COUNT(*) FROM dbo.[{t}]")
            cnt = cur.fetchone()[0]
            check(f"Table {t}", PASS if cnt > 0 else WARN, f"{cnt:,} rows")
        else:
            check(f"Table {t}", FAIL, "MISSING")

    # Check views
    cur.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS ORDER BY TABLE_NAME")
    views = [r[0] for r in cur.fetchall()]
    required_views = [
        "vw_SalesByRegion", "vw_RegionalSummary", "vw_ForecastAccuracy",
        "vw_AlertSummary", "vw_ETLPipelineHealth", "vw_DailyRevenueTrend"
    ]
    for v in required_views:
        check(f"View {v}", PASS if v in views else FAIL, "exists" if v in views else "MISSING")

    # Check data freshness
    cur.execute("SELECT MAX(event_time) FROM dbo.SalesTransactions")
    latest = cur.fetchone()[0]
    check("Data freshness (SalesTransactions)", PASS if latest else FAIL, str(latest))

    # Check regional data
    cur.execute("SELECT COUNT(DISTINCT region) FROM dbo.vw_SalesByRegion")
    regions = cur.fetchone()[0]
    check("Regional segmentation", PASS if regions >= 3 else FAIL, f"{regions} regions")

    conn.close()
except Exception as e:
    check("SQL Connection", FAIL, str(e)[:100])

# ============================================================
# 2. Databricks
# ============================================================
print("\n--- Databricks ---")
try:
    import urllib.request
    HOST = os.getenv("DATABRICKS_HOST", "").rstrip("/")
    TOKEN = os.getenv("DATABRICKS_TOKEN", "")
    HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

    def dbx_api(path):
        req = urllib.request.Request(f"{HOST}{path}", headers=HEADERS)
        return json.loads(urllib.request.urlopen(req, timeout=15).read())

    me = dbx_api("/api/2.0/preview/scim/v2/Me")
    check("Databricks connection", PASS, me.get("userName"))

    # Check notebooks
    try:
        nb = dbx_api("/api/2.0/workspace/list?path=/Shared/azure-realtime-sales-analytics/notebooks")
        nb_count = len(nb.get("objects", []))
        check("Notebooks uploaded", PASS if nb_count >= 5 else FAIL, f"{nb_count} notebooks")
    except Exception:
        check("Notebooks uploaded", FAIL, "directory not found")

    # Check job
    r = dbx_api("/api/2.1/jobs/list?name=Sales_Lakehouse_Pipeline")
    jobs = r.get("jobs", [])
    if jobs:
        j = jobs[0]
        jid = j["job_id"]
        sched = j["settings"].get("schedule", {})
        pause = sched.get("pause_status", "N/A")
        check("Pipeline job exists", PASS, f"ID={jid}")
        check("Pipeline schedule", PASS if pause == "PAUSED" else WARN,
              f"{sched.get('quartz_cron_expression','?')} ({pause})")

        # Check tasks
        tasks = j["settings"].get("tasks", [])
        all_workspace = all(
            t.get("notebook_task", {}).get("source") == "WORKSPACE"
            for t in tasks
        )
        check("Task source mode", PASS if all_workspace else FAIL,
              "WORKSPACE" if all_workspace else "has non-WORKSPACE tasks")
    else:
        check("Pipeline job exists", FAIL, "not found")

except Exception as e:
    check("Databricks connection", FAIL, str(e)[:100])

# ============================================================
# 3. Event Hub (via .env config)
# ============================================================
print("\n--- Event Hub ---")
eh_conn = os.getenv("EVENT_HUB_CONNECTION_STRING", "")
eh_name = os.getenv("EVENT_HUB_NAME", "")
check("Event Hub config", PASS if eh_conn and eh_name else FAIL,
      f"hub={eh_name}" if eh_name else "missing config")

# ============================================================
# 4. Local ML Models
# ============================================================
print("\n--- ML Models ---")
models = {
    "revenue_model.pkl": "ml/model_output/revenue_model.pkl",
    "quantity_model.pkl": "ml/model_output/quantity_model.pkl",
    "model_metadata.json": "ml/model_output/model_metadata.json",
    "label_encoders.pkl": "ml/model_output/label_encoders.pkl",
}
for name, path in models.items():
    if os.path.exists(path):
        sz = os.path.getsize(path)
        check(f"ML {name}", PASS, f"{sz/1024:.0f} KB")
    else:
        check(f"ML {name}", FAIL, "missing")

if os.path.exists("ml/model_output/model_metadata.json"):
    with open("ml/model_output/model_metadata.json") as f:
        meta = json.load(f)
    r2 = meta.get("revenue_metrics", {}).get("r2_score", 0)
    check("Model quality (R²)", PASS if r2 > 0.7 else WARN, f"revenue R²={r2}")

# ============================================================
# 5. Key project files
# ============================================================
print("\n--- Project Files ---")
key_files = [
    "powerbi/dax_measures.dax",
    "powerbi/semantic_model.json",
    "powerbi/dashboard_layout.json",
    "sql/create_tables.sql",
    "sql/create_powerbi_views.sql",
    "stream_analytics/stream_query.sql",
    "data_generator/sales_generator.py",
    "databricks/jobs/upload_notebooks.py",
]
for f in key_files:
    check(f"File {f}", PASS if os.path.exists(f) else FAIL,
          "exists" if os.path.exists(f) else "MISSING")

# ============================================================
# Summary
# ============================================================
print("\n" + "=" * 60)
passes = sum(1 for _, s, _ in results if s == PASS)
fails = sum(1 for _, s, _ in results if s == FAIL)
warns = sum(1 for _, s, _ in results if s == WARN)
print(f"  TOTAL: {passes} PASS, {warns} WARN, {fails} FAIL")
if fails == 0:
    print("  STATUS: READY FOR DEMO")
elif fails <= 3:
    print("  STATUS: ALMOST READY (fix FAIL items)")
else:
    print("  STATUS: NOT READY")
print("=" * 60)
