"""Comprehensive audit: SQL data, DAX measures, MLOps, webapp state"""
import pyodbc, os, json, sys
from pathlib import Path

# --- Connection ---
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
os.environ.setdefault('KEY_VAULT_URI', 'DISABLED')
from config.settings import SQL_SERVER, SQL_DATABASE, SQL_USERNAME, SQL_PASSWORD, SQL_DRIVER

SERVER = SQL_SERVER
DATABASE = SQL_DATABASE
USERNAME = SQL_USERNAME
PASSWORD = SQL_PASSWORD
DRIVER = SQL_DRIVER  # {ODBC Driver 18 for SQL Server}

try:
    conn = pyodbc.connect(
        f"DRIVER={DRIVER};SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD};TrustServerCertificate=yes",
        timeout=15
    )
    cur = conn.cursor()
    print("✅ SQL Connection OK\n")
except Exception as e:
    print(f"❌ SQL Connection FAILED: {e}")
    sys.exit(1)

# ================================================================
print("=" * 60)
print("1. DAILY REVENUE (DoD check)")
print("=" * 60)
cur.execute("""
    SELECT 
        CAST(event_time AS DATE) as dt,
        COUNT(*) as txns,
        ROUND(SUM(revenue),0) as revenue
    FROM SalesTransactions 
    GROUP BY CAST(event_time AS DATE) 
    ORDER BY dt DESC
""")
rows = cur.fetchall()
prev_rev = None
for r in rows:
    if prev_rev and prev_rev > 0:
        dod = (r.revenue - prev_rev) / prev_rev * 100
        print(f"  {r.dt}: {r.txns:6,} txns  ${r.revenue:>10,.0f}  DoD: {dod:+.1f}%")
    else:
        print(f"  {r.dt}: {r.txns:6,} txns  ${r.revenue:>10,.0f}  (baseline)")
    prev_rev = r.revenue

# Check Power BI "today vs yesterday" based on MAX date in data
if rows:
    today_rev = rows[0].revenue
    yesterday_rev = rows[1].revenue if len(rows) > 1 else 0
    dod = (today_rev - yesterday_rev) / yesterday_rev * 100 if yesterday_rev else 0
    print(f"\n  Power BI DoD card should show: {dod:+.1f}%")
    if abs(dod) > 50:
        print(f"  ⚠️  DoD is {dod:.1f}% → check if today's data is realistic vs screenshot 106.53%")

# ================================================================
print("\n" + "=" * 60)
print("2. AVERAGE ORDER VALUE + KEY METRICS")
print("=" * 60)
cur.execute("""
    SELECT 
        COUNT(*) as total_txns,
        ROUND(SUM(revenue),0) as total_revenue,
        ROUND(AVG(revenue),2) as avg_order_value,
        COUNT(DISTINCT store_id) as stores,
        COUNT(DISTINCT product_id) as products,
        MIN(CAST(event_time AS DATE)) as min_date,
        MAX(CAST(event_time AS DATE)) as max_date
    FROM SalesTransactions
""")
r = cur.fetchone()
print(f"  Total txns:     {r.total_txns:,}")
print(f"  Total revenue:  ${r.total_revenue:,.0f}")
print(f"  Avg Order Value: ${r.avg_order_value:.2f}")
print(f"  Stores: {r.stores}, Products: {r.products}")
print(f"  Date range: {r.min_date} → {r.max_date}")

if r.avg_order_value < 5 or r.avg_order_value > 1000:
    print(f"  ⚠️  Avg Order Value ${r.avg_order_value:.2f} may be out of range")
else:
    print(f"  ✅ Avg Order Value looks realistic")

# ================================================================
print("\n" + "=" * 60)
print("3. VIEWS STATUS")
print("=" * 60)
views = ['vw_DoDGrowthOverall', 'vw_DoDGrowth', 'vw_ProductSales', 'vw_PerformanceMetrics']
for v in views:
    try:
        cur.execute(f"SELECT TOP 3 * FROM {v}")
        rows_v = cur.fetchall()
        cols = [col[0] for col in cur.description]
        print(f"  ✅ {v}: {len(rows_v)} rows, cols: {cols}")
        for rv in rows_v:
            print(f"     {list(rv)}")
    except Exception as e:
        print(f"  ❌ {v}: FAILED - {e}")

# ================================================================
print("\n" + "=" * 60)
print("4. ALL TABLES ROW COUNTS")
print("=" * 60)
tables = ['SalesTransactions','Products','StoreRegions','SalesForecast',
          'SalesAlerts','ModelRegistry','HourlySalesSummary',
          'SecurityMapping','AccessAudit','LatencyBenchmark']
for t in tables:
    try:
        cur.execute(f"SELECT COUNT(*) FROM {t}")
        cnt = cur.fetchone()[0]
        status = "✅" if cnt > 0 else "⚠️  EMPTY"
        print(f"  {status} {t}: {cnt:,} rows")
    except Exception as e:
        print(f"  ❌ {t}: FAILED - {e}")

# ================================================================
print("\n" + "=" * 60)
print("5. SALESTRANSACTIONS COLUMN CHECK")
print("=" * 60)
cur.execute("SELECT TOP 1 * FROM SalesTransactions")
cols = [col[0] for col in cur.description]
print(f"  Columns: {cols}")
expected = ['transaction_id','store_id','product_id','event_time','quantity','revenue']
for col in expected:
    if col in cols:
        print(f"  ✅ {col}")
    else:
        print(f"  ❌ MISSING: {col}")

# ================================================================
print("\n" + "=" * 60)
print("6. SALESALERTS COLUMN CHECK")
print("=" * 60)
cur.execute("SELECT TOP 1 * FROM SalesAlerts")
cols_alerts = [col[0] for col in cur.description]
print(f"  Columns: {cols_alerts}")

# ================================================================
print("\n" + "=" * 60)
print("7. PRODUCTS TABLE CHECK")
print("=" * 60)
cur.execute("SELECT TOP 5 product_id, product_name, category, unit_price FROM Products ORDER BY product_id")
for r in cur.fetchall():
    print(f"  {r[0]}: {r[1]} ({r[2]}) @ ${r[3]:.2f}")

# ================================================================
print("\n" + "=" * 60)
print("8. STOREREGIONS CHECK")
print("=" * 60)
cur.execute("SELECT * FROM StoreRegions")
for r in cur.fetchall():
    print(f"  {list(r)}")

# ================================================================
print("\n" + "=" * 60)
print("9. SALESFORECAST SAMPLE")
print("=" * 60)
cur.execute("""
    SELECT TOP 5 store_id, CAST(forecast_date AS DATE) as dt, 
           ROUND(predicted_revenue,0) as pred_rev, ROUND(actual_revenue,0) as act_rev,
           ROUND(ABS(predicted_revenue-actual_revenue)/NULLIF(actual_revenue,0)*100,1) as mape
    FROM SalesForecast 
    ORDER BY forecast_date DESC
""")
for r in cur.fetchall():
    print(f"  {r[0]}: {r[1]}  pred=${r[2]:,.0f}  act=${r[3]:,.0f}  MAPE={r[4]}%")

# ================================================================
print("\n" + "=" * 60)
print("10. MODELREGISTRY SAMPLE")
print("=" * 60)
cur.execute("SELECT TOP 5 * FROM ModelRegistry")
cols_mr = [col[0] for col in cur.description]
print(f"  Columns: {cols_mr}")
for r in cur.fetchall():
    print(f"  {list(r)}")

conn.close()

# ================================================================
print("\n" + "=" * 60)
print("11. ML MODEL FILES")
print("=" * 60)
ml_files = [
    'ml/model_output/revenue_model.pkl',
    'ml/model_output/quantity_model.pkl',
    'ml/model_output/model_metrics.json',
]
for f in ml_files:
    p = Path(f)
    if p.exists():
        print(f"  ✅ {f} ({p.stat().st_size/1024:.1f} KB)")
    else:
        print(f"  ❌ MISSING: {f}")

# ================================================================
print("\n" + "=" * 60)
print("12. WEBAPP FILES")
print("=" * 60)
webapp_files = list(Path('webapp').rglob('*.py'))
for f in webapp_files:
    print(f"  📄 {f}")

# ================================================================
print("\n" + "=" * 60)
print("13. MLOPS FILES")
print("=" * 60)
mlops_files = list(Path('mlops').rglob('*.py'))
for f in mlops_files:
    print(f"  📄 {f}")

print("\n" + "=" * 60)
print("AUDIT COMPLETE")
print("=" * 60)
