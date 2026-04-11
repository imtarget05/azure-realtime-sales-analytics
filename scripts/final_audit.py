"""Final comprehensive audit of the entire project."""
import sys, os, json
sys.path.insert(0, '.')
os.environ.setdefault('KEY_VAULT_URI', 'DISABLED')
from pathlib import Path
from config.settings import SQL_SERVER, SQL_DATABASE, SQL_USERNAME, SQL_PASSWORD, SQL_DRIVER
import pyodbc

issues = []
ok_count = 0

def check(condition, ok_msg, fail_msg):
    global ok_count
    if condition:
        ok_count += 1
        print("  [OK] " + ok_msg)
    else:
        issues.append(fail_msg)
        print("  [!!] " + fail_msg)

conn = pyodbc.connect(
    f"DRIVER={SQL_DRIVER};SERVER={SQL_SERVER};DATABASE={SQL_DATABASE};UID={SQL_USERNAME};PWD={SQL_PASSWORD};TrustServerCertificate=yes",
    timeout=20
)
cur = conn.cursor()

# 1. SQL Data
print("=" * 60)
print("1. SQL DATA INTEGRITY")
print("=" * 60)

cur.execute("SELECT COUNT(*) FROM SalesTransactions")
cnt = cur.fetchone()[0]
check(cnt > 50000, "SalesTransactions: {:,} rows".format(cnt), "SalesTransactions too few: {:,}".format(cnt))

cur.execute("SELECT COUNT(DISTINCT CAST(event_time AS DATE)) FROM SalesTransactions")
days = cur.fetchone()[0]
check(days == 7, "7 days of data", "Expected 7 days, got {}".format(days))

cur.execute("SELECT ROUND(AVG(revenue),2) FROM SalesTransactions")
avg = float(cur.fetchone()[0])
check(10 <= avg <= 60, "Avg order value: ${:.2f}".format(avg), "Avg order unrealistic: ${:.2f}".format(avg))

# DoD
cur.execute("SELECT dod_growth_pct FROM vw_DoDGrowthOverall WHERE date_rank=1")
dod = float(cur.fetchone()[0])
check(-30 <= dod <= 30, "DoD growth: {:.1f}%".format(dod), "DoD extreme: {:.1f}%".format(dod))

# All stores have regions
cur.execute("""
    SELECT COUNT(*) FROM SalesTransactions st
    LEFT JOIN StoreRegions sr ON st.store_id = sr.store_id
    WHERE sr.store_id IS NULL
""")
orphan = cur.fetchone()[0]
check(orphan == 0, "All stores have matching regions", "Orphan stores: {:,}".format(orphan))

# All products match
cur.execute("""
    SELECT COUNT(*) FROM SalesTransactions st
    LEFT JOIN Products p ON st.product_id = p.product_id
    WHERE p.product_id IS NULL
""")
orphan = cur.fetchone()[0]
check(orphan == 0, "All products match Products table", "Orphan products: {:,}".format(orphan))

# Tables exist with data
tables = ['Products','StoreRegions','SalesForecast','SalesAlerts','ModelRegistry','HourlySalesSummary','SecurityMapping','AccessAudit','LatencyBenchmark']
for t in tables:
    try:
        cur.execute(f"SELECT COUNT(*) FROM {t}")
        cnt = cur.fetchone()[0]
        check(cnt > 0, "{}: {:,} rows".format(t, cnt), "{} is EMPTY".format(t))
    except:
        check(False, "", "{} does not exist".format(t))

# Views exist
views = ['vw_DoDGrowthOverall','vw_DoDGrowth','vw_ProductSales','vw_PerformanceMetrics']
for v in views:
    try:
        cur.execute(f"SELECT TOP 1 * FROM {v}")
        r = cur.fetchone()
        check(r is not None, "{} returns data".format(v), "{} is empty".format(v))
    except Exception as e:
        check(False, "", "{} error: {}".format(v, e))

conn.close()

# 2. ML Models
print("\n" + "=" * 60)
print("2. ML MODELS")
print("=" * 60)

rev_model = Path("ml/model_output/revenue_model.pkl")
qty_model = Path("ml/model_output/quantity_model.pkl")
meta_file = Path("ml/model_output/model_metadata.json")
enc_file = Path("ml/model_output/label_encoders.pkl")

check(rev_model.exists(), "revenue_model.pkl exists ({:.1f} KB)".format(rev_model.stat().st_size/1024), "revenue_model.pkl MISSING")
check(qty_model.exists(), "quantity_model.pkl exists ({:.1f} KB)".format(qty_model.stat().st_size/1024), "quantity_model.pkl MISSING")
check(meta_file.exists(), "model_metadata.json exists", "model_metadata.json MISSING")
check(enc_file.exists(), "label_encoders.pkl exists", "label_encoders.pkl MISSING")

if meta_file.exists():
    with open(meta_file) as f:
        meta = json.load(f)
    ver = meta.get("model_version", "?")
    check(True, "Model version: {}".format(ver), "")
    rev_metrics = meta.get("revenue_metrics", {})
    r2 = rev_metrics.get("r2")
    rmse = rev_metrics.get("rmse")
    if r2:
        check(r2 > 0.5, "Revenue R2: {:.3f}".format(r2), "Revenue R2 too low: {:.3f}".format(r2))
    elif rmse:
        check(rmse < 100, "Revenue RMSE: {:.2f}".format(rmse), "Revenue RMSE too high: {:.2f}".format(rmse))

# Check sklearn version match
try:
    import sklearn
    current_sklearn = sklearn.__version__
    trained_sklearn = meta.get("sklearn_version", "unknown")
    check(trained_sklearn == current_sklearn or trained_sklearn == "unknown",
          "sklearn version: {} (trained with {})".format(current_sklearn, trained_sklearn),
          "sklearn MISMATCH: current={} trained={}".format(current_sklearn, trained_sklearn))
except:
    pass

# 3. Webapp
print("\n" + "=" * 60)
print("3. WEBAPP")
print("=" * 60)

check(Path("webapp/app.py").exists(), "webapp/app.py exists", "webapp/app.py MISSING")
check(Path("webapp/templates/index.html").exists(), "index.html template exists", "index.html MISSING")
check(Path("webapp/templates/result.html").exists(), "result.html template exists", "result.html MISSING")
check(Path("webapp/templates/dashboard.html").exists(), "dashboard.html template exists", "dashboard.html MISSING")
check(Path("webapp/templates/model_report.html").exists(), "model_report.html template exists", "model_report.html MISSING")

# Test import
try:
    from webapp.app import app
    routes = [str(r) for r in app.url_map.iter_rules()]
    expected_routes = ['/predict', '/api/predict', '/api/health', '/dashboard', '/model-report']
    for er in expected_routes:
        check(er in routes, "Route {} exists".format(er), "Route {} MISSING".format(er))
except Exception as e:
    check(False, "", "Webapp import failed: {}".format(e))

# 4. MLOps
print("\n" + "=" * 60)
print("4. MLOPS")
print("=" * 60)

mlops_files = ['mlops/deploy_to_endpoint.py', 'mlops/local_first_pipeline.py', 'mlops/model_registry.py', 'mlops/trigger_training_pipeline.py']
for f in mlops_files:
    check(Path(f).exists(), "{} exists".format(f), "{} MISSING".format(f))

# 5. Power BI
print("\n" + "=" * 60)
print("5. POWER BI FILES")
print("=" * 60)

pbi_files = ['powerbi/dax_measures_sqlserver.dax', 'powerbi/POWERBI_DASHBOARD_GUIDE.md', 'powerbi/rls_config.dax', 'powerbi/semantic_model.json']
for f in pbi_files:
    check(Path(f).exists(), "{} exists".format(f), "{} MISSING".format(f))

# 6. Demo Scenarios
print("\n" + "=" * 60)
print("6. DEMO SCENARIOS")
print("=" * 60)

check(Path("scripts/demo_scenarios.py").exists(), "demo_scenarios.py exists", "demo_scenarios.py MISSING")

# 7. Tests
print("\n" + "=" * 60)
print("7. TEST FILES")
print("=" * 60)

test_files = list(Path("tests").glob("test_*.py"))
check(len(test_files) >= 5, "{} test files found".format(len(test_files)), "Only {} test files".format(len(test_files)))

# SUMMARY
print("\n" + "=" * 60)
print("AUDIT SUMMARY")
print("=" * 60)
print("  Passed: {}".format(ok_count))
print("  Issues: {}".format(len(issues)))
if issues:
    print("\n  ISSUES:")
    for i, issue in enumerate(issues, 1):
        print("    {}. {}".format(i, issue))
else:
    print("\n  ALL CHECKS PASSED!")
