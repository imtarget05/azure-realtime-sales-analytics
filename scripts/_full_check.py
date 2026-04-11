"""Full system health check for demo readiness"""
import sys, os, json
sys.path.insert(0, '.')
os.environ['KEY_VAULT_URI'] = 'DISABLED'
from pathlib import Path
ROOT = Path(__file__).resolve().parent.parent

passed = 0
failed = 0
warns = 0
issues = []

def ok(msg):
    global passed; passed += 1; print(f'  [OK]   {msg}')
def fail(msg):
    global failed; failed += 1; issues.append(msg); print(f'  [FAIL] {msg}')
def warn(msg):
    global warns; warns += 1; print(f'  [WARN] {msg}')

# 1. SQL SERVER
print('=' * 70)
print('  1. SQL SERVER')
print('=' * 70)
try:
    from config.settings import SQL_SERVER, SQL_DATABASE, SQL_USERNAME, SQL_PASSWORD, SQL_DRIVER
    import pyodbc
    conn = pyodbc.connect(
        f'DRIVER={SQL_DRIVER};SERVER={SQL_SERVER};DATABASE={SQL_DATABASE};UID={SQL_USERNAME};PWD={SQL_PASSWORD}',
        timeout=15
    )
    cur = conn.cursor()
    ok(f'Connected to {SQL_SERVER}')

    tables = {
        'SalesTransactions': 80000, 'Products': 30, 'StoreRegions': 3,
        'SalesForecast': 1000, 'SalesAlerts': 1000, 'ModelRegistry': 1,
        'HourlySalesSummary': 1000, 'SecurityMapping': 3, 'AccessAudit': 10,
        'LatencyBenchmark': 3,
    }
    for t, min_rows in tables.items():
        cur.execute(f'SELECT COUNT(*) FROM {t}')
        cnt = cur.fetchone()[0]
        if cnt >= min_rows:
            ok(f'{t}: {cnt:,} rows')
        elif cnt > 0:
            warn(f'{t}: {cnt:,} rows (expected >= {min_rows})')
        else:
            fail(f'{t}: EMPTY')

    # Views used by demo_scenarios
    required_views = [
        'vw_RealtimeDashboard', 'vw_PerformanceMetrics',
        'vw_DailyRevenueTrend', 'vw_DoDGrowth', 'vw_SalesByRegion',
        'vw_ProductSales', 'vw_ForecastAccuracy', 'vw_AlertSummary',
        'vw_RegionalSummary', 'vw_HourlyTrend',
    ]
    for v in required_views:
        try:
            cur.execute(f'SELECT TOP 1 * FROM {v}')
            cur.fetchone()
            ok(f'View {v}')
        except Exception:
            fail(f'View {v}: missing')

    # Data integrity
    cur.execute('SELECT COUNT(*) FROM SalesTransactions s LEFT JOIN Products p ON s.product_id=p.product_id WHERE p.product_id IS NULL')
    orphans = cur.fetchone()[0]
    if orphans == 0: ok('No orphan product_ids')
    else: fail(f'{orphans} orphan product_ids')

    cur.execute('SELECT COUNT(*) FROM SalesTransactions s LEFT JOIN StoreRegions r ON s.store_id=r.store_id WHERE r.store_id IS NULL')
    orphans2 = cur.fetchone()[0]
    if orphans2 == 0: ok('No orphan store_ids')
    else: fail(f'{orphans2} orphan store_ids')

    conn.close()
except Exception as e:
    fail(f'SQL Server: {e}')

# 2. DATABRICKS
print()
print('=' * 70)
print('  2. DATABRICKS')
print('=' * 70)
try:
    from config.settings import DATABRICKS_HOST, DATABRICKS_TOKEN
    has_host = bool(DATABRICKS_HOST and not str(DATABRICKS_HOST).startswith('<'))
    has_token = bool(DATABRICKS_TOKEN and len(str(DATABRICKS_TOKEN)) > 10 and not str(DATABRICKS_TOKEN).startswith('<'))
    if has_host: ok(f'Host: {DATABRICKS_HOST}')
    else: fail('DATABRICKS_HOST not configured')
    if has_token: ok('Token configured')
    else: fail('DATABRICKS_TOKEN not configured')

    if has_host and has_token:
        import requests
        headers = {'Authorization': f'Bearer {DATABRICKS_TOKEN}'}
        r = requests.get(f'{DATABRICKS_HOST}/api/2.1/jobs/list', headers=headers, timeout=10, params={'limit': 5})
        if r.status_code == 200:
            jobs = r.json().get('jobs', [])
            ok(f'API reachable, {len(jobs)} jobs')
            for j in jobs[:3]:
                jid = j.get('job_id', '?')
                jname = j.get('settings', {}).get('name', '?')
                print(f'         Job {jid}: {jname}')
        else:
            warn(f'API status {r.status_code}')

        r2 = requests.get(f'{DATABRICKS_HOST}/api/2.0/clusters/list', headers=headers, timeout=10)
        if r2.status_code == 200:
            clusters = r2.json().get('clusters', [])
            if not clusters:
                warn('No clusters found')
            for c in clusters:
                cname = c.get('cluster_name', '?')
                cstate = c.get('state', '?')
                if cstate == 'RUNNING': ok(f'Cluster {cname}: {cstate}')
                else: warn(f'Cluster {cname}: {cstate}')
except Exception as e:
    warn(f'Databricks: {e}')

# 3. ML MODELS
print()
print('=' * 70)
print('  3. ML MODELS')
print('=' * 70)
model_files = {
    'revenue_model.pkl': ROOT / 'ml' / 'model_output' / 'revenue_model.pkl',
    'quantity_model.pkl': ROOT / 'ml' / 'model_output' / 'quantity_model.pkl',
    'model_metadata.json': ROOT / 'ml' / 'model_output' / 'model_metadata.json',
    'label_encoders.pkl': ROOT / 'ml' / 'model_output' / 'label_encoders.pkl',
}
for name, path in model_files.items():
    if path.exists():
        sz = path.stat().st_size / 1024
        ok(f'{name}: {sz:.1f} KB')
    else:
        fail(f'{name}: NOT FOUND')

meta_path = ROOT / 'ml' / 'model_output' / 'model_metadata.json'
if meta_path.exists():
    with open(meta_path) as f:
        meta = json.load(f)
    print(f'         Version: {meta.get("model_version", "?")}')
    print(f'         Revenue R2: {meta.get("revenue_r2", "?")}')
    print(f'         sklearn: {meta.get("sklearn_version", "?")}')

try:
    import joblib, sklearn
    model = joblib.load(str(ROOT / 'ml' / 'model_output' / 'revenue_model.pkl'))
    ok(f'Model loads: {type(model).__name__}')
    model_skv = meta.get('sklearn_version', '')
    current_skv = sklearn.__version__
    if model_skv == current_skv: ok(f'sklearn match: {current_skv}')
    else: warn(f'sklearn mismatch: model={model_skv} vs current={current_skv}')
except Exception as e:
    fail(f'Model load: {e}')

# 4. WEBAPP
print()
print('=' * 70)
print('  4. WEBAPP')
print('=' * 70)
webapp_files = [
    'webapp/app.py', 'webapp/requirements.txt',
    'webapp/templates/index.html', 'webapp/templates/dashboard.html',
    'webapp/templates/forecast.html', 'webapp/templates/alerts.html',
]
for f in webapp_files:
    if os.path.exists(f): ok(f)
    else: fail(f'{f} missing')

try:
    import py_compile
    py_compile.compile(str(ROOT / 'webapp' / 'app.py'), doraise=True)
    ok('app.py compiles OK')
except Exception as e:
    fail(f'app.py compile: {e}')

# 5. MLOPS
print()
print('=' * 70)
print('  5. MLOPS')
print('=' * 70)
mlops_files = [
    'mlops/trigger_training_pipeline.py', 'mlops/model_registry.py',
    'mlops/deploy_to_endpoint.py', 'mlops/local_first_pipeline.py',
    'ml/train_model.py', 'ml/retrain_and_compare.py',
    'ml/drift_monitor.py', 'ml/deploy_model.py',
    'ml/score.py', 'ml/realtime_forecast.py',
]
for f in mlops_files:
    if os.path.exists(f): ok(f)
    else: fail(f'{f} missing')

# 6. POWER BI
print()
print('=' * 70)
print('  6. POWER BI')
print('=' * 70)
pbi_files = [
    'powerbi/dax_measures_sqlserver.dax', 'powerbi/POWERBI_DASHBOARD_GUIDE.md',
    'powerbi/rls_config.dax', 'powerbi/semantic_model.json',
    'powerbi/dashboard_layout.json', 'powerbi/push_to_powerbi.py',
]
for f in pbi_files:
    if os.path.exists(f): ok(f)
    else: fail(f'{f} missing')

# 7. DEMO & TESTS
print()
print('=' * 70)
print('  7. DEMO & TESTS')
print('=' * 70)
for f in ['scripts/demo_scenarios.py', 'scripts/simulate_drift.py']:
    if os.path.exists(f): ok(f)
    else: fail(f'{f} missing')

test_count = len(list(Path('tests').glob('test_*.py')))
if test_count >= 10: ok(f'{test_count} test files')
else: warn(f'Only {test_count} test files')

# 8. INFRASTRUCTURE
print()
print('=' * 70)
print('  8. INFRASTRUCTURE')
print('=' * 70)
for f in ['terraform/main.tf', 'terraform/variables.tf', 'terraform/terraform.tfvars',
          'infrastructure/deploy_azure.ps1', 'infrastructure/deploy_azure.sh',
          'stream_analytics/stream_query.sql', 'Dockerfile']:
    if os.path.exists(f): ok(f)
    else: fail(f'{f} missing')

# SUMMARY
print()
print('=' * 70)
total = passed + failed + warns
print(f'  RESULT: {passed} OK / {warns} WARN / {failed} FAIL (total {total})')
print('=' * 70)
if issues:
    print('  ISSUES TO FIX:')
    for i, issue in enumerate(issues, 1):
        print(f'    {i}. {issue}')
else:
    print('  ALL CHECKS PASSED!')
print()
