"""Quick SQL health check for demo."""
import pyodbc, os
from dotenv import load_dotenv
load_dotenv()

conn = pyodbc.connect(
    f'DRIVER={{ODBC Driver 18 for SQL Server}};'
    f'SERVER={os.getenv("SQL_SERVER")};'
    f'DATABASE={os.getenv("SQL_DATABASE")};'
    f'UID={os.getenv("SQL_USERNAME")};'
    f'PWD={os.getenv("SQL_PASSWORD")};'
    f'Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30'
)
cur = conn.cursor()

print("=" * 60)
print("  SQL DATA STATUS")
print("=" * 60)

tables = ['SalesTransactions', 'HourlySalesSummary', 'SalesAlerts', 'SalesForecast']
for t in tables:
    cur.execute(f'SELECT COUNT(*) FROM dbo.{t}')
    cnt = cur.fetchone()[0]
    status = "OK" if cnt > 0 else "EMPTY"
    print(f"  [{status:5}] {t:25} = {cnt:>8} rows")

print("-" * 60)

cur.execute('SELECT TOP 1 event_time FROM dbo.SalesTransactions ORDER BY event_time DESC')
r = cur.fetchone()
print(f"  Latest Transaction : {r[0] if r else 'NONE'}")

cur.execute('SELECT TOP 1 alert_time FROM dbo.SalesAlerts ORDER BY alert_time DESC')
r = cur.fetchone()
print(f"  Latest Alert       : {r[0] if r else 'NONE'}")

cur.execute('SELECT TOP 1 window_end FROM dbo.HourlySalesSummary ORDER BY window_end DESC')
r = cur.fetchone()
print(f"  Latest Summary     : {r[0] if r else 'NONE'}")

# Check views
for v in ['vw_RealtimeDashboard', 'vw_ForecastVsActual']:
    try:
        cur.execute(f'SELECT TOP 1 * FROM dbo.{v}')
        print(f"  View {v}: EXISTS")
    except:
        print(f"  View {v}: MISSING")

# Sample latest 3 transactions
print("-" * 60)
print("  Latest 3 transactions:")
cur.execute('SELECT TOP 3 event_time, store_id, product_id, revenue, category FROM dbo.SalesTransactions ORDER BY event_time DESC')
for row in cur.fetchall():
    print(f"    {row[0]} | {row[1]} | {row[2]:8} | ${row[3]:>8.2f} | {row[4]}")

# Sample latest 3 alerts  
print("  Latest 3 alerts:")
cur.execute('SELECT TOP 3 alert_time, store_id, type, value FROM dbo.SalesAlerts ORDER BY alert_time DESC')
for row in cur.fetchall():
    print(f"    {row[0]} | {row[1]} | {row[2]:6} | ${row[3]:>8.2f}")

print("=" * 60)
conn.close()
