import sys, os
sys.path.insert(0, '.')
os.environ.setdefault('KEY_VAULT_URI', 'DISABLED')
from config.settings import SQL_SERVER, SQL_DATABASE, SQL_USERNAME, SQL_PASSWORD, SQL_DRIVER
import pyodbc
conn = pyodbc.connect(f'DRIVER={SQL_DRIVER};SERVER={SQL_SERVER};DATABASE={SQL_DATABASE};UID={SQL_USERNAME};PWD={SQL_PASSWORD};TrustServerCertificate=yes', timeout=10)
cur = conn.cursor()
cur.execute("""
    SELECT CAST(event_time AS DATE) as dt, COUNT(*) as cnt, ROUND(SUM(revenue),0) as rev
    FROM SalesTransactions GROUP BY CAST(event_time AS DATE) ORDER BY dt DESC
""")
rows = cur.fetchall()
prev = None
total = 0
for r in rows:
    dod = f"{(r.rev-prev)/prev*100:+.1f}%" if prev else "(today/latest)"
    print(f"  {r.dt}: {r.cnt:6,} txns  ${r.rev:>10,.0f}  {dod}")
    prev = r.rev
    total += r.rev
print(f"  TOTAL: ${total:,.0f}")
conn.close()
