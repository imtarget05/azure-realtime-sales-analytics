import sys, os
sys.path.insert(0, '.')
os.environ['KEY_VAULT_URI'] = 'DISABLED'
from config.settings import SQL_SERVER, SQL_DATABASE, SQL_USERNAME, SQL_PASSWORD, SQL_DRIVER
import pyodbc
conn = pyodbc.connect(f'DRIVER={SQL_DRIVER};SERVER={SQL_SERVER};DATABASE={SQL_DATABASE};UID={SQL_USERNAME};PWD={SQL_PASSWORD}', timeout=15)
cur = conn.cursor()
cur.execute("""
    SELECT DATEPART(HOUR, event_time) AS hr, COUNT(*) AS cnt, 
           CAST(SUM(revenue) AS INT) AS rev
    FROM SalesTransactions
    WHERE CAST(event_time AS DATE) = '2026-04-09'
    GROUP BY DATEPART(HOUR, event_time)
    ORDER BY hr
""")
total_txns = 0
total_rev = 0
for r in cur.fetchall():
    total_txns += r[1]
    total_rev += r[2]
    print(f'  H{r[0]:02d}: {r[1]:>5} txns  ${r[2]:>8,}')
print(f'  Total: {total_txns} txns  ${total_rev:,}')

# Overall
cur.execute('SELECT COUNT(*), CAST(SUM(revenue) AS INT) FROM SalesTransactions')
r = cur.fetchone()
print(f'\n  Grand total: {r[0]:,} txns  ${r[1]:,}')
conn.close()
