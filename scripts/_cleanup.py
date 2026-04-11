import sys, os
sys.path.insert(0, '.')
os.environ['KEY_VAULT_URI'] = 'DISABLED'
from config.settings import SQL_SERVER, SQL_DATABASE, SQL_USERNAME, SQL_PASSWORD, SQL_DRIVER
import pyodbc
conn = pyodbc.connect(f'DRIVER={SQL_DRIVER};SERVER={SQL_SERVER};DATABASE={SQL_DATABASE};UID={SQL_USERNAME};PWD={SQL_PASSWORD}', timeout=15, autocommit=True)
cur = conn.cursor()
cur.execute('SELECT COUNT(*) FROM SalesTransactions')
before = cur.fetchone()[0]
cur.execute("DELETE FROM SalesTransactions WHERE event_time >= '2026-04-09 19:00:00'")
deleted = cur.rowcount
cur.execute('SELECT COUNT(*) FROM SalesTransactions')
after = cur.fetchone()[0]
cur.execute('SELECT CAST(SUM(revenue) AS INT) FROM SalesTransactions')
rev = cur.fetchone()[0]
print(f'Before: {before:,} | Deleted: {deleted:,} | After: {after:,} | Revenue: ${rev:,}')
conn.close()
