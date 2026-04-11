import sys, os
sys.path.insert(0, '.')
os.environ['KEY_VAULT_URI'] = 'DISABLED'
from config.settings import SQL_SERVER, SQL_DATABASE, SQL_USERNAME, SQL_PASSWORD, SQL_DRIVER
import pyodbc

conn = pyodbc.connect(f'DRIVER={SQL_DRIVER};SERVER={SQL_SERVER};DATABASE={SQL_DATABASE};UID={SQL_USERNAME};PWD={SQL_PASSWORD}')
cur = conn.cursor()

# Check distinct product_id values in SalesTransactions
cur.execute("SELECT DISTINCT product_id, COUNT(*) as cnt FROM SalesTransactions GROUP BY product_id ORDER BY product_id")
print('=== product_id values in SalesTransactions ===')
for r in cur.fetchall():
    print(f'  {r[0]:20s} -> {r[1]} rows')

# Check Products table
print('\n=== Products table ===')
cur.execute("SELECT product_id, product_name, category, base_price FROM Products ORDER BY product_id")
for r in cur.fetchall():
    print(f'  {r[0]:10s} | {r[1]:20s} | {r[2]:15s} | ${r[3]}')

conn.close()
