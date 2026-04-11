import sys, os
sys.path.insert(0, '.')
os.environ['KEY_VAULT_URI'] = 'DISABLED'
from config.settings import SQL_SERVER, SQL_DATABASE, SQL_USERNAME, SQL_PASSWORD, SQL_DRIVER
import pyodbc

conn = pyodbc.connect(f'DRIVER={SQL_DRIVER};SERVER={SQL_SERVER};DATABASE={SQL_DATABASE};UID={SQL_USERNAME};PWD={SQL_PASSWORD}')
cur = conn.cursor()

# Check current columns
cur.execute("SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='SalesTransactions' ORDER BY ORDINAL_POSITION")
print('=== Current SalesTransactions columns ===')
for r in cur.fetchall():
    print(f'  {r[0]} ({r[1]})')

# Check if product_id exists
cur.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='SalesTransactions' AND COLUMN_NAME='product_id'")
has_pid = cur.fetchone()[0]
print(f'\nproduct_id column exists: {has_pid}')

if has_pid:
    cur.execute("SELECT TOP 5 id, product_id, category, unit_price, revenue FROM SalesTransactions")
    for r in cur.fetchall():
        print(r)
else:
    # Check if category still exists (we can recover product_id from it)
    cur.execute("SELECT TOP 5 id, category, unit_price, revenue FROM SalesTransactions")
    print('\nSample rows (no product_id):')
    for r in cur.fetchall():
        print(r)
    
    # Check Products table
    cur.execute("SELECT product_id, product_name, category, base_price FROM Products ORDER BY product_id")
    print('\n=== Products table ===')
    for r in cur.fetchall():
        print(f'  {r[0]} | {r[1]} | {r[2]} | ${r[3]}')

    # Count rows
    cur.execute("SELECT COUNT(*) FROM SalesTransactions")
    print(f'\nTotal SalesTransactions rows: {cur.fetchone()[0]}')

conn.close()
