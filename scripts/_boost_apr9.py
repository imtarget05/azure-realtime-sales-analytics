"""Add more Apr 9 rows to reach ~$420K revenue target"""
import sys, os, random
sys.path.insert(0, '.')
os.environ['KEY_VAULT_URI'] = 'DISABLED'
from config.settings import SQL_SERVER, SQL_DATABASE, SQL_USERNAME, SQL_PASSWORD, SQL_DRIVER
import pyodbc
from datetime import datetime, timedelta

conn = pyodbc.connect(f'DRIVER={SQL_DRIVER};SERVER={SQL_SERVER};DATABASE={SQL_DATABASE};UID={SQL_USERNAME};PWD={SQL_PASSWORD}', timeout=15, autocommit=True)
cur = conn.cursor()

# Current Apr 9 revenue
cur.execute("SELECT CAST(SUM(revenue) AS INT) FROM SalesTransactions WHERE CAST(event_time AS DATE)='2026-04-09'")
current = cur.fetchone()[0]
target = 420000
gap = target - current
print(f'Current Apr 9: ${current:,}, Target: ${target:,}, Gap: ${gap:,}')

# Get products with moderate prices for realistic fill
cur.execute("SELECT product_id, category, base_price FROM Products WHERE base_price BETWEEN 5 AND 150")
products = cur.fetchall()

stores = ['S01', 'S02', 'S03']
weathers = ['sunny', 'cloudy', 'rainy']

rows = []
rev_sum = 0
while rev_sum < gap:
    p = random.choice(products)
    pid, cat, base = p
    price = round(float(base) * random.uniform(0.85, 1.15), 2)
    units = random.randint(1, 3)
    rev = round(price * units, 2)
    hour = random.choices([8,9,10,11,12,13,14,15,16,17,18,19,20,21,22],
                          weights=[3,5,7,10,14,12,10,9,10,12,12,10,8,6,3], k=1)[0]
    t = datetime(2026, 4, 9, hour, random.randint(0, 59), random.randint(0, 59))
    store = random.choice(stores)
    weather = random.choice(weathers)
    temp = round(random.uniform(25, 35), 1)
    lag = random.randint(0, 4)
    enq = t + timedelta(seconds=lag)
    rows.append((t, store, pid, units, price, rev, temp, weather, 0, cat, enq, lag))
    rev_sum += rev

cur.executemany("""
    INSERT INTO SalesTransactions
    (event_time, store_id, product_id, units_sold, unit_price,
     revenue, temperature, weather, holiday, category,
     enqueued_time, ingest_lag_seconds)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
""", rows)
print(f'Inserted {len(rows)} rows, revenue: ${rev_sum:,.0f}')

# Final
cur.execute("SELECT COUNT(*), CAST(SUM(revenue) AS INT) FROM SalesTransactions WHERE CAST(event_time AS DATE)='2026-04-09'")
r = cur.fetchone()
print(f'Apr 9: {r[0]:,} txns, ${r[1]:,}')

# DoD check
cur.execute("""
    SELECT CAST(event_time AS DATE) as d, CAST(SUM(revenue) AS INT) as rev
    FROM SalesTransactions
    GROUP BY CAST(event_time AS DATE)
    ORDER BY d DESC
""")
prev = None
for r in cur.fetchall():
    if prev:
        dod = (prev[1] - r[1]) / r[1] * 100
        print(f'  {prev[0]}: ${prev[1]:,}  DoD: {dod:+.1f}%')
    prev = r
if prev:
    print(f'  {prev[0]}: ${prev[1]:,}')

cur.execute('SELECT COUNT(*), CAST(SUM(revenue) AS INT) FROM SalesTransactions')
r = cur.fetchone()
print(f'\nGrand total: {r[0]:,} txns  ${r[1]:,}')
conn.close()
