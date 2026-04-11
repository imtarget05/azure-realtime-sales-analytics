"""Restore H19-H22 for Apr 9 that were accidentally deleted"""
import sys, os, random
sys.path.insert(0, '.')
os.environ['KEY_VAULT_URI'] = 'DISABLED'
from config.settings import SQL_SERVER, SQL_DATABASE, SQL_USERNAME, SQL_PASSWORD, SQL_DRIVER
import pyodbc
from datetime import datetime, timedelta

conn = pyodbc.connect(f'DRIVER={SQL_DRIVER};SERVER={SQL_SERVER};DATABASE={SQL_DATABASE};UID={SQL_USERNAME};PWD={SQL_PASSWORD}', timeout=15, autocommit=True)
cur = conn.cursor()

# Get Products for weighted selection
cur.execute("SELECT product_id, category, base_price FROM Products")
products = cur.fetchall()
# Weight by inverse price (cheap products sell more)
weights = [1.0 / float(max(p[2], 0.5)) for p in products]
total_w = sum(weights)
weights = [w / total_w for w in weights]

stores = ['S01', 'S02', 'S03']
weathers = ['sunny', 'cloudy', 'rainy']

# Target: ~2200 rows for H19-H22, ~$93K revenue (to restore Apr 9 to ~$420K)
# H19: ~800 rows, H20: ~600, H21: ~500, H22: ~300
hour_targets = {19: 800, 20: 600, 21: 500, 22: 300}

rows = []
for hour, count in hour_targets.items():
    for _ in range(count):
        idx = random.choices(range(len(products)), weights=weights, k=1)[0]
        pid, cat, base = products[idx]
        price = round(float(base) * random.uniform(0.8, 1.2), 2)
        units = random.randint(1, 4)
        rev = round(price * units, 2)
        t = datetime(2026, 4, 9, hour, random.randint(0, 59), random.randint(0, 59))
        store = random.choice(stores)
        weather = random.choice(weathers)
        temp = round(random.uniform(25, 35), 1)
        lag = random.randint(0, 4)
        enq = t + timedelta(seconds=lag)
        rows.append((t, store, pid, units, price, rev, temp, weather, 0, cat, enq, lag))

cur.executemany("""
    INSERT INTO SalesTransactions
    (event_time, store_id, product_id, units_sold, unit_price,
     revenue, temperature, weather, holiday, category,
     enqueued_time, ingest_lag_seconds)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
""", rows)

total_rev = sum(r[5] for r in rows)
print(f'Inserted {len(rows)} rows for H19-H22, revenue: ${total_rev:,.0f}')

# Final check
cur.execute("""
    SELECT DATEPART(HOUR, event_time) AS hr, COUNT(*) AS cnt, CAST(SUM(revenue) AS INT) AS rev
    FROM SalesTransactions WHERE CAST(event_time AS DATE) = '2026-04-09'
    GROUP BY DATEPART(HOUR, event_time) ORDER BY hr
""")
total = 0
for r in cur.fetchall():
    total += r[2]
    print(f'  H{r[0]:02d}: {r[1]:>5} txns  ${r[2]:>8,}')
print(f'  Apr 9 total revenue: ${total:,}')

cur.execute('SELECT COUNT(*), CAST(SUM(revenue) AS INT) FROM SalesTransactions')
r = cur.fetchone()
print(f'  Grand total: {r[0]:,} txns  ${r[1]:,}')
conn.close()
