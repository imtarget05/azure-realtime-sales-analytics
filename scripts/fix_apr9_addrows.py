"""Add rows to Apr 9 to bring from $253K to $420K target"""
import sys, os, random
from pathlib import Path
from datetime import datetime, timedelta
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
os.environ.setdefault('KEY_VAULT_URI', 'DISABLED')
from config.settings import SQL_SERVER, SQL_DATABASE, SQL_USERNAME, SQL_PASSWORD, SQL_DRIVER
import pyodbc

conn = pyodbc.connect(
    f"DRIVER={SQL_DRIVER};SERVER={SQL_SERVER};DATABASE={SQL_DATABASE};UID={SQL_USERNAME};PWD={SQL_PASSWORD};TrustServerCertificate=yes",
    timeout=30
)
conn.autocommit = False
cur = conn.cursor()

# Check current Apr 9
cur.execute("SELECT COUNT(*), ROUND(SUM(revenue),0) FROM SalesTransactions WHERE CAST(event_time AS DATE) = '2026-04-09'")
r = cur.fetchone()
current_count, current_rev = r[0], float(r[1])
print(f"Apr 9 current: {current_count:,} txns  ${current_rev:,.0f}")

TARGET_REV = 420_000
needed_rev = TARGET_REV - current_rev
print(f"Need to add: ${needed_rev:,.0f}")

# Get products info
cur.execute("SELECT product_id, category FROM Products")
products = [(r[0], r[1]) for r in cur.fetchall()]
print(f"Products available: {len(products)}")

# Price tiers per category (realistic)
PRICE_TIERS = {
    'Beverage': (15, 45),
    'Snack': (8, 25),
    'Fresh': (20, 80),
    'Dairy': (12, 35),
    'Bakery': (5, 20),
    'Frozen': (25, 70),
    'Grocery': (10, 40),
    'Seafood': (40, 120),
    'Meat': (50, 150),
}

stores = ['S01', 'S02', 'S03']
store_dist = {'S01': 0.35, 'S02': 0.33, 'S03': 0.32}  # proportional
weathers = ['Clear', 'Cloudy', 'Light Rain', 'Sunny']
hours_dist = list(range(8, 23))  # 8am to 10pm

random.seed(42)
rows_to_insert = []
cumulative_rev = 0.0

# Estimate avg revenue per txn to determine how many to generate
avg_per_txn = 38  # realistic avg for our product mix
estimated_txns = int(needed_rev / avg_per_txn) + 200  # buffer

for _ in range(estimated_txns * 2):  # generate 2x, stop when target met
    if cumulative_rev >= needed_rev:
        break
    
    store_id = random.choices(stores, weights=[store_dist[s] for s in stores])[0]
    prod_id, category = random.choice(products)
    
    lo, hi = PRICE_TIERS.get(category, (10, 50))
    unit_price = round(random.uniform(lo, hi), 2)
    qty = random.randint(1, 4)
    revenue = round(unit_price * qty, 2)
    
    # Random hour weighted toward midday
    hour_weights = [0.3, 1.0, 2.0, 3.0, 4.5, 5.5, 6.0, 6.5, 6.0, 5.5, 4.5, 3.5, 2.5, 1.5, 0.8]
    hour = random.choices(hours_dist, weights=hour_weights)[0]
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    event_time = datetime(2026, 4, 9, hour, minute, second) + timedelta(microseconds=random.randint(0, 999999))
    ingest_lag = round(random.uniform(0.1, 5.0), 2)
    enqueued_time = event_time + timedelta(seconds=ingest_lag)
    temperature = round(random.uniform(25, 38), 1)
    weather = random.choice(weathers)
    holiday = 0
    
    rows_to_insert.append((
        event_time, store_id, prod_id, qty, unit_price, revenue,
        temperature, weather, holiday, category,
        enqueued_time, ingest_lag
    ))
    cumulative_rev += revenue

print(f"Generated {len(rows_to_insert)} rows  (${cumulative_rev:,.0f})")

# Insert in batches
batch_size = 500
inserted = 0
for i in range(0, len(rows_to_insert), batch_size):
    batch = rows_to_insert[i:i+batch_size]
    cur.executemany("""
        INSERT INTO SalesTransactions 
            (event_time, store_id, product_id, units_sold, unit_price, revenue,
             temperature, weather, holiday, category, enqueued_time, ingest_lag_seconds)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
    """, batch)
    inserted += len(batch)

conn.commit()
print(f"✅ Inserted {inserted:,} rows")

# Verify
cur.execute("""
    SELECT CAST(event_time AS DATE) as dt, COUNT(*) as cnt, ROUND(SUM(revenue),0) as rev
    FROM SalesTransactions
    GROUP BY CAST(event_time AS DATE)
    ORDER BY dt DESC
""")
rows = cur.fetchall()
print("\n=== FINAL STATE ===")
prev = None
for r in rows:
    if prev:
        dod = (prev[0] - r.rev) / r.rev * 100  # today vs day before
        print(f"  {r.dt}: {r.cnt:,} txns  ${r.rev:,.0f}")
    else:
        print(f"  {r.dt}: {r.cnt:,} txns  ${r.rev:,.0f}  (today/latest)")
    prev = (r.rev, r.dt)

if len(rows) >= 2:
    today_rev = rows[0].rev
    yesterday_rev = rows[1].rev
    dod = (today_rev - yesterday_rev) / yesterday_rev * 100
    print(f"\n✅ Power BI DoD: {dod:+.1f}%  (target: ~+18%)")

conn.close()
