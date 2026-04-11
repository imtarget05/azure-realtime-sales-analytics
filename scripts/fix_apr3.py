"""
Fix Apr 3 outlier: 61,413 transactions at avg $4.23 (unrealistic).
Strategy: DELETE all Apr 3 rows, regenerate ~8,000 rows with realistic pricing.
Target: ~$260K revenue with avg ~$32-35/txn
"""
import sys, os, random, math
from pathlib import Path
from datetime import datetime, timedelta
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
os.environ.setdefault('KEY_VAULT_URI', 'DISABLED')
from config.settings import SQL_SERVER, SQL_DATABASE, SQL_USERNAME, SQL_PASSWORD, SQL_DRIVER
import pyodbc

conn = pyodbc.connect(
    f"DRIVER={SQL_DRIVER};SERVER={SQL_SERVER};DATABASE={SQL_DATABASE};UID={SQL_USERNAME};PWD={SQL_PASSWORD};TrustServerCertificate=yes",
    timeout=20
)
conn.autocommit = False
cur = conn.cursor()

# Check current state
cur.execute("""
    SELECT CAST(event_time AS DATE) as dt, COUNT(*) as cnt, ROUND(SUM(revenue),0) as rev,
           ROUND(AVG(revenue),2) as avg_rev
    FROM SalesTransactions 
    GROUP BY CAST(event_time AS DATE) ORDER BY dt
""")
rows = cur.fetchall()
print("=== CURRENT STATE ===")
for r in rows:
    print(f"  {r.dt}: {r.cnt:6,} txns  ${r.rev:>10,.0f}  avg=${r.avg_rev:.2f}/txn")

# Delete all Apr 3 data
print("\nDeleting Apr 3 data...")
cur.execute("DELETE FROM SalesTransactions WHERE CAST(event_time AS DATE) = '2026-04-03'")
deleted = cur.rowcount
print(f"Deleted {deleted:,} Apr 3 rows")

# Generate fresh Apr 3 data with realistic pricing
TARGET_REV = 260_000  # keep $260K baseline
# Price tiers per category (realistic for a Vietnamese retail chain)
PRICE_TIERS = {
    'Beverage':      (15, 45),
    'Snack':         (8, 28),
    'Snacks':        (8, 28),
    'Fresh':         (20, 80),
    'Dairy':         (12, 35),
    'Bakery':        (5, 25),
    'Frozen':        (25, 70),
    'Grocery':       (10, 40),
    'Seafood':       (40, 120),
    'Meat':          (50, 150),
    'Electronics':   (150, 500),
    'Clothing':      (25, 100),
    'Home':          (30, 120),
    'Accessories':   (40, 150),
    'Health & Beauty': (8, 45),
    'Sports':        (15, 80),
    'Stationery':    (3, 15),
    'Toys':          (8, 50),
}

stores = ['S01', 'S02', 'S03']
store_weights = [0.36, 0.30, 0.34]
weathers = ['Clear', 'Cloudy', 'Sunny', 'Light Rain']
weather_weights = [0.4, 0.25, 0.25, 0.1]
# Hours weighted to peak shopping hours
hours = list(range(8, 23))
hour_weights = [0.3, 0.5, 1.0, 2.0, 3.5, 5.0, 6.5, 7.0, 7.0, 6.5, 5.5, 4.0, 3.0, 2.0, 1.0]

# Get products
cur.execute("SELECT product_id, category FROM Products")
products = [(r[0], r[1]) for r in cur.fetchall()]

random.seed(2026_04_03)
rows_to_insert = []
cumulative_rev = 0.0

max_iters = 200_000
for _ in range(max_iters):
    if cumulative_rev >= TARGET_REV:
        break
    
    store_id = random.choices(stores, weights=store_weights)[0]
    prod_id, category = random.choice(products)
    
    lo, hi = PRICE_TIERS.get(category, (10, 50))
    unit_price = round(random.uniform(lo, hi), 2)
    qty = random.randint(1, 4)
    revenue = round(unit_price * qty, 2)
    
    hour = random.choices(hours, weights=hour_weights)[0]
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    event_time = datetime(2026, 4, 3, hour, minute, second)
    ingest_lag = round(random.uniform(0.1, 5.0), 2)
    enqueued_time = event_time + timedelta(seconds=ingest_lag)
    temperature = round(random.uniform(24, 36), 1)
    weather = random.choices(weathers, weights=weather_weights)[0]
    
    rows_to_insert.append((
        event_time, store_id, prod_id, qty, unit_price, revenue,
        temperature, weather, 0, category,
        enqueued_time, ingest_lag
    ))
    cumulative_rev += revenue

total_new = len(rows_to_insert)
print(f"\nGenerated {total_new:,} rows  (${cumulative_rev:,.0f})")

# Insert in batches
batch_size = 500
inserted = 0
for i in range(0, total_new, batch_size):
    batch = rows_to_insert[i:i+batch_size]
    cur.executemany("""
        INSERT INTO SalesTransactions 
            (event_time, store_id, product_id, units_sold, unit_price, revenue,
             temperature, weather, holiday, category, enqueued_time, ingest_lag_seconds)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
    """, batch)
    inserted += len(batch)

conn.commit()
print(f"Inserted {inserted:,} rows")

# Final verification
cur.execute("""
    SELECT CAST(event_time AS DATE) as dt, COUNT(*) as cnt, ROUND(SUM(revenue),0) as rev,
           ROUND(AVG(revenue),2) as avg_rev, COUNT(*) as total
    FROM SalesTransactions
    GROUP BY CAST(event_time AS DATE) ORDER BY dt DESC
""")
rows_after = cur.fetchall()
print("\n=== FINAL STATE ===")
prev = None
total_all = 0
for r in rows_after:
    dod = f"{(r.rev - prev) / prev * 100:+.1f}%" if prev else "(today)"
    print(f"  {r.dt}: {r.cnt:6,} txns  ${r.rev:>10,.0f}  avg=${r.avg_rev:.2f}/txn  {dod}")
    prev = r.rev
    total_all += r.rev

print(f"\n  TOTAL txns: {sum(r.cnt for r in rows_after):,}")
print(f"  TOTAL rev:  ${total_all:,.0f}")

# DoD for Apr 9 vs Apr 8
if len(rows_after) >= 2:
    today = rows_after[0].rev
    yesterday = rows_after[1].rev
    dod = (today - yesterday) / yesterday * 100
    print(f"\n  Power BI DoD (Apr9 vs Apr8): {dod:+.1f}%")

conn.close()
