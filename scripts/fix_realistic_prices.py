"""
Fix Apr 3 + Apr 8: Use ACTUAL base_price from Products table.
Problem: Price tiers were too high ($150-500 for Electronics).
Solution: Use Products.base_price * random(0.85, 1.25) for realistic pricing.
"""
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

# Get ACTUAL products with base_price
cur.execute("SELECT product_id, category, base_price FROM Products")
products = [(r[0], r[1], float(r[2])) for r in cur.fetchall()]
print(f"Products: {len(products)}")
avg_base = sum(p[2] for p in products) / len(products)
print(f"Avg base_price: ${avg_base:.2f}")

# Show sample
for p in products[:5]:
    print(f"  {p[0]}: {p[1]} @ ${p[2]:.2f}")

stores = ['S01', 'S02', 'S03']
store_weights = [0.36, 0.30, 0.34]
weathers = ['Clear', 'Cloudy', 'Sunny', 'Light Rain']
w_weights = [0.4, 0.25, 0.25, 0.1]
hours = list(range(8, 23))
h_weights = [0.3, 0.5, 1.0, 2.0, 3.5, 5.0, 6.5, 7.0, 7.0, 6.5, 5.5, 4.0, 3.0, 2.0, 1.0]

def gen_rows(dt, target_rev, seed_val):
    random.seed(seed_val)
    rows = []
    cum = 0.0
    for _ in range(200_000):
        if cum >= target_rev:
            break
        store = random.choices(stores, weights=store_weights)[0]
        prod_id, cat, base_price = random.choice(products)
        # Use actual base_price with small variance (±20%)
        unit_price = round(base_price * random.uniform(0.85, 1.25), 2)
        qty = random.randint(1, 5)
        revenue = round(unit_price * qty, 2)
        
        hour = random.choices(hours, weights=h_weights)[0]
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        et = datetime(dt.year, dt.month, dt.day, hour, minute, second)
        lag = round(random.uniform(0.1, 5.0), 2)
        eq = et + timedelta(seconds=lag)
        temp = round(random.uniform(25, 36), 1)
        weather = random.choices(weathers, weights=w_weights)[0]
        
        rows.append((et, store, prod_id, qty, unit_price, revenue, temp, weather, 0, cat, eq, lag))
        cum += revenue
    return rows, cum

# Fix Apr 3 and Apr 8
from datetime import date

fixes = [
    (date(2026, 4, 3), 260_000, 20260403),
    (date(2026, 4, 8), 355_000, 20260408),
]

for dt, target_rev, seed in fixes:
    # Delete existing
    cur.execute("DELETE FROM SalesTransactions WHERE CAST(event_time AS DATE) = ?", dt)
    print(f"\n{dt}: Deleted {cur.rowcount:,} rows")
    
    new_rows, new_rev = gen_rows(dt, target_rev, seed)
    print(f"  Generated {len(new_rows):,} rows (${new_rev:,.0f})")
    
    batch_size = 500
    for i in range(0, len(new_rows), batch_size):
        batch = new_rows[i:i+batch_size]
        cur.executemany("""
            INSERT INTO SalesTransactions 
                (event_time, store_id, product_id, units_sold, unit_price, revenue,
                 temperature, weather, holiday, category, enqueued_time, ingest_lag_seconds)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, batch)

conn.commit()
print("\n✅ Committed")

# Final state
cur.execute("""
    SELECT CAST(event_time AS DATE) as dt, COUNT(*) as cnt, 
           ROUND(SUM(revenue),0) as rev, ROUND(AVG(revenue),2) as avg_rev
    FROM SalesTransactions
    GROUP BY CAST(event_time AS DATE) ORDER BY dt DESC
""")
rows = cur.fetchall()
print("\n=== FINAL STATE ===")
prev = None
total_txns = 0
total_rev = 0
for r in rows:
    dod = f"{(r.rev-prev)/prev*100:+.1f}%" if prev else "(today)"
    flag = "⚠️" if r.avg_rev > 50 or r.avg_rev < 10 else "✅"
    print(f"  {flag} {r.dt}: {r.cnt:7,} txns  ${r.rev:>10,.0f}  avg=${r.avg_rev:>6.2f}  {dod}")
    prev = r.rev
    total_txns += r.cnt
    total_rev += r.rev
print(f"\n  TOTAL: {total_txns:,} txns  ${total_rev:,.0f}")
print(f"  Overall Avg Order: ${total_rev/total_txns:.2f}")

conn.close()
