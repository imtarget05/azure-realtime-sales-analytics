"""
Fix Apr 3 + Apr 8: Weight product selection by inverse price (cheap products sell more).
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

# Get ACTUAL products
cur.execute("SELECT product_id, category, base_price FROM Products ORDER BY base_price")
products = [(r[0], r[1], float(r[2])) for r in cur.fetchall()]

# Create weights: inverse of price (cheap products sell much more often)
# e.g., $1.50 item → weight 100, $1000 item → weight 0.15
product_weights = [100.0 / max(p[2], 0.5) for p in products]
total_w = sum(product_weights)
product_weights = [w/total_w for w in product_weights]

print(f"Products: {len(products)}")
print("Top 5 by selection probability:")
sorted_idx = sorted(range(len(products)), key=lambda i: product_weights[i], reverse=True)
for i in sorted_idx[:5]:
    print(f"  {products[i][0]}: ${products[i][2]:.2f} → weight {product_weights[i]*100:.1f}%")
print("Bottom 3 by selection probability:")
for i in sorted_idx[-3:]:
    print(f"  {products[i][0]}: ${products[i][2]:.2f} → weight {product_weights[i]*100:.2f}%")

stores = ['S01', 'S02', 'S03']
store_weights = [0.36, 0.30, 0.34]
weathers = ['Clear', 'Cloudy', 'Sunny', 'Light Rain']
w_wts = [0.4, 0.25, 0.25, 0.1]
hours = list(range(8, 23))
h_wts = [0.3, 0.5, 1.0, 2.0, 3.5, 5.0, 6.5, 7.0, 7.0, 6.5, 5.5, 4.0, 3.0, 2.0, 1.0]

def gen_rows(dt, target_rev, seed_val):
    random.seed(seed_val)
    rows = []
    cum = 0.0
    for _ in range(500_000):
        if cum >= target_rev:
            break
        store = random.choices(stores, weights=store_weights)[0]
        idx = random.choices(range(len(products)), weights=product_weights)[0]
        prod_id, cat, base_price = products[idx]
        unit_price = round(base_price * random.uniform(0.90, 1.15), 2)
        qty = random.randint(1, 5)
        revenue = round(unit_price * qty, 2)
        
        hour = random.choices(hours, weights=h_wts)[0]
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        et = datetime(dt.year, dt.month, dt.day, hour, minute, second)
        lag = round(random.uniform(0.1, 5.0), 2)
        eq = et + timedelta(seconds=lag)
        temp = round(random.uniform(25, 36), 1)
        weather = random.choices(weathers, weights=w_wts)[0]
        
        rows.append((et, store, prod_id, qty, unit_price, revenue, temp, weather, 0, cat, eq, lag))
        cum += revenue
    return rows, cum

from datetime import date

fixes = [
    (date(2026, 4, 3), 260_000, 20260403),
    (date(2026, 4, 8), 355_000, 20260408),
]

for dt, target_rev, seed in fixes:
    cur.execute("DELETE FROM SalesTransactions WHERE CAST(event_time AS DATE) = ?", dt)
    print(f"\n{dt}: Deleted {cur.rowcount:,} rows")
    
    new_rows, new_rev = gen_rows(dt, target_rev, seed)
    avg = new_rev / len(new_rows) if new_rows else 0
    print(f"  Generated {len(new_rows):,} rows (${new_rev:,.0f}, avg=${avg:.2f})")
    
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
    flag = "OK" if 10 <= r.avg_rev <= 55 else "FIX"
    print(f"  [{flag}] {r.dt}: {r.cnt:7,} txns  ${r.rev:>10,.0f}  avg=${r.avg_rev:>6.2f}  {dod}")
    prev = r.rev
    total_txns += r.cnt
    total_rev += r.rev
print(f"\n  TOTAL: {total_txns:,} txns  ${total_rev:,.0f}  avg=${total_rev/total_txns:.2f}")

conn.close()
