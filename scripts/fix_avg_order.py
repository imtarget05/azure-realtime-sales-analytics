"""
Rebalance transaction counts: Apr 3 and Apr 8 have unrealistic avg order values.
Target range: avg $28-45/txn for all days.

Current issues:
  Apr 3:  988 txns @ $263.67 avg → needs more rows at lower prices
  Apr 8: 4,625 txns @ $76.77 avg → needs more rows at lower prices
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

# Get products
cur.execute("SELECT product_id, category FROM Products")
products = [(r[0], r[1]) for r in cur.fetchall()]

PRICE_TIERS = {
    'Beverage': (15, 45), 'Snack': (8, 28), 'Snacks': (8, 28),
    'Fresh': (20, 80), 'Dairy': (12, 35), 'Bakery': (5, 25),
    'Frozen': (25, 70), 'Grocery': (10, 40), 'Seafood': (40, 120),
    'Meat': (50, 150), 'Electronics': (150, 500), 'Clothing': (25, 100),
    'Home': (30, 120), 'Accessories': (40, 150), 'Health & Beauty': (8, 45),
    'Sports': (15, 80), 'Stationery': (3, 15), 'Toys': (8, 50),
}
stores = ['S01', 'S02', 'S03']
store_weights = [0.36, 0.30, 0.34]
weathers = ['Clear', 'Cloudy', 'Sunny', 'Light Rain']
w_weights = [0.4, 0.25, 0.25, 0.1]
hours = list(range(8, 23))
h_weights = [0.3, 0.5, 1.0, 2.0, 3.5, 5.0, 6.5, 7.0, 7.0, 6.5, 5.5, 4.0, 3.0, 2.0, 1.0]

def generate_day_rows(target_date, target_revenue, target_avg_order=35.0, seed_val=None):
    """Generate rows for a specific date targeting a revenue and avg order value."""
    if seed_val:
        random.seed(seed_val)
    
    est_count = int(target_revenue / target_avg_order)
    rows = []
    cumulative = 0.0
    
    for _ in range(est_count * 3):  # overgenerate
        if cumulative >= target_revenue:
            break
        store = random.choices(stores, weights=store_weights)[0]
        prod_id, cat = random.choice(products)
        lo, hi = PRICE_TIERS.get(cat, (10, 50))
        unit_price = round(random.uniform(lo, hi), 2)
        qty = random.randint(1, 3)  # keep qty low for avg order control
        revenue = round(unit_price * qty, 2)
        
        hour = random.choices(hours, weights=h_weights)[0]
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        et = datetime(target_date.year, target_date.month, target_date.day, hour, minute, second)
        lag = round(random.uniform(0.1, 5.0), 2)
        eq = et + timedelta(seconds=lag)
        temp = round(random.uniform(25, 36), 1)
        weather = random.choices(weathers, weights=w_weights)[0]
        
        rows.append((et, store, prod_id, qty, unit_price, revenue, temp, weather, 0, cat, eq, lag))
        cumulative += revenue
    
    return rows, cumulative

# ========== Fix Apr 3: Delete + Regenerate with realistic avg ==========
from datetime import date

# Check what we need to fix
days_to_fix = {}
cur.execute("""
    SELECT CAST(event_time AS DATE) as dt, COUNT(*) as cnt, 
           ROUND(SUM(revenue),0) as rev, ROUND(AVG(revenue),2) as avg_rev
    FROM SalesTransactions
    GROUP BY CAST(event_time AS DATE) ORDER BY dt
""")
for r in cur.fetchall():
    if r.avg_rev > 50 or r.avg_rev < 20:  # unrealistic avg
        days_to_fix[r.dt] = {'count': r.cnt, 'revenue': float(r.rev), 'avg': float(r.avg_rev)}
        print(f"  FIX NEEDED: {r.dt} → {r.cnt:,} txns, ${r.rev:,.0f}, avg=${r.avg_rev:.2f}")
    else:
        print(f"  OK: {r.dt} → {r.cnt:,} txns, ${r.rev:,.0f}, avg=${r.avg_rev:.2f}")

for dt, info in days_to_fix.items():
    target_rev = info['revenue']
    print(f"\n--- Fixing {dt}: ${target_rev:,.0f} ---")
    
    # Delete existing
    cur.execute("DELETE FROM SalesTransactions WHERE CAST(event_time AS DATE) = ?", dt)
    print(f"  Deleted {cur.rowcount:,} rows")
    
    # Regenerate with realistic avg
    new_rows, new_rev = generate_day_rows(dt, target_rev, target_avg_order=35.0, seed_val=int(dt.strftime('%Y%m%d')))
    
    batch_size = 500
    inserted = 0
    for i in range(0, len(new_rows), batch_size):
        batch = new_rows[i:i+batch_size]
        cur.executemany("""
            INSERT INTO SalesTransactions 
                (event_time, store_id, product_id, units_sold, unit_price, revenue,
                 temperature, weather, holiday, category, enqueued_time, ingest_lag_seconds)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, batch)
        inserted += len(batch)
    
    print(f"  Inserted {inserted:,} rows (${new_rev:,.0f})")

conn.commit()
print("\n✅ All fixes committed")

# Verify
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
    print(f"  {r.dt}: {r.cnt:6,} txns  ${r.rev:>10,.0f}  avg=${r.avg_rev:.2f}  {dod}")
    prev = r.rev
    total_txns += r.cnt
    total_rev += r.rev
print(f"\n  TOTAL: {total_txns:,} txns  ${total_rev:,.0f}")

conn.close()
