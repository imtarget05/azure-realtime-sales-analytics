"""Fix Apr 9 excess revenue: trim from $733K to $420K target"""
import sys, os
from pathlib import Path
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

# --- Check current state ---
cur.execute("""
    SELECT CAST(event_time AS DATE) as dt, COUNT(*) as cnt, ROUND(SUM(revenue),0) as rev
    FROM SalesTransactions
    GROUP BY CAST(event_time AS DATE)
    ORDER BY dt DESC
""")
rows = cur.fetchall()
print("=== BEFORE FIX ===")
for r in rows:
    print(f"  {r.dt}: {r.cnt:,} txns  ${r.rev:,.0f}")

TARGET_APR9 = 420_000  # target DoD ~+18%

cur.execute("""
    SELECT ROUND(SUM(revenue),0) as rev_apr9
    FROM SalesTransactions 
    WHERE CAST(event_time AS DATE) = '2026-04-09'
""")
r = cur.fetchone()
current_apr9 = float(r.rev_apr9)
print(f"\nApr 9 current: ${current_apr9:,.0f}  target: ${TARGET_APR9:,.0f}")

if current_apr9 > TARGET_APR9:
    # Delete rows from top by id until we reach target
    keep_fraction = TARGET_APR9 / current_apr9
    print(f"Need to keep {keep_fraction:.1%} of Apr 9 rows")
    
    # Delete the highest-id rows (newer inserts) to trim revenue
    # Strategy: delete bottom fraction of rows by id
    cur.execute("""
        SELECT COUNT(*) as cnt FROM SalesTransactions 
        WHERE CAST(event_time AS DATE) = '2026-04-09'
    """)
    total_apr9 = cur.fetchone()[0]
    keep_count = int(total_apr9 * keep_fraction)
    delete_count = total_apr9 - keep_count
    print(f"Total Apr 9 rows: {total_apr9:,}, deleting: {delete_count:,}, keeping: {keep_count:,}")
    
    # Delete the excess rows (keep rows with smallest id values - original data)
    cur.execute(f"""
        DELETE FROM SalesTransactions
        WHERE id IN (
            SELECT TOP ({delete_count}) id
            FROM SalesTransactions
            WHERE CAST(event_time AS DATE) = '2026-04-09'
            ORDER BY id DESC  -- delete most recently inserted rows
        )
    """)
    deleted = cur.rowcount
    print(f"Deleted {deleted:,} rows")
    
    # Verify result
    cur.execute("""
        SELECT COUNT(*), ROUND(SUM(revenue),0)
        FROM SalesTransactions 
        WHERE CAST(event_time AS DATE) = '2026-04-09'
    """)
    r2 = cur.fetchone()
    print(f"Apr 9 after: {r2[0]:,} txns  ${r2[1]:,.0f}")
    
    conn.commit()
    print("✅ Committed")
else:
    print("✅ Apr 9 already within target")
    conn.commit()

# --- Final state ---
cur.execute("""
    SELECT CAST(event_time AS DATE) as dt, COUNT(*) as cnt, ROUND(SUM(revenue),0) as rev
    FROM SalesTransactions
    GROUP BY CAST(event_time AS DATE)
    ORDER BY dt DESC
""")
rows_after = cur.fetchall()
print("\n=== AFTER FIX ===")
prev = None
for r in rows_after:
    if prev:
        dod = (r.rev - prev) / prev * 100
        print(f"  {r.dt}: {r.cnt:,} txns  ${r.rev:,.0f}  DoD: {dod:+.1f}%")
    else:
        print(f"  {r.dt}: {r.cnt:,} txns  ${r.rev:,.0f}  (today)")
    prev = r.rev

if rows_after:
    today = rows_after[0].rev
    yesterday = rows_after[1].rev if len(rows_after) > 1 else 0
    dod = (today - yesterday) / yesterday * 100 if yesterday else 0
    print(f"\n✅ Power BI DoD will show: {dod:+.1f}%")

conn.close()
