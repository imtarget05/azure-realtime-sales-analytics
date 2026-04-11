import sys, os
sys.path.insert(0, '.')
os.environ.setdefault('KEY_VAULT_URI', 'DISABLED')
from config.settings import SQL_SERVER, SQL_DATABASE, SQL_USERNAME, SQL_PASSWORD, SQL_DRIVER
import pyodbc
conn = pyodbc.connect(f'DRIVER={SQL_DRIVER};SERVER={SQL_SERVER};DATABASE={SQL_DATABASE};UID={SQL_USERNAME};PWD={SQL_PASSWORD};TrustServerCertificate=yes')
cur = conn.cursor()

# Top 5 products
cur.execute("""
    SELECT TOP 5 p.product_name, p.category, 
           ROUND(SUM(st.revenue),0) as total_rev, SUM(st.units_sold) as total_units
    FROM SalesTransactions st
    JOIN Products p ON st.product_id = p.product_id
    GROUP BY p.product_name, p.category
    ORDER BY SUM(st.revenue) DESC
""")
print("Top 5 Products by Revenue:")
for r in cur.fetchall():
    rev = int(r[2])
    units = int(r[3])
    print("  {} ({}): ${:,} / {:,} units".format(r[0], r[1], rev, units))

# Revenue by region
cur.execute("""
    SELECT sr.region, sr.store_id, ROUND(SUM(st.revenue),0) as rev, COUNT(*) as cnt
    FROM SalesTransactions st
    JOIN StoreRegions sr ON st.store_id = sr.store_id
    GROUP BY sr.region, sr.store_id
    ORDER BY SUM(st.revenue) DESC
""")
print("\nRevenue by Region:")
for r in cur.fetchall():
    rev = int(r[2])
    cnt = int(r[3])
    print("  {} ({}): ${:,} / {:,} txns".format(r[0], r[1], rev, cnt))

# Overall
cur.execute("SELECT COUNT(*), ROUND(SUM(revenue),0), ROUND(AVG(revenue),2) FROM SalesTransactions")
r = cur.fetchone()
print("\nOverall: {:,} txns / ${:,} total / ${:.2f} avg".format(int(r[0]), int(r[1]), float(r[2])))

# Table counts
tables = ['SalesTransactions','Products','StoreRegions','SalesForecast','SalesAlerts','ModelRegistry','HourlySalesSummary','SecurityMapping','AccessAudit','LatencyBenchmark']
print("\nTable row counts:")
for t in tables:
    cur.execute(f"SELECT COUNT(*) FROM {t}")
    cnt = cur.fetchone()[0]
    print("  {}: {:,}".format(t, cnt))

conn.close()
