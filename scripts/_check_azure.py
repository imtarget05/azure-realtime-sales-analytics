"""Quick inspection of Azure SQL SalesTransactions table."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import pyodbc
from config.settings import SQL_DRIVER, SQL_SERVER, SQL_DATABASE, SQL_USERNAME, SQL_PASSWORD

from config.settings import SQL_DRIVER, SQL_SERVER, SQL_DATABASE, SQL_USERNAME, SQL_PASSWORD
conn_str = (
    f"Driver={SQL_DRIVER};"
    f"Server=tcp:{SQL_SERVER},1433;"
    f"Database={SQL_DATABASE};"
    f"Uid={SQL_USERNAME};Pwd={SQL_PASSWORD};"
    f"Encrypt=yes;TrustServerCertificate=no;"
    f"Connection Timeout=30;Command Timeout=300;"
)
conn = pyodbc.connect(conn_str)
cur = conn.cursor()

print("=== AZURE SQL: SalesTransactions ===\n")

# Row count
cur.execute("SELECT COUNT(*) FROM SalesTransactions")
cnt = cur.fetchone()[0]
print(f"Total rows: {cnt:,}")

# Table size
cur.execute("""
SELECT 
    SUM(reserved_page_count) * 8.0 / 1024 AS size_mb,
    SUM(row_count) AS rows_approx
FROM sys.dm_db_partition_stats 
WHERE object_id = OBJECT_ID('SalesTransactions') AND index_id IN (0,1)
""")
row = cur.fetchone()
print(f"Table size: {row[0]:.1f} MB (approx rows: {row[1]:,})")

# Revenue summary
cur.execute("SELECT SUM(revenue), MIN(revenue), MAX(revenue), AVG(revenue) FROM SalesTransactions")
r = cur.fetchone()
print(f"Revenue: SUM={r[0]:,.2f} | MIN={r[1]:.2f} | MAX={r[2]:.2f} | AVG={r[3]:.2f}")

# Store distribution
print("\nStore distribution:")
cur.execute("SELECT store_id, COUNT(*) AS cnt, SUM(revenue) AS rev FROM SalesTransactions GROUP BY store_id ORDER BY rev DESC")
for row in cur.fetchall():
    print(f"  {row[0]}: {row[1]:,} rows | revenue={row[2]:,.2f}")

# Category distribution
print("\nCategory distribution:")
cur.execute("SELECT category, COUNT(*) AS cnt, SUM(revenue) AS rev FROM SalesTransactions GROUP BY category ORDER BY rev DESC")
for row in cur.fetchall():
    print(f"  {row[0]}: {row[1]:,} rows | revenue={row[2]:,.2f}")

# Indexes
print("\nIndexes:")
cur.execute("""
SELECT i.name, i.type_desc, 
       STRING_AGG(c.name, ', ') WITHIN GROUP (ORDER BY ic.key_ordinal)
FROM sys.indexes i
JOIN sys.index_columns ic ON i.object_id=ic.object_id AND i.index_id=ic.index_id
JOIN sys.columns c ON ic.object_id=c.object_id AND ic.column_id=c.column_id
WHERE i.object_id=OBJECT_ID('SalesTransactions')
GROUP BY i.name, i.type_desc
""")
for row in cur.fetchall():
    print(f"  {row[0]} ({row[1]}): [{row[2]}]")

# Service tier
print()
cur.execute("SELECT edition, service_objective FROM sys.database_service_objectives")
for row in cur.fetchall():
    print(f"Service tier: {row[0]} / {row[1]}")

# Date range
print()
cur.execute("SELECT MIN(event_time), MAX(event_time) FROM SalesTransactions")
r = cur.fetchone()
print(f"Date range: {r[0]} -> {r[1]}")

# Sample rows
print("\nSample (5 rows):")
cur.execute("SELECT TOP 5 event_time, store_id, product_id, units_sold, revenue, category FROM SalesTransactions ORDER BY event_time DESC")
for row in cur.fetchall():
    print(f"  {row[0]} | {row[1]} | {row[2]} | units={row[3]} | rev={row[4]:.2f} | {row[5]}")

conn.close()
print("\nDone.")
