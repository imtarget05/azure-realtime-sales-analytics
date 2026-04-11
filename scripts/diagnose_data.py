#!/usr/bin/env python3
"""Diagnose all Power BI data issues."""
import pyodbc

conn = pyodbc.connect(
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=sql-sales-analytics-d9bt2m.database.windows.net;"
    "DATABASE=SalesAnalyticsDB;UID=sqladmin;PWD=SqlP@ssw0rd2026!;"
    "Encrypt=yes;TrustServerCertificate=no"
)
cur = conn.cursor()

print("=== 1. Products table ===")
cur.execute("SELECT COUNT(*) FROM Products")
print(f"  {cur.fetchone()[0]} rows")

print("\n=== 2. Product distribution in SalesTransactions ===")
cur.execute("""
    SELECT product_id, COUNT(*) cnt, CAST(SUM(revenue) AS DECIMAL(18,2)) rev 
    FROM SalesTransactions GROUP BY product_id ORDER BY rev DESC
""")
rows = cur.fetchall()
print(f"  {len(rows)} distinct products")
for r in rows[:10]:
    print(f"    {r[0]}: {r[1]:,} txns, ${r[2]:,.2f}")

print("\n=== 3. Store distribution ===")
cur.execute("""
    SELECT store_id, COUNT(*) cnt, CAST(SUM(revenue) AS DECIMAL(18,2)) rev 
    FROM SalesTransactions GROUP BY store_id ORDER BY store_id
""")
for r in cur.fetchall():
    print(f"    {r[0]}: {r[1]:,} txns, ${r[2]:,.2f}")

print("\n=== 4. Date distribution ===")
cur.execute("""
    SELECT CAST(event_time AS DATE) d, COUNT(*) cnt, CAST(SUM(revenue) AS DECIMAL(18,2)) rev
    FROM SalesTransactions GROUP BY CAST(event_time AS DATE) ORDER BY d
""")
for r in cur.fetchall():
    print(f"    {r[0]}: {r[1]:,} txns, ${r[2]:,.2f}")

print("\n=== 5. Hourly distribution (sample day) ===")
cur.execute("""
    SELECT DATEPART(HOUR, event_time) h, COUNT(*) cnt 
    FROM SalesTransactions 
    WHERE CAST(event_time AS DATE) = '2026-04-08'
    GROUP BY DATEPART(HOUR, event_time) ORDER BY h
""")
for r in cur.fetchall():
    print(f"    Hour {r[0]:02d}: {r[1]:,}")

print("\n=== 6. All tables and views ===")
cur.execute("""
    SELECT TABLE_NAME, TABLE_TYPE 
    FROM INFORMATION_SCHEMA.TABLES 
    ORDER BY TABLE_TYPE, TABLE_NAME
""")
for r in cur.fetchall():
    print(f"    [{r[1]}] {r[0]}")

print("\n=== 7. SalesForecast sample ===")
cur.execute("SELECT TOP 5 * FROM SalesForecast")
cols = [c[0] for c in cur.description]
print(f"    Columns: {cols}")
for r in cur.fetchall():
    print(f"    {list(r)}")

print("\n=== 8. Category distribution ===")
cur.execute("""
    SELECT category, COUNT(*) cnt, CAST(SUM(revenue) AS DECIMAL(18,2)) rev
    FROM SalesTransactions GROUP BY category ORDER BY rev DESC
""")
for r in cur.fetchall():
    print(f"    {r[0]}: {r[1]:,} txns, ${r[2]:,.2f}")

print("\n=== 9. Weather distribution ===")
cur.execute("""
    SELECT weather, COUNT(*) cnt FROM SalesTransactions GROUP BY weather
""")
for r in cur.fetchall():
    print(f"    {r[0]}: {r[1]:,}")

print("\n=== 10. ingest_lag_seconds stats ===")
cur.execute("""
    SELECT 
        MIN(ingest_lag_seconds) mn, MAX(ingest_lag_seconds) mx, 
        AVG(ingest_lag_seconds) av, COUNT(*) cnt
    FROM SalesTransactions WHERE ingest_lag_seconds IS NOT NULL
""")
r = cur.fetchone()
print(f"    min={r[0]}, max={r[1]}, avg={r[2]}, count={r[3]}")

conn.close()
print("\nDiagnosis complete.")
