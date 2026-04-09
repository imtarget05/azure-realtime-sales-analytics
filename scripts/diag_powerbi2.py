#!/usr/bin/env python3
import os, sys
sys.path.insert(0, ".")
from dotenv import load_dotenv; load_dotenv()
import pyodbc

srv = os.getenv("SQL_SERVER")
uid = os.getenv("SQL_USERNAME")
pwd = os.getenv("SQL_PASSWORD")
cs = (
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER={srv};DATABASE=SalesAnalyticsDB;"
    f"UID={uid};PWD={pwd};Encrypt=yes;TrustServerCertificate=no"
)
conn = pyodbc.connect(cs, timeout=10)
cur = conn.cursor()

# ModelRegistry
cur.execute("SELECT TOP 5 * FROM dbo.ModelRegistry")
cols = [d[0] for d in cur.description]
print("ModelRegistry cols:", cols)
for r in cur.fetchall():
    print(" ", dict(zip(cols, r)))

# Today data
cur.execute("""
    SELECT CAST(event_time AS DATE) as dt, COUNT(*) as cnt, CAST(SUM(revenue) AS BIGINT) as rev
    FROM dbo.SalesTransactions
    GROUP BY CAST(event_time AS DATE)
    ORDER BY dt DESC
""")
print("\nSalesTransactions by date (UTC):")
for r in cur.fetchall():
    print(f"  {r[0]}: {r[1]} rows, rev={r[2]}")

# SalesForecast scale
cur.execute("""
    SELECT store_id, CAST(AVG(CAST(predicted_revenue AS FLOAT)) AS DECIMAL(10,2)) avg_pred, COUNT(*) cnt
    FROM dbo.SalesForecast GROUP BY store_id
""")
print("\nSalesForecast avg predicted_revenue by store:")
for r in cur.fetchall():
    print(f"  {r[0]}: avg_pred={r[1]}, cnt={r[2]}")

# SalesAlerts high value
cur.execute("""
    SELECT
        SUM(CASE WHEN value > 1000 THEN 1 ELSE 0 END) as high_1000,
        SUM(CASE WHEN value > 500 THEN 1 ELSE 0 END) as high_500,
        SUM(CASE WHEN value > 100 THEN 1 ELSE 0 END) as high_100,
        COUNT(*) as total,
        CAST(MAX(value) AS INT) as max_val,
        CAST(AVG(value) AS INT) as avg_val
    FROM dbo.SalesAlerts
""")
r = cur.fetchone()
print(f"\nSalesAlerts high value breakdown:")
print(f"  >1000: {r[0]}, >500: {r[1]}, >100: {r[2]}, total: {r[3]}, max: {r[4]}, avg: {r[5]}")

# HourlySalesSummary check for today
cur.execute("""
    SELECT CAST(window_start AS DATE) as dt, COUNT(*) as cnt, CAST(SUM(revenue) as BIGINT) as rev
    FROM dbo.HourlySalesSummary
    GROUP BY CAST(window_start AS DATE)
    ORDER BY dt DESC
""")
print("\nHourlySalesSummary by date:")
for r in cur.fetchall():
    print(f"  {r[0]}: {r[1]} rows, rev={r[2]}")

conn.close()
