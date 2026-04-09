#!/usr/bin/env python3
"""Diagnose Power BI no-data panels via SQL inspection."""
import os, sys
sys.path.insert(0, ".")
from dotenv import load_dotenv; load_dotenv()

import pyodbc

server = os.getenv("SQL_SERVER", "")
conn_str = (
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER={server};DATABASE=SalesAnalyticsDB;"
    f"UID={os.getenv('SQL_USERNAME')};PWD={os.getenv('SQL_PASSWORD')};"
    f"Encrypt=yes;TrustServerCertificate=no;Connection Timeout=10"
)
conn = pyodbc.connect(conn_str, timeout=10)
cur = conn.cursor()

print("=== SalesAlerts columns & sample ===")
cur.execute("SELECT TOP 3 * FROM dbo.SalesAlerts ORDER BY alert_time DESC")
cols = [d[0] for d in cur.description]
print("Columns:", cols)
for r in cur.fetchall():
    print(" ", dict(zip(cols, r)))

print("\n=== SalesForecast columns & sample ===")
cur.execute("SELECT TOP 3 * FROM dbo.SalesForecast ORDER BY forecast_date DESC")
cols2 = [d[0] for d in cur.description]
print("Columns:", cols2)
for r in cur.fetchall():
    print(" ", dict(zip(cols2, r)))

print("\n=== SalesTransactions recent days (UTC) ===")
cur.execute("""
    SELECT CAST(event_time AS DATE) as dt, COUNT(*) as cnt, SUM(revenue) as rev
    FROM dbo.SalesTransactions
    WHERE event_time >= DATEADD(day, -3, GETUTCDATE())
    GROUP BY CAST(event_time AS DATE)
    ORDER BY dt DESC
""")
for r in cur.fetchall():
    print(f"  {r[0]}: {r[1]} rows, revenue={r[2]:.0f}")

print("\n=== HourlySalesSummary — columns ===")
cur.execute("SELECT TOP 2 * FROM dbo.HourlySalesSummary")
cols3 = [d[0] for d in cur.description]
print("Columns:", cols3)
for r in cur.fetchall():
    print(" ", dict(zip(cols3, r)))

print("\n=== SalesAlerts — high_value check ===")
cur.execute("""
    SELECT type, value, alert_time
    FROM dbo.SalesAlerts
    ORDER BY alert_time DESC
""")
col4 = [d[0] for d in cur.description]
rows = cur.fetchall()
print(f"Total alerts: {len(rows)}")
vals = [r[1] for r in rows if r[1] is not None]
if vals:
    print(f"Value range: {min(vals):.0f} - {max(vals):.0f}, avg={sum(vals)/len(vals):.0f}")

print("\n=== SalesForecast accuracy check ===")
cur.execute("""
    SELECT store_id, AVG(mae) as avg_mae, AVG(predicted_revenue) as avg_pred,
           AVG(actual_revenue) as avg_actual,
           AVG(CASE WHEN actual_revenue > 0 THEN 1 - ABS(predicted_revenue - actual_revenue)/actual_revenue ELSE NULL END) as avg_acc
    FROM dbo.SalesForecast
    GROUP BY store_id
""")
col5 = [d[0] for d in cur.description]
for r in cur.fetchall():
    print(" ", dict(zip(col5, r)))

conn.close()
