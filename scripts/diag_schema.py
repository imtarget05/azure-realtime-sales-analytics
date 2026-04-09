#!/usr/bin/env python3
"""Discover actual SQL column names for Power BI diagnostic."""
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

TABLES = ["SalesAlerts", "SalesForecast", "HourlySalesSummary", "SalesTransactions"]
for tbl in TABLES:
    cur.execute(f"SELECT TOP 1 * FROM dbo.[{tbl}]")
    cols = [d[0] for d in cur.description]
    print(f"\n{tbl}:")
    print(f"  Columns: {cols}")
    r = cur.fetchone()
    if r:
        print(f"  Sample: {dict(zip(cols, r))}")

# Today's alert count (check what column has alert_time)
print("\n=== SalesAlerts date check ===")
cur.execute("SELECT TOP 1 * FROM dbo.SalesAlerts")
alert_cols = [d[0] for d in cur.description]
# Find time column
time_col = next((c for c in alert_cols if "time" in c.lower() or "date" in c.lower()), None)
if time_col:
    cur.execute(f"SELECT MIN([{time_col}]), MAX([{time_col}]), COUNT(*) FROM dbo.SalesAlerts")
    r = cur.fetchone()
    print(f"  [{time_col}] range: {r[0]} to {r[1]}, total={r[2]}")
    
    cur.execute(f"SELECT CAST([{time_col}] AS DATE), COUNT(*) FROM dbo.SalesAlerts GROUP BY CAST([{time_col}] AS DATE) ORDER BY 1 DESC")
    print("  Alerts by date:")
    for row in cur.fetchall():
        print(f"    {row[0]}: {row[1]}")

# Check value column in SalesAlerts
val_col = next((c for c in alert_cols if "value" in c.lower() or "revenue" in c.lower() or "amount" in c.lower()), None)
if val_col:
    cur.execute(f"SELECT MIN([{val_col}]), MAX([{val_col}]), AVG([{val_col}]) FROM dbo.SalesAlerts")
    r = cur.fetchone()
    print(f"  [{val_col}] min={r[0]:.1f}, max={r[1]:.1f}, avg={r[2]:.1f}")

# Forecast accuracy check with actual columns
print("\n=== SalesForecast accuracy ===")
cur.execute("SELECT TOP 1 * FROM dbo.SalesForecast")
fc_cols = [d[0] for d in cur.description]
pred_col = next((c for c in fc_cols if "predict" in c.lower()), None)
actual_col = next((c for c in fc_cols if "actual" in c.lower()), None)
date_col = next((c for c in fc_cols if "date" in c.lower()), None)
print(f"  pred_col={pred_col}, actual_col={actual_col}, date_col={date_col}")
if pred_col and actual_col:
    cur.execute(f"""
        SELECT AVG(ABS([{pred_col}] - [{actual_col}])) as mae,
               AVG([{pred_col}]) as avg_pred, AVG([{actual_col}]) as avg_actual,
               COUNT(*) as cnt
        FROM dbo.SalesForecast WHERE [{actual_col}] > 0
    """)
    r = cur.fetchone()
    if r:
        print(f"  MAE={r[0]:.1f}, avg_pred={r[1]:.1f}, avg_actual={r[2]:.1f}, cnt={r[3]}")
        if r[2] and r[2] > 0:
            accuracy = 1 - r[0] / r[2]
            print(f"  True Accuracy = 1 - MAE/AvgActual = {accuracy:.4f}")

conn.close()
