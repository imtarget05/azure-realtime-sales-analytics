"""
Setup Power BI: Create streaming dataset + push real-time data from Azure SQL.
Run this AFTER configuring POWERBI_PUSH_URL in .env.

Usage:
    # Step 1: Create streaming dataset in Power BI Service manually (see instructions below)
    # Step 2: Set POWERBI_PUSH_URL in .env  
    # Step 3: Run this script to push data continuously
    python _setup_powerbi.py --push

    # Or just test SQL connection
    python _setup_powerbi.py --test
"""
import argparse
import json
import os
import sys
import time
import urllib.request
from datetime import datetime, timezone

import pyodbc
from dotenv import load_dotenv
load_dotenv()

SQL_SERVER = os.getenv("SQL_SERVER")
SQL_DATABASE = os.getenv("SQL_DATABASE")
SQL_USERNAME = os.getenv("SQL_USERNAME")
SQL_PASSWORD = os.getenv("SQL_PASSWORD")
POWERBI_PUSH_URL = os.getenv("POWERBI_PUSH_URL", "")


def get_sql_connection():
    return pyodbc.connect(
        f'DRIVER={{ODBC Driver 18 for SQL Server}};'
        f'SERVER={SQL_SERVER};DATABASE={SQL_DATABASE};'
        f'UID={SQL_USERNAME};PWD={SQL_PASSWORD};'
        f'Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30'
    )


def test_sql():
    """Test SQL connection and show data summary for Power BI."""
    conn = get_sql_connection()
    cur = conn.cursor()
    
    print("=" * 70)
    print("  POWER BI DATA SOURCE VERIFICATION")
    print("=" * 70)
    
    # Tables available for Power BI
    queries = {
        "SalesTransactions (Raw events)": "SELECT COUNT(*) FROM dbo.SalesTransactions",
        "HourlySalesSummary (5-min agg)": "SELECT COUNT(*) FROM dbo.HourlySalesSummary",
        "SalesAlerts (Anomaly alerts)":   "SELECT COUNT(*) FROM dbo.SalesAlerts",
        "SalesForecast (ML predictions)": "SELECT COUNT(*) FROM dbo.SalesForecast",
    }
    
    for label, q in queries.items():
        cur.execute(q)
        cnt = cur.fetchone()[0]
        print(f"  [{cnt:>8} rows] {label}")
    
    # Views for Power BI
    views = {
        "vw_RealtimeDashboard": "Top 1000 recent transactions",
        "vw_ForecastVsActual": "Forecast vs actual comparison",
    }
    print("\n  Views (ready for Power BI DirectQuery):")
    for v, desc in views.items():
        try:
            cur.execute(f"SELECT COUNT(*) FROM dbo.{v}")
            cnt = cur.fetchone()[0]
            print(f"  [{cnt:>8} rows] {v} — {desc}")
        except:
            print(f"  [  MISSING] {v} — {desc}")
    
    # Summary data for demo
    print("\n  Revenue by Store (last 24h):")
    cur.execute("""
        SELECT store_id, 
               COUNT(*) as transactions,
               SUM(revenue) as total_revenue,
               AVG(unit_price) as avg_price
        FROM dbo.SalesTransactions 
        WHERE event_time >= DATEADD(hour, -24, SYSUTCDATETIME())
        GROUP BY store_id ORDER BY total_revenue DESC
    """)
    for row in cur.fetchall():
        print(f"    {row[0]}: {row[1]:>6} txns | ${row[2]:>12,.2f} revenue | avg ${row[3]:>.2f}")
    
    print("\n  Revenue by Category (last 24h):")
    cur.execute("""
        SELECT category, COUNT(*) as cnt, SUM(revenue) as rev
        FROM dbo.SalesTransactions 
        WHERE event_time >= DATEADD(hour, -24, SYSUTCDATETIME())
        GROUP BY category ORDER BY rev DESC
    """)
    for row in cur.fetchall():
        print(f"    {row[0]:12}: {row[1]:>6} txns | ${row[2]:>12,.2f}")
    
    print("\n  Recent Alerts:")
    cur.execute("SELECT TOP 5 alert_time, store_id, type, value FROM dbo.SalesAlerts ORDER BY alert_time DESC")
    for row in cur.fetchall():
        print(f"    {row[0]} | {row[1]} | {row[2]:12} | ${row[3]:>8.2f}")
    
    conn.close()
    
    # Power BI connection info
    print("\n" + "=" * 70)
    print("  POWER BI CONNECTION INFO")
    print("=" * 70)
    print(f"""
  Option A: DirectQuery to Azure SQL (Recommended for demo)
  ──────────────────────────────────────────────────────────
  Power BI Desktop → Get Data → Azure SQL Database
    Server:   {SQL_SERVER}
    Database: {SQL_DATABASE}
    Mode:     DirectQuery
    Auth:     Database (login={SQL_USERNAME})
  
  Tables to select:
    ✓ dbo.SalesTransactions    (raw events — fact table)
    ✓ dbo.HourlySalesSummary   (5-min aggregation — fact table)
    ✓ dbo.SalesAlerts          (anomaly alerts — fact table)
    ✓ dbo.SalesForecast        (ML predictions — fact table)
  
  Views to select:
    ✓ dbo.vw_RealtimeDashboard (top 1000 recent — dashboard)
    ✓ dbo.vw_ForecastVsActual  (forecast vs actual — drift view)

  Option B: DirectQuery to Databricks SQL Warehouse
  ──────────────────────────────────────────────────
  Power BI Desktop → Get Data → Azure Databricks
    Server:    adb-7405611397181783.3.azuredatabricks.net
    HTTP Path: /sql/1.0/warehouses/{os.getenv('DATABRICKS_SQL_WAREHOUSE_ID', '60ef070340bed923')}
    Auth:      Azure Active Directory
  
  Tables: sales_analytics.gold.*
""")


def push_to_powerbi():
    """Continuously push data to Power BI streaming dataset."""
    if not POWERBI_PUSH_URL or POWERBI_PUSH_URL.startswith("<"):
        print("ERROR: Set POWERBI_PUSH_URL in .env first!")
        print("See instructions in powerbi/POWERBI_SETUP.md section 5")
        return
    
    conn = get_sql_connection()
    print("Connected to SQL. Pushing to Power BI every 5 seconds...")
    print("Press Ctrl+C to stop.\n")
    
    while True:
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT 
                    GETUTCDATE() as timestamp,
                    store_id,
                    category,
                    COUNT(*) as transaction_count,
                    SUM(units_sold) as total_quantity,
                    SUM(revenue) as total_revenue,
                    AVG(unit_price) as avg_order_value
                FROM dbo.SalesTransactions
                WHERE event_time >= DATEADD(minute, -5, SYSUTCDATETIME())
                GROUP BY store_id, category
            """)
            
            rows = []
            for r in cur.fetchall():
                rows.append({
                    "timestamp": r[0].isoformat() + "Z",
                    "region": r[1],
                    "category": r[2],
                    "transaction_count": r[3],
                    "total_quantity": int(r[4]),
                    "total_revenue": round(float(r[5]), 2),
                    "avg_order_value": round(float(r[6]), 2),
                    "avg_rating": 4.5,
                })
            
            if rows:
                body = json.dumps(rows).encode()
                req = urllib.request.Request(POWERBI_PUSH_URL, body, {"Content-Type": "application/json"})
                urllib.request.urlopen(req)
                print(f"  [{datetime.now().strftime('%H:%M:%S')}] Pushed {len(rows)} rows")
            else:
                print(f"  [{datetime.now().strftime('%H:%M:%S')}] No recent data")
            
            time.sleep(5)
        except KeyboardInterrupt:
            print("\nStopped.")
            break
        except Exception as e:
            print(f"  Error: {e}")
            time.sleep(5)
    
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--test", action="store_true", help="Test SQL + show Power BI connection info")
    parser.add_argument("--push", action="store_true", help="Push data to Power BI streaming dataset")
    args = parser.parse_args()
    
    if args.push:
        push_to_powerbi()
    else:
        test_sql()
