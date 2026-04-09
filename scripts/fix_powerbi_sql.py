#!/usr/bin/env python3
"""Fix Power BI no-data panels by patching SQL schema and views."""
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
conn = pyodbc.connect(cs, timeout=15, autocommit=True)
cur = conn.cursor()

print("=== Fix 1: Add severity + is_high_value computed columns to SalesAlerts ===")
# Check if column already exists
cur.execute("""
    SELECT COUNT(*) FROM sys.columns
    WHERE object_id = OBJECT_ID('dbo.SalesAlerts') AND name = 'severity'
""")
if cur.fetchone()[0] == 0:
    cur.execute("""
        ALTER TABLE dbo.SalesAlerts
        ADD severity AS (
            CASE WHEN [value] > 1000 THEN 'high'
                 WHEN [value] > 100  THEN 'medium'
                 ELSE 'low' END
        ) PERSISTED
    """)
    print("  Added computed column: severity")
else:
    print("  Column 'severity' already exists, skipping.")

cur.execute("""
    SELECT COUNT(*) FROM sys.columns
    WHERE object_id = OBJECT_ID('dbo.SalesAlerts') AND name = 'is_high_value'
""")
if cur.fetchone()[0] == 0:
    cur.execute("""
        ALTER TABLE dbo.SalesAlerts
        ADD is_high_value AS (CASE WHEN [value] > 1000 THEN 1 ELSE 0 END) PERSISTED
    """)
    print("  Added computed column: is_high_value")
else:
    print("  Column 'is_high_value' already exists, skipping.")

# Verify
cur.execute("SELECT SUM(is_high_value) as high_cnt, COUNT(*) as total FROM dbo.SalesAlerts")
r = cur.fetchone()
print(f"  Verification: is_high_value=1 count={r[0]}, total={r[1]}")

print("\n=== Fix 2: Recreate vw_ForecastAccuracy with proper accuracy metric ===")
cur.execute("IF OBJECT_ID('dbo.vw_ForecastAccuracy', 'V') IS NOT NULL DROP VIEW dbo.vw_ForecastAccuracy")
cur.execute("""
CREATE VIEW dbo.vw_ForecastAccuracy AS
SELECT
    f.forecast_date,
    f.forecast_hour,
    f.store_id,
    f.product_id,
    f.category,
    CAST(f.predicted_revenue AS FLOAT) AS predicted_revenue,
    f.confidence_lower,
    f.confidence_upper,
    f.model_version,
    COALESCE(a.actual_revenue, 0.0) AS actual_revenue,
    ABS(CAST(f.predicted_revenue AS FLOAT) - COALESCE(a.actual_revenue, 0.0)) AS absolute_error,
    CASE
        WHEN COALESCE(a.actual_revenue, 0) = 0 THEN NULL
        ELSE ABS(CAST(f.predicted_revenue AS FLOAT) - a.actual_revenue) / a.actual_revenue * 100
    END AS pct_error,
    CASE
        WHEN COALESCE(a.actual_revenue, 0) = 0 THEN NULL
        WHEN ABS(CAST(f.predicted_revenue AS FLOAT) - a.actual_revenue) >= a.actual_revenue THEN 0.0
        ELSE 1.0 - ABS(CAST(f.predicted_revenue AS FLOAT) - a.actual_revenue) / a.actual_revenue
    END AS accuracy_score,
    CASE
        WHEN COALESCE(a.actual_revenue, 0) BETWEEN f.confidence_lower AND f.confidence_upper THEN 1
        ELSE 0
    END AS within_confidence
FROM dbo.SalesForecast f
LEFT JOIN (
    SELECT
        CAST(event_time AS DATE) AS sale_date,
        DATEPART(HOUR, event_time) AS sale_hour,
        store_id,
        product_id,
        SUM(units_sold) AS actual_quantity,
        SUM(revenue) AS actual_revenue
    FROM dbo.SalesTransactions
    GROUP BY CAST(event_time AS DATE), DATEPART(HOUR, event_time), store_id, product_id
) a ON f.forecast_date = a.sale_date
    AND f.forecast_hour = a.sale_hour
    AND f.store_id = a.store_id
    AND f.product_id = a.product_id
""")
print("  Recreated vw_ForecastAccuracy with accuracy_score column (0-1 capped)")

# Check accuracy
cur.execute("""
    SELECT AVG(CASE WHEN accuracy_score IS NOT NULL THEN accuracy_score END) avg_acc,
           AVG(pct_error) avg_pct_err,
           COUNT(*) total,
           SUM(CASE WHEN actual_revenue > 0 THEN 1 ELSE 0 END) has_actual
    FROM dbo.vw_ForecastAccuracy
""")
r = cur.fetchone()
avg_acc_str = f"{r[0]:.4f}" if r[0] is not None else "N/A"
avg_pct_str = f"{r[1]:.1f}" if r[1] is not None else "N/A"
print(f"  vw_ForecastAccuracy: avg_accuracy={avg_acc_str}, avg_pct_err={avg_pct_str}%, has_actual={r[3]}/{r[2]}")

print("\n=== Fix 3: Add DOD data view to handle missing today ===")
cur.execute("IF OBJECT_ID('dbo.vw_DailyRevenueTrend', 'V') IS NOT NULL DROP VIEW dbo.vw_DailyRevenueTrend")
cur.execute("""
CREATE VIEW dbo.vw_DailyRevenueTrend AS
SELECT
    sale_date,
    store_id,
    category,
    total_revenue,
    total_orders,
    avg_order_value,
    LAG(total_revenue) OVER (PARTITION BY store_id, category ORDER BY sale_date) AS prev_day_revenue,
    CASE
        WHEN LAG(total_revenue) OVER (PARTITION BY store_id, category ORDER BY sale_date) > 0
        THEN (total_revenue - LAG(total_revenue) OVER (PARTITION BY store_id, category ORDER BY sale_date))
             / LAG(total_revenue) OVER (PARTITION BY store_id, category ORDER BY sale_date) * 100
        ELSE NULL
    END AS dod_growth_pct
FROM (
    SELECT
        CAST(event_time AS DATE) AS sale_date,
        store_id,
        category,
        SUM(revenue) AS total_revenue,
        COUNT(*) AS total_orders,
        AVG(revenue) AS avg_order_value
    FROM dbo.SalesTransactions
    GROUP BY CAST(event_time AS DATE), store_id, category
) base
""")
print("  Created vw_DailyRevenueTrend with dod_growth_pct (uses SQL Window Function)")

# Check the view
cur.execute("""
    SELECT sale_date, SUM(total_revenue) as rev, AVG(dod_growth_pct) as dod
    FROM dbo.vw_DailyRevenueTrend
    GROUP BY sale_date ORDER BY sale_date DESC
""")
print("  DOD trend by date:")
for r in cur.fetchall():
    dod = f"{r[2]:.1f}%" if r[2] is not None else "N/A (first day)"
    print(f"    {r[0]}: rev={r[1]:.0f}, DoD={dod}")

conn.close()
print("\n=== All SQL fixes applied. ===")
