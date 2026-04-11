#!/usr/bin/env python3
"""Quick verification of current DB state after setup_demo_data.py"""
import pyodbc

CS = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=sql-sales-analytics-d9bt2m.database.windows.net;"
    "DATABASE=SalesAnalyticsDB;UID=sqladmin;PWD=SqlP@ssw0rd2026!;"
    "Encrypt=yes;TrustServerCertificate=no"
)
conn = pyodbc.connect(CS, timeout=30)
cur = conn.cursor()

print("=== TABLE ROW COUNTS ===")
tables = [
    "SalesTransactions", "SalesForecast", "SalesAlerts", "HourlySalesSummary",
    "Products", "StoreRegions", "SecurityMapping", "AccessAudit",
    "LatencyBenchmark", "ModelRegistry"
]
for t in tables:
    try:
        cur.execute(f"SELECT COUNT(*) FROM [{t}]")
        print(f"  {t}: {cur.fetchone()[0]:,}")
    except Exception as e:
        print(f"  {t}: MISSING ({e})")

print("\n=== DATE DISTRIBUTION ===")
cur.execute("""
    SELECT CAST(event_time AS DATE) d, COUNT(*) cnt,
           CAST(SUM(revenue) AS DECIMAL(18,0)) rev,
           MIN(DATEPART(HOUR, event_time)) AS min_h,
           MAX(DATEPART(HOUR, event_time)) AS max_h
    FROM SalesTransactions GROUP BY CAST(event_time AS DATE) ORDER BY d
""")
for r in cur.fetchall():
    print(f"  {r[0]}: {r[1]:>8,} txns | ${r[2]:>12,} | hours {r[3]}-{r[4]}")

print("\n=== STORE BREAKDOWN ===")
cur.execute("""
    SELECT store_id, COUNT(*) cnt, CAST(SUM(revenue) AS DECIMAL(18,0)) rev
    FROM SalesTransactions GROUP BY store_id ORDER BY store_id
""")
for r in cur.fetchall():
    print(f"  {r[0]}: {r[1]:>8,} txns | ${r[2]:>12,}")

print("\n=== TOP 10 PRODUCTS ===")
cur.execute("""
    SELECT TOP 10 p.product_name, t.product_id, COUNT(*) cnt,
           CAST(SUM(t.revenue) AS DECIMAL(18,0)) rev
    FROM SalesTransactions t
    LEFT JOIN Products p ON t.product_id = p.product_id
    GROUP BY p.product_name, t.product_id ORDER BY rev DESC
""")
for r in cur.fetchall():
    name = r[0] or r[1]
    print(f"  {name:>20}: {r[2]:>8,} txns | ${r[3]:>12,}")

print("\n=== INGEST LAG ===")
cur.execute("""
    SELECT MIN(ingest_lag_seconds), MAX(ingest_lag_seconds),
           AVG(CAST(ingest_lag_seconds AS FLOAT))
    FROM SalesTransactions
""")
r = cur.fetchone()
print(f"  min={r[0]}, max={r[1]}, avg={r[2]:.2f}s")

print("\n=== DoD GROWTH ===")
cur.execute("SELECT * FROM vw_DoDGrowthOverall WHERE date_rank <= 5 ORDER BY date_rank")
for r in cur.fetchall():
    dod = f"{r.dod_growth_pct:.2f}%" if r.dod_growth_pct is not None else "N/A"
    print(f"  {r.sale_date} | ${r.daily_revenue:,.0f} | DoD={dod}")

print("\n=== PERFORMANCE METRICS (recent) ===")
try:
    cur.execute("""
        SELECT TOP 5 metric_date, metric_hour, event_count, events_per_second,
               avg_latency_sec, sla_pct_under_5sec
        FROM vw_PerformanceMetrics ORDER BY metric_date DESC, metric_hour DESC
    """)
    for r in cur.fetchall():
        print(f"  {r[0]} H{r[1]:02d}: {r[2]:,} events, {r[3]}/s, lat={r[4]}s, SLA={r[5]}%")
except Exception as e:
    print(f"  vw_PerformanceMetrics: {e}")

print("\n=== VIEWS CHECK ===")
views = ["vw_ProductSales", "vw_DoDGrowth", "vw_DoDGrowthOverall",
         "vw_PerformanceMetrics", "vw_SalesByRegion", "vw_RegionalSummary",
         "vw_ForecastAccuracy", "vw_DailyRevenueTrend"]
for v in views:
    try:
        cur.execute(f"SELECT TOP 1 * FROM [{v}]")
        print(f"  {v}: OK")
    except:
        print(f"  {v}: MISSING")

conn.close()
print("\nDone!")
