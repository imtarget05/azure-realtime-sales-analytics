#!/usr/bin/env python3
"""Complete remaining steps 7-9 that failed in complete_setup.py"""
import pyodbc

CS = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=sql-sales-analytics-d9bt2m.database.windows.net;"
    "DATABASE=SalesAnalyticsDB;UID=sqladmin;PWD=SqlP@ssw0rd2026!;"
    "Encrypt=yes;TrustServerCertificate=no"
)
conn = pyodbc.connect(CS, timeout=60, autocommit=True)
cur = conn.cursor()

# ── STEP 7: Refresh HourlySalesSummary ──
print("=== STEP 7: Refresh HourlySalesSummary ===")
cur.execute("DELETE FROM HourlySalesSummary")
# Actual schema: window_start, window_end, store_id, product_id, category,
#   units_sold, revenue, avg_price, tx_count, prev_5m_revenue, revenue_delta_5m,
#   rolling_15m_units, rolling_15m_revenue
cur.execute("""
    INSERT INTO HourlySalesSummary
    (window_start, window_end, store_id, product_id, category,
     units_sold, revenue, avg_price, tx_count,
     prev_5m_revenue, revenue_delta_5m, rolling_15m_units, rolling_15m_revenue)
    SELECT
        DATEADD(HOUR, DATEDIFF(HOUR, 0, event_time), 0),
        DATEADD(HOUR, DATEDIFF(HOUR, 0, event_time) + 1, 0),
        store_id,
        product_id,
        category,
        SUM(units_sold),
        SUM(revenue),
        AVG(unit_price),
        COUNT(*),
        0, 0, 0, 0
    FROM SalesTransactions
    GROUP BY DATEADD(HOUR, DATEDIFF(HOUR, 0, event_time), 0),
             DATEADD(HOUR, DATEDIFF(HOUR, 0, event_time) + 1, 0),
             store_id, product_id, category
""")
print(f"  Inserted {cur.rowcount} rows")

# ── STEP 8: Refresh DoD views ──
print("\n=== STEP 8: Refresh DoD views ===")
cur.execute("DROP VIEW IF EXISTS vw_DoDGrowth")
cur.execute("""
CREATE VIEW vw_DoDGrowth AS
WITH daily AS (
    SELECT sr.region, CAST(st.event_time AS DATE) AS sale_date,
        SUM(st.revenue) AS daily_revenue, SUM(st.units_sold) AS daily_quantity
    FROM SalesTransactions st
    JOIN StoreRegions sr ON st.store_id = sr.store_id
    GROUP BY sr.region, CAST(st.event_time AS DATE)
    HAVING SUM(st.revenue) > 100
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY sale_date DESC) AS date_rank
    FROM daily
)
SELECT r1.region, r1.sale_date, r1.daily_revenue, r1.daily_quantity,
    r2.daily_revenue AS prev_revenue, r2.sale_date AS prev_date,
    CASE WHEN r2.daily_revenue > 0
         THEN ROUND((r1.daily_revenue - r2.daily_revenue) * 100.0 / r2.daily_revenue, 2)
         ELSE NULL END AS dod_growth_pct,
    r1.date_rank
FROM ranked r1
LEFT JOIN ranked r2 ON r1.region = r2.region AND r1.date_rank = r2.date_rank - 1
""")

cur.execute("DROP VIEW IF EXISTS vw_DoDGrowthOverall")
cur.execute("""
CREATE VIEW vw_DoDGrowthOverall AS
WITH daily AS (
    SELECT CAST(event_time AS DATE) AS sale_date,
        SUM(revenue) AS daily_revenue, SUM(units_sold) AS daily_quantity
    FROM SalesTransactions
    GROUP BY CAST(event_time AS DATE)
    HAVING SUM(revenue) > 100
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (ORDER BY sale_date DESC) AS date_rank
    FROM daily
)
SELECT r1.sale_date, r1.daily_revenue, r1.daily_quantity,
    r2.daily_revenue AS prev_revenue, r2.sale_date AS prev_date,
    CASE WHEN r2.daily_revenue > 0
         THEN ROUND((r1.daily_revenue - r2.daily_revenue) * 100.0 / r2.daily_revenue, 2)
         ELSE NULL END AS dod_growth_pct,
    r1.date_rank
FROM ranked r1
LEFT JOIN ranked r2 ON r1.date_rank = r2.date_rank - 1
""")

cur.execute("DROP VIEW IF EXISTS vw_ProductSales")
cur.execute("""
CREATE VIEW dbo.vw_ProductSales AS
SELECT
    p.product_id, p.product_name, p.category, p.base_price,
    r.region, r.city,
    CAST(t.event_time AS DATE) AS sale_date,
    COUNT(*) AS transactions,
    SUM(t.units_sold) AS units_sold,
    CAST(SUM(t.revenue) AS DECIMAL(18,2)) AS revenue,
    CAST(AVG(t.unit_price) AS DECIMAL(10,2)) AS avg_price
FROM dbo.SalesTransactions t
INNER JOIN dbo.Products p ON t.product_id = p.product_id
INNER JOIN dbo.StoreRegions r ON t.store_id = r.store_id
GROUP BY p.product_id, p.product_name, p.category, p.base_price,
         r.region, r.city, CAST(t.event_time AS DATE)
""")
print("  All views refreshed")

# ── STEP 9: Final verification ──
print("\n=== FINAL VERIFICATION ===")
tables = ["SalesTransactions", "SalesForecast", "SalesAlerts", "HourlySalesSummary",
          "Products", "StoreRegions", "SecurityMapping", "AccessAudit",
          "LatencyBenchmark", "ModelRegistry"]
for t in tables:
    cur.execute(f"SELECT COUNT(*) FROM [{t}]")
    print(f"  {t}: {cur.fetchone()[0]:,}")

print("\nDate distribution:")
cur.execute("""
    SELECT CAST(event_time AS DATE) d, COUNT(*) cnt,
           CAST(SUM(revenue) AS DECIMAL(18,0)) rev,
           MIN(DATEPART(HOUR, event_time)) minh,
           MAX(DATEPART(HOUR, event_time)) maxh,
           COUNT(DISTINCT DATEPART(HOUR, event_time)) hours
    FROM SalesTransactions
    GROUP BY CAST(event_time AS DATE) ORDER BY d
""")
total_rev = 0
for r in cur.fetchall():
    total_rev += r[2]
    print(f"  {r[0]}: {r[1]:>8,} txns | ${r[2]:>12,} | hours {r[3]}-{r[4]} ({r[5]} unique)")
print(f"  TOTAL REVENUE: ${total_rev:>12,.0f}")

print("\nDoD Growth:")
cur.execute("SELECT * FROM vw_DoDGrowthOverall WHERE date_rank <= 7 ORDER BY date_rank")
for r in cur.fetchall():
    dod = f"{r.dod_growth_pct:+.1f}%" if r.dod_growth_pct is not None else "N/A"
    print(f"  {r.sale_date} | ${r.daily_revenue:>12,.0f} | DoD={dod}")

print("\nStore breakdown:")
cur.execute("""
    SELECT store_id, COUNT(*) cnt, CAST(SUM(revenue) AS DECIMAL(18,0)) rev
    FROM SalesTransactions GROUP BY store_id ORDER BY store_id
""")
for r in cur.fetchall():
    print(f"  {r[0]}: {r[1]:>8,} txns | ${r[2]:>12,}")

print("\nTop 5 products:")
cur.execute("""
    SELECT TOP 5 p.product_name, CAST(SUM(t.revenue) AS DECIMAL(18,0)) rev
    FROM SalesTransactions t JOIN Products p ON t.product_id = p.product_id
    GROUP BY p.product_name ORDER BY rev DESC
""")
for r in cur.fetchall():
    print(f"  {r[0]:>20}: ${r[1]:>12,}")

print("\nPerformance metrics (latest 5):")
cur.execute("""
    SELECT TOP 5 metric_date, metric_hour, event_count, events_per_second,
           avg_latency_sec, sla_pct_under_5sec
    FROM vw_PerformanceMetrics ORDER BY metric_date DESC, metric_hour DESC
""")
for r in cur.fetchall():
    print(f"  {r[0]} H{r[1]:02d}: {r[2]:,} evts, {r[3]}/s, lat={r[4]}s, SLA={r[5]}%")

print("\nLatency Benchmarks:")
cur.execute("SELECT test_type, events_per_second, avg_latency_ms, p95_latency_ms, error_rate_pct FROM LatencyBenchmark ORDER BY events_per_second")
for r in cur.fetchall():
    print(f"  {r[0]:>12}: {r[1]:>5} evt/s, avg={r[2]:.0f}ms, p95={r[3]:.0f}ms, err={r[4]:.3f}%")

conn.close()
print("\n=== ALL DONE ===")
