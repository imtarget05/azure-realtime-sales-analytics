#!/usr/bin/env python3
"""
Complete all remaining setup that setup_demo_data.py missed:
1. Generate missing data for Apr 6 (remainder), Apr 7, Apr 9
2. Spread Apr 8 data across more hours
3. Create SecurityMapping, AccessAudit, LatencyBenchmark tables
4. Create vw_PerformanceMetrics view
5. Refresh HourlySalesSummary and DoD views
"""
import pyodbc, random
from datetime import datetime, timedelta

CS = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=sql-sales-analytics-d9bt2m.database.windows.net;"
    "DATABASE=SalesAnalyticsDB;UID=sqladmin;PWD=SqlP@ssw0rd2026!;"
    "Encrypt=yes;TrustServerCertificate=no"
)
conn = pyodbc.connect(CS, timeout=60, autocommit=True)
cur = conn.cursor()

products = {
    "BREAD": ("Bakery", 0.80, 1.50), "COKE": ("Beverage", 1.20, 1.80),
    "MILK": ("Dairy", 1.30, 1.90), "PEPSI": ("Beverage", 1.10, 1.70),
    "P001": ("Electronics", 800, 1200), "P002": ("Electronics", 500, 900),
    "P003": ("Electronics", 100, 200), "P004": ("Electronics", 350, 650),
    "P005": ("Electronics", 200, 400), "P006": ("Clothing", 20, 45),
    "P007": ("Clothing", 40, 80), "P008": ("Clothing", 60, 120),
    "P009": ("Home", 50, 110), "P010": ("Home", 30, 70),
    "P011": ("Home", 20, 50), "P012": ("Accessories", 30, 65),
    "P013": ("Accessories", 80, 175), "P014": ("Electronics", 25, 55),
    "P015": ("Electronics", 45, 95), "P016": ("Beverage", 1.80, 3.20),
    "P017": ("Beverage", 1.20, 2.30), "P018": ("Bakery", 2.50, 4.50),
    "P019": ("Dairy", 0.80, 1.60), "P020": ("Dairy", 3.50, 6.30),
    "P021": ("Snacks", 1.30, 2.70), "P022": ("Snacks", 2.50, 4.50),
    "P023": ("Snacks", 1.50, 3.10), "P024": ("Health & Beauty", 8, 18),
    "P025": ("Health & Beauty", 5, 12), "P026": ("Health & Beauty", 2, 5),
    "P027": ("Sports", 16, 35), "P028": ("Sports", 9, 21),
    "P029": ("Stationery", 3.5, 8.5), "P030": ("Stationery", 0.80, 2.10),
    "P031": ("Toys", 5.5, 12),
}
stores = ["S01", "S02", "S03"]
store_weights = [0.38, 0.28, 0.34]
weathers = ["sunny", "cloudy", "rainy"]
weather_weights = [0.4, 0.4, 0.2]
hourly_weight = {
    8: 0.5, 9: 0.7, 10: 0.9, 11: 1.2, 12: 1.5, 13: 1.3,
    14: 1.0, 15: 0.9, 16: 1.0, 17: 1.3, 18: 1.5, 19: 1.4,
    20: 1.1, 21: 0.8, 22: 0.4
}
pids = list(products.keys())
p_weights = []
for pid in pids:
    cat = products[pid][0]
    if cat in ("Beverage", "Dairy", "Bakery"):
        p_weights.append(10)
    elif cat == "Electronics":
        p_weights.append(0.3)
    else:
        p_weights.append(1)


def generate_day(date, target_rev):
    base_temp = random.uniform(25, 35)
    is_holiday = 1 if date.weekday() == 6 else 0
    batch = []
    acc = 0
    while acc < target_rev:
        hour = random.choices(list(hourly_weight.keys()), weights=list(hourly_weight.values()))[0]
        event_time = date.replace(hour=hour, minute=random.randint(0,59), second=random.randint(0,59))
        store = random.choices(stores, weights=store_weights)[0]
        product_id = random.choices(pids, weights=p_weights)[0]
        cat, lo, hi = products[product_id]
        price = round(random.uniform(lo, hi), 2)
        units = random.randint(1, 8) if cat in ("Beverage","Dairy","Bakery","Snacks") else (random.randint(1,2) if cat == "Electronics" else random.randint(1,4))
        rev = round(price * units, 2)
        temp = round(base_temp + random.uniform(-3, 3), 1)
        weather = random.choices(weathers, weights=weather_weights)[0]
        lag = random.randint(1, 5)
        enqueued = event_time + timedelta(seconds=lag)
        batch.append((event_time, store, product_id, units, price, rev, temp, weather, is_holiday, cat, enqueued, lag))
        acc += rev
        if len(batch) >= 2000:
            cur.executemany("""
                INSERT INTO SalesTransactions
                (event_time, store_id, product_id, units_sold, unit_price,
                 revenue, temperature, weather, holiday, category,
                 enqueued_time, ingest_lag_seconds)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """, batch)
            batch = []
    if batch:
        cur.executemany("""
            INSERT INTO SalesTransactions
            (event_time, store_id, product_id, units_sold, unit_price,
             revenue, temperature, weather, holiday, category,
             enqueued_time, ingest_lag_seconds)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, batch)
    return acc


# ── STEP 1: Check and fill missing days ──
print("=== STEP 1: Fill missing date data ===")
cur.execute("""
    SELECT CAST(event_time AS DATE) d, CAST(SUM(revenue) AS INT) rev
    FROM SalesTransactions
    GROUP BY CAST(event_time AS DATE)
    ORDER BY d
""")
existing = {str(r[0]): r[1] for r in cur.fetchall()}
print(f"  Existing: {existing}")

targets = {
    "2026-04-06": 195000,  # partial, needs ~85K more
    "2026-04-07": 250000,  # completely missing
    "2026-04-09": 210000,  # completely missing
}

for date_str, target in targets.items():
    current = existing.get(date_str, 0)
    if current < target * 0.95:
        needed = target - current
        print(f"  {date_str}: have ${current:,}, need ${target:,}, generating ${needed:,}...")
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        actual = generate_day(dt, needed)
        print(f"    Generated ${actual:,.0f}")
    else:
        print(f"  {date_str}: already at ${current:,} (target ${target:,}), skip")

# ── STEP 2: Spread Apr 8 data across more hours ──
print("\n=== STEP 2: Spread Apr 8 across hours ===")
cur.execute("""
    SELECT COUNT(*), CAST(SUM(revenue) AS INT)
    FROM SalesTransactions
    WHERE CAST(event_time AS DATE) = '2026-04-08'
    AND DATEPART(HOUR, event_time) = 13
""")
r = cur.fetchone()
h13_count, h13_rev = r[0], r[1]
print(f"  Apr 8 hour 13: {h13_count:,} txns, ${h13_rev:,}")

# Redistribute some existing hour-13 transactions to other hours
# Update random ~75% of hour-13 to spread across 8-22
cur.execute("""
    UPDATE TOP (10000) SalesTransactions
    SET event_time = DATEADD(HOUR, 
        CAST(ABS(CHECKSUM(NEWID())) % 15 AS INT) - 5,
        event_time),
        enqueued_time = DATEADD(HOUR, 
        CAST(ABS(CHECKSUM(NEWID())) % 15 AS INT) - 5,
        enqueued_time)
    WHERE CAST(event_time AS DATE) = '2026-04-08'
    AND DATEPART(HOUR, event_time) = 13
""")
print(f"  Redistributed {cur.rowcount} txns from hour 13 to spread")

# Also spread Apr 3 data (currently 17-19 only)
print("\n=== STEP 2b: Spread Apr 3 across hours ===")
cur.execute("""
    UPDATE TOP (220000) SalesTransactions
    SET event_time = DATEADD(HOUR,
        CAST(ABS(CHECKSUM(NEWID())) % 15 AS INT) - 9,
        event_time),
        enqueued_time = DATEADD(HOUR,
        CAST(ABS(CHECKSUM(NEWID())) % 15 AS INT) - 9,
        enqueued_time)
    WHERE CAST(event_time AS DATE) = '2026-04-03'
    AND DATEPART(HOUR, event_time) BETWEEN 17 AND 19
""")
print(f"  Redistributed {cur.rowcount} Apr 3 txns across hours")

# ── STEP 3: Create SecurityMapping ──
print("\n=== STEP 3: Create SecurityMapping ===")
cur.execute("""
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'SecurityMapping')
    CREATE TABLE dbo.SecurityMapping (
        id INT IDENTITY(1,1) PRIMARY KEY,
        user_email NVARCHAR(255) NOT NULL,
        user_role NVARCHAR(50) NOT NULL,
        allowed_store_ids NVARCHAR(255) NOT NULL,
        allowed_regions NVARCHAR(255) NOT NULL,
        created_at DATETIME2 DEFAULT GETUTCDATE(),
        last_modified DATETIME2 DEFAULT GETUTCDATE()
    )
""")
cur.execute("SELECT COUNT(*) FROM SecurityMapping")
if cur.fetchone()[0] == 0:
    mappings = [
        ("manager_north@company.com", "Store Manager", "S02", "Miền Bắc"),
        ("manager_south@company.com", "Store Manager", "S01", "Miền Nam"),
        ("manager_central@company.com", "Store Manager", "S03", "Miền Trung"),
        ("director@company.com", "Director", "S01,S02,S03", "Miền Bắc,Miền Nam,Miền Trung"),
        ("analyst@company.com", "Analyst", "S01,S02,S03", "Miền Bắc,Miền Nam,Miền Trung"),
    ]
    for m in mappings:
        cur.execute("INSERT INTO SecurityMapping (user_email,user_role,allowed_store_ids,allowed_regions) VALUES (?,?,?,?)", m)
    print(f"  Inserted {len(mappings)} mappings")
else:
    print("  Already populated")

# ── STEP 4: Create AccessAudit ──
print("\n=== STEP 4: Create AccessAudit ===")
cur.execute("""
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'AccessAudit')
    CREATE TABLE dbo.AccessAudit (
        id INT IDENTITY(1,1) PRIMARY KEY,
        access_time DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
        user_email NVARCHAR(255) NOT NULL,
        action NVARCHAR(50) NOT NULL,
        table_name NVARCHAR(128),
        row_count INT,
        ip_address NVARCHAR(50),
        user_agent NVARCHAR(255),
        result NVARCHAR(20) DEFAULT 'SUCCESS'
    )
""")
cur.execute("SELECT COUNT(*) FROM AccessAudit")
if cur.fetchone()[0] == 0:
    entries = []
    users = [
        ("director@company.com", ["SalesTransactions", "SalesForecast", "SalesAlerts", "ModelRegistry"]),
        ("manager_north@company.com", ["SalesTransactions", "HourlySalesSummary"]),
        ("manager_south@company.com", ["SalesTransactions", "HourlySalesSummary"]),
        ("manager_central@company.com", ["SalesTransactions", "HourlySalesSummary"]),
        ("analyst@company.com", ["SalesTransactions", "SalesForecast", "vw_ProductSales"]),
    ]
    for days_ago in range(7):
        base = datetime(2026, 4, 9) - timedelta(days=days_ago)
        for user, tables in users:
            for _ in range(random.randint(2, 8)):
                t = base.replace(hour=random.randint(8,18), minute=random.randint(0,59), second=random.randint(0,59))
                tbl = random.choice(tables)
                entries.append((t, user, "VIEW", tbl, random.randint(100,50000),
                               f"10.0.{random.randint(1,5)}.{random.randint(10,200)}", "Power BI Desktop", "SUCCESS"))
        # Denied attempt
        dt = base.replace(hour=random.randint(9,17), minute=random.randint(0,59))
        entries.append((dt, "manager_north@company.com", "VIEW", "SalesTransactions", 0,
                        "10.0.2.55", "Power BI Desktop", "DENIED"))
    cur.executemany("""
        INSERT INTO AccessAudit (access_time,user_email,action,table_name,row_count,ip_address,user_agent,result)
        VALUES (?,?,?,?,?,?,?,?)
    """, entries)
    print(f"  Inserted {len(entries)} entries")
else:
    print("  Already populated")

# ── STEP 5: Create LatencyBenchmark ──
print("\n=== STEP 5: Create LatencyBenchmark ===")
cur.execute("""
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'LatencyBenchmark')
    CREATE TABLE dbo.LatencyBenchmark (
        id INT IDENTITY(1,1) PRIMARY KEY,
        test_time DATETIME2 DEFAULT GETUTCDATE(),
        test_type NVARCHAR(50) NOT NULL,
        events_per_second INT,
        avg_latency_ms DECIMAL(10,2),
        p50_latency_ms DECIMAL(10,2),
        p95_latency_ms DECIMAL(10,2),
        p99_latency_ms DECIMAL(10,2),
        error_rate_pct DECIMAL(5,3),
        cpu_pct DECIMAL(5,1),
        memory_gb DECIMAL(5,2)
    )
""")
cur.execute("SELECT COUNT(*) FROM LatencyBenchmark")
if cur.fetchone()[0] == 0:
    benchmarks = [
        ("baseline",    100,  2100, 1800, 3200, 4500, 0.01,  8.5, 0.6),
        ("medium_load", 250,  2300, 2000, 3800, 5100, 0.02, 18.2, 0.8),
        ("high_load",   500,  2800, 2400, 4500, 6200, 0.05, 35.0, 1.2),
        ("stress",      750,  3400, 2900, 5200, 7100, 0.08, 52.0, 1.5),
        ("peak",       1000,  4100, 3500, 6100, 8000, 0.15, 68.0, 1.8),
    ]
    for b in benchmarks:
        cur.execute("""
            INSERT INTO LatencyBenchmark
            (test_type,events_per_second,avg_latency_ms,p50_latency_ms,
             p95_latency_ms,p99_latency_ms,error_rate_pct,cpu_pct,memory_gb)
            VALUES (?,?,?,?,?,?,?,?,?)
        """, b)
    print(f"  Inserted {len(benchmarks)} benchmarks")
else:
    print("  Already populated")

# ── STEP 6: Create vw_PerformanceMetrics ──
print("\n=== STEP 6: Create vw_PerformanceMetrics ===")
cur.execute("DROP VIEW IF EXISTS vw_PerformanceMetrics")
cur.execute("""
CREATE VIEW vw_PerformanceMetrics AS
SELECT
    CAST(event_time AS DATE) AS metric_date,
    DATEPART(HOUR, event_time) AS metric_hour,
    COUNT(*) AS event_count,
    CAST(COUNT(*) * 1.0 / 3600 AS DECIMAL(10,2)) AS events_per_second,
    MIN(ingest_lag_seconds) AS min_latency_sec,
    MAX(ingest_lag_seconds) AS max_latency_sec,
    CAST(AVG(CAST(ingest_lag_seconds AS FLOAT)) AS DECIMAL(5,2)) AS avg_latency_sec,
    CAST(SUM(revenue) AS DECIMAL(18,2)) AS hourly_revenue,
    COUNT(DISTINCT store_id) AS active_stores,
    COUNT(DISTINCT product_id) AS active_products,
    CAST(100.0 * SUM(CASE WHEN ingest_lag_seconds <= 5 THEN 1 ELSE 0 END)
         / COUNT(*) AS DECIMAL(5,2)) AS sla_pct_under_5sec
FROM SalesTransactions
GROUP BY CAST(event_time AS DATE), DATEPART(HOUR, event_time)
""")
print("  Created")

# ── STEP 7: Refresh HourlySalesSummary ──
print("\n=== STEP 7: Refresh HourlySalesSummary ===")
cur.execute("DELETE FROM HourlySalesSummary")
cur.execute("""
    INSERT INTO HourlySalesSummary
    (window_start, window_end, store_id, total_events, total_revenue,
     avg_price, total_units, category_breakdown)
    SELECT
        DATEADD(HOUR, DATEDIFF(HOUR, 0, event_time), 0),
        DATEADD(HOUR, DATEDIFF(HOUR, 0, event_time) + 1, 0),
        store_id,
        COUNT(*),
        CAST(SUM(revenue) AS DECIMAL(18,2)),
        CAST(AVG(unit_price) AS DECIMAL(10,2)),
        SUM(units_sold),
        STRING_AGG(DISTINCT category, ',')
    FROM SalesTransactions
    GROUP BY DATEADD(HOUR, DATEDIFF(HOUR, 0, event_time), 0),
             DATEADD(HOUR, DATEDIFF(HOUR, 0, event_time) + 1, 0),
             store_id
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

# Also refresh vw_ProductSales
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
for r in cur.fetchall():
    print(f"  {r[0]}: {r[1]:>8,} txns | ${r[2]:>12,} | hours {r[3]}-{r[4]} ({r[5]} unique)")

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

print("\nPerformance metrics sample:")
cur.execute("""
    SELECT TOP 5 metric_date, metric_hour, event_count, events_per_second,
           avg_latency_sec, sla_pct_under_5sec
    FROM vw_PerformanceMetrics ORDER BY metric_date DESC, metric_hour DESC
""")
for r in cur.fetchall():
    print(f"  {r[0]} H{r[1]:02d}: {r[2]:,} evts, {r[3]}/s, lat={r[4]}s, SLA={r[5]}%")

conn.close()
print("\n=== ALL DONE ===")
