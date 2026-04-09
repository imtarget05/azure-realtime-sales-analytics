#!/usr/bin/env python3
"""Add store region mapping + sample data for Power BI regional segmentation."""
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

# =================================================================
# 1. Create StoreRegions dimension table
# =================================================================
print("=== 1. Create StoreRegions dimension table ===")
cur.execute("""
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'StoreRegions')
BEGIN
    CREATE TABLE dbo.StoreRegions (
        store_id    NVARCHAR(20)  PRIMARY KEY,
        store_name  NVARCHAR(100) NOT NULL,
        region      NVARCHAR(50)  NOT NULL,
        city        NVARCHAR(100) NOT NULL,
        province    NVARCHAR(100) NOT NULL,
        latitude    FLOAT         NULL,
        longitude   FLOAT         NULL,
        store_type  NVARCHAR(50)  NOT NULL DEFAULT 'Standard',
        open_date   DATE          NOT NULL DEFAULT '2026-01-01'
    );
    PRINT 'Created table StoreRegions';
END
ELSE
    PRINT 'Table StoreRegions already exists';
""")

# Insert store data with regions
cur.execute("SELECT COUNT(*) FROM dbo.StoreRegions")
cnt = cur.fetchone()[0]
if cnt == 0:
    stores = [
        ("S01", "Cửa hàng Quận 1",     "Miền Nam", "TP. Hồ Chí Minh", "TP.HCM",     10.7769, 106.7009, "Flagship",  "2025-06-15"),
        ("S02", "Cửa hàng Hoàn Kiếm",  "Miền Bắc", "Hà Nội",          "Hà Nội",      21.0285, 105.8542, "Standard",  "2025-08-01"),
        ("S03", "Cửa hàng Hải Châu",    "Miền Trung","Đà Nẵng",        "Đà Nẵng",     16.0544, 108.2022, "Standard",  "2025-10-01"),
    ]
    for s in stores:
        cur.execute("""
            INSERT INTO dbo.StoreRegions (store_id, store_name, region, city, province, latitude, longitude, store_type, open_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, s)
    print(f"  Inserted {len(stores)} stores with regions")
else:
    print(f"  Already has {cnt} stores, skipping")

# Verify
cur.execute("SELECT store_id, store_name, region, city FROM dbo.StoreRegions ORDER BY store_id")
for r in cur.fetchall():
    print(f"  {r[0]}: {r[1]} ({r[2]} - {r[3]})")

# =================================================================
# 2. Create vw_SalesByRegion view for Power BI
# =================================================================
print("\n=== 2. Create vw_SalesByRegion view ===")
cur.execute("IF OBJECT_ID('dbo.vw_SalesByRegion', 'V') IS NOT NULL DROP VIEW dbo.vw_SalesByRegion")
cur.execute("""
CREATE VIEW dbo.vw_SalesByRegion AS
SELECT
    t.event_time,
    CAST(t.event_time AS DATE) AS sale_date,
    DATEPART(HOUR, t.event_time) AS sale_hour,
    t.store_id,
    r.store_name,
    r.region,
    r.city,
    r.province,
    r.store_type,
    t.product_id,
    t.category,
    t.units_sold,
    t.unit_price,
    t.revenue,
    t.temperature,
    t.weather,
    t.holiday
FROM dbo.SalesTransactions t
INNER JOIN dbo.StoreRegions r ON t.store_id = r.store_id
""")
print("  Created vw_SalesByRegion (SalesTransactions JOIN StoreRegions)")

# Verify
cur.execute("""
    SELECT region, COUNT(*) as txns, CAST(SUM(revenue) AS BIGINT) as rev
    FROM dbo.vw_SalesByRegion
    GROUP BY region
    ORDER BY rev DESC
""")
print("  Revenue by region:")
for r in cur.fetchall():
    print(f"    {r[0]}: {r[1]:,} txns, revenue={r[2]:,}")

# =================================================================
# 3. Create vw_RegionalSummary for dashboard cards
# =================================================================
print("\n=== 3. Create vw_RegionalSummary view ===")
cur.execute("IF OBJECT_ID('dbo.vw_RegionalSummary', 'V') IS NOT NULL DROP VIEW dbo.vw_RegionalSummary")
cur.execute("""
CREATE VIEW dbo.vw_RegionalSummary AS
SELECT
    r.region,
    r.city,
    CAST(t.event_time AS DATE) AS sale_date,
    COUNT(*) AS total_transactions,
    SUM(t.units_sold) AS total_units,
    CAST(SUM(t.revenue) AS DECIMAL(18,2)) AS total_revenue,
    CAST(AVG(t.revenue) AS DECIMAL(10,2)) AS avg_order_value,
    COUNT(DISTINCT t.product_id) AS unique_products,
    COUNT(DISTINCT t.category) AS unique_categories,
    AVG(t.temperature) AS avg_temperature
FROM dbo.SalesTransactions t
INNER JOIN dbo.StoreRegions r ON t.store_id = r.store_id
GROUP BY r.region, r.city, CAST(t.event_time AS DATE)
""")
print("  Created vw_RegionalSummary")

# =================================================================
# 4. Update vw_AlertSummary with regions
# =================================================================
print("\n=== 4. Update vw_AlertSummary with regions ===")
cur.execute("IF OBJECT_ID('dbo.vw_AlertSummaryRegion', 'V') IS NOT NULL DROP VIEW dbo.vw_AlertSummaryRegion")
cur.execute("""
CREATE VIEW dbo.vw_AlertSummaryRegion AS
SELECT
    CAST(a.alert_time AS DATE) AS alert_date,
    a.store_id,
    r.region,
    r.city,
    a.type AS alert_type,
    a.severity,
    COUNT(*) AS alert_count,
    AVG(a.value) AS avg_value,
    MAX(a.value) AS max_value,
    SUM(a.is_high_value) AS high_value_count
FROM dbo.SalesAlerts a
INNER JOIN dbo.StoreRegions r ON a.store_id = r.store_id
GROUP BY CAST(a.alert_time AS DATE), a.store_id, r.region, r.city, a.type, a.severity
""")
print("  Created vw_AlertSummaryRegion")

# =================================================================
# 5. Update vw_ForecastAccuracy with regions
# =================================================================
print("\n=== 5. Update vw_ForecastAccuracy with regions ===")
cur.execute("IF OBJECT_ID('dbo.vw_ForecastByRegion', 'V') IS NOT NULL DROP VIEW dbo.vw_ForecastByRegion")
cur.execute("""
CREATE VIEW dbo.vw_ForecastByRegion AS
SELECT
    f.forecast_date,
    f.store_id,
    r.region,
    r.city,
    f.category,
    CAST(SUM(CAST(f.predicted_revenue AS FLOAT)) AS DECIMAL(18,2)) AS predicted_revenue,
    COALESCE(a.actual_revenue, 0) AS actual_revenue,
    f.model_version
FROM dbo.SalesForecast f
INNER JOIN dbo.StoreRegions r ON f.store_id = r.store_id
LEFT JOIN (
    SELECT
        CAST(event_time AS DATE) AS sale_date,
        store_id,
        category,
        CAST(SUM(revenue) AS DECIMAL(18,2)) AS actual_revenue
    FROM dbo.SalesTransactions
    GROUP BY CAST(event_time AS DATE), store_id, category
) a ON f.forecast_date = a.sale_date
    AND f.store_id = a.store_id
    AND f.category = a.category
GROUP BY f.forecast_date, f.store_id, r.region, r.city, f.category, a.actual_revenue, f.model_version
""")
print("  Created vw_ForecastByRegion")

conn.close()
print("\n=== All regional SQL entities created. ===")
