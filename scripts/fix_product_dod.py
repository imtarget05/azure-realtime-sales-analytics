#!/usr/bin/env python3
"""Fix remaining Power BI issues: product table + DoD + Products dimension."""
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
# 1. Create Products dimension table (so product_id slicer works)
# =================================================================
print("=== 1. Create Products dimension table ===")
cur.execute("""
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Products')
BEGIN
    CREATE TABLE dbo.Products (
        product_id    NVARCHAR(20)  PRIMARY KEY,
        product_name  NVARCHAR(100) NOT NULL,
        category      NVARCHAR(50)  NOT NULL,
        base_price    DECIMAL(10,2) NOT NULL DEFAULT 0
    );
    PRINT 'Created table Products';
END
ELSE
    PRINT 'Table Products already exists';
""")

cur.execute("SELECT COUNT(*) FROM dbo.Products")
cnt = cur.fetchone()[0]
if cnt == 0:
    # Get distinct products from SalesTransactions
    cur.execute("""
        SELECT DISTINCT product_id, category,
               CAST(AVG(unit_price) AS DECIMAL(10,2)) as avg_price
        FROM dbo.SalesTransactions
        GROUP BY product_id, category
    """)
    products = cur.fetchall()
    
    # Product name mapping
    names = {
        "BREAD": "Baguette Bread", "COKE": "Coca-Cola 330ml",
        "MILK": "Fresh Milk 1L", "PEPSI": "Pepsi 330ml",
    }
    
    for p in products:
        pid = p[0]
        cat = p[1]
        price = p[2]
        name = names.get(pid, f"Product {pid}")
        cur.execute(
            "INSERT INTO dbo.Products (product_id, product_name, category, base_price) VALUES (?, ?, ?, ?)",
            (pid, name, cat, price)
        )
    print(f"  Inserted {len(products)} products")
else:
    print(f"  Already has {cnt} products")

cur.execute("SELECT product_id, product_name, category, base_price FROM dbo.Products ORDER BY product_id")
for r in cur.fetchall():
    print(f"  {r[0]}: {r[1]} ({r[2]}) ${r[3]}")

# =================================================================
# 2. Create vw_ProductSales — pre-aggregated per product
# =================================================================
print("\n=== 2. Create vw_ProductSales view ===")
cur.execute("IF OBJECT_ID('dbo.vw_ProductSales', 'V') IS NOT NULL DROP VIEW dbo.vw_ProductSales")
cur.execute("""
CREATE VIEW dbo.vw_ProductSales AS
SELECT
    p.product_id,
    p.product_name,
    p.category,
    p.base_price,
    r.region,
    r.city,
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
print("  Created vw_ProductSales (aggregated per product per region per day)")

# Verify
cur.execute("""
    SELECT product_id, SUM(units_sold) as units, CAST(SUM(revenue) AS BIGINT) as rev
    FROM dbo.vw_ProductSales GROUP BY product_id ORDER BY rev DESC
""")
print("  Product sales (correct per-product values):")
for r in cur.fetchall():
    print(f"    {r[0]}: {r[1]:,} units, ${r[2]:,}")

# =================================================================
# 3. Fix DoD — create vw with consecutive date comparison
# =================================================================
print("\n=== 3. Fix DoD Growth view ===")
cur.execute("IF OBJECT_ID('dbo.vw_DoDGrowth', 'V') IS NOT NULL DROP VIEW dbo.vw_DoDGrowth")
cur.execute("""
CREATE VIEW dbo.vw_DoDGrowth AS
WITH daily AS (
    SELECT
        CAST(event_time AS DATE) AS sale_date,
        r.region,
        CAST(SUM(t.revenue) AS DECIMAL(18,2)) AS daily_revenue,
        COUNT(*) AS daily_orders,
        ROW_NUMBER() OVER (PARTITION BY r.region ORDER BY CAST(event_time AS DATE) DESC) AS date_rank
    FROM dbo.SalesTransactions t
    INNER JOIN dbo.StoreRegions r ON t.store_id = r.store_id
    GROUP BY CAST(event_time AS DATE), r.region
)
SELECT
    curr.sale_date,
    curr.region,
    curr.daily_revenue,
    curr.daily_orders,
    prev.daily_revenue AS prev_revenue,
    prev.daily_orders AS prev_orders,
    curr.date_rank,
    CASE
        WHEN prev.daily_revenue > 0
        THEN CAST((curr.daily_revenue - prev.daily_revenue) / prev.daily_revenue * 100 AS DECIMAL(10,2))
        ELSE NULL
    END AS dod_growth_pct
FROM daily curr
LEFT JOIN daily prev ON curr.region = prev.region AND curr.date_rank = prev.date_rank - 1
""")
print("  Created vw_DoDGrowth (compares latest vs previous date, ignoring gaps)")

cur.execute("""
    SELECT sale_date, region, daily_revenue, prev_revenue, dod_growth_pct, date_rank
    FROM dbo.vw_DoDGrowth
    WHERE date_rank <= 2
    ORDER BY region, date_rank
""")
print("  DoD Growth (latest 2 dates per region):")
for r in cur.fetchall():
    dod = f"{r[4]:.1f}%" if r[4] is not None else "N/A"
    print(f"    {r[1]} | {r[0]} | rev={r[2]} | prev={r[3]} | DoD={dod} | rank={r[5]}")

conn.close()
print("\n=== All product + DoD fixes applied. ===")
