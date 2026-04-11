"""Fix SQL schema: add forecast_datetime computed column and recreate view."""
import pyodbc

conn_str = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=sql-sales-analytics-d9bt2m.database.windows.net;"
    "DATABASE=SalesAnalyticsDB;"
    "UID=sqladmin;PWD=SqlP@ssw0rd2026!;"
    "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30"
)
conn = pyodbc.connect(conn_str)
conn.autocommit = True
cursor = conn.cursor()

# 1. Check current columns
cursor.execute(
    "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
    "WHERE TABLE_NAME='SalesForecast' ORDER BY ORDINAL_POSITION"
)
cols = [r[0] for r in cursor.fetchall()]
print("SalesForecast columns:", cols)

# 2. Add forecast_datetime computed column if missing
if "forecast_datetime" not in cols:
    print("Adding forecast_datetime computed column...")
    cursor.execute(
        "ALTER TABLE dbo.SalesForecast ADD forecast_datetime AS "
        "DATEADD(HOUR, ISNULL(forecast_hour, 0), CAST(forecast_date AS DATETIME2)) PERSISTED"
    )
    print("  -> Added successfully")
else:
    print("forecast_datetime column already exists")

# 3. Recreate the view
print("Recreating vw_ForecastVsActual view...")
cursor.execute("IF EXISTS (SELECT * FROM sys.views WHERE name='vw_ForecastVsActual') DROP VIEW vw_ForecastVsActual")
cursor.execute("""
CREATE VIEW dbo.vw_ForecastVsActual AS
SELECT
    f.forecast_date,
    f.forecast_hour,
    f.forecast_datetime,
    f.store_id,
    f.category,
    f.predicted_quantity,
    f.predicted_revenue,
    f.confidence_lower,
    f.confidence_upper,
    ISNULL(a.actual_quantity, 0) AS actual_quantity,
    ISNULL(a.actual_revenue, 0) AS actual_revenue,
    ABS(f.predicted_revenue - ISNULL(a.actual_revenue, 0)) AS forecast_error,
    f.model_version
FROM dbo.SalesForecast f
LEFT JOIN (
    SELECT
        CAST(event_time AS DATE) AS sale_date,
        DATEPART(HOUR, event_time) AS sale_hour,
        store_id,
        category,
        SUM(units_sold) AS actual_quantity,
        SUM(revenue) AS actual_revenue
    FROM dbo.SalesTransactions
    GROUP BY CAST(event_time AS DATE), DATEPART(HOUR, event_time), store_id, category
) a ON f.forecast_date = a.sale_date
    AND f.forecast_hour = a.sale_hour
    AND f.store_id = a.store_id
    AND f.category = a.category
""")
print("  -> View recreated successfully")

# 4. Verify
cursor.execute(
    "SELECT TOP 1 * FROM dbo.vw_ForecastVsActual"
)
cols = [desc[0] for desc in cursor.description]
print(f"View columns: {cols}")
row = cursor.fetchone()
print(f"Has data: {row is not None}")

conn.close()
print("\nDone! SQL schema fixed.")
