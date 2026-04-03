"""Seed SalesForecast table from existing SalesTransactions data."""
import pyodbc, os
from dotenv import load_dotenv
load_dotenv()

conn = pyodbc.connect(
    f'DRIVER={{ODBC Driver 18 for SQL Server}};'
    f'SERVER={os.getenv("SQL_SERVER")};'
    f'DATABASE={os.getenv("SQL_DATABASE")};'
    f'UID={os.getenv("SQL_USERNAME")};'
    f'PWD={os.getenv("SQL_PASSWORD")};'
    f'Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30'
)
cur = conn.cursor()

# Check current count
cur.execute('SELECT COUNT(*) FROM dbo.SalesForecast')
before = cur.fetchone()[0]
print(f"SalesForecast before seed: {before} rows")

if before == 0:
    # Seed from actual transactions with slight offset to simulate "old model predictions"
    seed_sql = """
    INSERT INTO dbo.SalesForecast
        (forecast_date, forecast_hour, store_id, category,
         predicted_quantity, predicted_revenue, 
         confidence_lower, confidence_upper, model_version)
    SELECT
        CAST(event_time AS date) AS forecast_date,
        DATEPART(hour, event_time) AS forecast_hour,
        store_id,
        category,
        CAST(SUM(units_sold) * 0.92 AS int) AS predicted_quantity,
        ROUND(SUM(revenue) * 0.90, 2) AS predicted_revenue,
        ROUND(SUM(revenue) * 0.80, 2) AS confidence_lower,
        ROUND(SUM(revenue) * 1.05, 2) AS confidence_upper,
        'v5.0-baseline' AS model_version
    FROM dbo.SalesTransactions
    GROUP BY CAST(event_time AS date), DATEPART(hour, event_time), store_id, category
    HAVING COUNT(*) >= 2
    ORDER BY CAST(event_time AS date) DESC, forecast_hour DESC;
    """
    cur.execute(seed_sql)
    conn.commit()
    
    cur.execute('SELECT COUNT(*) FROM dbo.SalesForecast')
    after = cur.fetchone()[0]
    print(f"SalesForecast after seed: {after} rows")
    
    # Show sample
    cur.execute('SELECT TOP 5 forecast_date, forecast_hour, store_id, category, predicted_revenue, model_version FROM dbo.SalesForecast ORDER BY forecast_date DESC')
    print("\nSample rows:")
    for row in cur.fetchall():
        print(f"  {row[0]} H{row[1]:02d} | {row[2]} | {row[3]:10} | ${row[4]:>8.2f} | {row[5]}")
else:
    print("SalesForecast already has data, skipping seed.")

conn.close()
print("\nDone!")
