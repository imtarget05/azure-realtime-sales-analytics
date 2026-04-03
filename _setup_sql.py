"""Create SQL tables on Azure SQL Database."""
import os
import pyodbc

SQL_SERVER = os.getenv("SQL_SERVER", "")
SQL_DATABASE = os.getenv("SQL_DATABASE", "SalesAnalyticsDB")
SQL_USERNAME = os.getenv("SQL_USERNAME", "")
SQL_PASSWORD = os.getenv("SQL_PASSWORD", "")

if not all([SQL_SERVER, SQL_DATABASE, SQL_USERNAME, SQL_PASSWORD]):
    raise RuntimeError("Missing SQL env vars: SQL_SERVER, SQL_DATABASE, SQL_USERNAME, SQL_PASSWORD")

conn_str = (
    "Driver={ODBC Driver 18 for SQL Server};"
    f"Server=tcp:{SQL_SERVER},1433;"
    f"Database={SQL_DATABASE};"
    f"Uid={SQL_USERNAME};"
    f"Pwd={SQL_PASSWORD};"
    "Encrypt=yes;TrustServerCertificate=no;"
)

conn = pyodbc.connect(conn_str)
conn.autocommit = True
cursor = conn.cursor()

# Execute DDL directly (pyodbc doesn't need GO separators)
tables_sql = """
-- SalesTransactions
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'SalesTransactions')
    CREATE TABLE dbo.SalesTransactions (
        id                  BIGINT IDENTITY(1,1) PRIMARY KEY,
        event_time          DATETIME2     NOT NULL,
        store_id            NVARCHAR(20)  NOT NULL,
        product_id          NVARCHAR(20)  NOT NULL,
        units_sold          BIGINT        NOT NULL,
        unit_price          FLOAT         NOT NULL,
        revenue             FLOAT         NOT NULL,
        temperature         FLOAT         NULL,
        weather             NVARCHAR(30)  NULL,
        holiday             BIGINT        NOT NULL DEFAULT 0,
        category            NVARCHAR(50)  NULL,
        enqueued_time       DATETIME2     NULL,
        ingest_lag_seconds  BIGINT        NULL,
        created_at          DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
    );
"""

cursor.execute(tables_sql)
print("SalesTransactions created")

cursor.execute("""
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'HourlySalesSummary')
    CREATE TABLE dbo.HourlySalesSummary (
        id                  BIGINT IDENTITY(1,1) PRIMARY KEY,
        window_start        DATETIME2     NOT NULL,
        window_end          DATETIME2     NOT NULL,
        store_id            NVARCHAR(20)  NOT NULL,
        product_id          NVARCHAR(20)  NOT NULL,
        category            NVARCHAR(50)  NULL,
        units_sold          BIGINT        NOT NULL,
        revenue             FLOAT         NOT NULL,
        avg_price           FLOAT         NULL,
        tx_count            BIGINT        NOT NULL,
        prev_5m_revenue     FLOAT         NOT NULL DEFAULT 0,
        revenue_delta_5m    FLOAT         NOT NULL DEFAULT 0,
        rolling_15m_units   BIGINT        NOT NULL DEFAULT 0,
        rolling_15m_revenue FLOAT         NOT NULL DEFAULT 0,
        created_at          DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
    );
""")
print("HourlySalesSummary created")

cursor.execute("""
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'SalesForecast')
    CREATE TABLE dbo.SalesForecast (
        id                  BIGINT IDENTITY(1,1) PRIMARY KEY,
        forecast_date       DATE          NOT NULL,
        forecast_hour       INT           NULL,
        store_id            NVARCHAR(20)  NULL,
        product_id          NVARCHAR(20)  NULL,
        category            NVARCHAR(50)  NULL,
        predicted_quantity  INT           NULL,
        predicted_revenue   DECIMAL(15,2) NULL,
        confidence_lower    DECIMAL(15,2) NULL,
        confidence_upper    DECIMAL(15,2) NULL,
        model_version       NVARCHAR(50)  NULL,
        created_at          DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
    );
""")
print("SalesForecast created")

cursor.execute("""
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'SalesAlerts')
    CREATE TABLE dbo.SalesAlerts (
        id                  BIGINT IDENTITY(1,1) PRIMARY KEY,
        alert_time          DATETIME2     NOT NULL,
        store_id            NVARCHAR(20)  NOT NULL,
        type                NVARCHAR(20)  NOT NULL,
        value               FLOAT         NOT NULL,
        created_at          DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
    );
""")
print("SalesAlerts created")

# Indexes
indexes = [
    ("IX_SalesTransactions_EventTime", "dbo.SalesTransactions(event_time, store_id, product_id)"),
    ("IX_SalesTransactions_StoreProduct", "dbo.SalesTransactions(store_id, product_id)"),
    ("IX_HourlySalesSummary_Window", "dbo.HourlySalesSummary(window_end, store_id, product_id)"),
    ("IX_SalesForecast_Date", "dbo.SalesForecast(forecast_date, store_id, category)"),
]

for idx_name, idx_def in indexes:
    try:
        cursor.execute(f"""
            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = '{idx_name}')
                CREATE NONCLUSTERED INDEX {idx_name} ON {idx_def};
        """)
        print(f"Index {idx_name} created")
    except Exception as e:
        print(f"Index {idx_name} error: {e}")

# View
try:
    cursor.execute("IF EXISTS (SELECT * FROM sys.views WHERE name = 'vw_RealtimeDashboard') DROP VIEW vw_RealtimeDashboard;")
    cursor.execute("""
        CREATE VIEW dbo.vw_RealtimeDashboard AS
        SELECT TOP 1000
            event_time, store_id, product_id, category,
            units_sold, unit_price, revenue, temperature, weather, holiday
        FROM dbo.SalesTransactions
        ORDER BY event_time DESC;
    """)
    print("View vw_RealtimeDashboard created")
except Exception as e:
    print(f"View error: {e}")

# Stored procedures
with open("sql/stored_procedures.sql", "r", encoding="utf-8-sig") as f:
    sp_sql = f.read()

import re
sp_batches = re.split(r'^\s*GO\s*$', sp_sql, flags=re.MULTILINE | re.IGNORECASE)
for i, batch in enumerate(sp_batches):
    clean = batch.strip()
    if clean and not all(line.strip().startswith('--') or not line.strip() for line in clean.split('\n')):
        try:
            cursor.execute(clean)
            print(f"SP batch {i+1} OK")
        except Exception as e:
            print(f"SP batch {i+1} error: {e}")

# Verify
cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE' ORDER BY TABLE_NAME")
for row in cursor.fetchall():
    print(f"  Table: {row[0]}")

cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS ORDER BY TABLE_NAME")
for row in cursor.fetchall():
    print(f"  View: {row[0]}")

cursor.close()
conn.close()
print("All SQL setup done!")
