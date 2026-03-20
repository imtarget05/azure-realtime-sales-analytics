-- ============================================================
-- Azure SQL schema for Stream Analytics outputs
-- Raw + Aggregated tables for 28/03/2026 streaming milestone
-- ============================================================

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'SalesRaw')
BEGIN
    CREATE TABLE dbo.SalesRaw (
        id BIGINT IDENTITY(1,1) PRIMARY KEY,
        event_time DATETIME2 NOT NULL,
        store_id NVARCHAR(20) NOT NULL,
        product_id NVARCHAR(20) NOT NULL,
        units_sold BIGINT NOT NULL,
        unit_price FLOAT NOT NULL,
        revenue FLOAT NOT NULL,
        temperature FLOAT NULL,
        weather NVARCHAR(30) NULL,
        holiday BIGINT NOT NULL,
        category NVARCHAR(50) NULL,
        enqueued_time DATETIME2 NULL,
        ingest_lag_seconds BIGINT NULL,
        inserted_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
END
GO

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'SalesAgg5m')
BEGIN
    CREATE TABLE dbo.SalesAgg5m (
        id BIGINT IDENTITY(1,1) PRIMARY KEY,
        window_start DATETIME2 NOT NULL,
        window_end DATETIME2 NOT NULL,
        store_id NVARCHAR(20) NOT NULL,
        product_id NVARCHAR(20) NOT NULL,
        category NVARCHAR(50) NULL,
        units_sold BIGINT NOT NULL,
        revenue FLOAT NOT NULL,
        avg_price FLOAT NULL,
        tx_count BIGINT NOT NULL,
        prev_5m_revenue FLOAT NOT NULL,
        revenue_delta_5m FLOAT NOT NULL,
        rolling_15m_units BIGINT NOT NULL,
        rolling_15m_revenue FLOAT NOT NULL,
        inserted_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'IX_SalesRaw_EventTime_Store_Product'
)
BEGIN
    CREATE INDEX IX_SalesRaw_EventTime_Store_Product
    ON dbo.SalesRaw(event_time, store_id, product_id);
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'IX_SalesAgg5m_WindowEnd_Store_Product'
)
BEGIN
    CREATE INDEX IX_SalesAgg5m_WindowEnd_Store_Product
    ON dbo.SalesAgg5m(window_end, store_id, product_id);
END
GO

-- Quick validation queries (run after Stream job starts)
SELECT TOP 20 *
FROM dbo.SalesRaw
ORDER BY id DESC;

SELECT TOP 20 *
FROM dbo.SalesAgg5m
ORDER BY id DESC;

SELECT
    AVG(CAST(ingest_lag_seconds AS FLOAT)) AS avg_ingest_lag_seconds,
    MAX(ingest_lag_seconds) AS max_ingest_lag_seconds
FROM dbo.SalesRaw
WHERE inserted_at >= DATEADD(minute, -15, SYSUTCDATETIME());
