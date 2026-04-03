-- ============================================================
-- Azure SQL Schema – Aligned with Stream Analytics outputs
-- Tables: SalesTransactions, HourlySalesSummary, SalesForecast, SalesAlerts
-- Run this script on Azure SQL Database after creating the database.
-- ============================================================

-- ========================
-- 1. SalesTransactions (from SalesTransactionsOutput in Stream Analytics)
--    Raw enriched events with derived revenue and category.
-- ========================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'SalesTransactions')
BEGIN
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
    PRINT 'Created table: SalesTransactions';
END
GO

-- ========================
-- 2. HourlySalesSummary (from HourlySalesSummaryOutput in Stream Analytics)
--    5-minute tumbling window aggregation with rolling & LAG metrics.
-- ========================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'HourlySalesSummary')
BEGIN
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
    PRINT 'Created table: HourlySalesSummary';
END
GO

-- ========================
-- 3. SalesForecast (ML predictions written by ml/score.py or stored proc)
-- ========================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'SalesForecast')
BEGIN
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
    PRINT 'Created table: SalesForecast';
END
GO

-- ========================
-- 4. SalesAlerts (from SalesAlertsOutput in Stream Analytics)
-- ========================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'SalesAlerts')
BEGIN
    CREATE TABLE dbo.SalesAlerts (
        id          BIGINT IDENTITY(1,1) PRIMARY KEY,
        alert_time  DATETIME2     NOT NULL,
        store_id    NVARCHAR(20)  NOT NULL,
        type        NVARCHAR(20)  NOT NULL,
        value       FLOAT         NOT NULL,
        created_at  DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
    );
    PRINT 'Created table: SalesAlerts';
END
GO

-- ========================
-- INDEXES
-- ========================
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_SalesTransactions_EventTime')
    CREATE NONCLUSTERED INDEX IX_SalesTransactions_EventTime
        ON dbo.SalesTransactions(event_time, store_id, product_id);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_SalesTransactions_StoreProduct')
    CREATE NONCLUSTERED INDEX IX_SalesTransactions_StoreProduct
        ON dbo.SalesTransactions(store_id, product_id);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_HourlySalesSummary_Window')
    CREATE NONCLUSTERED INDEX IX_HourlySalesSummary_Window
        ON dbo.HourlySalesSummary(window_end, store_id, product_id);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_SalesForecast_Date')
    CREATE NONCLUSTERED INDEX IX_SalesForecast_Date
        ON dbo.SalesForecast(forecast_date, store_id, category);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_SalesAlerts_TimeStore')
    CREATE NONCLUSTERED INDEX IX_SalesAlerts_TimeStore
        ON dbo.SalesAlerts(alert_time, store_id);
GO

PRINT 'All indexes created.';
GO

-- ========================
-- VIEW: Real-time dashboard (for Power BI)
-- ========================
IF EXISTS (SELECT * FROM sys.views WHERE name = 'vw_RealtimeDashboard')
    DROP VIEW vw_RealtimeDashboard;
GO

CREATE VIEW dbo.vw_RealtimeDashboard AS
SELECT TOP 1000
    event_time,
    store_id,
    product_id,
    category,
    units_sold,
    unit_price,
    revenue,
    temperature,
    weather,
    holiday
FROM dbo.SalesTransactions
ORDER BY event_time DESC;
GO

PRINT 'View vw_RealtimeDashboard created.';
GO

-- ========================
-- VIEW: Forecast vs Actual (for Power BI)
-- ========================
IF EXISTS (SELECT * FROM sys.views WHERE name = 'vw_ForecastVsActual')
    DROP VIEW vw_ForecastVsActual;
GO

CREATE VIEW dbo.vw_ForecastVsActual AS
SELECT
    f.forecast_date,
    f.forecast_hour,
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
        SUM(revenue)    AS actual_revenue
    FROM dbo.SalesTransactions
    GROUP BY CAST(event_time AS DATE), DATEPART(HOUR, event_time), store_id, category
) a ON f.forecast_date  = a.sale_date
    AND f.forecast_hour = a.sale_hour
    AND f.store_id      = a.store_id
    AND f.category      = a.category;
GO

PRINT 'View vw_ForecastVsActual created.';
GO

PRINT '============================================================';
PRINT '  DATABASE SCHEMA CREATION COMPLETED';
PRINT '============================================================';
