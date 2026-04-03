-- ============================================================
-- Stored Procedures – Aligned with SalesTransactions / HourlySalesSummary
-- ============================================================

-- ========================
-- SP 1: Prepare ML training data from last 90 days of sales
-- ========================
IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'sp_PrepareTrainingData')
    DROP PROCEDURE sp_PrepareTrainingData;
GO

CREATE PROCEDURE sp_PrepareTrainingData
AS
BEGIN
    SET NOCOUNT ON;

    IF OBJECT_ID('dbo.MLTrainingData', 'U') IS NOT NULL
        DROP TABLE dbo.MLTrainingData;

    SELECT
        CAST(event_time AS DATE)          AS [date],
        DATEPART(HOUR, event_time)        AS [hour],
        DATEPART(DAY, event_time)         AS day_of_month,
        DATEPART(MONTH, event_time)       AS [month],
        CASE WHEN DATEPART(WEEKDAY, event_time) IN (1,7) THEN 1 ELSE 0 END AS is_weekend,
        store_id,
        product_id,
        category,
        AVG(temperature)                  AS temperature,
        MAX(CASE WHEN weather IN ('rainy','stormy') THEN 1 ELSE 0 END) AS is_rainy,
        MAX(holiday)                      AS holiday,
        SUM(units_sold)                   AS quantity,
        SUM(revenue)                      AS revenue
    INTO dbo.MLTrainingData
    FROM dbo.SalesTransactions
    WHERE event_time >= DATEADD(day, -90, SYSUTCDATETIME())
    GROUP BY
        CAST(event_time AS DATE),
        DATEPART(HOUR, event_time),
        DATEPART(DAY, event_time),
        DATEPART(MONTH, event_time),
        DATEPART(WEEKDAY, event_time),
        store_id, product_id, category;

    DECLARE @rowcount INT = (SELECT COUNT(*) FROM dbo.MLTrainingData);
    PRINT CONCAT('Prepared ', @rowcount, ' training records.');
END
GO

PRINT 'Created sp_PrepareTrainingData';
GO

-- ========================
-- SP 2: Clean old forecasts and resolved alerts
-- ========================
IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'sp_UpdateForecasts')
    DROP PROCEDURE sp_UpdateForecasts;
GO

CREATE PROCEDURE sp_UpdateForecasts
AS
BEGIN
    SET NOCOUNT ON;

    DELETE FROM dbo.SalesForecast
    WHERE forecast_date < DATEADD(day, -7, CAST(SYSUTCDATETIME() AS DATE));

    PRINT 'Old forecasts cleaned.';
END
GO

PRINT 'Created sp_UpdateForecasts';
GO
