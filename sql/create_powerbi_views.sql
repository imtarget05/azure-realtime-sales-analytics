-- ============================================================
-- Power BI Dashboard Views + MLOps Demo Data
-- Thể hiện: ETL Pipeline, Truy vấn phân tích, MLOps monitoring
-- ============================================================

-- ============================================================
-- 1. VIEW: ETL Pipeline Monitoring (thể hiện latency, throughput)
-- ============================================================
IF OBJECT_ID('dbo.vw_ETLPipelineHealth', 'V') IS NOT NULL DROP VIEW dbo.vw_ETLPipelineHealth;
GO
CREATE VIEW dbo.vw_ETLPipelineHealth AS
SELECT
    DATEADD(MINUTE, DATEDIFF(MINUTE, 0, event_time) / 5 * 5, 0) AS time_bucket,
    COUNT(*) AS events_processed,
    AVG(CAST(ABS(ingest_lag_seconds) AS FLOAT)) AS avg_latency_seconds,
    MAX(ABS(ingest_lag_seconds)) AS max_latency_seconds,
    MIN(ABS(ingest_lag_seconds)) AS min_latency_seconds,
    COUNT(DISTINCT store_id) AS active_stores,
    COUNT(DISTINCT product_id) AS active_products,
    COUNT(DISTINCT category) AS active_categories,
    SUM(revenue) AS total_revenue,
    CASE
        WHEN AVG(CAST(ABS(ingest_lag_seconds) AS FLOAT)) < 5 THEN 'Healthy'
        WHEN AVG(CAST(ABS(ingest_lag_seconds) AS FLOAT)) < 15 THEN 'Warning'
        ELSE 'Critical'
    END AS pipeline_status
FROM dbo.SalesTransactions
WHERE event_time >= DATEADD(HOUR, -24, SYSUTCDATETIME())
GROUP BY DATEADD(MINUTE, DATEDIFF(MINUTE, 0, event_time) / 5 * 5, 0);
GO

-- ============================================================
-- 2. VIEW: Product Performance Analytics (top products, trends)
-- ============================================================
IF OBJECT_ID('dbo.vw_ProductPerformance', 'V') IS NOT NULL DROP VIEW dbo.vw_ProductPerformance;
GO
CREATE VIEW dbo.vw_ProductPerformance AS
SELECT
    product_id,
    category,
    COUNT(*) AS total_transactions,
    SUM(units_sold) AS total_units,
    SUM(revenue) AS total_revenue,
    AVG(unit_price) AS avg_price,
    MIN(unit_price) AS min_price,
    MAX(unit_price) AS max_price,
    STDEV(revenue) AS revenue_stddev,
    COUNT(DISTINCT store_id) AS stores_selling,
    MIN(event_time) AS first_sale,
    MAX(event_time) AS last_sale
FROM dbo.SalesTransactions
WHERE event_time >= DATEADD(HOUR, -24, SYSUTCDATETIME())
GROUP BY product_id, category;
GO

-- ============================================================
-- 3. VIEW: Store Comparison Dashboard
-- ============================================================
IF OBJECT_ID('dbo.vw_StoreComparison', 'V') IS NOT NULL DROP VIEW dbo.vw_StoreComparison;
GO
CREATE VIEW dbo.vw_StoreComparison AS
SELECT
    store_id,
    CAST(event_time AS DATE) AS sale_date,
    DATEPART(HOUR, event_time) AS sale_hour,
    COUNT(*) AS transactions,
    SUM(units_sold) AS units_sold,
    SUM(revenue) AS revenue,
    AVG(unit_price) AS avg_price,
    COUNT(DISTINCT product_id) AS unique_products,
    COUNT(DISTINCT category) AS unique_categories,
    AVG(temperature) AS avg_temperature,
    SUM(CASE WHEN holiday = 1 THEN 1 ELSE 0 END) AS holiday_transactions
FROM dbo.SalesTransactions
WHERE event_time >= DATEADD(DAY, -7, SYSUTCDATETIME())
GROUP BY store_id, CAST(event_time AS DATE), DATEPART(HOUR, event_time);
GO

-- ============================================================
-- 4. VIEW: Weather Impact Analysis
-- ============================================================
IF OBJECT_ID('dbo.vw_WeatherImpact', 'V') IS NOT NULL DROP VIEW dbo.vw_WeatherImpact;
GO
CREATE VIEW dbo.vw_WeatherImpact AS
SELECT
    weather,
    CASE
        WHEN temperature < 20 THEN 'Cold (<20C)'
        WHEN temperature < 30 THEN 'Warm (20-30C)'
        ELSE 'Hot (>30C)'
    END AS temp_range,
    store_id,
    category,
    COUNT(*) AS transactions,
    SUM(revenue) AS total_revenue,
    AVG(revenue) AS avg_revenue_per_tx,
    AVG(CAST(units_sold AS FLOAT)) AS avg_units_per_tx
FROM dbo.SalesTransactions
WHERE event_time >= DATEADD(DAY, -7, SYSUTCDATETIME())
GROUP BY weather, store_id, category,
    CASE
        WHEN temperature < 20 THEN 'Cold (<20C)'
        WHEN temperature < 30 THEN 'Warm (20-30C)'
        ELSE 'Hot (>30C)'
    END;
GO

-- ============================================================
-- 5. VIEW: Anomaly Alert Summary (for MLOps dashboard)
-- ============================================================
IF OBJECT_ID('dbo.vw_AlertSummary', 'V') IS NOT NULL DROP VIEW dbo.vw_AlertSummary;
GO
CREATE VIEW dbo.vw_AlertSummary AS
SELECT
    CAST(alert_time AS DATE) AS alert_date,
    DATEPART(HOUR, alert_time) AS alert_hour,
    store_id,
    type AS alert_type,
    CASE
        WHEN value > 50 THEN 'high'
        WHEN value > 20 THEN 'medium'
        ELSE 'low'
    END AS severity,
    COUNT(*) AS alert_count,
    AVG(value) AS avg_alert_value,
    MAX(value) AS max_alert_value
FROM dbo.SalesAlerts
GROUP BY CAST(alert_time AS DATE), DATEPART(HOUR, alert_time), store_id, type,
    CASE
        WHEN value > 50 THEN 'high'
        WHEN value > 20 THEN 'medium'
        ELSE 'low'
    END;
GO

-- ============================================================
-- 6. VIEW: Forecast vs Actual (MLOps accuracy tracking)
-- ============================================================
IF OBJECT_ID('dbo.vw_ForecastAccuracy', 'V') IS NOT NULL DROP VIEW dbo.vw_ForecastAccuracy;
GO
CREATE VIEW dbo.vw_ForecastAccuracy AS
SELECT
    f.forecast_date,
    f.forecast_hour,
    f.store_id,
    f.product_id,
    f.category,
    f.predicted_quantity,
    f.predicted_revenue,
    f.confidence_lower,
    f.confidence_upper,
    f.model_version,
    COALESCE(a.actual_quantity, 0) AS actual_quantity,
    COALESCE(a.actual_revenue, 0) AS actual_revenue,
    ABS(f.predicted_revenue - COALESCE(a.actual_revenue, 0)) AS absolute_error,
    CASE
        WHEN COALESCE(a.actual_revenue, 0) = 0 THEN NULL
        ELSE ABS(f.predicted_revenue - a.actual_revenue) / a.actual_revenue * 100
    END AS pct_error,
    CASE
        WHEN COALESCE(a.actual_revenue, 0) BETWEEN f.confidence_lower AND f.confidence_upper THEN 1
        ELSE 0
    END AS within_confidence
FROM dbo.SalesForecast f
LEFT JOIN (
    SELECT
        CAST(event_time AS DATE) AS sale_date,
        DATEPART(HOUR, event_time) AS sale_hour,
        store_id,
        product_id,
        SUM(units_sold) AS actual_quantity,
        SUM(revenue) AS actual_revenue
    FROM dbo.SalesTransactions
    GROUP BY CAST(event_time AS DATE), DATEPART(HOUR, event_time), store_id, product_id
) a ON f.forecast_date = a.sale_date
    AND f.forecast_hour = a.sale_hour
    AND f.store_id = a.store_id
    AND f.product_id = a.product_id;
GO

-- ============================================================
-- 7. VIEW: Hourly Trend (rolling aggregation - thể hiện ETL)
-- ============================================================
IF OBJECT_ID('dbo.vw_HourlyTrend', 'V') IS NOT NULL DROP VIEW dbo.vw_HourlyTrend;
GO
CREATE VIEW dbo.vw_HourlyTrend AS
SELECT
    window_start,
    window_end,
    store_id,
    category,
    SUM(units_sold) AS total_units,
    SUM(revenue) AS total_revenue,
    SUM(tx_count) AS total_transactions,
    AVG(avg_price) AS avg_price,
    COUNT(DISTINCT product_id) AS unique_products
FROM dbo.HourlySalesSummary
WHERE window_start >= DATEADD(DAY, -3, SYSUTCDATETIME())
GROUP BY window_start, window_end, store_id, category;
GO
