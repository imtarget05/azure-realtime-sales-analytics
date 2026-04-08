-- ============================================================
-- Power BI Verify Pack (5 KPI result sets)
-- Run once in Azure SQL to quickly validate dashboard data readiness.
-- ============================================================

SET NOCOUNT ON;
DECLARE @utc_now DATETIME2 = SYSUTCDATETIME();

-- ------------------------------------------------------------
-- RESULT SET 1: Realtime KPI snapshot (last 60 minutes)
-- ------------------------------------------------------------
IF OBJECT_ID('dbo.SalesTransactions', 'U') IS NOT NULL
BEGIN
    SELECT
        'KPI_1_REALTIME_SNAPSHOT' AS kpi_pack,
        @utc_now AS generated_at_utc,
        COUNT(*) AS tx_count_60m,
        ISNULL(SUM(units_sold), 0) AS total_units_60m,
        ISNULL(SUM(revenue), 0.0) AS total_revenue_60m,
        CAST(ISNULL(AVG(unit_price), 0.0) AS DECIMAL(18, 4)) AS avg_unit_price_60m,
        MIN(event_time) AS min_event_time,
        MAX(event_time) AS max_event_time
    FROM dbo.SalesTransactions
    WHERE event_time >= DATEADD(minute, -60, @utc_now);
END
ELSE
BEGIN
    SELECT
        'KPI_1_REALTIME_SNAPSHOT' AS kpi_pack,
        @utc_now AS generated_at_utc,
        0 AS tx_count_60m,
        0 AS total_units_60m,
        0.0 AS total_revenue_60m,
        CAST(0.0 AS DECIMAL(18, 4)) AS avg_unit_price_60m,
        NULL AS min_event_time,
        NULL AS max_event_time;
END;

-- ------------------------------------------------------------
-- RESULT SET 2: Top stores by revenue (last 24 hours)
-- ------------------------------------------------------------
IF OBJECT_ID('dbo.SalesTransactions', 'U') IS NOT NULL
BEGIN
    SELECT TOP 5
        'KPI_2_TOP_STORES_24H' AS kpi_pack,
        store_id,
        COUNT(*) AS tx_count_24h,
        ISNULL(SUM(units_sold), 0) AS total_units_24h,
        ISNULL(SUM(revenue), 0.0) AS total_revenue_24h,
        CAST(ISNULL(AVG(unit_price), 0.0) AS DECIMAL(18, 4)) AS avg_unit_price_24h
    FROM dbo.SalesTransactions
    WHERE event_time >= DATEADD(hour, -24, @utc_now)
    GROUP BY store_id
    ORDER BY total_revenue_24h DESC;
END
ELSE
BEGIN
    SELECT
        'KPI_2_TOP_STORES_24H' AS kpi_pack,
        CAST(NULL AS NVARCHAR(20)) AS store_id,
        0 AS tx_count_24h,
        0 AS total_units_24h,
        0.0 AS total_revenue_24h,
        CAST(0.0 AS DECIMAL(18, 4)) AS avg_unit_price_24h
    WHERE 1 = 0;
END;

-- ------------------------------------------------------------
-- RESULT SET 3: Alert summary (last 24 hours)
-- ------------------------------------------------------------
IF OBJECT_ID('dbo.SalesAlerts', 'U') IS NOT NULL
BEGIN
    SELECT
        'KPI_3_ALERTS_24H' AS kpi_pack,
        type AS alert_type,
        COUNT(*) AS alert_count,
        CAST(ISNULL(AVG(value), 0.0) AS DECIMAL(18, 4)) AS avg_alert_value,
        MIN(alert_time) AS first_alert_time,
        MAX(alert_time) AS last_alert_time
    FROM dbo.SalesAlerts
    WHERE alert_time >= DATEADD(hour, -24, @utc_now)
    GROUP BY type
    ORDER BY alert_count DESC;
END
ELSE
BEGIN
    SELECT
        'KPI_3_ALERTS_24H' AS kpi_pack,
        CAST(NULL AS NVARCHAR(20)) AS alert_type,
        0 AS alert_count,
        CAST(0.0 AS DECIMAL(18, 4)) AS avg_alert_value,
        NULL AS first_alert_time,
        NULL AS last_alert_time
    WHERE 1 = 0;
END;

-- ------------------------------------------------------------
-- RESULT SET 4: Forecast vs Actual accuracy (last 48 hours)
-- ------------------------------------------------------------
IF OBJECT_ID('dbo.vw_ForecastVsActual', 'V') IS NOT NULL
BEGIN
    SELECT
        'KPI_4_FORECAST_ACCURACY_48H' AS kpi_pack,
        COUNT(*) AS compared_points,
        CAST(ISNULL(AVG(forecast_error), 0.0) AS DECIMAL(18, 4)) AS mae_48h,
        CAST(ISNULL(MAX(forecast_error), 0.0) AS DECIMAL(18, 4)) AS worst_error_48h,
        CAST(ISNULL(AVG(CASE WHEN actual_revenue = 0 THEN NULL ELSE (forecast_error / NULLIF(actual_revenue, 0.0)) END), 0.0) AS DECIMAL(18, 6)) AS mape_48h,
        MIN(forecast_datetime) AS min_forecast_time,
        MAX(forecast_datetime) AS max_forecast_time
    FROM dbo.vw_ForecastVsActual
    WHERE forecast_datetime >= DATEADD(hour, -48, @utc_now);
END
ELSE
BEGIN
    SELECT
        'KPI_4_FORECAST_ACCURACY_48H' AS kpi_pack,
        0 AS compared_points,
        CAST(0.0 AS DECIMAL(18, 4)) AS mae_48h,
        CAST(0.0 AS DECIMAL(18, 4)) AS worst_error_48h,
        CAST(0.0 AS DECIMAL(18, 6)) AS mape_48h,
        NULL AS min_forecast_time,
        NULL AS max_forecast_time;
END;

-- ------------------------------------------------------------
-- RESULT SET 5: Weather-Sales correlation signal summary (last 24 hours)
-- ------------------------------------------------------------
IF OBJECT_ID('dbo.WeatherSalesCorrelation', 'U') IS NOT NULL
BEGIN
    SELECT
        'KPI_5_WEATHER_CORRELATION_24H' AS kpi_pack,
        correlation_signal,
        COUNT(*) AS signal_count,
        CAST(ISNULL(AVG(avg_temperature), 0.0) AS DECIMAL(18, 4)) AS avg_temperature,
        CAST(ISNULL(AVG(avg_stock_price), 0.0) AS DECIMAL(18, 4)) AS avg_stock_price,
        CAST(ISNULL(AVG(total_revenue), 0.0) AS DECIMAL(18, 4)) AS avg_revenue,
        CAST(ISNULL(SUM(total_revenue), 0.0) AS DECIMAL(18, 4)) AS total_revenue_24h,
        MIN(window_end) AS first_window,
        MAX(window_end) AS last_window
    FROM dbo.WeatherSalesCorrelation
    WHERE window_end >= DATEADD(hour, -24, @utc_now)
    GROUP BY correlation_signal
    ORDER BY total_revenue_24h DESC;
END
ELSE
BEGIN
    SELECT
        'KPI_5_WEATHER_CORRELATION_24H' AS kpi_pack,
        CAST(NULL AS NVARCHAR(50)) AS correlation_signal,
        0 AS signal_count,
        CAST(0.0 AS DECIMAL(18, 4)) AS avg_temperature,
        CAST(0.0 AS DECIMAL(18, 4)) AS avg_stock_price,
        CAST(0.0 AS DECIMAL(18, 4)) AS avg_revenue,
        CAST(0.0 AS DECIMAL(18, 4)) AS total_revenue_24h,
        NULL AS first_window,
        NULL AS last_window
    WHERE 1 = 0;
END;
