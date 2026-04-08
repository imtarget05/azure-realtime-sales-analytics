-- ============================================================
-- Insert mock data for dbo.WeatherSalesCorrelation
-- Purpose: Test Power BI visuals immediately (scatter, trend, cards)
-- Safe to rerun: script deletes previous MOCK_* rows first.
-- ============================================================

SET NOCOUNT ON;

IF OBJECT_ID('dbo.WeatherSalesCorrelation', 'U') IS NULL
BEGIN
    RAISERROR('Table dbo.WeatherSalesCorrelation does not exist. Run sql/create_tables.sql first.', 16, 1);
    RETURN;
END;

BEGIN TRY
    BEGIN TRAN;

    -- Keep script idempotent for repeated demo runs
    DELETE FROM dbo.WeatherSalesCorrelation
    WHERE store_id LIKE 'MOCK_STORE_%';

    DECLARE @now DATETIME2 = SYSUTCDATETIME();

    ;WITH src AS (
        SELECT * FROM (VALUES
            (0,   'MOCK_STORE_01', 'sunny', 34.2, 286.5, 'MSFT', 12890.50, 410, 120, 31.44, 'hot_weather_high_revenue'),
            (0,   'MOCK_STORE_02', 'cloudy', 27.1, 241.2, 'AAPL',  8420.80, 305,  95, 27.61, 'normal'),
            (0,   'MOCK_STORE_03', 'rainy',  19.8, 198.7, 'TSLA',  5120.40, 220,  80, 23.27, 'normal'),
            (15,  'MOCK_STORE_01', 'sunny', 35.0, 289.1, 'MSFT', 13640.20, 435, 126, 31.35, 'hot_weather_high_revenue'),
            (15,  'MOCK_STORE_02', 'storm',  17.6, 192.4, 'GOOG',  2988.10, 140,  60, 21.34, 'cold_weather_low_revenue'),
            (15,  'MOCK_STORE_03', 'cloudy', 24.5, 255.4, 'NVDA',  6730.30, 260,  88, 25.89, 'stock_bullish_sales_watch'),
            (30,  'MOCK_STORE_01', 'sunny', 33.6, 282.0, 'MSFT', 12175.90, 398, 118, 30.59, 'hot_weather_high_revenue'),
            (30,  'MOCK_STORE_02', 'rainy',  18.3, 201.0, 'AAPL',  3615.45, 160,  64, 22.60, 'cold_weather_low_revenue'),
            (30,  'MOCK_STORE_03', 'windy',  22.9, 268.8, 'NVDA',  7422.75, 278,  92, 26.70, 'stock_bullish_sales_watch'),
            (45,  'MOCK_STORE_01', 'cloudy', 29.4, 276.2, 'MSFT', 10980.10, 370, 110, 29.68, 'normal'),
            (45,  'MOCK_STORE_02', 'sunny',  31.1, 243.5, 'AAPL',  9350.55, 332, 102, 28.16, 'normal'),
            (45,  'MOCK_STORE_03', 'rainy',  16.9, 189.2, 'TSLA',  2744.90, 130,  56, 21.11, 'cold_weather_low_revenue')
        ) v(offset_min, store_id, weather, avg_temperature, avg_stock_price, stock_symbol, total_revenue, total_units, tx_count, avg_unit_price, correlation_signal)
    )
    INSERT INTO dbo.WeatherSalesCorrelation (
        window_end,
        store_id,
        weather,
        avg_temperature,
        avg_stock_price,
        stock_symbol,
        total_revenue,
        total_units,
        tx_count,
        avg_unit_price,
        correlation_signal
    )
    SELECT
        DATEADD(minute, -offset_min, @now) AS window_end,
        store_id,
        weather,
        avg_temperature,
        avg_stock_price,
        stock_symbol,
        total_revenue,
        total_units,
        tx_count,
        avg_unit_price,
        correlation_signal
    FROM src;

    COMMIT TRAN;

    PRINT 'Inserted mock rows into dbo.WeatherSalesCorrelation successfully.';

    -- Quick verification output for Power BI filters
    SELECT TOP 50
        window_end,
        store_id,
        weather,
        avg_temperature,
        avg_stock_price,
        total_revenue,
        tx_count,
        correlation_signal
    FROM dbo.WeatherSalesCorrelation
    WHERE store_id LIKE 'MOCK_STORE_%'
    ORDER BY window_end DESC, store_id;

END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0
        ROLLBACK TRAN;

    DECLARE @err NVARCHAR(4000) = ERROR_MESSAGE();
    RAISERROR('insert_mock_weather_sales_correlation.sql failed: %s', 16, 1, @err);
END CATCH;
