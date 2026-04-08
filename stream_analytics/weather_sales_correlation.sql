-- ============================================================
-- Stream Analytics Query - Weather x Sales x Stock Correlation
-- Inputs:
--   SalesInput   (Event Hub: sales-events)
--   WeatherInput (Event Hub: weather-events)
--   StockInput   (Event Hub: stock-events)
-- Output:
--   WeatherSalesCorrelationOutput -> Azure SQL dbo.WeatherSalesCorrelation
-- ============================================================

WITH Sales AS (
    SELECT
        TRY_CAST([timestamp] AS datetime) AS sales_time,
        CAST(store_id AS nvarchar(max)) AS store_id,
        TRY_CAST(quantity AS bigint) AS units_sold,
        TRY_CAST(price AS float) AS unit_price,
        TRY_CAST(quantity AS bigint) * TRY_CAST(price AS float) AS revenue,
        CAST(product_id AS nvarchar(max)) AS product_id
    FROM SalesInput TIMESTAMP BY [timestamp]
    WHERE
        TRY_CAST([timestamp] AS datetime) IS NOT NULL
        AND store_id IS NOT NULL
        AND TRY_CAST(quantity AS bigint) IS NOT NULL
        AND TRY_CAST(price AS float) IS NOT NULL
),

Weather AS (
    SELECT
        TRY_CAST([timestamp] AS datetime) AS weather_time,
        CAST(store_id AS nvarchar(max)) AS store_id,
        TRY_CAST(temperature AS float) AS temperature,
        CASE
            WHEN weather IS NULL OR LTRIM(RTRIM(weather)) = '' THEN 'unknown'
            ELSE LOWER(CAST(weather AS nvarchar(max)))
        END AS weather
    FROM WeatherInput TIMESTAMP BY [timestamp]
    WHERE TRY_CAST([timestamp] AS datetime) IS NOT NULL
),

Stock AS (
    SELECT
        TRY_CAST([timestamp] AS datetime) AS stock_time,
        CAST(symbol AS nvarchar(max)) AS stock_symbol,
        TRY_CAST(price AS float) AS stock_price
    FROM StockInput TIMESTAMP BY [timestamp]
    WHERE
        TRY_CAST([timestamp] AS datetime) IS NOT NULL
        AND TRY_CAST(price AS float) IS NOT NULL
),

JoinedStreams AS (
    SELECT
        s.store_id,
        s.sales_time,
        s.units_sold,
        s.unit_price,
        s.revenue,
        w.temperature,
        w.weather,
        st.stock_symbol,
        st.stock_price
    FROM Sales s
    LEFT JOIN Weather w
        ON s.store_id = w.store_id
        AND DATEDIFF(minute, s, w) BETWEEN 0 AND 15
    LEFT JOIN Stock st
        ON DATEDIFF(minute, s, st) BETWEEN 0 AND 15
)

SELECT
    System.Timestamp() AS window_end,
    store_id,
    weather,
    CAST(AVG(temperature) AS float) AS avg_temperature,
    CAST(AVG(stock_price) AS float) AS avg_stock_price,
    MAX(stock_symbol) AS stock_symbol,
    CAST(SUM(revenue) AS float) AS total_revenue,
    CAST(SUM(units_sold) AS bigint) AS total_units,
    CAST(COUNT(*) AS bigint) AS tx_count,
    CAST(AVG(unit_price) AS float) AS avg_unit_price,
    CASE
        WHEN AVG(temperature) >= 32 AND SUM(revenue) > 5000 THEN 'hot_weather_high_revenue'
        WHEN AVG(temperature) <= 18 AND SUM(revenue) < 3000 THEN 'cold_weather_low_revenue'
        WHEN AVG(stock_price) IS NOT NULL AND AVG(stock_price) > 250 THEN 'stock_bullish_sales_watch'
        ELSE 'normal'
    END AS correlation_signal
INTO WeatherSalesCorrelationOutput
FROM JoinedStreams
GROUP BY store_id, weather, TumblingWindow(minute, 15);
