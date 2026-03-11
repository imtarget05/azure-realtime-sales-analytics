-- ============================================================
-- Azure Stream Analytics Queries
-- Cấu hình các query này trong Stream Analytics Job trên Azure Portal
-- ============================================================

-- ============================================================
-- INPUT:
--   SalesInput      -> Event Hub: sales-events
--   WeatherInput    -> Event Hub: weather-events
--   StockInput      -> Event Hub: stock-events
--
-- OUTPUT:
--   SQLOutput              -> Azure SQL: SalesTransactions
--   HourlySummaryOutput    -> Azure SQL: HourlySalesSummary
--   ProductSummaryOutput   -> Azure SQL: ProductSalesSummary
--   AlertsOutput           -> Azure SQL: SalesAlerts
--   WeatherOutput          -> Azure SQL: WeatherData
--   StockOutput            -> Azure SQL: StockData
--   PowerBIOutput          -> Power BI Dataset (streaming)
-- ============================================================


-- ========================
-- Query 1: Ghi trực tiếp tất cả giao dịch vào SQL Database
-- ========================
SELECT
    transaction_id,
    CAST([timestamp] AS datetime) AS event_timestamp,
    CAST([date] AS date) AS sale_date,
    [hour] AS sale_hour,
    day_of_week,
    product_id,
    product_name,
    category,
    quantity,
    unit_price,
    total_amount,
    discount_percent,
    discount_amount,
    final_amount,
    customer_id,
    customer_segment,
    region,
    payment_method,
    is_online,
    rating
INTO SQLOutput
FROM SalesInput;


-- ========================
-- Query 2: Tổng hợp bán hàng theo giờ, theo vùng và danh mục
-- Sử dụng Tumbling Window 1 giờ
-- ========================
SELECT
    System.Timestamp() AS window_start,
    DATEADD(hour, 1, System.Timestamp()) AS window_end,
    region,
    category,
    COUNT(*) AS total_transactions,
    SUM(quantity) AS total_quantity,
    SUM(final_amount) AS total_revenue,
    AVG(final_amount) AS avg_order_value,
    MAX(final_amount) AS max_order_value,
    MIN(final_amount) AS min_order_value
INTO HourlySummaryOutput
FROM SalesInput
TIMESTAMP BY CAST([timestamp] AS datetime)
GROUP BY
    region,
    category,
    TumblingWindow(hour, 1);


-- ========================
-- Query 3: Tổng hợp theo sản phẩm (mỗi 30 phút)
-- Sử dụng Tumbling Window 30 phút
-- ========================
SELECT
    System.Timestamp() AS window_start,
    DATEADD(minute, 30, System.Timestamp()) AS window_end,
    product_id,
    product_name,
    category,
    SUM(quantity) AS total_sold,
    SUM(final_amount) AS total_revenue,
    AVG(unit_price) AS avg_price,
    AVG(CAST(rating AS float)) AS avg_rating
INTO ProductSummaryOutput
FROM SalesInput
TIMESTAMP BY CAST([timestamp] AS datetime)
GROUP BY
    product_id,
    product_name,
    category,
    TumblingWindow(minute, 30);


-- ========================
-- Query 4: Phát hiện bất thường - Đơn hàng giá trị cao
-- Cảnh báo khi đơn hàng > $5000 trong 5 phút
-- ========================
SELECT
    System.Timestamp() AS alert_timestamp,
    'HIGH_VALUE_ORDER' AS alert_type,
    'High' AS severity,
    region,
    category,
    product_id,
    final_amount AS metric_value,
    5000.00 AS threshold_value,
    CONCAT('Đơn hàng giá trị cao: $', CAST(final_amount AS nvarchar(20)), 
           ' - Sản phẩm: ', product_name, ' - Vùng: ', region) AS description
INTO AlertsOutput
FROM SalesInput
TIMESTAMP BY CAST([timestamp] AS datetime)
WHERE final_amount > 5000;


-- ========================
-- Query 5: Phát hiện bất thường - Spike doanh thu theo vùng
-- Cảnh báo khi doanh thu 5 phút vượt ngưỡng
-- ========================
SELECT
    System.Timestamp() AS alert_timestamp,
    'REVENUE_SPIKE' AS alert_type,
    CASE 
        WHEN SUM(final_amount) > 50000 THEN 'Critical'
        WHEN SUM(final_amount) > 25000 THEN 'High'
        ELSE 'Medium'
    END AS severity,
    region,
    'ALL' AS category,
    'ALL' AS product_id,
    SUM(final_amount) AS metric_value,
    10000.00 AS threshold_value,
    CONCAT('Doanh thu đột biến tại vùng ', region, 
           ': $', CAST(SUM(final_amount) AS nvarchar(20)),
           ' trong 5 phút') AS description
INTO AlertsOutput
FROM SalesInput
TIMESTAMP BY CAST([timestamp] AS datetime)
GROUP BY
    region,
    TumblingWindow(minute, 5)
HAVING SUM(final_amount) > 10000;


-- ========================
-- Query 6: Ghi dữ liệu thời tiết vào SQL
-- ========================
SELECT
    CAST([timestamp] AS datetime) AS event_timestamp,
    CAST([date] AS date) AS weather_date,
    [hour] AS weather_hour,
    region,
    temperature_celsius,
    humidity_percent,
    wind_speed_kmh,
    precipitation_mm,
    weather_condition,
    uv_index
INTO WeatherOutput
FROM WeatherInput;


-- ========================
-- Query 7: Ghi dữ liệu chứng khoán vào SQL
-- ========================
SELECT
    CAST([timestamp] AS datetime) AS event_timestamp,
    CAST([date] AS date) AS stock_date,
    [hour] AS stock_hour,
    [minute] AS stock_minute,
    symbol,
    company_name,
    sector,
    open_price,
    close_price,
    high_price,
    low_price,
    price_change,
    change_percent,
    volume,
    market_cap_millions
INTO StockOutput
FROM StockInput;


-- ========================
-- Query 8: Streaming real-time data đến Power BI
-- Tổng hợp mỗi 1 phút cho dashboard thời gian thực
-- ========================
SELECT
    System.Timestamp() AS timestamp,
    region,
    category,
    COUNT(*) AS transactions_per_minute,
    SUM(quantity) AS items_sold,
    SUM(final_amount) AS revenue,
    AVG(final_amount) AS avg_order_value,
    COUNT(DISTINCT customer_id) AS unique_customers
INTO PowerBIOutput
FROM SalesInput
TIMESTAMP BY CAST([timestamp] AS datetime)
GROUP BY
    region,
    category,
    TumblingWindow(minute, 1);


-- ========================
-- Query 9: Kết hợp dữ liệu bán hàng với thời tiết (JOIN)
-- Phân tích tác động thời tiết đến bán hàng
-- ========================
SELECT
    S.region,
    S.category,
    System.Timestamp() AS window_time,
    SUM(S.final_amount) AS total_sales,
    COUNT(*) AS transaction_count,
    AVG(W.temperature_celsius) AS avg_temperature,
    AVG(W.humidity_percent) AS avg_humidity,
    MAX(W.weather_condition) AS weather_condition
INTO WeatherSalesCorrelationOutput
FROM SalesInput S
TIMESTAMP BY CAST(S.[timestamp] AS datetime)
JOIN WeatherInput W
TIMESTAMP BY CAST(W.[timestamp] AS datetime)
ON S.region = W.region
AND DATEDIFF(minute, S, W) BETWEEN -30 AND 30
GROUP BY
    S.region,
    S.category,
    TumblingWindow(minute, 30);
