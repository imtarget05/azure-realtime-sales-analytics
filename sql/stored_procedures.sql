-- ============================================================
-- Stored Procedures cho Azure Data Factory pipeline
-- Chạy sau create_tables.sql
-- ============================================================

-- ========================
-- SP 1: Chuẩn bị dữ liệu huấn luyện ML
-- Data Factory gọi trước khi trigger ML training
-- ========================
IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'sp_PrepareTrainingData')
    DROP PROCEDURE sp_PrepareTrainingData;
GO

CREATE PROCEDURE sp_PrepareTrainingData
AS
BEGIN
    SET NOCOUNT ON;

    -- Tạo bảng tạm chứa training data (tổng hợp theo giờ)
    IF OBJECT_ID('dbo.MLTrainingData', 'U') IS NOT NULL
        DROP TABLE dbo.MLTrainingData;

    SELECT 
        s.sale_date AS [date],
        s.sale_hour AS [hour],
        s.day_of_week,
        DAY(s.sale_date) AS day_of_month,
        MONTH(s.sale_date) AS [month],
        CASE WHEN s.day_of_week IN ('Saturday', 'Sunday') THEN 1 ELSE 0 END AS is_weekend,
        s.region,
        s.category,
        ISNULL(w.temperature_celsius, 22.0) AS temperature,
        ISNULL(w.humidity_percent, 60) AS humidity,
        CASE WHEN w.weather_condition IN ('Rainy', 'Stormy') THEN 1 ELSE 0 END AS is_rainy,
        SUM(s.quantity) AS quantity,
        SUM(s.final_amount) AS revenue
    INTO dbo.MLTrainingData
    FROM SalesTransactions s
    LEFT JOIN WeatherData w 
        ON s.region = w.region 
        AND s.sale_date = w.weather_date 
        AND s.sale_hour = w.weather_hour
    WHERE s.sale_date >= DATEADD(day, -90, GETUTCDATE())  -- 90 ngày gần nhất
    GROUP BY 
        s.sale_date, s.sale_hour, s.day_of_week,
        s.region, s.category,
        w.temperature_celsius, w.humidity_percent, w.weather_condition;

    DECLARE @rowcount INT = (SELECT COUNT(*) FROM dbo.MLTrainingData);
    PRINT CONCAT('Prepared ', @rowcount, ' training records.');
END
GO

PRINT 'Created sp_PrepareTrainingData';
GO


-- ========================
-- SP 2: Cập nhật bảng dự đoán sau khi ML hoàn thành
-- Data Factory gọi sau khi ML pipeline chạy xong
-- ========================
IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'sp_UpdateForecasts')
    DROP PROCEDURE sp_UpdateForecasts;
GO

CREATE PROCEDURE sp_UpdateForecasts
AS
BEGIN
    SET NOCOUNT ON;

    -- Xóa dự đoán cũ (quá 7 ngày)
    DELETE FROM SalesForecast
    WHERE forecast_date < DATEADD(day, -7, GETUTCDATE());

    -- Cập nhật trạng thái cảnh báo đã resolve
    UPDATE SalesAlerts
    SET is_resolved = 1
    WHERE alert_timestamp < DATEADD(hour, -24, GETUTCDATE())
      AND is_resolved = 0;

    DECLARE @deleted INT = @@ROWCOUNT;
    PRINT CONCAT('Cleaned up ', @deleted, ' old alerts. Forecasts updated.');
END
GO

PRINT 'Created sp_UpdateForecasts';
GO


-- ========================
-- SP 3: Tổng hợp dữ liệu cho Power BI refresh
-- Có thể gọi từ Data Factory hoặc scheduled job
-- ========================
IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'sp_RefreshPowerBISummary')
    DROP PROCEDURE sp_RefreshPowerBISummary;
GO

CREATE PROCEDURE sp_RefreshPowerBISummary
AS
BEGIN
    SET NOCOUNT ON;

    -- Tạo/cập nhật bảng summary cho Power BI
    IF OBJECT_ID('dbo.PowerBIDailySummary', 'U') IS NOT NULL
        DROP TABLE dbo.PowerBIDailySummary;

    SELECT
        sale_date,
        region,
        category,
        COUNT(*) AS total_transactions,
        SUM(quantity) AS total_items,
        SUM(final_amount) AS total_revenue,
        AVG(final_amount) AS avg_order_value,
        COUNT(DISTINCT customer_id) AS unique_customers,
        AVG(CAST(rating AS FLOAT)) AS avg_rating,
        SUM(CASE WHEN is_online = 1 THEN 1 ELSE 0 END) AS online_orders,
        SUM(CASE WHEN is_online = 0 THEN 1 ELSE 0 END) AS offline_orders
    INTO dbo.PowerBIDailySummary
    FROM SalesTransactions
    GROUP BY sale_date, region, category;

    DECLARE @rowcount INT = (SELECT COUNT(*) FROM dbo.PowerBIDailySummary);
    PRINT CONCAT('Power BI summary refreshed: ', @rowcount, ' rows.');
END
GO

PRINT 'Created sp_RefreshPowerBISummary';
GO

PRINT '============================================================';
PRINT '  STORED PROCEDURES CREATED SUCCESSFULLY!';
PRINT '============================================================';
