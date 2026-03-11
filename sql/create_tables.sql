-- ============================================================
-- Schema cho Azure SQL Database - Hệ thống phân tích bán hàng
-- Chạy script này trên Azure SQL Database sau khi tạo database
-- ============================================================

-- ========================
-- 1. Bảng giao dịch bán hàng (nhận từ Stream Analytics)
-- ========================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'SalesTransactions')
BEGIN
    CREATE TABLE SalesTransactions (
        id                  INT IDENTITY(1,1) PRIMARY KEY,
        transaction_id      NVARCHAR(50) NOT NULL,
        event_timestamp     DATETIME2 NOT NULL,
        sale_date           DATE NOT NULL,
        sale_hour           INT NOT NULL,
        day_of_week         NVARCHAR(20),
        product_id          NVARCHAR(10) NOT NULL,
        product_name        NVARCHAR(100),
        category            NVARCHAR(50),
        quantity            INT NOT NULL,
        unit_price          DECIMAL(10, 2) NOT NULL,
        total_amount        DECIMAL(12, 2) NOT NULL,
        discount_percent    INT DEFAULT 0,
        discount_amount     DECIMAL(10, 2) DEFAULT 0,
        final_amount        DECIMAL(12, 2) NOT NULL,
        customer_id         NVARCHAR(10),
        customer_segment    NVARCHAR(20),
        region              NVARCHAR(20),
        payment_method      NVARCHAR(20),
        is_online           BIT,
        rating              INT,
        created_at          DATETIME2 DEFAULT GETUTCDATE()
    );
    PRINT 'Created table: SalesTransactions';
END
GO

-- ========================
-- 2. Bảng tổng hợp bán hàng theo giờ (từ Stream Analytics)
-- ========================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'HourlySalesSummary')
BEGIN
    CREATE TABLE HourlySalesSummary (
        id                  INT IDENTITY(1,1) PRIMARY KEY,
        window_start        DATETIME2 NOT NULL,
        window_end          DATETIME2 NOT NULL,
        region              NVARCHAR(20),
        category            NVARCHAR(50),
        total_transactions  INT,
        total_quantity      INT,
        total_revenue       DECIMAL(15, 2),
        avg_order_value     DECIMAL(10, 2),
        max_order_value     DECIMAL(12, 2),
        min_order_value     DECIMAL(12, 2),
        created_at          DATETIME2 DEFAULT GETUTCDATE()
    );
    PRINT 'Created table: HourlySalesSummary';
END
GO

-- ========================
-- 3. Bảng tổng hợp theo sản phẩm (từ Stream Analytics)
-- ========================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'ProductSalesSummary')
BEGIN
    CREATE TABLE ProductSalesSummary (
        id                  INT IDENTITY(1,1) PRIMARY KEY,
        window_start        DATETIME2 NOT NULL,
        window_end          DATETIME2 NOT NULL,
        product_id          NVARCHAR(10),
        product_name        NVARCHAR(100),
        category            NVARCHAR(50),
        total_sold          INT,
        total_revenue       DECIMAL(15, 2),
        avg_price           DECIMAL(10, 2),
        avg_rating          DECIMAL(3, 1),
        created_at          DATETIME2 DEFAULT GETUTCDATE()
    );
    PRINT 'Created table: ProductSalesSummary';
END
GO

-- ========================
-- 4. Bảng cảnh báo bất thường (từ Stream Analytics)
-- ========================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'SalesAlerts')
BEGIN
    CREATE TABLE SalesAlerts (
        id                  INT IDENTITY(1,1) PRIMARY KEY,
        alert_timestamp     DATETIME2 NOT NULL,
        alert_type          NVARCHAR(50) NOT NULL,
        severity            NVARCHAR(20) NOT NULL,     -- Low, Medium, High, Critical
        region              NVARCHAR(20),
        category            NVARCHAR(50),
        product_id          NVARCHAR(10),
        metric_value        DECIMAL(15, 2),
        threshold_value     DECIMAL(15, 2),
        description         NVARCHAR(500),
        is_resolved         BIT DEFAULT 0,
        created_at          DATETIME2 DEFAULT GETUTCDATE()
    );
    PRINT 'Created table: SalesAlerts';
END
GO

-- ========================
-- 5. Bảng dữ liệu thời tiết
-- ========================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'WeatherData')
BEGIN
    CREATE TABLE WeatherData (
        id                      INT IDENTITY(1,1) PRIMARY KEY,
        event_timestamp         DATETIME2 NOT NULL,
        weather_date            DATE NOT NULL,
        weather_hour            INT,
        region                  NVARCHAR(20),
        temperature_celsius     DECIMAL(5, 1),
        humidity_percent        INT,
        wind_speed_kmh          DECIMAL(5, 1),
        precipitation_mm        DECIMAL(6, 1),
        weather_condition       NVARCHAR(20),
        uv_index                INT,
        created_at              DATETIME2 DEFAULT GETUTCDATE()
    );
    PRINT 'Created table: WeatherData';
END
GO

-- ========================
-- 6. Bảng dữ liệu chứng khoán
-- ========================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'StockData')
BEGIN
    CREATE TABLE StockData (
        id                  INT IDENTITY(1,1) PRIMARY KEY,
        event_timestamp     DATETIME2 NOT NULL,
        stock_date          DATE NOT NULL,
        stock_hour          INT,
        stock_minute        INT,
        symbol              NVARCHAR(10) NOT NULL,
        company_name        NVARCHAR(100),
        sector              NVARCHAR(50),
        open_price          DECIMAL(10, 2),
        close_price         DECIMAL(10, 2),
        high_price          DECIMAL(10, 2),
        low_price           DECIMAL(10, 2),
        price_change        DECIMAL(10, 2),
        change_percent      DECIMAL(8, 4),
        volume              INT,
        market_cap_millions DECIMAL(15, 2),
        created_at          DATETIME2 DEFAULT GETUTCDATE()
    );
    PRINT 'Created table: StockData';
END
GO

-- ========================
-- 7. Bảng dữ liệu dự đoán từ ML
-- ========================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'SalesForecast')
BEGIN
    CREATE TABLE SalesForecast (
        id                  INT IDENTITY(1,1) PRIMARY KEY,
        forecast_date       DATE NOT NULL,
        forecast_hour       INT,
        region              NVARCHAR(20),
        category            NVARCHAR(50),
        predicted_quantity  INT,
        predicted_revenue   DECIMAL(15, 2),
        confidence_lower    DECIMAL(15, 2),
        confidence_upper    DECIMAL(15, 2),
        model_version       NVARCHAR(50),
        created_at          DATETIME2 DEFAULT GETUTCDATE()
    );
    PRINT 'Created table: SalesForecast';
END
GO

-- ========================
-- INDEXES để tối ưu hiệu suất truy vấn
-- ========================
CREATE NONCLUSTERED INDEX IX_SalesTransactions_Date 
    ON SalesTransactions(sale_date, sale_hour);

CREATE NONCLUSTERED INDEX IX_SalesTransactions_Product 
    ON SalesTransactions(product_id, category);

CREATE NONCLUSTERED INDEX IX_SalesTransactions_Region 
    ON SalesTransactions(region);

CREATE NONCLUSTERED INDEX IX_SalesTransactions_TransactionId 
    ON SalesTransactions(transaction_id);

CREATE NONCLUSTERED INDEX IX_HourlySalesSummary_Window 
    ON HourlySalesSummary(window_start, window_end);

CREATE NONCLUSTERED INDEX IX_ProductSalesSummary_Product 
    ON ProductSalesSummary(product_id, window_start);

CREATE NONCLUSTERED INDEX IX_SalesAlerts_Type 
    ON SalesAlerts(alert_type, alert_timestamp);

CREATE NONCLUSTERED INDEX IX_WeatherData_Region_Date 
    ON WeatherData(region, weather_date);

CREATE NONCLUSTERED INDEX IX_StockData_Symbol_Date 
    ON StockData(symbol, stock_date);

CREATE NONCLUSTERED INDEX IX_SalesForecast_Date 
    ON SalesForecast(forecast_date, region, category);

PRINT 'All indexes created successfully.';
GO

-- ========================
-- VIEW: Tổng quan bán hàng (cho Power BI)
-- ========================
IF EXISTS (SELECT * FROM sys.views WHERE name = 'vw_SalesOverview')
    DROP VIEW vw_SalesOverview;
GO

CREATE VIEW vw_SalesOverview AS
SELECT 
    sale_date,
    sale_hour,
    day_of_week,
    region,
    category,
    product_name,
    customer_segment,
    payment_method,
    is_online,
    COUNT(*) AS transaction_count,
    SUM(quantity) AS total_quantity,
    SUM(final_amount) AS total_revenue,
    AVG(final_amount) AS avg_order_value,
    AVG(CAST(discount_percent AS DECIMAL(5,2))) AS avg_discount,
    AVG(CAST(rating AS DECIMAL(3,1))) AS avg_rating
FROM SalesTransactions
GROUP BY 
    sale_date, sale_hour, day_of_week, region, category, 
    product_name, customer_segment, payment_method, is_online;
GO

PRINT 'View vw_SalesOverview created.';
GO

-- ========================
-- VIEW: So sánh dự đoán vs thực tế (cho Power BI)
-- ========================
IF EXISTS (SELECT * FROM sys.views WHERE name = 'vw_ForecastVsActual')
    DROP VIEW vw_ForecastVsActual;
GO

CREATE VIEW vw_ForecastVsActual AS
SELECT 
    f.forecast_date,
    f.forecast_hour,
    f.region,
    f.category,
    f.predicted_quantity,
    f.predicted_revenue,
    f.confidence_lower,
    f.confidence_upper,
    ISNULL(a.actual_quantity, 0) AS actual_quantity,
    ISNULL(a.actual_revenue, 0) AS actual_revenue,
    ABS(f.predicted_revenue - ISNULL(a.actual_revenue, 0)) AS forecast_error,
    f.model_version
FROM SalesForecast f
LEFT JOIN (
    SELECT 
        sale_date,
        sale_hour,
        region,
        category,
        SUM(quantity) AS actual_quantity,
        SUM(final_amount) AS actual_revenue
    FROM SalesTransactions
    GROUP BY sale_date, sale_hour, region, category
) a ON f.forecast_date = a.sale_date 
    AND f.forecast_hour = a.sale_hour 
    AND f.region = a.region 
    AND f.category = a.category;
GO

PRINT 'View vw_ForecastVsActual created.';
GO

-- ========================
-- VIEW: Dashboard thời gian thực (cho Power BI)
-- ========================
IF EXISTS (SELECT * FROM sys.views WHERE name = 'vw_RealtimeDashboard')
    DROP VIEW vw_RealtimeDashboard;
GO

CREATE VIEW vw_RealtimeDashboard AS
SELECT TOP 1000
    transaction_id,
    event_timestamp,
    product_name,
    category,
    quantity,
    final_amount,
    region,
    customer_segment,
    payment_method,
    is_online,
    rating
FROM SalesTransactions
ORDER BY event_timestamp DESC;
GO

PRINT 'View vw_RealtimeDashboard created.';
GO

PRINT '============================================================';
PRINT '  DATABASE SCHEMA CREATION COMPLETED SUCCESSFULLY!';
PRINT '============================================================';
