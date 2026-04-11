# SQL QUERIES TRONG CÁC KỊCH BẢN DEMO
## Phần này thể hiện truy vấn SQL cụ thể cho mỗi kịch bản

---

## 📊 KỊCH BẢN 1: "Realtime Revenue Surge Detection"
**SQL Queries để monitor & phát hiện anomaly**

### Vị trí trong demo:
- **Step 5:** Xem Stream Analytics xử lý metrics (Watermark Delay)
- **Step 7:** Giải thích Stream Analytics Query
- **SQL phần:** Xem kết quả sau khi Stream Analytics ghi dữ liệu vào Azure SQL

### SQL Queries:

#### Query 1.1: Xem dữ liệu RAW sau khi ETL
```sql
-- File: stream_analytics/stream_query.sql (lines 114-129)
-- Dữ liệu này được ghi từ Stream Analytics vào bảng SalesTransactions

SELECT TOP 100
    event_time,
    store_id,
    product_id,
    category,
    units_sold,
    unit_price,
    revenue,
    temperature,
    weather,
    holiday,
    ingest_lag_seconds  -- ⭐ Độ trễ từ lúc event tạo đến lúc được ingest vào hub
FROM dbo.SalesTransactions
ORDER BY event_time DESC;
```

**Output Example:**
```
event_time           | store_id | product_id | revenue | ingest_lag_seconds
2025-04-09 12:05:30 | S01      | P001       | 150.50  | 2
2025-04-09 12:05:29 | S02      | P023       | 45.00   | 1
2025-04-09 12:05:28 | S01      | COKE       | 3.50    | 2
...
```

#### Query 1.2: Phát hiện Revenue Spike (tăng đột biến)
```sql
-- Xem dữ liệu từ view vw_RealtimeDashboard
-- File: sql/create_tables.sql (lines 183-196)

SELECT TOP 1000
    event_time,
    store_id,
    category,
    units_sold,
    revenue,
    weather,
    temperature
FROM dbo.vw_RealtimeDashboard
ORDER BY event_time DESC;
```

#### Query 1.3: Xem cảnh báo ANOMALY được Stream Analytics phát hiện
```sql
-- Bảng SalesAlerts được Stream Analytics populate (lines 149-156 trong stream_query.sql)

SELECT TOP 50
    alert_time,
    store_id,
    type,               -- 'spike' hoặc 'dip'
    value              -- revenue value của alert
FROM dbo.SalesAlerts
ORDER BY alert_time DESC;
```

**Output Example:**
```
alert_time           | store_id | type  | value
2025-04-09 12:06:15 | S01      | spike | 1500.25  ⚠️ Revenue tăng đột biến
2025-04-09 12:05:45 | S02      | spike | 980.50
2025-04-09 12:04:30 | S03      | dip   | 120.00
```

#### Query 1.4: Tính Revenue theo 5-phút window
```sql
-- Stream Analytics tính toán (lines 67-84 trong stream_query.sql)
-- Dữ liệu được ghi vào bảng HourlySalesSummary

SELECT
    window_start,
    window_end,
    store_id,
    SUM(units_sold) AS total_units,
    SUM(revenue) AS total_revenue,
    COUNT(*) AS transaction_count,
    AVG(avg_price) AS avg_unit_price
FROM dbo.HourlySalesSummary
WHERE window_end >= DATEADD(HOUR, -1, GETUTCDATE())  -- Last 1 hour
GROUP BY window_start, window_end, store_id
ORDER BY window_end DESC;
```

**Output Example:**
```
window_start        | window_end          | store_id | total_revenue | transaction_count
2025-04-09 12:05:00 | 2025-04-09 12:10:00 | S01      | 5200.00       | 125  ⬆️⬆️⬆️ (spike!)
2025-04-09 12:00:00 | 2025-04-09 12:05:00 | S01      | 1200.00       | 25   (normal)
```

#### Query 1.5: So sánh Revenue Before & After Flash Sale
```sql
-- Để minh họa trong Power BI hoặc terminal

DECLARE @NormalPeriod_Start DATETIME2 = DATEADD(HOUR, -3, GETUTCDATE());
DECLARE @NormalPeriod_End DATETIME2 = DATEADD(HOUR, -1, GETUTCDATE());
DECLARE @SpikePeriod_Start DATETIME2 = DATEADD(MINUTE, -5, GETUTCDATE());
DECLARE @SpikePeriod_End DATETIME2 = GETUTCDATE();

SELECT
    'Normal Period' AS period,
    SUM(revenue) AS total_revenue,
    COUNT(*) AS tx_count,
    AVG(revenue / units_sold) AS avg_transaction_value
FROM dbo.SalesTransactions
WHERE event_time BETWEEN @NormalPeriod_Start AND @NormalPeriod_End

UNION ALL

SELECT
    'Spike Period' AS period,
    SUM(revenue) AS total_revenue,
    COUNT(*) AS tx_count,
    AVG(revenue / units_sold) AS avg_transaction_value
FROM dbo.SalesTransactions
WHERE event_time BETWEEN @SpikePeriod_Start AND @SpikePeriod_End;
```

**Output Example:**
```
period         | total_revenue | tx_count | avg_transaction_value
Normal Period  | 1200.00       | 25       | 48.00
Spike Period   | 5200.00       | 125      | 41.60  ⬆️ 300% tăng!
```

---

## 🤖 KỊCH BẢN 2: "ML-Powered Viral Product Prediction"
**SQL Queries để quản lý predictions và model metadata**

### Vị trí trong demo:
- **Step 4:** Mở Power BI bảng "Forecast vs Actual"
- **Step 5:** So sánh Model Versions

### SQL Queries:

#### Query 2.1: Xem Model Metadata (model version, accuracy)
```sql
-- Bảng này chứa metadata của các model versions
-- Được tạo bởi ml/train_model.py sau khi train xong

SELECT TOP 10
    model_version,
    created_at,
    f1_score,
    auc_score,
    rmse,
    training_data_size,
    model_status  -- 'development', 'staging', 'production'
FROM dbo.ModelMetadata
ORDER BY created_at DESC;
```

**Output Example:**
```
model_version | created_at              | f1_score | auc_score | rmse  | model_status
v3            | 2025-04-09 09:30:00    | 0.89     | 0.94      | 11.2  | production ✅
v2            | 2025-04-05 08:15:00    | 0.87     | 0.92      | 12.5  | archived
v1            | 2025-04-02 10:00:00    | 0.82     | 0.89      | 15.3  | archived
```

#### Query 2.2: Xem Predictions từ ML Model
```sql
-- Bảng SalesForecast chứa predictions cho từng product/store/hour
-- Được populate bởi ml/score.py chạy realtime

SELECT TOP 100
    forecast_date,
    forecast_hour,
    store_id,
    product_id,
    category,
    predicted_quantity,
    predicted_revenue,
    confidence_lower,
    confidence_upper,
    model_version
FROM dbo.SalesForecast
WHERE forecast_date = CAST(GETUTCDATE() AS DATE)
ORDER BY forecast_hour DESC, predicted_revenue DESC;
```

**Output Example:**
```
forecast_date | forecast_hour | store_id | product_id | category    | predicted_revenue | model_version
2025-04-09    | 14            | S01      | P001       | Electronics | 1250.00           | v3
2025-04-09    | 14            | S02      | P023       | Snacks      | 450.00            | v3
2025-04-09    | 14            | S01      | COKE       | Beverage    | 350.75            | v3
```

#### Query 2.3: Forecast vs Actual - Accuracy Check (Power BI bảng này)
```sql
-- View: vw_ForecastVsActual (sql/create_tables.sql lines 209-238)
-- So sánh dự đoán vs kết quả thực tế

SELECT
    forecast_date,
    forecast_hour,
    store_id,
    category,
    predicted_quantity,
    actual_quantity,
    predicted_revenue,
    actual_revenue,
    forecast_error,         -- |predicted - actual|
    CAST(100.0 * ABS(predicted_revenue - actual_revenue) / 
         NULLIF(actual_revenue, 0) AS DECIMAL(5,2)) AS error_percentage,
    model_version
FROM dbo.vw_ForecastVsActual
WHERE forecast_datetime >= DATEADD(DAY, -7, GETUTCDATE())  -- Last 7 days
ORDER BY forecast_datetime DESC;
```

**Output Example:**
```
forecast_date | store_id | predicted_revenue | actual_revenue | forecast_error | error_percentage | accuracy
2025-04-08    | S01      | 1250.00           | 1230.50        | 19.50          | 1.6%             | 98.4% ✅
2025-04-08    | S02      | 450.00            | 420.75         | 29.25          | 6.5%             | 93.5% ✅
2025-04-08    | S03      | 300.00            | 280.50         | 19.50          | 6.5%             | 93.5% ✅
```

#### Query 2.4: Top 10 Viral Products (highest probability)
```sql
-- Để hiển thị trong Power BI bảng "Top Viral Products"
-- Sắp xếp theo predicted_revenue cao nhất cho hôm nay

SELECT TOP 10
    forecast_date,
    product_id,
    category,
    SUM(predicted_quantity) AS total_predicted_units,
    SUM(predicted_revenue) AS total_predicted_revenue,
    COUNT(*) AS forecast_count,
    CAST(100.0 * SUM(predicted_revenue) / 
         SUM(SUM(predicted_revenue)) OVER() AS DECIMAL(5,2)) AS revenue_percentage
FROM dbo.SalesForecast
WHERE forecast_date = CAST(GETUTCDATE() AS DATE)
GROUP BY forecast_date, product_id, category
ORDER BY total_predicted_revenue DESC;
```

**Output Example:**
```
product_id | category    | predicted_units | predicted_revenue | revenue_percentage | recommendation
P001       | Electronics | 320 units       | 8000.00           | 15.2%              | 👑 VIRAL! Stock 500 units
P023       | Snacks      | 480 un
