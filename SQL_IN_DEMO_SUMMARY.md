# TÓM TẮT: SQL TRONG 5 KỊCH BẢN DEMO

## 📍 VỊ TRÍ SQL XUẤT HIỆN TRONG MỖI KỊCH BẢN

---

### 🎬 KỊCH BẢN 1: "Realtime Revenue Surge Detection" 
**⏱️ 5-7 phút**

#### Nơi SQL xuất hiện:
1. **Step 1 → 4:** Data được sinh ra từ Python → gửi vào Event Hub → Stream Analytics xử lý

2. **Step 7 - CHÍNH LÀ PHẦN SQL:**
   - Giải thích **Stream Analytics Query** (file: `stream_analytics/stream_query.sql`)
   - Query này không phải SQL Server truyền thống, mà là **Azure Stream Analytics Query Language** (gần giống SQL nhưng có extensions)
   - Các thành phần chính trong query:
   ```sql
   WITH Cleaned AS (...)       -- Step 1: Data validation & cleaning
   WITH Enriched AS (...)      -- Step 2: Calculate revenue, map category
   WITH Agg5m AS (...)         -- Step 3: Aggregate theo 5-minute window
   WITH AnomalySignals AS (...) -- Step 4: Detect spike/dip
   SELECT ... INTO SalesTransactionsOutput  -- Output 1: Raw data → SQL table
   SELECT ... INTO HourlySalesSummaryOutput -- Output 2: Aggregated → SQL table
   SELECT ... INTO SalesAlertsOutput        -- Output 3: Anomalies → SQL table
   ```

3. **Step 5 (Azure Portal):** Xem metrics của Stream Analytics (throughput, latency)

4. **DEMO SQL trong bước này:**
   - Mở **Azure SQL Query Editor** và chạy:
   ```sql
   -- Query 1.1: Xem raw data sau ETL
   SELECT TOP 100 * FROM dbo.SalesTransactions ORDER BY event_time DESC;
   
   -- Query 1.3: Xem anomaly alerts
   SELECT TOP 50 * FROM dbo.SalesAlerts ORDER BY alert_time DESC;
   
   -- Query 1.4: Tính revenue theo 5-phút window
   SELECT window_start, store_id, SUM(revenue) AS total_revenue 
   FROM dbo.HourlySalesSummary 
   WHERE window_end >= DATEADD(HOUR, -1, GETUTCDATE())
   GROUP BY window_start, store_id
   ORDER BY window_end DESC;
   ```

**🎯 Điểm mạnh SQL ở đây:**
- Show được data flow từ Event Hub → Stream Analytics → SQL Database
- Chứng minh latency thấp (ingest_lag_seconds < 5 giây)
- Anomaly detection dùng **AnomalyDetection_SpikeAndDip()** function
- Demonstrare real-time aggregation (5-minute tumbling window)

---

### 🤖 KỊCH BẢN 2: "ML-Powered Viral Product Prediction"
**⏱️ 5-7 phút**

#### Nơi SQL xuất hiện:
1. **Step 1-3:** ML training & inference (Python code)

2. **Step 4 - CHÍNH LÀ PHẦN SQL:**
   - Mở Power BI bảng **"Forecast vs Actual"** 
   - Bảng này dùng SQL View: `vw_ForecastVsActual` (file: `sql/create_tables.sql` lines 209-238)
   - View này JOIN giữa:
     - `SalesForecast` table (ML predictions)
     - Subquery tính `actual_quantity`, `actual_revenue` từ `SalesTransactions`
   ```sql
   SELECT
       f.forecast_date,
       f.predicted_quantity,
       a.actual_quantity,
       f.predicted_revenue,
       a.actual_revenue,
       ABS(f.predicted_revenue - a.actual_revenue) AS forecast_error
   FROM dbo.SalesForecast f
   LEFT JOIN (
       SELECT CAST(event_time AS DATE) AS sale_date,
              SUM(units_sold) AS actual_quantity,
              SUM(revenue) AS actual_revenue
       FROM dbo.SalesTransactions
       GROUP BY CAST(event_time AS DATE)
   ) a ON f.forecast_date = a.sale_date;
   ```

3. **Step 6 - DEMO SQL:**
   ```sql
   -- Query 2.4: Top 10 Viral Products
   SELECT TOP 10
       product_id,
       SUM(predicted_revenue) AS predicted_revenue
   FROM dbo.SalesForecast
   WHERE forecast_date = CAST(GETUTCDATE() AS DATE)
   GROUP BY product_id
   ORDER BY predicted_revenue DESC;
   ```

**🎯 Điểm mạnh SQL ở đây:**
- Thể hiện được **Forecast vs Actual comparison** (chứng minh model accuracy)
- Dùng SQL VIEW để encapsulate logic phức tạp
- LEFT JOIN tự động handle trường hợp chưa có actual data
- DAX + SQL kết hợp để Power BI dashboard dynamic

---

### 🔄 KỊCH BẢN 3: "Data Drift Detection & Auto-Retrain"
**⏱️ 4-5 phút**

#### Nơi SQL xuất hiện:
1. **Step 1-6:** Python scripts (drift_monitor.py, retrain_and_compare.py) tính toán metrics

2. **Step 7 - CHÍNH LÀ PHẦN SQL:**
   - Power BI bảng **"Model Health Dashboard"** hiển thị:
     - Retrain history (model versions & decisions)
     - Accuracy trend over time
   - SQL Query:
   ```sql
   -- Query 3.1: Retrain History
   SELECT TOP 20
       retrain_date,
       old_model_version,
       new_model_version,
       old_f1_score,
       new_f1_score,
       decision  -- 'APPROVE' hoặc 'REJECT'
   FROM dbo.RetrainHistory
   ORDER BY retrain_date DESC;
   
   -- Query 3.2: Accuracy Timeline (cho biểu đồ)
   SELECT
       CAST(forecast_datetime AS DATE) AS date,
       AVG(forecast_error) AS avg_error
   FROM dbo.vw_ForecastVsActual
   WHERE forecast_datetime >= DATEADD(DAY, -14, GETUTCDATE())
   GROUP BY CAST(forecast_datetime AS DATE)
   ORDER BY date DESC;
   ```

3. **DEMO SQL để trigger rollback:**
   ```sql
   -- Query 3.4: Health Check
   SELECT
       model_version,
       DATEDIFF(HOUR, created_at, GETUTCDATE()) AS hours_in_prod,
       (SELECT AVG(forecast_error) FROM dbo.vw_ForecastVsActual 
        WHERE model_version = ModelMetadata.model_version 
        AND forecast_datetime >= DATEADD(HOUR, -2, GETUTCDATE())) AS recent_error,
       CASE 
           WHEN recent_error > 25.0 THEN '🔴 ROLLBACK REQUIRED'
           ELSE '✅ HEALTHY'
       END AS status
   FROM dbo.ModelMetadata
   WHERE model_status = 'production';
   ```

**🎯 Điểm mạnh SQL ở đây:**
- Thể hiện **trend line** của accuracy qua thời gian
- Auto-trigger logic dùng SQL để detect degradation
- Window functions (DATEPART, DATEADD, AVG) để tính rolling metrics
- Chứng minh hệ thống **tự động phát hiện & trigger retrain**

---

### 🔐 KỊCH BẢN 4: "Security & Governance - Row-Level Security"
**⏱️ 3-4 phút**

#### Nơi SQL xuất hiện:
1. **Step 2:** Power BI RLS dùng DAX formula (không phải SQL trực tiếp, nhưng underlying data từ SQL)

2. **Step 3 - CHÍNH LÀ PHẦN SQL:**
   - Mở **Azure SQL Query Editor** chạy:
   ```sql
   -- Query 4.1: Security Mapping Table
   SELECT
       user_email,
       user_role,
       allowed_store_ids,
       allowed_regions
   FROM dbo.SecurityMapping;
   
   -- Output:
   -- manager_north@co.com   | Store Manager | S01       | North
   -- director@co.com        | Director      | S01,S02,S03 | North,South,Central
   ```

3. **Step 4 - Giải thích DAX RLS formula:**
   ```dax
   -- Power BI DAX (nhưng tương đương SQL logic):
   [StoreRegion] IN VALUES(SecurityMapping[Region])
   ```
   - Tương đương SQL:
   ```sql
   WHERE store_id IN (SELECT allowed_store_ids 
                      FROM SecurityMapping 
                      WHERE user_email = CURRENT_USER)
   ```

4. **Step 5 - Access Audit Log:**
   ```sql
   -- Query 4.2: Audit Trail
   SELECT TOP 100
       access_time,
       user_email,
       action,
       table_name,
       result  -- 'SUCCESS' hoặc 'DENIED'
   FROM dbo.AccessAudit
   ORDER BY access_time DESC;
   ```

**🎯 Điểm mạnh SQL ở đây:**
- Thể hiện **Row-Level Security** mapping trong SQL
- **Audit log** chứng minh governance (ai truy cập gì, khi nào)
- Chứng minh **DENIED access** khi user cố xem data ngoài phạm vi
- Security mapping được centralize trong SQL (không hardcode trong code)

---

### 📈 KỊCH BẢN 5: "System Performance & Latency Metrics"
**⏱️ 4-5 phút**

#### Nơi SQL xuất hiện:
1. **Step 4 - CHÍNH LÀ PHẦN SQL:**
   - Chạy benchmark script: `benchmarks/benchmark_latency.py`
   - Script này query SQL để tính latency:
   ```sql
   -- Query 5.1: Latency Distribution
   SELECT
       DATEPART(HOUR, event_time) AS hour,
       COUNT(*) AS total_events,
       MIN(ingest_lag_seconds) AS min_latency,
       MAX(ingest_lag_seconds) AS max_latency,
       AVG(ingest_lag_seconds) AS avg_latency,
       PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY ingest_lag_seconds) AS p95,
       PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY ingest_lag_seconds) AS p99
   FROM dbo.SalesTransactions
   WHERE event_time >= DATEADD(DAY, -1, GETUTCDATE())
   GROUP BY DATEPART(HOUR, event_time)
   ORDER BY hour DESC;
   ```

2. **Step 5 - Throughput Query:**
   ```sql
   -- Query 5.2: Throughput Analysis
   SELECT
       DATEPART(MINUTE, event_time) / 5 * 5 AS five_min_bucket,
       COUNT(*) AS event_count,
       COUNT(*) / 300.0 AS events_per_second
   FROM dbo.SalesTransacti
