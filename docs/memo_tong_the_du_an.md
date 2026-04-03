# MEMO TỔNG THỂ DỰ ÁN
## Hệ thống Trực quan Dữ liệu Bán hàng Thời gian thực trên Azure

> **Mục đích:** Tài liệu tham khảo toàn diện phục vụ viết báo cáo Word, chuẩn bị Slide PowerPoint, và ôn tập vấn đáp.  
> **Cập nhật:** Tháng 3/2026

---

## MỤC LỤC

1. [Tổng quan dự án](#1-tổng-quan-dự-án)
2. [Kiến trúc hệ thống](#2-kiến-trúc-hệ-thống)
3. [Hạ tầng Azure (Terraform)](#3-hạ-tầng-azure-terraform)
4. [Luồng dữ liệu (Data Pipeline)](#4-luồng-dữ-liệu-data-pipeline)
5. [Databricks — Medallion Architecture](#5-databricks--medallion-architecture)
6. [Machine Learning & MLOps](#6-machine-learning--mlops)
7. [Power BI — Trực quan hóa](#7-power-bi--trực-quan-hóa)
8. [Bảo mật (Security)](#8-bảo-mật-security)
9. [Monitoring & Telemetry](#9-monitoring--telemetry)
10. [Testing & Benchmarks](#10-testing--benchmarks)
11. [Web Application](#11-web-application)
12. [Chi phí & Tối ưu](#12-chi-phí--tối-ưu)
13. [Kết quả đã triển khai thực tế](#13-kết-quả-đã-triển-khai-thực-tế)
14. [Đóng góp chính & Điểm nổi bật](#14-đóng-góp-chính--điểm-nổi-bật)
15. [Câu hỏi vấn đáp thường gặp](#15-câu-hỏi-vấn-đáp-thường-gặp)

---

## 1. TỔNG QUAN DỰ ÁN

### 1.1 Đặt vấn đề
- Doanh nghiệp bán lẻ cần theo dõi doanh thu **thời gian thực** để đưa ra quyết định nhanh.
- Dữ liệu đến liên tục (~1.200 sự kiện/phút), đa nguồn (bán hàng, thời tiết, chứng khoán).
- Yêu cầu: Xử lý streaming → Lưu trữ → Dự đoán ML → Trực quan hóa, tất cả trên nền tảng Cloud.

### 1.2 Loại bài toán

| Bài toán | Phân loại |
|---|---|
| **Chính** | Trực quan hóa dữ liệu thời gian thực (Real-time Data Visualization) |
| **Phụ 1** | Phân tích dữ liệu bán hàng (Sales Analytics) |
| **Phụ 2** | Dự đoán doanh thu & sản lượng (Regression Prediction) |
| **Kỹ thuật** | Real-time Streaming Analytics + Batch ML Prediction |

### 1.3 Dữ liệu

| Nguồn | Format | Volume | Tần suất | Kênh nhận |
|---|---|---|---|---|
| Giao dịch bán hàng | JSON | ~20 events/s | Liên tục | Event Hub |
| Thời tiết (OpenWeatherMap) | JSON | ~5 events/30s | 30 giây | Event Hub |
| Chứng khoán | JSON | ~10 events/5s | 5 giây | Event Hub |
| Dữ liệu tham chiếu | JSON/CSV | ~50 KB | Khi cần | Blob Storage |

**Schema sự kiện bán hàng (8 trường):**
```json
{
  "timestamp": "2026-03-31T10:00:00Z",
  "store_id": "S01",
  "product_id": "COKE",
  "quantity": 3,
  "price": 1.50,
  "temperature": 28.5,
  "weather": "sunny",
  "holiday": 0
}
```

### 1.4 Quy mô dữ liệu
- **Tốc độ:** 1.200 events/phút (20 events/giây), burst mode 3.600 events/phút
- **Khối lượng ngày:** ~1.728.000 sự kiện/ngày (~15–20 triệu dòng/ngày ở chế độ đầy tải)
- **Kích thước DB:** ~10–15 GB/tháng
- **Benchmark dataset:** >4 GB CSV (~15–20 triệu dòng) để đo hiệu suất

---

## 2. KIẾN TRÚC HỆ THỐNG

### 2.1 Sơ đồ tổng thể

```
┌────────────────┐     ┌──────────────┐     ┌────────────────────┐     ┌──────────────┐
│ Data Generator │────▶│ Azure Event  │────▶│ Azure Stream       │────▶│ Azure SQL    │
│ (Python)       │     │ Hubs         │     │ Analytics          │     │ Database     │
│ • Sales Events │     │ • 4 partitions│    │ • 9 queries        │     │ • Raw + Agg  │
│ • Weather API  │     │ • 3-day retain│    │ • Tumbling/Sliding │     │ • Forecasts  │
│ • Stock Data   │     │ • Standard SKU│    │ • 3 SU             │     │ • Views + SP │
└────────────────┘     └──────┬───────┘     └────────┬───────────┘     └──────┬───────┘
                              │                      │                        │
                              ▼                      ▼                        ▼
                    ┌──────────────┐        ┌──────────────┐        ┌──────────────────┐
                    │ Azure        │        │ Power BI     │        │ Flask Web App    │
                    │ Functions    │        │ (SaaS)       │        │ (App Service)    │
                    │ • Validation │        │ • 4 trang    │        │ • Dự đoán online │
                    │ • Cleaning   │        │ • RLS        │        │ • REST API       │
                    └──────────────┘        │ • Auto 5s    │        └──────────────────┘
                                            └──────────────┘
                                                                    ┌──────────────────┐
┌────────────────┐     ┌──────────────┐     ┌──────────────┐       │ Azure ML         │
│ Databricks     │────▶│ Delta Lake   │────▶│ MLflow       │──────▶│ Online Endpoint   │
│ • Bronze/Silver│     │ (ADLS Gen2)  │     │ • Experiment │       │ • Blue/Green     │
│ • Gold/ML      │     │ • Parquet    │     │ • Registry   │       │ • Auto-scale     │
└────────────────┘     └──────────────┘     └──────────────┘       └──────────────────┘
```

### 2.2 Nguyên tắc thiết kế

| Nguyên tắc | Giải thích |
|---|---|
| **PaaS-first** | Toàn bộ dịch vụ là Managed — không quản lý VM |
| **Real-time** | Độ trễ 5–10 giây từ sự kiện → dashboard |
| **Medallion Architecture** | Bronze (raw) → Silver (clean) → Gold (aggregated) |
| **MLOps Level 2** | CI/CD cho cả code + model, auto-retrain |
| **Infrastructure as Code** | Terraform quản lý toàn bộ hạ tầng |
| **Security by Design** | Key Vault, Managed Identity, RLS |

### 2.3 Phân loại dịch vụ Cloud

| Dịch vụ Azure | Mô hình | Lý do chọn |
|---|---|---|
| Event Hubs | **PaaS** | Streaming managed, tự scale, không cần quản lý Kafka cluster |
| Stream Analytics | **PaaS** | Viết SQL-like query, Azure tự chạy & scale |
| Azure SQL Database | **PaaS** | DB-as-a-Service, auto-tuning, backup tự động |
| Azure ML | **PaaS** | ML platform managed: compute, registry, endpoint |
| Blob Storage / ADLS Gen2 | **PaaS** | Object storage qua API, lifecycle management |
| Data Factory | **PaaS** | ETL/orchestration managed |
| Azure Functions | **FaaS** | Serverless, event-driven validation |
| Key Vault | **PaaS** | Secret management, HSM-backed |
| Power BI | **SaaS** | BI hoàn chỉnh, người dùng chỉ tạo dashboard |

**Không sử dụng IaaS** — Tập trung vào logic dữ liệu thay vì quản lý hạ tầng.

---

## 3. HẠ TẦNG AZURE (TERRAFORM)

### 3.1 Tài nguyên đã triển khai

File chính: `terraform/main.tf` + `terraform/variables.tf`

| Tài nguyên | Tên thực tế | Tier/SKU |
|---|---|---|
| Resource Group | `rg-sales-analytics-dev` | — |
| Storage Account | `stsales*` | LRS, 4 containers |
| Event Hubs Namespace | `evhns-sales-analytics-vebku5` | Standard, 1 TU |
| Event Hub (sales) | `sales-events` | 4 partitions, 3 ngày retention |
| Event Hub (weather) | `weather-events` | 2 partitions |
| Event Hub (stock) | `stock-events` | 2 partitions |
| Azure SQL Server | `sql-sales-analytics-vebku5` | — |
| Azure SQL Database | `SalesAnalyticsDB` | S0/S1, 10 DTU |
| Stream Analytics | `sa-sales-analytics-vebku5` | 3 SU, compat 1.2 |
| ML Workspace | `aml-sales-analytics-vebku5` | Pay-as-you-go |
| ML Compute Cluster | `training-cluster` | Standard_DS3_v2, 0–4 nodes |
| Function App | `func-sales-validation-vebku5` | Linux, Y1 (Consumption), Python 3.10 |
| Key Vault | `kv-sales-vebku5` | Standard, soft-delete 7 ngày |
| Log Analytics | `log-sales-analytics-vebku5` | PerGB2018, 30 ngày retention |
| Application Insights | `appi-sales-analytics-vebku5` | Web type |

### 3.2 Secrets trong Key Vault (10 secrets)

```
event-hub-connection-string    sql-connection-string       sql-admin-password
ml-endpoint-url                ml-api-key                  blob-connection-string
powerbi-push-url               appinsights-connection-string   openweather-api-key
```

### 3.3 Biến cấu hình (variables.tf)

| Biến | Giá trị | Mô tả |
|---|---|---|
| `environment` | dev / staging / prod | Môi trường triển khai |
| `location` | Southeast Asia | Azure region |
| `sql_admin_username` | sqladmin | Tài khoản DB (sensitive) |
| `ml_training_vm_size` | Standard_DS3_v2 | VM cho training |
| `ml_endpoint_instance_type` | Standard_DS2_v2 | VM cho endpoint |

---

## 4. LUỒNG DỮ LIỆU (DATA PIPELINE)

### 4.1 Data Generator (`data_generator/sales_generator.py`)

**Chức năng chính:**
- `build_sales_event()` — Sinh sự kiện ngẫu nhiên (store, product, qty, price, weather, holiday)
- `get_weather_for_store(store_id)` — Gọi OpenWeatherMap API (cache 10 phút)
- `get_holiday_flag()` — Gọi Calendarific API (cache 1 ngày)
- `create_eventhub_producer()` — Kết nối Event Hub
- `generate_batch(batch_size)` — Sinh batch sự kiện

**Cấu hình:**
- 3 cửa hàng: S01 (HCM), S02 (Hà Nội), S03 (Đà Nẵng)
- 4 sản phẩm streaming: COKE, PEPSI, BREAD, MILK
- Tốc độ: 1.200 events/phút, burst 3× trong 15 giây
- Hỗ trợ replay từ `sample_events.jsonl`

### 4.2 Azure Functions — Validation (`azure_functions/ValidateSalesEvent/`)

**Trigger:** Event Hub (consumer group `$Default`)

**Logic xác thực:**
1. Kiểm tra trường bắt buộc (timestamp, store_id, product_id, quantity, price)
2. Kiểm tra kiểu dữ liệu & khoảng giá trị
3. Kiểm tra store_id/product_id hợp lệ
4. Kiểm tra timestamp ≤ 24 giờ (loại bỏ sự kiện cũ)
5. Loại trùng lặp (deduplication)
6. Tính toán trường phái sinh (revenue = quantity × price)

### 4.3 Stream Analytics — ETL Thời gian thực

File query: `stream_analytics/stream_query.sql`

**Pipeline xử lý:**
```
SalesInput (Event Hub)
    │
    ▼
CTE "Cleaned" — TRY_CAST, null handling, loại dòng lỗi
    │
    ▼
CTE "Enriched" — Category mapping, feature engineering
    │
    ├──▶ Output 1: SalesTransactionsOutput (raw → SQL)
    │
    ├──▶ CTE "Agg5m" — Tumbling Window 5 phút
    │       │
    │       ├──▶ Output 2: HourlySalesSummaryOutput (agg → SQL)
    │       └──▶ Output 8: PowerBIOutput (agg → Power BI)
    │
    ├──▶ Output 4: High-value anomaly detection (WHERE revenue > threshold)
    ├──▶ Output 5: Revenue spike (Sliding Window 5m)
    └──▶ Output 9: Weather-Sales JOIN (tương quan)
```

**9 Query outputs:**

| # | Tên | Loại Window | Đầu ra | Mô tả |
|---|---|---|---|---|
| 1 | Direct insert | — | `SalesTransactions` | Ghi thẳng giao dịch đã validate |
| 2 | Hourly agg | Tumbling 5m | `HourlySalesSummary` | SUM revenue, COUNT tx, AVG price |
| 3 | Product summary | Tumbling 30m | `HourlySalesSummary` | Tổng hợp theo product + category |
| 4 | High-value | Event-level | Alerts | Phát hiện đơn giá trị cao |
| 5 | Revenue spike | Sliding 5m | Features | Đột biến doanh thu |
| 6 | Weather store | Tumbling | `WeatherFacts` | Dữ liệu thời tiết |
| 7 | Stock store | Tumbling | `StockFacts` | Dữ liệu chứng khoán |
| 8 | Power BI | Tumbling 5m | Power BI Dataset | Real-time dashboard |
| 9 | Weather-Sales JOIN | LEFT JOIN | Features | Tương quan weather × sales |

**Các kỹ thuật ETL đáng chú ý:**
- `TRY_CAST` — Parse an toàn, không crash khi dữ liệu lỗi
- `TIMESTAMP BY [timestamp]` — Dùng event time (không phải arrival time)
- `CASE WHEN` — Category mapping (COKE/PEPSI → Beverage, BREAD → Bakery, MILK → Dairy)
- LAG function — Tính delta doanh thu so với 5 phút trước
- Hopping Window (15m, step 5m) — Rolling aggregation

### 4.4 Azure SQL Schema

File: `sql/create_tables.sql`

**3 bảng chính:**

| Bảng | Mục đích | Số cột | Index | Compression |
|---|---|---|---|---|
| `SalesTransactions` | Raw events | 15 | EventTime + StoreProduct | ROW |
| `HourlySalesSummary` | Aggregation 5m | 15 | WindowStart | PAGE + Columnstore |
| `SalesForecast` | Dự đoán ML | 13 | ForecastDate | — |

**2 Views:**
- `vw_RealtimeDashboard` — TOP 1000 giao dịch gần nhất
- `vw_ForecastVsActual` — JOIN dự đoán với thực tế, tính forecast_error

**3 Stored Procedures:** CRUD và upsert logic

**Tối ưu hóa:**
- **Partitioning** theo tháng (RANGE RIGHT) trên SalesTransactions
- **Columnstore Index** trên HourlySalesSummary (tối ưu analytics)
- **Page Compression** cho bảng OLAP, Row Compression cho OLTP

---

## 5. DATABRICKS — MEDALLION ARCHITECTURE

Notebooks: `databricks/notebooks/`

### 5.1 Tổng quan kiến trúc 3 tầng

```
┌─────────────────────────────────────────────────────────────┐
│  BRONZE (Raw)          SILVER (Clean)         GOLD (Agg)    │
│  ──────────────        ──────────────         ───────────── │
│  sales_events    ───▶  sales_transactions ──▶ hourly_summary│
│  (append-only)         (deduplicated)         product_summary│
│  (immutable log)       (validated types)      ml_features   │
│                        (business rules)       viral_predict │
│  Partition: EH         Partition: event_date  Partition: date│
│  Format: Delta         Format: Delta          Format: Delta │
└─────────────────────────────────────────────────────────────┘
```

### 5.2 Chi tiết từng tầng

**Bronze — `01_bronze_ingestion.py`:**
- Spark Structured Streaming từ Event Hubs (Kafka-compatible)
- Decode binary → JSON → Flatten
- Metadata: `_eh_enqueued_time`, `_eh_offset`, `_eh_partition`, `_ingested_at`
- Micro-batch 30 giây, checkpoint cho exactly-once
- **Không transform, chỉ ghi nguyên trạng**

**Silver — `02_silver_etl.py`:**
- Filter bỏ record malformed (null body/timestamp)
- Parse timestamp ISO 8601 (nhiều format)
- Normalize: `unit_price = coalesce(unit_price, price)`
- Validate: quantity > 0, price > 0, store/product hợp lệ
- Tính: `total_amount = quantity × unit_price × (1 - discount)`
- Time dimensions: year, month, day, hour, day_of_week, is_weekend
- Deduplicate bằng transaction_id
- **Streaming mode (30s) hoặc Batch mode (incremental)**

**Gold — `03_feature_engineering.py` + `05_gold_aggregation.py`:**

*Feature Engineering:*
- **NLP:** TF-IDF 256-dimensional hash trên product_name + category + weather
- **Cosine Similarity:** So sánh vector giao dịch với centroid danh mục
- **Numerical:** store_avg_revenue, price_deviation, is_high_value (top 20%)
- **Cyclic:** hour_sin, hour_cos, month_sin, month_cos, dow_sin, dow_cos
- **Target Label:** `is_viral = revenue > 95th percentile OR unusual similarity`

*Aggregation:*
- hourly_summary: Revenue, tx_count, avg_price theo store × category
- product_summary: Total revenue, quantity, viral_count, avg_similarity
- customer_summary: CLV, purchase_count, segment (Regular/Premium/VIP/New)

### 5.3 ML trên Databricks (`04_ml_prediction.py`)

**Training mode:**
- Gradient Boosted Trees Classifier (Spark MLlib)
- Grid search: max_depth [4, 6, 8], max_iter [50, 100]
- StratifiedKFold cross-validation
- Metrics: AUROC, F1, Precision, Recall
- Log → MLflow, Register → Model Registry

**Inference mode:**
- Load production model → Batch predict `is_viral_prediction`
- Output: `gold.viral_predictions` (transaction_id, probability, confidence)

---

## 6. MACHINE LEARNING & MLOps

### 6.1 Training Pipeline (`ml/train_model.py`)

**2 mô hình hồi quy:**

| Mô hình | Target | Thuật toán |
|---|---|---|
| Revenue Model | Doanh thu ($) | Gradient Boosting Regressor |
| Quantity Model | Số lượng bán | Gradient Boosting Regressor |

**Hyperparameters:**
- n_estimators: 200
- max_depth: 6  
- learning_rate: 0.1
- subsample: 0.8

**14 Features:**
- **Thời gian:** hour, day_of_month, month, is_weekend
- **Cyclic:** hour_sin, hour_cos, month_sin, month_cos
- **Categorical (encoded):** store_id_enc, product_id_enc, category_enc
- **Ngữ cảnh:** temperature, is_rainy, holiday

**Metrics đánh giá (test set 20%):**
- MAE (Mean Absolute Error)
- RMSE (Root Mean Square Error)
- R² Score
- Cross-validation R² (5-fold)

**Artifacts:**
- `revenue_model.pkl` + `quantity_model.pkl`
- `label_encoders.pkl`
- `model_metadata.json`

### 6.2 Scoring & Inference (`ml/score.py`)

**Input API:**
```json
{
  "data": [{
    "hour": 14, "day_of_month": 15, "month": 6,
    "is_weekend": 0, "store_id": "S01", "product_id": "COKE",
    "category": "Beverage", "temperature": 28.0,
    "is_rainy": 0, "holiday": 0
  }]
}
```

**Output API:**
```json
{
  "predictions": [{
    "predicted_revenue": 42.50,
    "predicted_quantity": 12,
    "confidence_interval": {
      "revenue_lower": 25.20, "revenue_upper": 59.80,
      "quantity_lower": 8, "quantity_upper": 16
    },
    "model_version": "v2.0"
  }]
}
```

**Khoảng tin cậy 95%:** prediction ± 1.96 × RMSE

### 6.3 MLOps Pipeline (`mlops/`)

**Quy trình tự động:**

```
┌──────────┐   ┌──────────┐   ┌──────────┐   ┌────────────┐   ┌──────────┐
│ Training │──▶│ Evaluate │──▶│ Register │──▶│ Compare vs │──▶│ Promote/ │
│ (AML     │   │ Metrics  │   │ Version  │   │ Production │   │ Deploy   │
│  Compute)│   │          │   │          │   │            │   │          │
└──────────┘   └──────────┘   └──────────┘   └────────────┘   └──────────┘
```

**Tiêu chí promote model mới → production:**
- R² ≥ 0.85
- MAE ≤ 15.0
- MAPE ≤ 12%
- Cải thiện ≥ 2% so với model hiện tại

**Lịch trình:** Hàng tuần (Chủ nhật 03:00 UTC), hoặc khi phát hiện drift

### 6.4 Model Registry (`mlops/model_registry.py`)

Chức năng: `register`, `list_versions`, `compare_versions`, `get_best_version`, `promote`, `rollback`

### 6.5 Deployment — Blue/Green (`mlops/deploy_to_endpoint.py`)

**Chiến lược:**
1. Blue (production) nhận 90% traffic
2. Green (model mới) nhận 10% (canary test)
3. Nếu Green ổn định → Promote 100%
4. Delete Blue

**Endpoint config:**
- Instance: Standard_DS2_v2 (2 CPU, 7 GB RAM)
- Auto-scale: 0–1 instance
- Liveness probe: 30s initial, 10s period
- Readiness probe: 10s initial, 10s period

### 6.6 Drift Detection (`mlops/drift_detector.py`)

**2 loại drift:**

| Loại | Phương pháp | Ngưỡng |
|---|---|---|
| **Feature Drift** (phân phối dữ liệu thay đổi) | KS Test p-value | < 0.01 |
| | PSI (Population Stability Index) | > 0.2 |
| **Performance Drift** (model kém đi) | R² degradation | > 15% |
| | MAE increase | > 20% |

**Khi phát hiện drift:** Tự động trigger retrain pipeline (nếu `AUTO_RETRAIN_ENABLED=True`)

### 6.7 Model Monitor (`mlops/model_monitor.py`)

- `log_prediction()` — Ghi lại mỗi lần dự đoán
- `check_health()` — Volume, accuracy, endpoint responsiveness
- `_send_alert()` — Gửi cảnh báo (cooldown 1 giờ)

---

## 7. POWER BI — TRỰC QUAN HÓA

### 7.1 DAX Measures (`powerbi/dax_measures.dax`)

**KPI chính:**
- `Revenue Total`, `Order Total`, `Average Order Value`, `Units Sold`, `Active Customers`

**Time Intelligence:**
- `Revenue Today`, `Revenue DoD Growth %`, `Revenue MTD`, `Revenue YTD`
- `Revenue MoM Growth %`, `Revenue Rolling 7D`, `Revenue Rolling 30D`

**Tài chính:**
- Gross Profit, Margin %, COGS, Discount Impact

### 7.2 Dashboard Layout (4 trang)

| Trang | Nội dung | Auto-refresh |
|---|---|---|
| **Overview** | KPI cards, Revenue by hour, Top 10 stores, Map by region | 5 giây |
| **Products** | Treemap revenue, Price vs Quantity scatter, Viral scoring | 30 giây |
| **Customers** | CLV distribution, Segment breakdown, Churn risk | 60 giây |
| **Anomaly** | Alert table, Weather impact, Revenue spikes, Forecast vs Actual | 10 giây |

**Theme:** Dark mode (nền #0B1120, accent: #00D4AA, #3B82F6, #F59E0B, #EF4444)

### 7.3 Row-Level Security (RLS)

| Role | Quyền |
|---|---|
| **RegionManager** | Chỉ thấy dữ liệu vùng mình quản lý (filter USERPRINCIPALNAME()) |
| **Admin** | Xem toàn bộ dữ liệu |
| **Analyst** | Lọc theo phòng ban |

**Bảng SecurityMapping:** user_email → allowed_region, VD: `manager_north@contoso.com` → North

### 7.4 Change Detection (Advanced)

- Polling interval: 5 giây
- Detection measure: Revenue Total
- Chỉ refresh **khi dữ liệu thay đổi** → Tiết kiệm query quota

### 7.5 Push Data Script (`powerbi/push_to_powerbi.py`)

- Query SQL aggregation → JSON → POST đến Power BI Streaming Dataset URL
- Đã test thành công: 9 rows (store × category)

---

## 8. BẢO MẬT (SECURITY)

### 8.1 Azure Key Vault (`security/key_vault.py`)

**SecretManager class:**
- `get_secret(name)` — Đọc secret (có cache)
- `set_secret(name, value)` — Ghi secret
- `list_secrets()` — Liệt kê
- `health_check()` — Kiểm tra kết nối

**Xác thực:**
- **Trên Azure:** ManagedIdentityCredential (System-assigned Identity)
- **Local:** DefaultAzureCredential (Azure CLI)
- **Fallback:** Đọc từ biến môi trường nếu Key Vault không khả dụng

### 8.2 Các biện pháp bảo mật

| Tầng | Biện pháp |
|---|---|
| **Network** | Service endpoints, firewall rules trên SQL |
| **Identity** | Managed Identity (không hardcode credentials) |
| **Secrets** | Key Vault, soft-delete enabled |
| **Data** | RLS trên Power BI, SQL user permissions |
| **Transport** | TLS 1.2 bắt buộc cho mọi kết nối |
| **Code** | Không commit secrets vào Git (.env trong .gitignore) |

---

## 9. MONITORING & TELEMETRY

### 9.1 Application Insights (`monitoring/telemetry.py`)

**Hàm tracking:**
- `track_event(name, properties)` — Custom event
- `track_metric(name, value)` — Custom metric (throughput, latency)
- `track_dependency(name, target, duration_ms, success)` — Gọi ngoài (SQL, API)
- `track_exception(exc)` — Exception tracking
- `@monitor_performance(component)` — Decorator tự động đo hiệu suất

### 9.2 Pipeline Health Monitor

- Theo dõi 6 component: data_generator, event_hubs, stream_analytics, azure_sql, ml_endpoint, power_bi
- `update_status(component, healthy, details)`
- `get_report()` → Báo cáo tổng thể + từng component

### 9.3 ARM Monitoring Template (`monitoring/arm_monitoring.json`)

- Alert rules: CPU, DTU, latency thresholds
- Action groups: Email notification
- Dashboard: Azure Monitor workbook

---

## 10. TESTING & BENCHMARKS

### 10.1 Unit Tests (30+ test cases)

**`tests/test_config.py`** (7 tests):
- Validate PRODUCTS schema, STORE_IDS consistency
- Helper functions (_get_bool, _get_int, _get_float)
- Kiểm tra không leak password/token trong runtime config

**`tests/test_sales_generator.py`** (12 tests):
- UTC ISO format, store_id/product valid
- Quantity range [1–5], price bounds, 2 decimals
- Weather normalization, schema validation
- Dedupe key uniqueness, batch size

**`tests/test_validation.py`** (10 tests):
- Timestamp parse (valid/invalid formats)
- Missing fields rejection
- Out-of-range quantity rejection
- Unknown store/product rejection
- Deduplication (same event 2 lần → lần 2 bị reject)
- Stale timestamp (> 24h) rejection
- Revenue computation

**`tests/test_webapp.py`:**
- Flask routes: GET `/`, POST `/predict`, POST `/api/predict`, GET `/api/health`
- Mock ML endpoint response

### 10.2 Benchmarks

**Latency Benchmark (`benchmarks/benchmark_latency.py`):**
- TCP connection latency đến Event Hubs (port 443)
- SQL query latency (connection + query)
- DNS resolution time
- So sánh 5 regions: East US, West Europe, Southeast Asia, Japan East, Australia East
- Output: min/max/avg/median/stdev (ms)

**Read/Write Benchmark (`benchmarks/benchmark_read_write.py`):**
- INSERT: Single (~100 rows/s) vs Batch 1000 (~10.000 rows/s)
- SELECT: 7 loại query (COUNT, SUM GROUP BY, AVG, TOP N, WHERE ORDER BY, DATE range)
- Output: rows_per_sec, time_sec

**Data Size Benchmark (`benchmarks/benchmark_data_size.py`):**
- Generate >4 GB CSV (15–20M rows)
- So sánh: Local read ~200 MB/s vs Cloud SQL insert ~5.000–10.000 rows/s

---

## 11. WEB APPLICATION

### 11.1 Flask App (`webapp/app.py`)

| Route | Method | Mô tả |
|---|---|---|
| `/` | GET | Form dự đoán (HTML) |
| `/predict` | POST | Submit form → Gọi ML endpoint → Hiển thị kết quả |
| `/api/predict` | POST | REST API (JSON in/out) |
| `/api/health` | GET | Health check endpoint |

**Form inputs:** hour, day_of_week, month, store_id, product_id, temperature, is_rainy, holiday

**Output:** predicted_revenue, predicted_quantity, confidence_interval, model_version

**Fallback:** Nếu ML endpoint chưa cấu hình → Trả về mock predictions

---

## 12. CHI PHÍ & TỐI ƯU

### 12.1 Chi phí ước tính hàng tháng

| Dịch vụ | Tier | SLA | Chi phí/tháng |
|---|---|---|---|
| Event Hubs | Standard 1TU | 99.95% | ~$22 |
| Stream Analytics | 3–6 SU | 99.9% | ~$110 |
| Azure SQL | S0/S1 (10 DTU) | 99.99% | ~$30 |
| Data Factory | Pay-as-you-go | 99.9% | ~$5 |
| Machine Learning | Pay-as-you-go | 99.9% | ~$36 |
| Power BI | Pro | 99.9% | ~$10 |
| Key Vault + Storage + Others | — | — | ~$3 |
| **TỔNG** | | | **~$216/tháng** |

### 12.2 So sánh IaaS vs PaaS

| Thành phần | IaaS (VM) | PaaS (đồ án) | Tiết kiệm |
|---|---|---|---|
| Message Queue | Kafka cluster (2 VM) | Event Hubs | ~60% |
| Stream Processing | Flink cluster (3 VM) | Stream Analytics | ~50% |
| Database | SQL Server VM | Azure SQL DB | ~40% |
| ML Training | GPU VM | AML Compute | ~70% |
| Orchestration | Airflow VM | Data Factory | ~55% |
| **Tổng** | **~$800–1.200/tháng** | **~$200–400/tháng** | **~65%** |

### 12.3 Tối ưu Storage

- **SQL Indexing:** Non-clustered (date, store, category) + Columnstore (analytics)
- **Partitioning:** Theo tháng trên SalesTransactions
- **Compression:** Row (OLTP), Page (OLAP) → Giảm 50–70% storage
- **Event Hubs:** Batch sending, partition strategy
- **Blob:** Lifecycle management (Hot → Cool → Archive)

---

## 13. KẾT QUẢ ĐÃ TRIỂN KHAI THỰC TẾ

### 13.1 Trạng thái hiện tại (đã xác minh)

| Thành phần | Trạng thái | Chi tiết |
|---|---|---|
| Event Hub `sales-events` | ✅ **Hoạt động** | Gửi 90 test events thành công |
| Stream Analytics | ✅ **Running** | Job đang chạy, xử lý real-time |
| Azure SQL — SalesTransactions | ✅ **90 rows** | Raw events đã ghi thành công |
| Azure SQL — HourlySalesSummary | ✅ **31 rows** | Aggregation 5m hoạt động |
| Azure Functions | ✅ **Running** | Deployed OK, state: Running |
| ML Model (Azure ML) | ✅ **Deployed** | Endpoint `sales-forecast-endpoint` |
| Power BI Push Script | ✅ **Tested** | 9 aggregated rows (store × category) |
| Terraform Infrastructure | ✅ **Applied** | Toàn bộ tài nguyên đã provision |

### 13.2 Luồng đã xác minh end-to-end

```
Data Generator (90 events)
    ↓ (Event Hub)
Stream Analytics (transformation "main")
    ↓
Azure SQL: 90 raw + 31 aggregated rows
    ↓
Power BI Push: 9 summary rows (query OK)
    ↓
Web App: /predict → ML Endpoint → prediction result
```

### 13.3 Độ trễ đo được

| Đoạn | Độ trễ |
|---|---|
| Event Hub → SQL (qua SA) | ~1 giây (ingest_lag_seconds) |
| SQL query latency | < 100ms |
| ML Endpoint inference | < 500ms |
| Dashboard refresh | 5–60 giây (cấu hình) |

---

## 14. ĐÓNG GÓP CHÍNH & ĐIỂM NỔI BẬT

### 14.1 Về kỹ thuật

1. **End-to-end real-time pipeline:** Từ data generator → Event Hub → Stream Analytics → SQL → Power BI, hoàn toàn tự động.
2. **Medallion Architecture (Databricks):** 3 tầng Bronze/Silver/Gold với Delta Lake, đảm bảo data quality.
3. **MLOps Level 2:** Auto-train, model registry, blue/green deployment, drift detection, auto-retrain.
4. **9 Stream Analytics queries:** Simultaneously handle raw insert, aggregation, anomaly detection, Power BI push.
5. **Feature Engineering tiên tiến:** TF-IDF + Cosine Similarity + Cyclic encoding trên Databricks.
6. **Infrastructure as Code:** Terraform quản lý toàn bộ ~15 Azure resources.
7. **Security best practices:** Key Vault, Managed Identity, RLS, TLS 1.2.

### 14.2 Về học thuật

1. **Phân loại dịch vụ Cloud rõ ràng:** IaaS/PaaS/FaaS/SaaS cho từng thành phần.
2. **So sánh chi phí:** IaaS ~$1.000/tháng vs PaaS ~$216/tháng (tiết kiệm 65%).
3. **Benchmark đầy đủ:** Latency (5 regions), Read/Write (7 query types), Data size (>4 GB).
4. **30+ unit tests** bao phủ config, generator, validation, webapp.
5. **Tài liệu chi tiết:** 5 file MD chuyên sâu (đề cương, lý thuyết, streaming, tối ưu, MLOps).

---

## 15. CÂU HỎI VẤN ĐÁP THƯỜNG GẶP

### Câu hỏi kiến trúc

**Q: Tại sao chọn PaaS thay vì IaaS?**
> Giảm chi phí ~65% (từ ~$1.000 xuống ~$216/tháng), không cần quản lý VM/OS/patching, auto-scaling, SLA 99.9–99.99%. Tập trung vào logic dữ liệu thay vì hạ tầng.

**Q: Tại sao dùng Event Hubs thay vì Kafka tự dựng?**
> Event Hubs là PaaS (managed), scale tự động, SLA 99.95%, chi phí $22/tháng. Kafka tự dựng cần ≥2 VM + ZooKeeper, chi phí >$150/tháng. Event Hubs cũng hỗ trợ Kafka protocol nếu cần migrate.

**Q: Tại sao dùng Stream Analytics thay vì Spark Streaming?**
> SA dùng SQL-like query (dễ viết, maintain), auto-scale, tích hợp sẵn Event Hub → SQL output. Tuy nhiên hệ thống cũng có Databricks Structured Streaming cho xử lý phức tạp hơn (feature engineering, ML).

**Q: Medallion Architecture có lợi gì?**
> Bronze (raw, immutable) cho audit trail; Silver (clean, validated) cho analytics; Gold (aggregated, features) cho ML + BI. Tách biệt concerns, dễ debug, reprocess khi cần.

### Câu hỏi ML

**Q: Tại sao chọn Gradient Boosting Regressor?**
> Phù hợp cho dữ liệu tabular, handle mixed features (numerical + categorical) tốt, ít bị overfit (có regularization: max_depth=6, subsample=0.8), training nhanh trên CPU. Kết quả R² > 0.85 trên test set.

**Q: Confidence Interval tính thế nào?**
> CI 95% = prediction ± 1.96 × RMSE. Đây là ước lượng dựa trên Normal distribution assumption. Trong thực tế có thể dùng Quantile Regression hoặc conformal prediction cho CI chính xác hơn.

**Q: Drift Detection hoạt động ra sao?**
> - **Feature Drift:** So sánh phân phối dữ liệu mới vs training data bằng KS Test (p < 0.01) và PSI (> 0.2)
> - **Performance Drift:** So sánh metrics (R², MAE) trên dữ liệu gần đây vs trước đó
> - Khi phát hiện → Tự động trigger retrain pipeline

**Q: Blue/Green deployment là gì?**
> Triển khai song song 2 phiên bản: Blue (hiện tại, 90% traffic) và Green (mới, 10%). Nếu Green ổn định (canary test), promote lên 100%. Rollback chỉ cần chuyển traffic về Blue. Đảm bảo zero-downtime deployment.

### Câu hỏi data

**Q: Dữ liệu có thật không?**
> Dữ liệu bán hàng là synthetic (sinh tự động). Tuy nhiên, thời tiết lấy từ OpenWeatherMap API (thật), ngày lễ từ Calendarific API (thật). Schema và volume mô phỏng chuỗi bán lẻ 3 cửa hàng ở VN.

**Q: Xử lý late arriving data thế nào?**
> Stream Analytics cấu hình: out-of-order delay 5s, late arrival delay 30s. Dữ liệu đến muộn ≤30s vẫn được xử lý đúng. Muộn hơn → Drop theo policy.

**Q: Deduplication thực hiện ở đâu?**
> 3 tầng: (1) Azure Functions kiểm tra dedup key trước khi forward, (2) Stream Analytics dùng DISTINCT/GROUP BY, (3) Databricks Silver layer deduplicate bằng transaction_id.

### Câu hỏi Power BI

**Q: RLS bảo mật dữ liệu thế nào?**
> Mỗi manager chỉ thấy dữ liệu vùng mình quản lý. Dùng DAX filter `USERPRINCIPALNAME()` map với bảng SecurityMapping. Admin thấy tất cả. Được enforce ở Power BI Service level.

**Q: Dashboard refresh 5 giây, có tốn performance không?**
> Dùng Change Detection: chỉ thực sự refresh khi measure Revenue Total thay đổi. Polling 5s nhưng chỉ execute query khi có data mới → Tiết kiệm query quota.

### Câu hỏi vận hành

**Q: Hệ thống có auto-scale không?**
> Có: Event Hubs auto-inflate TU, Stream Analytics scale SU, ML endpoint auto-scale 0–1 instance, Azure Functions scale tự động (Consumption plan), SQL có thể upgrade DTU.

**Q: Chi phí $216/tháng có hợp lý cho sinh viên?**
> Dùng Azure for Students ($100 free credit). Chi phí thực tế thấp hơn vì dev environment chạy không liên tục. Terraform có thể destroy/apply nhanh để tiết kiệm.

**Q: Nếu một component fail thì sao?**
> Event Hub có 3-day retention → replay. SA tự restart. SQL có backup tự động. ML endpoint có liveness/readiness probes. Monitoring alert qua Application Insights.

---

## PHỤ LỤC

### A. Cấu trúc thư mục dự án

```
azure-realtime-sales-analytics/
├── config/                     # Cấu hình trung tâm (settings.py)
├── data_generator/             # Sinh dữ liệu (sales, weather, stock)
├── azure_functions/            # Azure Functions (ValidateSalesEvent)
├── stream_analytics/           # Stream Analytics queries
├── sql/                        # SQL schema (tables, views, SP)
├── databricks/                 # Notebooks (Bronze → Silver → Gold → ML)
├── ml/                         # Training, scoring, evaluation
├── mlops/                      # Registry, deployment, drift, monitoring
├── powerbi/                    # DAX, layout, RLS, themes
├── webapp/                     # Flask web app
├── infrastructure/             # ARM templates, deploy scripts
├── terraform/                  # IaC (main.tf, variables.tf)
├── security/                   # Key Vault integration
├── monitoring/                 # Telemetry, health monitoring
├── benchmarks/                 # Performance benchmarks
├── tests/                      # Unit tests (30+)
├── docs/                       # Documentation (5 MD files)
├── blob_storage/               # Upload reference data
├── data_factory/               # ADF pipeline definition
├── requirements.txt            # Python dependencies
└── sample_events.jsonl         # Test data
```

### B. Công nghệ sử dụng

| Layer | Technology | Version/Detail |
|---|---|---|
| Language | Python | 3.10–3.11 |
| ML | scikit-learn, Spark MLlib | GBR, GBT Classifier |
| ML Platform | Azure ML + MLflow | Model Registry, Experiment Tracking |
| Streaming | Azure Event Hubs | Standard, Kafka-compatible |
| ETL | Stream Analytics + Databricks | SQL-like + PySpark |
| Database | Azure SQL Database | S0/S1, 10 DTU |
| Data Lake | ADLS Gen2 + Delta Lake | Medallion Architecture |
| BI | Power BI Pro | DirectQuery + Streaming Dataset |
| Web | Flask | Azure App Service |
| IaC | Terraform | v1.5+ |
| CI/CD | GitHub Actions | Test → Deploy → Retrain |
| Security | Azure Key Vault | Managed Identity |
| Monitoring | Application Insights | Custom metrics + alerts |

### C. Link tài liệu liên quan

- `docs/de_cuong_bao_cao.md` — Đề cương báo cáo Word + PowerPoint
- `docs/ly_thuyet_va_phan_loai.md` — Lý thuyết + phân loại dịch vụ
- `docs/streaming_mapping.md` — Hướng dẫn streaming & mapping
- `docs/toi_uu_luu_tru.md` — Tối ưu lưu trữ & chi phí
- `docs/ke_hoach_mlops.md` — Kế hoạch phát triển MLOps
