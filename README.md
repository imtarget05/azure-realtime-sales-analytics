# 📊 Hệ thống Trực quan Dữ liệu Bán hàng Thời gian Thực trên Azure

![Python](https://img.shields.io/badge/Python-3.10%2B-blue?logo=python)
![Azure](https://img.shields.io/badge/Azure-Cloud-0078D4?logo=microsoftazure)
![License](https://img.shields.io/badge/License-MIT-green)
![Status](https://img.shields.io/badge/Status-Active-brightgreen)

> Hệ thống end-to-end xử lý và trực quan hóa dữ liệu bán hàng thời gian thực trên nền tảng Microsoft Azure — từ thu thập dữ liệu, ETL streaming, dự đoán ML cho đến dashboard Power BI.

---

## 📋 Mục lục

1. [Tổng quan](#-tổng-quan)
2. [Kiến trúc hệ thống](#-kiến-trúc-hệ-thống)
3. [Cấu trúc dự án](#-cấu-trúc-dự-án)
4. [Yêu cầu tiên quyết](#-yêu-cầu-tiên-quyết)
5. [Hướng dẫn triển khai](#-hướng-dẫn-triển-khai)
6. [Luồng dữ liệu chi tiết](#-luồng-dữ-liệu-chi-tiết)
7. [Các dịch vụ Azure](#️-các-dịch-vụ-azure)
8. [Tài liệu](#-tài-liệu)
9. [Tài liệu tham khảo](#-tài-liệu-tham-khảo)

---

## 🎯 Tổng quan

Hệ thống tích hợp **7 dịch vụ Azure** để xây dựng pipeline phân tích dữ liệu bán hàng theo thời gian thực:

| Thành phần | Công nghệ | Mô tả |
|---|---|---|
| 📡 **Sinh dữ liệu** | Python | Giả lập giao dịch bán hàng, thời tiết, chứng khoán |
| 📥 **Thu thập** | Azure Event Hubs | Tiếp nhận hàng nghìn sự kiện/giây |
| 🔄 **ETL streaming** | Azure Stream Analytics | Tổng hợp, phát hiện bất thường, JOIN thời gian thực |
| 🗄️ **Lưu trữ** | Azure SQL Database + Blob Storage | Lưu kết quả đã xử lý và dữ liệu thô |
| 🤖 **Dự đoán** | Azure Machine Learning | Gradient Boosting dự báo doanh thu 24h tới |
| 📊 **Trực quan** | Power BI + Flask Web App | Dashboard real-time và giao diện web |
| ⚙️ **Điều phối** | Azure Data Factory | Tự động hóa pipeline theo lịch |

---

## 🏗️ Kiến trúc hệ thống

```
                        ┌──────────────────────┐
                        │   Azure Blob Storage  │
                        │  (Reference/Archive)  │
                        └──────────┬───────────┘
                                   │ Reference Data
                                   ▼
┌─────────────────┐   ┌────────────────────┐   ┌──────────────────────┐
│  Data Generator │──▶│  Azure Event Hubs  │──▶│  Stream Analytics    │
│   (Python)      │   │    (Ingestion)     │   │   (Real-time ETL)    │
│                 │   │                    │   │                      │
│ • Sales Data    │   │ • sales-events     │   │ • Aggregation        │
│ • Weather Data  │   │ • weather-events   │   │ • Anomaly Detection  │
│ • Stock Data    │   │ • stock-events     │   │ • JOIN Operations    │
└────────┬────────┘   └────────────────────┘   └──────────┬───────────┘
         │                                                 │
         │                      ┌──────────────────────────┤
         │                      ▼                          ▼
         │          ┌─────────────────────┐   ┌─────────────────────┐
         │          │  Azure SQL Database │   │      Power BI       │
         │          │     (Storage)       │   │   (Visualization)   │
         │          │                     │   │                     │
         │          │ • SalesTransactions │──▶│ • Real-time KPIs    │
         │          │ • HourlySummary     │   │ • Product Analysis  │
         │          │ • ProductSummary    │   │ • Forecast vs Actual│
         │          │ • Alerts            │   │ • Anomaly Alerts    │
         │          │ • Weather/Stock     │   │ • Weather Impact    │
         │          │ • Forecasts         │   └─────────────────────┘
         │          └──────┬──────────────┘
         │                 │           ▲
         │                 ▼           │
         │      ┌──────────────────────┐
         └─────▶│  Azure Machine       │
                │     Learning         │
                │                      │
                │ • Train Model        │
                │ • Deploy Endpoint    │
                │ • Real-time Predict  │
                └──────────┬───────────┘
                           ▲
                           │ Orchestrate
                ┌──────────────────────┐
                │  Azure Data Factory  │
                │   (Orchestration)    │
                │                      │
                │ • Blob → SQL Copy    │
                │ • ML Pipeline Trigger│
                │ • Scheduled at 02:00 │
                └──────────────────────┘
```

---

## 📁 Cấu trúc dự án

```
azure-realtime-sales-analytics/
│
├── 📂 config/                        # Cấu hình trung tâm
│   ├── __init__.py
│   └── settings.py                   # Tất cả biến cấu hình (đọc từ .env)
│
├── 📂 data_generator/                # Bộ sinh dữ liệu giả lập
│   ├── __init__.py
│   ├── sales_generator.py            # Giao dịch bán hàng → Event Hub
│   ├── weather_generator.py          # Dữ liệu thời tiết  → Event Hub
│   └── stock_generator.py            # Dữ liệu chứng khoán → Event Hub
│
├── 📂 blob_storage/                  # Quản lý Blob Storage
│   └── upload_reference_data.py      # Upload dữ liệu tham chiếu lên Blob
│
├── 📂 stream_analytics/              # ETL thời gian thực
│   ├── stream_query.sql              # Query chính: sales enrich + alerts + Power BI output
│   └── weather_sales_correlation.sql # Query JOIN 3 luồng: Weather × Sales × Stock
│
├── 📂 sql/                           # Cơ sở dữ liệu
│   ├── create_tables.sql             # Schema: 7 bảng + 3 view + index
│   ├── create_streaming_tables.sql   # Schema bảng streaming
│   └── stored_procedures.sql         # Stored procedures cho Data Factory
│
├── 📂 ml/                            # Machine Learning
│   ├── data/                         # Rossmann dataset (train/test/store.csv)
│   ├── model_output/                 # Outputs: model.pkl, charts/, metadata
│   │   ├── charts/                   # 5 biểu đồ từ notebook
│   │   └── evaluation_charts/        # 6 biểu đồ so sánh pipeline
│   ├── train.ipynb                   # Notebook chính – train + visualization
│   ├── evaluate_pipelines.py         # So sánh 10 models × 2 datasets
│   ├── train_model.py                # Huấn luyện Gradient Boosting
│   ├── compare_models.py             # So sánh 9 mô hình ML + biểu đồ
│   ├── score.py                      # Script chấm điểm cho endpoint
│   ├── score_rossmann.py             # Batch scoring Rossmann
│   ├── deploy_model.py               # Triển khai lên Azure ML Endpoint
│   ├── realtime_forecast.py          # Gọi endpoint để dự báo real-time
│   └── conda_env.yml                 # Môi trường Conda cho Azure ML
│
├── 📂 azure_functions/               # Azure Functions
│   ├── host.json
│   ├── requirements.txt
│   └── ValidateSalesEvent/           # Validate incoming events
│
├── 📂 data_factory/                  # Orchestration
│   ├── create_pipeline.py            # Tạo pipelines qua Python SDK
│   └── pipeline_definition.json      # Định nghĩa pipeline (ARM template)
│
├── 📂 infrastructure/                # Triển khai hạ tầng (IaC)
│   ├── deploy_azure.sh               # Bash script (Linux/macOS)
│   ├── deploy_azure.ps1              # PowerShell script (Windows)
│   ├── arm_streaming_job.json        # ARM template Stream Analytics
│   └── arm_streaming_job.parameters.example.json
│
├── 📂 monitoring/                    # Giám sát & Telemetry
│   ├── telemetry.py                  # Thu thập metrics
│   └── arm_monitoring.json           # ARM template monitoring
│
├── 📂 security/                      # Bảo mật
│   └── key_vault.py                  # Azure Key Vault integration
│
├── 📂 powerbi/                       # Power BI
│   ├── push_to_powerbi.py            # Đẩy dữ liệu tổng hợp lên Power BI
│   └── POWERBI_SETUP.md              # Hướng dẫn setup + RLS + Mobile
│
├── 📂 webapp/                        # Web Application (Flask)
│   ├── app.py                        # Flask app gọi ML endpoint
│   ├── static/result.js
│   └── templates/
│       ├── index.html                # Form nhập tham số dự đoán
│       └── result.html               # Hiển thị kết quả dự đoán
│
├── 📂 benchmarks/                    # Kiểm tra hiệu năng
│   ├── benchmark_data_size.py        # Sinh >4GB data, so sánh local vs cloud
│   ├── benchmark_read_write.py       # Đo tốc độ đọc/ghi Azure SQL
│   └── benchmark_latency.py          # Đo latency đến các Azure region
│
├── 📂 docs/                          # Tài liệu kỹ thuật
│   ├── ly_thuyet_va_phan_loai.md     # Lý thuyết + phân loại IaaS/PaaS/SaaS
│   ├── toi_uu_luu_tru.md             # Chiến lược tối ưu lưu trữ & chi phí
│   ├── de_cuong_bao_cao.md           # Đề cương báo cáo Word/PPT
│   ├── ke_hoach_mlops.md             # Kế hoạch phát triển MLOps
│   └── streaming_mapping.md          # Mapping streaming data
│
├── 📂 .github/workflows/            # CI/CD
│   ├── ci.yml
│   ├── deploy-functions.yml
│   ├── deploy-ml-endpoint.yml
│   └── deploy-simulator.yml
│
├── .env.example                      # Mẫu biến môi trường (copy → .env)
├── .gitignore
├── requirements.txt                  # Danh sách thư viện Python
├── sample_events.jsonl               # Dữ liệu mẫu Event Hub
└── README.md
```

---

## ✅ Yêu cầu tiên quyết

Trước khi bắt đầu, hãy đảm bảo đã cài đặt:

| Công cụ | Phiên bản | Ghi chú |
|---|---|---|
| Python | 3.10+ | [python.org](https://python.org) |
| Azure CLI | Mới nhất | `az login` để xác thực |
| ODBC Driver | 18 for SQL Server | Kết nối Azure SQL |
| Azure Subscription | — | Cần có tài khoản Azure |
| Power BI | Pro hoặc Premium | Cho dashboard streaming |

---

## 🚀 Hướng dẫn triển khai

### Bước 1 — Cài đặt thư viện Python

```bash
pip install -r requirements.txt
```

### Bước 2 — Triển khai hạ tầng Azure

> Script tự động tạo toàn bộ tài nguyên Azure cần thiết.

**Linux / macOS:**
```bash
az login
chmod +x infrastructure/deploy_azure.sh
./infrastructure/deploy_azure.sh
```

**Windows (PowerShell):**
```powershell
az login
.\infrastructure\deploy_azure.ps1
```

**Tài nguyên được tạo:**
- ✅ Resource Group
- ✅ Storage Account + 3 Blob Containers (`reference-data`, `sales-archive`, `data-factory-staging`)
- ✅ Event Hubs Namespace + 3 Event Hubs (với Capture → Blob)
- ✅ Azure SQL Server + Database
- ✅ Stream Analytics Job
- ✅ Azure Machine Learning Workspace
- ✅ Azure Data Factory

### Bước 3 — Cấu hình biến môi trường

```bash
cp .env.example .env
# Mở file .env và điền thông tin từ deployment_output.txt
```

<details>
<summary>📄 Xem danh sách biến cần cấu hình</summary>

```env
EVENT_HUB_CONNECTION_STRING=<connection-string>
SQL_SERVER=<server>.database.windows.net
SQL_USERNAME=<username>
SQL_PASSWORD=<password>
AML_ENDPOINT_URL=<ml-endpoint-url>
AML_API_KEY=<ml-api-key>
BLOB_CONNECTION_STRING=<blob-connection-string>
AZURE_SUBSCRIPTION_ID=<subscription-id>
AZURE_RESOURCE_GROUP=rg-sales-analytics
```
</details>

## 🧪 Local-First MLOps (Khuyến nghị trước khi đẩy Azure)

Mục tiêu: giữ nguyên toàn bộ nội dung dự án nhưng chạy ổn định ở local để demo,
chứng minh luồng MLOps tự học trước khi deploy cloud.

### Luồng local-first

1. Bootstrap model local nếu chưa có artifact.
2. Retrain và gate check old vs new.
3. Promote local model nếu cải thiện.
4. Smoke test đường dự đoán local (không phụ thuộc endpoint Azure).
5. Lưu report chạy pipeline vào `ml/model_output/local_pipeline_report.json`.

### Chạy 1 lệnh

```bash
python mlops/local_first_pipeline.py
```

Tuỳ chỉnh nhanh:

```bash
python mlops/local_first_pipeline.py --bootstrap-samples 20000 --retrain-samples 30000 --n-estimators 180
python mlops/local_first_pipeline.py --no-promote
```

### Kết quả mong đợi

- In ra `Status: success` trong terminal.
- Có file `ml/model_output/local_pipeline_report.json`.
- Web app `/predict` trả kết quả với `source` là `Local Model (vX.Y)` khi Azure endpoint không khả dụng.

### Sau khi local pass mới đẩy Azure

1. Chạy local-first pipeline để khóa model/metrics.
2. Kiểm tra UI retrain: `/retrain` và report: `/model-report`.
3. Chỉ khi local ổn định mới chạy CI/CD Azure (`.github/workflows/ci-cd-mlops.yml`).

## 🔁 Drift Monitor & Continuous Training

Script `ml/drift_monitor.py` đóng vòng lặp CT theo MAE từ SQL view `dbo.vw_ForecastVsActual`:

```bash
python ml/drift_monitor.py --threshold-mae 25 --window-hours 24 --min-samples 24
```

Nếu `MAE > threshold`, script sẽ tự động gọi:

```bash
python ml/retrain_and_compare.py --promote
```

Chạy đầy đủ local + Azure ML pipeline trigger:

```bash
python ml/drift_monitor.py --threshold-mae 25 --window-hours 24 --min-samples 24 --trigger-mode both
```

Verify Continuous Training (kịch bản chấm điểm):

1. Chạy web app và mở trang dự báo.
2. Tăng mạnh doanh thu từ simulator (hoặc replay file có outlier).
3. Kiểm tra cảnh báo realtime trong SQL bảng `dbo.SalesAlerts`.
4. Chạy `drift_monitor.py`, xác nhận `triggered=true` trong báo cáo.
5. Sau khi retrain hoàn tất, F5 `http://localhost:5000/model-report` để thấy biểu đồ/metrics mới.

Inject drift nhanh bằng .env (ví dụ đẩy giá Coca/Pepsi lên cao):

```env
PRICE_SHOCK_ENABLED=true
PRICE_SHOCK_MULTIPLIER=6.5
PRICE_SHOCK_PRODUCTS=COKE,PEPSI
```

Sau đó chạy lại simulator:

```bash
python data_generator/sales_generator.py
```

Nếu muốn drift monitor tự bắn GitHub Actions workflow khi vượt ngưỡng MAE:

```env
DRIFT_TRIGGER_GITHUB_ACTIONS=true
GITHUB_REPO=<owner/repo>
GITHUB_WORKFLOW_FILE=ci-cd-mlops.yml
GITHUB_REF=main
GITHUB_TOKEN=<token>
```

```bash
python ml/drift_monitor.py --trigger-mode local --trigger-github-actions
```

Query kiểm tra nhanh cảnh báo realtime:

```sql
SELECT TOP 20 alert_time, store_id, type, value
FROM dbo.SalesAlerts
ORDER BY alert_time DESC;
```

Artifacts để demo:
- `ml/model_output/drift_monitor_report.json`
- `ml/model_output/retrain_comparison/comparison_report.json`
- Trang web report: `http://localhost:5000/model-report`

## ✅ Success Criteria khi demo

1. Web App dự báo ban đầu còn thấp.
2. Simulator tạo spike doanh thu, SQL xuất hiện alert realtime trong `SalesAlerts`.
3. `drift_monitor.py` phát hiện MAE vượt ngưỡng và tự gọi retrain/promote.
4. Sau retrain, `model-report` và kết quả dự báo cập nhật theo xu hướng mới.

Chạy thử không trigger retrain:

```bash
python ml/drift_monitor.py --dry-run
```

## ☁ Runtime Health Check (Azure)

Quick check trạng thái dịch vụ chính sau deploy:

```bash
az functionapp show -g rg-sales-analytics-dev -n func-sales-validation-d9bt2m --query "{name:name,state:state}" -o table
az stream-analytics job show -g rg-sales-analytics-dev -n sa-sales-analytics-d9bt2m --query "{name:name,jobState:jobState,provisioningState:provisioningState}" -o table
```

Start lại runtime services khi cần:

```bash
az functionapp start -g rg-sales-analytics-dev -n func-sales-validation-d9bt2m
az stream-analytics job start -g rg-sales-analytics-dev -n sa-sales-analytics-d9bt2m --output-start-mode JobStartTime
```

Nếu Stream Analytics báo lỗi Event Hub signature/host, kiểm tra namespace + input mapping trước khi start lại.

### Bước 4 — Upload dữ liệu tham chiếu lên Blob Storage

```bash
python blob_storage/upload_reference_data.py
```

### Bước 5 — Tạo schema database

Chạy hai file SQL sau trên Azure SQL Database:

```
sql/create_tables.sql       ← Tạo 7 bảng + 3 view + index
sql/stored_procedures.sql   ← Tạo stored procedures
```

Test nhanh visual correlation trong Power BI (khong can cho du lieu that):

```
sql/insert_mock_weather_sales_correlation.sql
```

Kiem tra nhanh 5 nhom KPI cho Power BI (mot lenh):

```
sql/verify_powerbi_kpi_pack.sql
```

> 💡 Dùng **Azure Data Studio**, **SSMS**, hoặc **Azure Portal → Query editor**.

### Bước 6 — Cấu hình Stream Analytics

1. Vào **Azure Portal → Stream Analytics Job**

2. Thêm **Inputs**:

   | Input name | Nguồn |
   |---|---|
   | `SalesInput` | Event Hub `sales-events` |
   | `WeatherInput` | Event Hub `weather-events` |
   | `StockInput` | Event Hub `stock-events` |
   | `BlobReferenceInput` | Blob `reference-data` (Reference data) |

3. Thêm **Outputs**:

   | Output name | Đích |
   |---|---|
   | `SalesTransactionsOutput` | Azure SQL → `SalesTransactions` |
   | `HourlySalesSummaryOutput` | Azure SQL → `HourlySalesSummary` |
   | `SalesAlertsOutput` | Azure SQL → `SalesAlerts` |
   | `WeatherSalesCorrelationOutput` | Azure SQL → `WeatherSalesCorrelation` |
   | `PowerBIRealtimeOutput` | Power BI Streaming Dataset |

4. Dán nội dung `stream_analytics/stream_query.sql` vào **Query**
5. Nếu muốn chạy correlation 3 luồng riêng biệt, dùng thêm `stream_analytics/weather_sales_correlation.sql`
6. Nhấn **Start** để khởi chạy job

Checklist mapping output de copy-paste khi cau hinh Azure Portal:

```
stream_analytics/output_mapping_checklist.md
```

### Bước 7 — Chạy Data Generator

Mở **3 terminal riêng biệt** và chạy:

```bash
# Terminal 1 — Dữ liệu bán hàng
python data_generator/sales_generator.py

# Terminal 2 — Dữ liệu thời tiết
python data_generator/weather_generator.py

# Terminal 3 — Dữ liệu chứng khoán
python data_generator/stock_generator.py
```

### Bước 8 — Tạo Data Factory Pipeline

```bash
python data_factory/create_pipeline.py
```

**Pipelines được tạo:**

| Pipeline | Mô tả | Trigger |
|---|---|---|
| `CopyStagingToSQL` | Copy dữ liệu từ Blob staging → SQL | Thủ công / Event |
| `MLOrchestration` | Prepare Data → Train → Update Forecasts | Thủ công |
| `DailyMLTrigger` | Chạy `MLOrchestration` tự động | Mỗi ngày lúc 02:00 UTC |

### Bước 9 — Huấn luyện và triển khai ML Model

```bash
# 1. Huấn luyện model với dữ liệu giả lập
python ml/train_model.py --output-dir ./model_output

# 2. Triển khai lên Azure ML Online Endpoint
python ml/deploy_model.py --model-dir ./model_output

# 3. Chạy dự đoán thời gian thực
python ml/realtime_forecast.py
```

> 📈 Muốn so sánh nhiều mô hình? Chạy: `python ml/compare_models.py`

### Bước 10 — Cấu hình Power BI

Xem hướng dẫn chi tiết tại [`powerbi/POWERBI_SETUP.md`](powerbi/POWERBI_SETUP.md):

1. Kết nối Power BI Desktop với Azure SQL Database (DirectQuery)
2. Tạo Streaming Dataset trên Power BI Service
3. Thiết lập RLS (Row-Level Security) theo vùng
4. Tạo Mobile Layout cho điện thoại
5. Quy trình publish chi tiết: [`docs/POWERBI_PUBLISH_PROCESS.md`](docs/POWERBI_PUBLISH_PROCESS.md)

### Power BI Dashboard Screenshots

Thêm ảnh demo vào thư mục [`docs/screenshots`](docs/screenshots/README.md), sau đó cập nhật trực tiếp các ảnh sau trong README:

- Executive Overview: `docs/screenshots/powerbi-executive-overview.png`
- Realtime Monitoring: `docs/screenshots/powerbi-realtime-monitoring.png`
- Forecast vs Actual: `docs/screenshots/powerbi-forecast-vs-actual.png`
- Weather Correlation: `docs/screenshots/powerbi-weather-correlation.png`
- Alerts and Operations: `docs/screenshots/powerbi-alerts-operations.png`

---

## 🔀 Luồng dữ liệu chi tiết

### 1️⃣ Thu thập (Ingestion)

```
Python Generator
    └─▶ JSON Event
            └─▶ Azure Event Hub
                    └─▶ Consumer Group → Stream Analytics
```

### 2️⃣ Xử lý ETL (Stream Analytics — 9 queries)

| Query | Loại window | Mô tả |
|---|---|---|
| Direct insert | — | Ghi thẳng giao dịch vào SQL |
| Hourly aggregation | Tumbling 1h | Tổng hợp doanh thu theo giờ, vùng, danh mục |
| Product summary | Tumbling 30m | Tổng hợp theo sản phẩm |
| High-value anomaly | — | Phát hiện đơn hàng giá trị cao |
| Revenue spike | Sliding 5m | Phát hiện đột biến doanh thu |
| Weather store | — | Lưu dữ liệu thời tiết vào SQL |
| Stock store | — | Lưu dữ liệu chứng khoán vào SQL |
| Power BI stream | — | Đẩy dữ liệu real-time sang Power BI |
| Weather-Sales JOIN | Tumbling 1h | Tương quan thời tiết × doanh thu |

### 3️⃣ Machine Learning

| Thông tin | Chi tiết |
|---|---|
| **Thuật toán** | Gradient Boosting Regressor |
| **Input features** | Giờ, ngày, tháng, vùng, danh mục, thời tiết |
| **Output** | Dự đoán doanh thu + số lượng cho 24h tới |
| **Endpoint** | Azure ML Online Endpoint (REST API) |

### 4️⃣ Trực quan (Power BI)

| Chế độ | Mô tả |
|---|---|
| **DirectQuery** | Truy vấn trực tiếp Azure SQL, luôn cập nhật |
| **Streaming** | Nhận dữ liệu real-time từ Stream Analytics |
| **Push** | Script Python đẩy dữ liệu tổng hợp định kỳ |

---

## ☁️ Các dịch vụ Azure

| Dịch vụ | Loại | Vai trò | Tier |
|---|---|---|---|
| **Event Hubs** | PaaS | Thu thập sự kiện real-time | Standard (1 TU) |
| **Blob Storage** | PaaS | Reference data & archive | Standard LRS |
| **Stream Analytics** | PaaS | ETL streaming real-time | Standard (6 SU) |
| **SQL Database** | PaaS | Lưu trữ dữ liệu đã xử lý | S1 (20 DTU) |
| **Data Factory** | PaaS | Điều phối pipeline tự động | Pay-as-you-go |
| **Machine Learning** | PaaS | Huấn luyện & triển khai ML | Pay-as-you-go |
| **Power BI** | SaaS | Dashboard & trực quan hóa | Pro/Premium |

> 💰 **Chi phí ước tính:** ~$216/tháng (xem chi tiết tại [`docs/toi_uu_luu_tru.md`](docs/toi_uu_luu_tru.md))

---

## �� Tài liệu

| Tài liệu | Mô tả |
|---|---|
| [`docs/ly_thuyet_va_phan_loai.md`](docs/ly_thuyet_va_phan_loai.md) | Cơ sở lý thuyết: IaaS/PaaS/SaaS, Gradient Boosting, Streaming |
| [`docs/toi_uu_luu_tru.md`](docs/toi_uu_luu_tru.md) | Chiến lược tối ưu: indexing, partitioning, compression, cost |
| [`docs/de_cuong_bao_cao.md`](docs/de_cuong_bao_cao.md) | Đề cương báo cáo Word và slide thuyết trình |
| [`docs/ke_hoach_mlops.md`](docs/ke_hoach_mlops.md) | Lộ trình phát triển MLOps (CI/CD, monitoring, auto-retrain) |
| [`powerbi/POWERBI_SETUP.md`](powerbi/POWERBI_SETUP.md) | Hướng dẫn cấu hình Power BI, RLS, Mobile layout |

---

## 🔗 Tài liệu tham khảo

- 📖 [Azure Event Hubs — Parquet Capture](https://learn.microsoft.com/en-us/azure/stream-analytics/event-hubs-parquet-capture-tutorial)
- 📖 [Stream Analytics — Real-time Fraud Detection](https://learn.microsoft.com/en-us/azure/stream-analytics/stream-analytics-real-time-fraud-detection)
- 📖 [Demand Forecasting Architecture](https://learn.microsoft.com/en-us/azure/architecture/solution-ideas/articles/demand-forecasting)
- 📖 [Azure ML — Online Endpoints](https://learn.microsoft.com/en-us/azure/machine-learning/how-to-deploy-online-endpoints)
- 📖 [Power BI — Streaming Datasets](https://learn.microsoft.com/en-us/power-bi/connect-data/service-real-time-streaming)
