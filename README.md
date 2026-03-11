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
│   └── queries.sql                   # 9 queries: aggregation, anomaly, JOIN
│
├── 📂 sql/                           # Cơ sở dữ liệu
│   ├── create_tables.sql             # Schema: 7 bảng + 3 view + index
│   └── stored_procedures.sql         # Stored procedures cho Data Factory
│
├── 📂 ml/                            # Machine Learning
│   ├── train_model.py                # Huấn luyện Gradient Boosting
│   ├── deploy_model.py               # Triển khai lên Azure ML Endpoint
│   ├── score.py                      # Script chấm điểm cho endpoint
│   ├── realtime_forecast.py          # Gọi endpoint để dự báo real-time
│   ├── compare_models.py             # So sánh 9 mô hình ML + biểu đồ
│   └── conda_env.yml                 # Môi trường Conda cho Azure ML
│
├── 📂 data_factory/                  # Orchestration
│   ├── create_pipeline.py            # Tạo pipelines qua Python SDK
│   └── pipeline_definition.json      # Định nghĩa pipeline (ARM template)
│
├── 📂 infrastructure/                # Triển khai hạ tầng (IaC)
│   ├── deploy_azure.sh               # Bash script (Linux/macOS)
│   └── deploy_azure.ps1              # PowerShell script (Windows)
│
├── 📂 powerbi/                       # Power BI
│   ├── push_to_powerbi.py            # Đẩy dữ liệu tổng hợp lên Power BI
│   └── POWERBI_SETUP.md              # Hướng dẫn setup + RLS + Mobile
│
├── 📂 webapp/                        # Web Application (Flask)
│   ├── app.py                        # Flask app gọi ML endpoint
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
│   └── ke_hoach_mlops.md             # Kế hoạch phát triển MLOps
│
├── .env.example                      # Mẫu biến môi trường (copy → .env)
├── .gitignore
├── requirements.txt                  # Danh sách thư viện Python
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
   | `SQLOutput` | Azure SQL → `SalesTransactions` |
   | `HourlySummaryOutput` | Azure SQL → `HourlySalesSummary` |
   | `ProductSummaryOutput` | Azure SQL → `ProductSalesSummary` |
   | `AlertsOutput` | Azure SQL → `SalesAlerts` |
   | `WeatherOutput` | Azure SQL → `WeatherData` |
   | `StockOutput` | Azure SQL → `StockData` |
   | `PowerBIOutput` | Power BI Dataset |

4. Dán nội dung `stream_analytics/queries.sql` vào **Query**
5. Nhấn **Start** để khởi chạy job

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
