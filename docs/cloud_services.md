# Chi tiết các dịch vụ Cloud sử dụng

## Tổng quan phân loại

| Dịch vụ | Loại | Tier | Mô tả |
|---------|------|------|--------|
| Azure Event Hubs | **PaaS** | Standard | Message broker cho streaming data |
| Azure Stream Analytics | **PaaS** | Standard (6 SU) | Real-time ETL processing |
| Azure SQL Database | **PaaS** | Standard S2 | Relational database lưu trữ |
| Azure Machine Learning | **PaaS** | Enterprise | ML training & deployment |
| Power BI | **SaaS** | Pro | Business intelligence & dashboard |
| Azure App Service | **PaaS** | B1 | Web application hosting |
| Azure Functions | **FaaS** | Consumption | Serverless event processing |
| Azure Key Vault | **PaaS** | Standard | Quản lý secrets & certificates |
| Azure Data Factory | **PaaS** | V2 | Orchestration & ETL scheduling |
| Azure Databricks | **PaaS** | Standard | Big data processing (Spark) |
| Terraform | **IaC** | — | Infrastructure as Code |

## 1. Azure Event Hubs (PaaS — Message Broker)

**Vai trò trong hệ thống:** Thu thập dữ liệu real-time từ 3 nguồn (sales, weather, stock).

**Cấu hình:**
- Namespace: `eh-sales-analytics-d9bt2m`
- 3 Event Hub topics: `sales-events`, `weather-events`, `stock-events`
- Partition: 4 partitions/topic
- Throughput: 1 MB/s ingress, 2 MB/s egress (Standard tier)
- Retention: 1 ngày

**Tại sao chọn Event Hubs:**
- Thiết kế cho high-throughput streaming (hàng triệu events/giây)
- Tích hợp native với Stream Analytics
- Hỗ trợ Kafka protocol (tương thích hệ sinh thái)
- Auto-scale, managed service (không cần quản lý infrastructure)

## 2. Azure Stream Analytics (PaaS — Real-time ETL)

**Vai trò:** Xử lý, biến đổi và join 3 luồng dữ liệu real-time.

**Cấu hình:**
- Job: `sa-sales-analytics-d9bt2m`
- Streaming Units: 6 SU
- Compatibility level: 1.2
- 3 inputs (Event Hubs) → 4 outputs (SQL tables)

**Các query chính:**
1. `stream_query.sql`: Tổng hợp doanh thu theo tumbling window 5 phút, phát hiện anomaly
2. `weather_sales_correlation.sql`: JOIN 3 luồng, tính tương quan thời tiết-doanh thu-chứng khoán

**Outputs:**
| Output | Bảng SQL | Mô tả |
|--------|---------|--------|
| SalesTransactionsOutput | SalesTransactions | Giao dịch đã validated |
| HourlySalesSummaryOutput | HourlySalesSummary | Tổng hợp theo giờ |
| SalesAlertsOutput | SalesAlerts | Cảnh báo anomaly |
| WeatherSalesCorrelationOutput | WeatherSalesCorrelation | Tương quan 3 luồng |

## 3. Azure SQL Database (PaaS — Relational Storage)

**Vai trò:** Lưu trữ toàn bộ dữ liệu đã xử lý, phục vụ query từ Power BI và Web App.

**Cấu hình:**
- Server: `sql-sales-analytics-d9bt2m.database.windows.net`
- Database: `SalesAnalyticsDB`
- Tier: Standard S2 (50 DTU)
- Storage: 250GB max
- Backup: Automatic (7 ngày retention)
- Encryption: TDE (Transparent Data Encryption) bật mặc định

**10 bảng chính:**
SalesTransactions, HourlySalesSummary, SalesForecast, SalesAlerts, WeatherSalesCorrelation, Products, StoreRegions, MonitoringEvents, ModelRegistry, SecurityMapping

**10 views hỗ trợ Power BI:**
vw_DailySalesSummary, vw_HourlySalesTrend, vw_CategoryPerformance, vw_ProductSales, vw_StoreComparison, vw_DoDGrowth, vw_DoDGrowthOverall, vw_PerformanceMetrics, vw_LatestForecasts, vw_ModelPerformance

**Tối ưu hóa:**
- Clustered Index trên `event_time` cho range scan
- Non-clustered Index trên `store_id`, `product_id`, `category`
- Stored Procedures cho batch processing
- Partitioning theo tháng (planned)

## 4. Azure Machine Learning (PaaS — ML Platform)

**Vai trò:** Huấn luyện, đánh giá, và deploy model dự đoán doanh thu.

**Cấu hình:**
- Workspace: `aml-sales-analytics-d9bt2m2`
- Compute: Standard_DS1_v2 (training), ACI (inference)
- Model: GradientBoostingRegressor (scikit-learn 1.8.0)
- Endpoint: REST API cho real-time prediction

**MLOps Features:**
- Model Registry: quản lý phiên bản + metadata
- Automated Retrain: trigger khi drift detected
- A/B Testing: shadow mode comparison
- CI/CD: GitHub Actions → Azure ML Pipeline

## 5. Power BI (SaaS — Business Intelligence)

**Vai trò:** Trực quan dữ liệu cho business users.

**Cấu hình:**
- Dataset: DirectQuery từ Azure SQL Database
- 4+ Reports: Tổng quan, Chi tiết sản phẩm, Phân tích theo vùng, Dự đoán ML
- Navigation: Buttons điều hướng giữa các report
- RLS (Row-Level Security): phân quyền theo region
- Mobile Layout: responsive dashboard cho mobile
- Auto-refresh: 15 phút (DirectQuery)

## 6. Azure App Service (PaaS — Web Hosting)

**Vai trò:** Host Flask web application cho prediction và monitoring.

**Cấu hình:**
- App: `webapp-sales-analytics-d9bt2m`
- Runtime: Python 3.10
- Tier: B1 (Basic)
- Routes: `/predict`, `/dashboard`, `/model-report`, `/api/predict`, `/api/health`

## 7. Azure Functions (FaaS — Serverless)

**Vai trò:** Xử lý event-driven: validate sales events và monitor drift.

**2 Functions:**
1. **ValidateSalesEvent**: Trigger bởi Event Hub, validate schema trước khi ghi SQL
2. **DriftMonitor**: Timer trigger (mỗi giờ), kiểm tra drift model

**Ưu điểm FaaS:**
- Chỉ trả tiền khi chạy (consumption plan)
- Auto-scale từ 0 đến hàng nghìn instances
- Không cần quản lý server

## 8. Infrastructure as Code

**Terraform** (`terraform/main.tf`): Định nghĩa toàn bộ infrastructure:
- Resource Group, Event Hubs, SQL Database, Stream Analytics, Key Vault, App Service, Functions

**ARM Templates** (`infrastructure/`): Alternative deployment:
- `arm_streaming_job.json`: Stream Analytics job
- `arm_monitoring.json`: Monitoring alerts

**CI/CD** (`.github/workflows/`):
- `ci-cd-mlops.yml`: ML pipeline automation
- `deploy-functions.yml`: Azure Functions deployment
- `deploy-simulator.yml`: Data simulator deployment
