# 🚀 Azure Real-Time Sales Analytics — Hướng Dẫn Chạy Toàn Bộ Dự Án

> **Kiến trúc**: Event Hub → Databricks Lakehouse (Bronze/Silver/Gold) → ML Pipeline → Power BI  
> **MLOps**: Local training → Azure ML Registry → Online Endpoint → Drift Monitor → Auto-Retrain  
> **Region**: Southeast Asia | **Subscription**: Azure for Students

---

## Mục Lục

1. [Yêu Cầu Hệ Thống](#1-yêu-cầu-hệ-thống)
2. [Cài Đặt Môi Trường Local](#2-cài-đặt-môi-trường-local)
3. [Triển Khai Hạ Tầng Azure](#3-triển-khai-hạ-tầng-azure)
4. [Chạy Data Generator](#4-chạy-data-generator)
5. [Databricks Lakehouse Pipeline](#5-databricks-lakehouse-pipeline)
6. [ML Training & MLOps Pipeline](#6-ml-training--mlops-pipeline)
7. [Monitoring & Drift Detection](#7-monitoring--drift-detection)
8. [Web Application](#8-web-application)
9. [Power BI Dashboard](#9-power-bi-dashboard)
10. [Chạy Tests](#10-chạy-tests)
11. [4 Kịch Bản Demo Ấn Tượng](#11-4-kịch-bản-demo-ấn-tượng)
12. [Tắt Toàn Bộ Hạ Tầng](#12-tắt-toàn-bộ-hạ-tầng)

---

## 1. Yêu Cầu Hệ Thống

| Thành phần | Phiên bản |
|---|---|
| Python | 3.10+ |
| Azure CLI | 2.50+ |
| Terraform | 1.5+ |
| ODBC Driver | 18 for SQL Server |
| Git | 2.30+ |

### Azure Resources cần thiết

| Resource | SKU / Tier |
|---|---|
| Event Hubs Namespace | Standard (1 TU) |
| SQL Database | Basic S0 |
| Storage Account | Standard LRS |
| Azure Databricks | Standard |
| Azure ML Workspace | Basic |
| Key Vault | Standard |
| Stream Analytics | Standard (1 SU) |
| Application Insights | Pay-as-you-go |

---

## 2. Cài Đặt Môi Trường Local

```powershell
# Clone repository
git clone https://github.com/imtarget05/azure-realtime-sales-analytics.git
cd azure-realtime-sales-analytics

# Tạo virtual environment
python -m venv .venv
.\.venv\Scripts\Activate.ps1

# Cài đặt dependencies
pip install -r requirements.txt

# Tạo file cấu hình
copy .env.example .env
# → Điền thông tin Azure vào .env sau khi triển khai hạ tầng
```

### Xác thực Azure

```powershell
# Đăng nhập Azure CLI (BẮT BUỘC trước mọi thao tác Azure)
az login
az account set --subscription "<your-subscription-id>"

# Verify
az account show --query "{name:name, id:id, state:state}" -o table
```

> ⚠️ **QUAN TRỌNG**: Nếu tests hoặc scripts bị "treo", hãy chạy lại `az login` — token có thể đã hết hạn.

---

## 3. Triển Khai Hạ Tầng Azure

### Cách 1: Terraform (Khuyến nghị)

```powershell
cd terraform

# Cấu hình biến
copy terraform.tfvars.example terraform.tfvars
# → Chỉnh sửa terraform.tfvars với credentials thực

terraform init
terraform plan -out=tfplan
terraform apply tfplan
```

Sau khi apply xong, Terraform tạo 15 resources:
- Resource Group, Log Analytics, App Insights
- Storage Account (4 containers), Key Vault
- Event Hubs (3 hubs + consumer group)
- SQL Server + Database
- Stream Analytics Job
- Azure ML Workspace + Compute Cluster
- Function App

### Cách 2: PowerShell Script

```powershell
az login
.\infrastructure\deploy_azure.ps1
```

### Cập nhật `.env` sau khi triển khai

```powershell
# Lấy connection strings từ Azure
$ehConnStr = az eventhubs namespace authorization-rule keys list `
  --resource-group rg-sales-analytics-dev `
  --namespace-name evhns-sales-analytics-d9bt2m `
  --name RootManageSharedAccessKey `
  --query primaryConnectionString -o tsv

$blobConnStr = az storage account show-connection-string `
  --resource-group rg-sales-analytics-dev `
  --name stsalesanalyticsd9bt2m -o tsv

# Cập nhật .env
# EVENT_HUB_CONNECTION_STRING=<giá trị $ehConnStr>
# BLOB_CONNECTION_STRING=<giá trị $blobConnStr>
```

---

## 4. Chạy Data Generator

### 4.1 Sinh Events Thời Gian Thực → Event Hub

```powershell
# Sinh sales events liên tục (1 event/giây mặc định)
python -m data_generator.sales_generator

# Tùy chỉnh tốc độ
$env:BATCH_SIZE = "20"
$env:SALES_GENERATION_INTERVAL = "0.5"
python -m data_generator.sales_generator
```

### 4.2 Bật Burst Mode (Demo tải cao)

```powershell
$env:BURST_ENABLED = "true"
$env:BURST_MULTIPLIER = "5"
python -m data_generator.sales_generator
```

### 4.3 Bật Price Shock (Demo drift)

```powershell
$env:PRICE_SHOCK_ENABLED = "true"
$env:PRICE_SHOCK_MULTIPLIER = "2.5"
$env:PRICE_SHOCK_PRODUCTS = "COKE,PEPSI"
python -m data_generator.sales_generator
```

### 4.4 Replay Mode (Từ file mẫu)

```powershell
$env:REPLAY_MODE = "true"
$env:REPLAY_FILE = "sample_events.jsonl"
python -m data_generator.sales_generator
```

---

## 5. Databricks Lakehouse Pipeline

### 5.1 Cấu hình Databricks Workspace

```
1. Vào Azure Databricks Workspace
2. Tạo Repos: Repos → Add Repo → paste URL GitHub
3. Tạo Secret Scope:
   databricks secrets create-scope --scope kv-sales
   databricks secrets put --scope kv-sales --key eh-conn-str --string-value "<Event Hub connection string>"
```

### 5.2 Import Notebooks

Upload thư mục `databricks/notebooks/` vào Databricks Workspace:
```
/Workspace/Repos/azure-realtime-sales-analytics/databricks/notebooks/
  ├── 00_config.py          # Shared configuration
  ├── 01_bronze_ingestion.py # Event Hub → Bronze Delta
  ├── 02_silver_etl.py       # Bronze → Silver (clean + enrich)
  ├── 03_feature_engineering.py # NLP + similarity + viral labels
  ├── 04_ml_prediction.py    # GBT classifier + MLflow
  └── 05_gold_aggregation.py # Gold tables + Power BI views
```

### 5.3 Tạo & Chạy Pipeline Job

```powershell
# Upload job definition
databricks jobs create --json-file databricks/jobs/job_trigger.json

# Chạy thủ công
databricks jobs run-now --job-id <JOB_ID>
```

### 5.4 Pipeline DAG

```
bronze_ingestion (Photon ETL)
        ↓
    silver_etl (Photon ETL)
        ↓
feature_engineering (ML Runtime)
        ↓
  ml_prediction (ML Runtime)
        ↓
gold_aggregation (Photon ETL)
```

### 5.5 Xử Lý Lỗi Thường Gặp

| Lỗi | Nguyên nhân | Cách sửa |
|---|---|---|
| `RepositoryCheckoutFailed` | Git credentials chưa cấu hình | Vào Settings → Git → Add credential hoặc dùng `source: "WORKSPACE"` |
| `ResourceNotFound` | Notebook path sai | Đảm bảo notebooks đã import vào đúng path |
| `SchemaNotFoundException` | Catalog chưa tạo | Chạy notebook 00_config riêng trước hoặc tạo catalog thủ công |
| Event Hub timeout | Firewall/networking | Thêm IP của Databricks cluster vào Event Hub firewall |

---

## 6. ML Training & MLOps Pipeline

### 6.1 Training Cơ Bản

```powershell
# Train model với dữ liệu giả lập
python ml/train_model.py --n-samples 50000 --output-dir ml/model_output

# So sánh 9 thuật toán (Linear, Ridge, Lasso, DT, RF, GBT, AdaBoost, KNN, SVR)
python ml/compare_models.py
```

### 6.2 Local-First MLOps Pipeline (Khuyến nghị)

```powershell
# Chạy pipeline đầy đủ: bootstrap → retrain → smoke test
python mlops/local_first_pipeline.py

# Tùy chỉnh
python mlops/local_first_pipeline.py --bootstrap-samples 30000 --retrain-samples 50000

# Không tự động promote (an toàn hơn cho demo)
python mlops/local_first_pipeline.py --no-promote
```

**Output**: `ml/model_output/local_pipeline_report.json`

### 6.3 Retrain & Compare

```powershell
# So sánh Ridge baseline vs GradientBoosting challenger
python ml/retrain_and_compare.py --new-samples 50000 --promote

# Output:
#   ml/model_output/retrain_comparison/comparison_report.json
#   ml/model_output/retrain_comparison/*.png (charts)
```

### 6.4 Đăng Ký Model lên Azure ML

```powershell
# Đăng ký model vào Azure ML Registry
python mlops/model_registry.py

# Deploy lên Online Endpoint (blue/green)
python mlops/deploy_to_endpoint.py
```

### 6.5 Azure ML Training Job (Cloud)

```powershell
python mlops/trigger_training_pipeline.py
```

---

## 7. Monitoring & Drift Detection

### 7.1 Drift Monitor

```powershell
# Chạy drift monitor (đọc SQL → tính MAE → auto-retrain nếu drift)
python ml/drift_monitor.py

# Tùy chỉnh ngưỡng
python ml/drift_monitor.py --threshold-mae 20 --window-hours 12
```

### 7.2 Model Health Check

```powershell
# Health check với auto-rollback
python monitoring/model_health_check.py
```

### 7.3 A/B Shadow Testing

```powershell
# Bật shadow testing
python monitoring/ab_shadow_test.py --enable --traffic-percent 20
```

### 7.4 Simulate Drift (Demo)

```powershell
# 3 loại drift: price_inflation, data_corruption, category_shift
python scripts/simulate_drift.py --drift-type price_inflation --severity high
python scripts/simulate_drift.py --drift-type data_corruption --severity medium
python scripts/simulate_drift.py --drift-type category_shift --severity low
```

---

## 8. Web Application

```powershell
# Khởi động Flask web app
python -m webapp.app

# Truy cập: http://localhost:5000
# API endpoint: POST http://localhost:5000/api/predict
```

### Endpoints chính

| Route | Mô tả |
|---|---|
| `/` | Dashboard chính |
| `/health` | Health check JSON |
| `/api/predict` | Dự đoán revenue/quantity |
| `/dashboard/monitoring` | Real-time monitoring (SSE) |
| `/model-report` | Model performance report |

---

## 9. Power BI Dashboard

Xem chi tiết tại [`powerbi/POWERBI_SETUP.md`](powerbi/POWERBI_SETUP.md).

```powershell
# Push dữ liệu lên Power BI streaming dataset
python powerbi/push_to_powerbi.py
```

---

## 10. Chạy Tests

```powershell
# ⚠️ Quan trọng: Set KEY_VAULT_URI để tránh hang khi DefaultAzureCredential timeout
$env:KEY_VAULT_URI = "DISABLED"
$env:KEY_VAULT_NAME = "DISABLED"

# Chạy toàn bộ tests (164 tests)
python -m pytest tests/ -v --tb=short

# Chạy từng module
python -m pytest tests/test_config.py -v              # Config tests (9 tests)
python -m pytest tests/test_generators.py -v           # Generator tests (16 tests)
python -m pytest tests/test_sales_generator.py -v      # Sales generator (13 tests)
python -m pytest tests/test_ml.py -v                   # ML core tests (4 tests)
python -m pytest tests/test_ml_extended.py -v          # ML extended (17 tests)
python -m pytest tests/test_ml_pipeline.py -v          # Pipeline tests (5 tests)
python -m pytest tests/test_monitoring.py -v           # Monitoring tests (44 tests)
python -m pytest tests/test_webapp.py -v               # Webapp tests
python -m pytest tests/test_webapp_extended.py -v      # Webapp extended
python -m pytest tests/test_validation.py -v           # Validation tests (10 tests)
python -m pytest tests/test_validate_env.py -v         # Env validation (8 tests)

# Kết quả mong đợi: 164 passed, 1 skipped, 0 failures
```

### Coverage

```powershell
python -m pytest tests/ --cov=. --cov-report=html
# Mở htmlcov/index.html để xem coverage report
```

---

## 11. 4 Kịch Bản Demo Ấn Tượng

### 📊 Kịch Bản 1: Real-Time Lakehouse Pipeline End-to-End

**Mục tiêu**: Demo luồng dữ liệu từ nguồn → Bronze → Silver → Gold → Dashboard trong thời gian thực.

**Thời gian**: ~15 phút

```powershell
# Terminal 1: Bật Data Generator ở chế độ burst
$env:BURST_ENABLED = "true"
$env:BURST_MULTIPLIER = "3"
python -m data_generator.sales_generator

# Terminal 2: Bật Web App
python -m webapp.app

# Trên Databricks: Trigger job thủ công
# Jobs & Pipelines → Sales_Lakehouse_Pipeline → Run Now
```

**Điểm nhấn trình bày**:
1. Event Hub dashboard hiển thị throughput 3,600 events/phút
2. Databricks DAG chạy 5 tasks tuần tự (Bronze→Silver→Feature→ML→Gold)
3. Web app `/dashboard/monitoring` real-time SSE cập nhật
4. Power BI refresh tự động từ Gold views
5. SQL query trực tiếp trên Gold tables qua Databricks SQL

**Câu hỏi gây ấn tượng**: "Từ khi event được sinh ra đến khi hiển thị trên dashboard mất bao lâu?" → ~30 giây (micro-batch processing time).

---

### 🤖 Kịch Bản 2: MLOps Automated Drift Detection & Auto-Retrain

**Mục tiêu**: Demo vòng đời MLOps hoàn chỉnh — phát hiện drift → tự động retrain → so sánh model → promote/reject.

**Thời gian**: ~20 phút

```powershell
# Bước 1: Train baseline model
python ml/train_model.py --n-samples 50000

# Bước 2: Inject drift bằng price shock
$env:PRICE_SHOCK_ENABLED = "true"
$env:PRICE_SHOCK_MULTIPLIER = "3.0"
$env:PRICE_SHOCK_PRODUCTS = "COKE,MILK,BREAD"
python -m data_generator.sales_generator
# Chờ 2-3 phút gửi dữ liệu bị drift

# Bước 3: Chạy drift monitor (phát hiện MAE tăng vọt)
python ml/drift_monitor.py --threshold-mae 15

# Bước 4: Tự động retrain with gate check
python mlops/local_first_pipeline.py --retrain-samples 50000

# Bước 5: Xem comparison report
# Mở ml/model_output/retrain_comparison/retrain_summary_dashboard.png
```

**Điểm nhấn trình bày**:
1. Side-by-side comparison: Ridge (R²=0.58) vs GradientBoosting (R²=0.86)
2. Quality gate tự động: chỉ promote nếu R² cải thiện >5%
3. Rollback mechanism: nếu model mới kém hơn, tự động rollback
4. Full audit trail: JSON reports, charts, retrain history
5. A/B shadow testing: chạy 2 models song song trước khi promote

**Kết quả kỳ vọng**:
```
Revenue R² improvement: +0.28 (0.58 → 0.86)
Revenue MAE reduction:  -6.3  (14.9 → 8.6)
Decision: PROMOTE ✅
```

---

### ⚡ Kịch Bản 3: Stress Test & Anomaly Detection

**Mục tiêu**: Demo khả năng xử lý tải cao, phát hiện bất thường, và tự recovery.

**Thời gian**: ~15 phút

```powershell
# Bước 1: Baseline — load bình thường
$env:BURST_ENABLED = "false"
python -m data_generator.sales_generator &

# Bước 2: Trigger burst (x5 traffic)
$env:BURST_ENABLED = "true"
$env:BURST_MULTIPLIER = "5"
$env:BURST_DURATION_SECONDS = "60"
python -m data_generator.sales_generator

# Bước 3: Inject data corruption
python scripts/simulate_drift.py --drift-type data_corruption --severity high

# Bước 4: Monitor hệ thống
python monitoring/model_health_check.py

# Bước 5: Trigger auto-recovery
# Hệ thống tự rollback model nếu MAPE > threshold
```

**Điểm nhấn trình bày**:
1. Event Hubs auto-scale với TU (Throughput Units)
2. Databricks Auto-scale cluster: 1→4 workers khi tải tăng
3. Stream Analytics: 5-giây tumbling window phát hiện anomaly
4. Application Insights: Custom metrics + latency monitoring
5. Auto-rollback: Model health check phát hiện MAPE cao → rollback về version cũ

**Metrics theo dõi**:
- Event Hub: ingress rate, throttled requests
- Databricks: cluster utilization, job duration
- SQL: query latency, connection pool
- ML: prediction latency, MAPE, drift score

---

### 🏗️ Kịch Bản 4: Infrastructure-as-Code & Blue/Green Deployment

**Mục tiêu**: Demo khả năng triển khai hạ tầng hoàn toàn tự động, blue/green model deployment, và security best practices.

**Thời gian**: ~20 phút

```powershell
# Bước 1: Triển khai hạ tầng từ scratch bằng Terraform
cd terraform
terraform plan    # Xem 15 resources sẽ tạo
terraform apply   # Tạo toàn bộ trong ~10 phút

# Bước 2: Verify resources
az resource list --resource-group rg-sales-analytics-dev -o table

# Bước 3: Deploy model v1 (blue)
python mlops/deploy_to_endpoint.py

# Bước 4: Train model v2, deploy green
python mlops/local_first_pipeline.py --retrain-samples 80000
python mlops/deploy_to_endpoint.py  # Tự động tạo green deployment

# Bước 5: Traffic splitting (90% blue, 10% green)
# → Monitor green performance
# → Nếu OK → 100% green

# Bước 6: Security audit
python validate_env.py --mode all
# → Kiểm tra: Key Vault integration, RBAC, network rules
```

**Điểm nhấn trình bày**:
1. **IaC**: Terraform tạo 15 Azure resources chuẩn enterprise
2. **Blue/Green**: Zero-downtime model deployment
3. **Security**: Key Vault cho secrets, SQL firewall rules, RBAC
4. **Monitoring**: Application Insights + Log Analytics integrated
5. **Cost optimization**: Auto-pause SQL, spot instances cho Databricks
6. **Reproducibility**: `terraform destroy` + `terraform apply` = identical environment

**Architecture Diagram**:
```
┌─────────────┐     ┌──────────────┐     ┌─────────────────┐
│   Data Gen  │────→│  Event Hub   │────→│   Databricks    │
│  (Python)   │     │  (3 hubs)    │     │  (Bronze→Gold)  │
└─────────────┘     └──────────────┘     └────────┬────────┘
                                                   │
┌─────────────┐     ┌──────────────┐     ┌────────▼────────┐
│  Power BI   │←────│  Azure SQL   │←────│ Stream Analytics│
│  Dashboard  │     │  (Gold views)│     │  (Real-time)    │
└─────────────┘     └──────────────┘     └─────────────────┘
                                                   
┌─────────────┐     ┌──────────────┐     ┌─────────────────┐
│  Web App    │←────│  Azure ML    │←────│  MLOps Pipeline │
│  (Flask)    │     │  (Endpoint)  │     │  (Auto-retrain) │
└─────────────┘     └──────────────┘     └─────────────────┘
                           │
                    ┌──────▼──────┐
                    │  Key Vault  │
                    │  (Secrets)  │
                    └─────────────┘
```

---

## 12. Tắt Toàn Bộ Hạ Tầng

### ⚠️ QUAN TRỌNG: Thực hiện đúng thứ tự để tránh mất dữ liệu

### Bước 1: Dừng các processes đang chạy

```powershell
# Dừng Data Generator (Ctrl+C trong terminal)
# Dừng Web App (Ctrl+C trong terminal)
```

### Bước 2: Pause/Stop Azure Services (Tiết kiệm chi phí)

```powershell
# Pause Databricks Job Schedule
# Vào Databricks → Jobs → Sales_Lakehouse_Pipeline → Pause

# Stop Stream Analytics Job
az stream-analytics job stop `
  --resource-group rg-sales-analytics-dev `
  --job-name sa-sales-analytics-d9bt2m

# Pause SQL Database (tránh phát sinh chi phí)
az sql db update `
  --resource-group rg-sales-analytics-dev `
  --server sql-sales-analytics-d9bt2m `
  --name SalesAnalyticsDB `
  --service-objective Free

# Delete Azure ML Compute (tốn chi phí nhất)
az ml compute delete `
  --resource-group rg-sales-analytics-dev `
  --workspace-name aml-sales-analytics-d9bt2m2 `
  --name cpu-cluster `
  --yes

# Delete Azure ML Online Endpoint
az ml online-endpoint delete `
  --resource-group rg-sales-analytics-dev `
  --workspace-name aml-sales-analytics-d9bt2m2 `
  --name sales-forecast-endpoint `
  --yes
```

### Bước 3: Xóa TOÀN BỘ Hạ Tầng (nếu không cần nữa)

#### Cách 1: Terraform (Khuyến nghị — sạch nhất)

```powershell
cd terraform
terraform destroy -auto-approve
```

#### Cách 2: Xóa Resource Group (nhanh nhất)

```powershell
# ⚠️ XÓA TẤT CẢ resources trong resource group — KHÔNG THỂ HOÀN TÁC
az group delete --name rg-sales-analytics-dev --yes --no-wait

# Verify đã xóa
az group show --name rg-sales-analytics-dev 2>&1
# Output: "Resource group 'rg-sales-analytics-dev' could not be found."
```

#### Cách 3: Xóa từng resource (chọn lọc)

```powershell
$RG = "rg-sales-analytics-dev"

# Thứ tự xóa: endpoint → compute → workspace → databases → networking → storage

# 1. Azure ML Endpoint + Compute
az ml online-endpoint delete -g $RG -w aml-sales-analytics-d9bt2m2 -n sales-forecast-endpoint --yes
az ml compute delete -g $RG -w aml-sales-analytics-d9bt2m2 -n cpu-cluster --yes

# 2. Databricks Workspace
az databricks workspace delete -g $RG -n dbw-sales-analytics-d9bt2m --yes

# 3. Stream Analytics
az stream-analytics job delete -g $RG -n sa-sales-analytics-d9bt2m --yes

# 4. Function App
az functionapp delete -g $RG -n func-sales-validation-d9bt2m

# 5. SQL Database + Server
az sql db delete -g $RG -s sql-sales-analytics-d9bt2m -n SalesAnalyticsDB --yes
az sql server delete -g $RG -n sql-sales-analytics-d9bt2m --yes

# 6. Event Hubs Namespace
az eventhubs namespace delete -g $RG -n evhns-sales-analytics-d9bt2m

# 7. Key Vault (soft-delete enabled)
az keyvault delete -g $RG -n kv-sales-d9bt2m
az keyvault purge -n kv-sales-d9bt2m --location southeastasia

# 8. Storage Account
az storage account delete -g $RG -n stsalesanalyticsd9bt2m --yes

# 9. Application Insights + Log Analytics
az monitor app-insights component delete -g $RG --app appi-sales-d9bt2m
az monitor log-analytics workspace delete -g $RG -n log-sales-d9bt2m --yes

# 10. Xóa Resource Group rỗng
az group delete -n $RG --yes
```

### Bước 4: Verify Hoàn Tất

```powershell
# Liệt kê tất cả resources còn lại
az resource list --query "[?resourceGroup=='rg-sales-analytics-dev']" -o table

# Kiểm tra billing
az consumption usage list --start-date 2026-04-01 --end-date 2026-04-09 -o table

# Purge Key Vault (nếu còn soft-deleted)
az keyvault list-deleted --query "[?name=='kv-sales-d9bt2m']" -o table
```

---

## Cheat Sheet — Lệnh Thường Dùng

```powershell
# ── Quick Start ──
$env:KEY_VAULT_URI = "DISABLED"           # Tránh hung khi chạy local
python mlops/local_first_pipeline.py       # MLOps pipeline
python -m pytest tests/ -q                 # Chạy 164 tests

# ── Data Generator ──
python -m data_generator.sales_generator   # Real-time events
python -m data_generator.stock_generator   # Stock data
python -m data_generator.weather_generator # Weather data

# ── Monitoring ──
python ml/drift_monitor.py                 # Drift detection
python monitoring/model_health_check.py    # Health check
python monitoring/ab_shadow_test.py        # A/B testing

# ── Deploy ──
terraform -chdir=terraform apply           # Hạ tầng
terraform -chdir=terraform destroy         # Hủy hạ tầng
```

---

*Tài liệu được cập nhật: 2026-04-08 | Phiên bản: 2.0*
