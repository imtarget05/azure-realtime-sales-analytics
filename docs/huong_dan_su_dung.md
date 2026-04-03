# HƯỚNG DẪN SỬ DỤNG TOÀN BỘ HỆ THỐNG
## Từ Bật Hạ tầng → Vận hành → Tắt Hạ tầng

> **Đồ án:** Hệ thống trực quan dữ liệu bán hàng thời gian thực trên Azure  
> **Thời gian triển khai ước tính:** ~30 phút (Terraform) hoặc ~45 phút (CLI)

---

## MỤC LỤC

- [PHẦN A: CHUẨN BỊ MÁY LOCAL](#phần-a-chuẩn-bị-máy-local)
- [PHẦN B: BẬT HẠ TẦNG AZURE](#phần-b-bật-hạ-tầng-azure)
- [PHẦN C: CẤU HÌNH SAU KHI BẬT](#phần-c-cấu-hình-sau-khi-bật)
- [PHẦN D: VẬN HÀNH HỆ THỐNG](#phần-d-vận-hành-hệ-thống)
- [PHẦN E: XEM KẾT QUẢ & DASHBOARD](#phần-e-xem-kết-quả--dashboard)
- [PHẦN F: TẮT HẠ TẦNG AZURE](#phần-f-tắt-hạ-tầng-azure)
- [PHỤ LỤC: XỬ LÝ LỖI THƯỜNG GẶP](#phụ-lục-xử-lý-lỗi-thường-gặp)

---

# PHẦN A: CHUẨN BỊ MÁY LOCAL

## A1. Cài đặt phần mềm cần thiết

| Phần mềm | Lệnh kiểm tra | Link tải |
|---|---|---|
| Python 3.10+ | `python --version` | https://python.org |
| Azure CLI | `az --version` | https://aka.ms/installazurecli |
| Terraform 1.5+ | `terraform --version` | https://terraform.io/downloads |
| ODBC Driver 18 | `odbcinst -q -d` (Linux) | https://learn.microsoft.com/sql/connect/odbc |
| Git | `git --version` | https://git-scm.com |
| Power BI Desktop | — | https://powerbi.microsoft.com/desktop |

## A2. Clone project & cài dependencies

```powershell
# Clone repo
git clone <repo-url> azure-realtime-sales-analytics
cd azure-realtime-sales-analytics

# Tạo virtual environment
python -m venv .venv
.\.venv\Scripts\Activate.ps1    # Windows
# source .venv/bin/activate     # Linux/Mac

# Cài dependencies
pip install -r requirements.txt
```

## A3. Đăng nhập Azure

```powershell
# Đăng nhập
az login

# Xem subscription hiện tại
az account show --query "{name:name, id:id}" -o table

# Nếu cần chọn subscription khác
az account set --subscription "<subscription-id>"
```

---

# PHẦN B: BẬT HẠ TẦNG AZURE

> **Có 2 cách:** Terraform (khuyên dùng) hoặc PowerShell script.

## Cách 1: Terraform (Khuyên dùng)

### B1. Chuẩn bị file cấu hình

```powershell
cd terraform

# Copy file mẫu
Copy-Item terraform.tfvars.example terraform.tfvars

# Sửa terraform.tfvars — điền thông tin:
```

Mở `terraform/terraform.tfvars` và điền:
```hcl
environment           = "dev"
location              = "southeastasia"          # Gần VN nhất
sql_admin_username    = "sqladmin"
sql_admin_password    = "SqlP@ssw0rd2026!"       # Tối thiểu 12 ký tự
ml_training_vm_size   = "Standard_DS3_v2"
ml_endpoint_instance_type = "Standard_DS2_v2"
```

### B2. Khởi tạo Terraform

```powershell
# Tải providers (lần đầu)
terraform init
```

**Output mong đợi:**
```
Terraform has been successfully initialized!
```

### B3. Xem trước tài nguyên sẽ tạo

```powershell
terraform plan -out=tfplan
```

**Output mong đợi:** Hiển thị ~15 resources sẽ tạo:
```
Plan: 18 to add, 0 to change, 0 to destroy.
```

### B4. Triển khai hạ tầng (BẬT)

```powershell
terraform apply tfplan
```

**Đợi ~20–30 phút.** Terraform sẽ tạo:
1. Resource Group
2. Log Analytics + Application Insights
3. Storage Account + 4 Blob Containers
4. Key Vault + 4 Secrets tự động
5. Event Hub Namespace + 3 Event Hubs + Consumer Group
6. Azure SQL Server + Database (SalesAnalyticsDB)
7. Stream Analytics Job
8. ML Workspace + Training Cluster
9. Function App + Service Plan

**Khi hoàn tất, ghi nhận output:**
```powershell
# Xem output
terraform output

# Output quan trọng:
# sql_server_fqdn          = "sql-sales-analytics-xxxxx.database.windows.net"
# eventhub_namespace       = "evhns-sales-analytics-xxxxx"
# ml_workspace_name        = "aml-sales-analytics-xxxxx"
# key_vault_name           = "kv-sales-xxxxx"
# function_app_name        = "func-sales-validation-xxxxx"
# stream_analytics_job_name = "sa-sales-analytics-xxxxx"
```

### B5. Lưu output vào .env

```powershell
cd ..

# Copy file mẫu
Copy-Item .env.example .env
```

Mở `.env` và điền thông tin từ terraform output:
```bash
# Lấy Event Hub connection string
EVENT_HUB_CONNECTION_STRING=<từ Key Vault hoặc:>
# az eventhubs namespace authorization-rule keys list --name RootManageSharedAccessKey --namespace-name evhns-sales-analytics-xxxxx --resource-group rg-sales-analytics-dev --query "primaryConnectionString" -o tsv

EVENT_HUB_NAME=sales-events

SQL_SERVER=sql-sales-analytics-xxxxx.database.windows.net
SQL_DATABASE=SalesAnalyticsDB
SQL_USERNAME=sqladmin
SQL_PASSWORD=SqlP@ssw0rd2026!

AML_WORKSPACE_NAME=aml-sales-analytics-xxxxx
AML_SUBSCRIPTION_ID=<subscription-id>
AML_RESOURCE_GROUP=rg-sales-analytics-dev
```

**Lấy connection strings nhanh:**
```powershell
# Event Hub connection string
az eventhubs namespace authorization-rule keys list `
  --name RootManageSharedAccessKey `
  --namespace-name (terraform -chdir=terraform output -raw eventhub_namespace) `
  --resource-group rg-sales-analytics-dev `
  --query "primaryConnectionString" -o tsv

# Blob connection string
az storage account show-connection-string `
  --name (terraform -chdir=terraform output -raw storage_account_name) `
  --resource-group rg-sales-analytics-dev `
  --query "connectionString" -o tsv
```

---

## Cách 2: PowerShell Script (Thay thế)

```powershell
# Đăng nhập Azure trước
az login

# Chạy script triển khai
.\infrastructure\deploy_azure.ps1
```

Script tự động tạo 14 tài nguyên và lưu thông tin vào `deployment_output.txt`.

---

# PHẦN C: CẤU HÌNH SAU KHI BẬT

## C1. Thêm Firewall Rule cho SQL (để local kết nối được)

```powershell
# Lấy IP hiện tại
$MY_IP = (Invoke-RestMethod -Uri "https://api.ipify.org")

# Thêm firewall rule
az sql server firewall-rule create `
  --server sql-sales-analytics-vebku5 `
  --resource-group rg-sales-analytics-dev `
  --name "AllowMyIP" `
  --start-ip-address $MY_IP `
  --end-ip-address $MY_IP
```

## C2. Tạo bảng SQL

```powershell
# Cách 1: Dùng sqlcmd
sqlcmd -S sql-sales-analytics-vebku5.database.windows.net -d SalesAnalyticsDB `
  -U sqladmin -P "SqlP@ssw0rd2026!" -i sql/create_tables.sql

# Cách 2: Dùng Azure Portal Query Editor (dễ hơn)
# Mở link: https://portal.azure.com → SQL Database → Query Editor
# Đăng nhập bằng sqladmin
# Copy-paste nội dung sql/create_tables.sql → Run
```

**Xác minh 3 bảng đã tạo:**
```sql
SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'dbo';
-- Kết quả: SalesTransactions, HourlySalesSummary, SalesForecast
```

## C3. Cấu hình Stream Analytics

### C3a. Tạo Input (Event Hub → SA)

```powershell
# Lấy Event Hub key
$EH_KEY = az eventhubs namespace authorization-rule keys list `
  --name RootManageSharedAccessKey `
  --namespace-name evhns-sales-analytics-vebku5 `
  --resource-group rg-sales-analytics-dev `
  --query "primaryKey" -o tsv

# Tạo input
$inputBody = @{
  properties = @{
    type = "Stream"
    datasource = @{
      type = "Microsoft.ServiceBus/EventHub"
      properties = @{
        serviceBusNamespace = "evhns-sales-analytics-vebku5"
        sharedAccessPolicyName = "RootManageSharedAccessKey"
        sharedAccessPolicyKey = $EH_KEY
        eventHubName = "sales-events"
        consumerGroupName = "stream-analytics-cg"
      }
    }
    serialization = @{
      type = "Json"
      properties = @{ encoding = "UTF8" }
    }
  }
} | ConvertTo-Json -Depth 5

# Lưu ra file rồi gọi REST API
$inputBody | Out-File -FilePath _sa_input.json -Encoding UTF8
az rest --method PUT `
  --url "https://management.azure.com/subscriptions/<sub-id>/resourceGroups/rg-sales-analytics-dev/providers/Microsoft.StreamAnalytics/streamingjobs/sa-sales-analytics-vebku5/inputs/SalesInput?api-version=2021-10-01-preview" `
  --body "@_sa_input.json"
```

### C3b. Tạo Outputs (SA → SQL)

```powershell
# Output 1: SalesTransactionsOutput
$output1 = @{
  properties = @{
    datasource = @{
      type = "Microsoft.Sql/Server/Database"
      properties = @{
        server = "sql-sales-analytics-vebku5"
        database = "SalesAnalyticsDB"
        user = "sqladmin"
        password = "SqlP@ssw0rd2026!"
        table = "SalesTransactions"
      }
    }
  }
} | ConvertTo-Json -Depth 5

$output1 | Out-File -FilePath _sa_output1.json -Encoding UTF8
az rest --method PUT `
  --url "https://management.azure.com/subscriptions/<sub-id>/resourceGroups/rg-sales-analytics-dev/providers/Microsoft.StreamAnalytics/streamingjobs/sa-sales-analytics-vebku5/outputs/SalesTransactionsOutput?api-version=2021-10-01-preview" `
  --body "@_sa_output1.json"

# Output 2: HourlySalesSummaryOutput (tương tự, table = "HourlySalesSummary")
```

**Hoặc làm trên Portal (dễ hơn):**
1. Azure Portal → Stream Analytics job → **Inputs** → Add → Event Hub
2. Azure Portal → Stream Analytics job → **Outputs** → Add → SQL Database

### C3c. Upload Query

```powershell
# Upload transformation query
$queryBody = @{
  properties = @{
    streamingUnits = 3
    query = (Get-Content stream_analytics/stream_query.sql -Raw)
  }
} | ConvertTo-Json -Depth 3

$queryBody | Out-File -FilePath _sa_query.json -Encoding UTF8
az rest --method PUT `
  --url "https://management.azure.com/subscriptions/<sub-id>/resourceGroups/rg-sales-analytics-dev/providers/Microsoft.StreamAnalytics/streamingjobs/sa-sales-analytics-vebku5/transformations/main?api-version=2021-10-01-preview" `
  --body "@_sa_query.json"
```

### C3d. Start Stream Analytics Job

```powershell
az stream-analytics job start `
  --resource-group rg-sales-analytics-dev `
  --name sa-sales-analytics-vebku5 `
  --output-start-mode JobStartTime
```

**Đợi 1–2 phút.** Kiểm tra trạng thái:
```powershell
az stream-analytics job show `
  --resource-group rg-sales-analytics-dev `
  --name sa-sales-analytics-vebku5 `
  --query "jobState" -o tsv
# Kết quả mong đợi: Running
```

## C4. Deploy Azure Functions

```powershell
# Bước 1: Cài dependencies vào thư mục
cd azure_functions
pip install -r requirements.txt --target .python_packages/lib/site-packages
cd ..

# Bước 2: Tạo ZIP package
Compress-Archive -Path azure_functions/* -DestinationPath func_deploy.zip -Force

# Bước 3: Set app settings
az functionapp config appsettings set `
  --name func-sales-validation-vebku5 `
  --resource-group rg-sales-analytics-dev `
  --settings "EVENT_HUB_NAME=sales-events" "WEBSITE_RUN_FROM_PACKAGE=1"

# Bước 4: Deploy
az functionapp deployment source config-zip `
  --name func-sales-validation-vebku5 `
  --resource-group rg-sales-analytics-dev `
  --src func_deploy.zip

# Bước 5: Kiểm tra
az functionapp show --name func-sales-validation-vebku5 `
  --resource-group rg-sales-analytics-dev `
  --query "state" -o tsv
# Kết quả mong đợi: Running
```

## C5. Kiểm tra toàn bộ hạ tầng

```powershell
Write-Host "=== KIEM TRA HA TANG ===" -ForegroundColor Cyan

# 1. Resource Group
az group show --name rg-sales-analytics-dev --query "properties.provisioningState" -o tsv

# 2. Event Hub
az eventhubs namespace show --name evhns-sales-analytics-vebku5 `
  --resource-group rg-sales-analytics-dev --query "status" -o tsv

# 3. Stream Analytics
az stream-analytics job show --name sa-sales-analytics-vebku5 `
  --resource-group rg-sales-analytics-dev --query "jobState" -o tsv

# 4. SQL Database
az sql db show --server sql-sales-analytics-vebku5 `
  --resource-group rg-sales-analytics-dev `
  --name SalesAnalyticsDB --query "status" -o tsv

# 5. Function App
az functionapp show --name func-sales-validation-vebku5 `
  --resource-group rg-sales-analytics-dev --query "state" -o tsv

# 6. ML Workspace
az ml workspace show --name aml-sales-analytics-vebku5 `
  --resource-group rg-sales-analytics-dev --query "provisioningState" -o tsv
```

**Tất cả phải trả về:** `Succeeded` / `Running` / `Active` / `Online`

---

# PHẦN D: VẬN HÀNH HỆ THỐNG

## D1. Gửi dữ liệu lên Event Hub (Data Generator)

```powershell
# Đảm bảo đang ở thư mục gốc
cd C:\Users\Admin\azure-realtime-sales-analytics
.\.venv\Scripts\Activate.ps1

# Chạy generator — tự động gửi 1.200 events/phút
python data_generator/sales_generator.py
```

**Kết quả thấy trên terminal:**
```
2026-04-01 10:00:01 | INFO | sales_generator | eventhub_batch_sent store_id=S01 batch_size=4 total_sent=4
2026-04-01 10:00:01 | INFO | sales_generator | eventhub_batch_sent store_id=S02 batch_size=3 total_sent=3
2026-04-01 10:00:01 | INFO | sales_generator | eventhub_batch_sent store_id=S03 batch_size=3 total_sent=3
```

**Nhấn Ctrl+C để dừng** khi muốn.

### Luồng dữ liệu tự động sau khi gửi:

```
Generator gửi events
    ↓ (Event Hub: sales-events, 4 partitions)
Stream Analytics nhận events (consumer group: stream-analytics-cg)
    ↓ xử lý: validate, cast, enrich, aggregate
    ├── SalesTransactions (raw) ghi vào SQL
    ├── HourlySalesSummary (aggregation 5 phút) ghi vào SQL
    └── PowerBIOutput (nếu cấu hình)
```

**Tất cả tự động — không cần can thiệp thêm.**

## D2. Kiểm tra dữ liệu đã vào SQL

```powershell
python -c "
import pyodbc, os
from dotenv import load_dotenv
load_dotenv()
conn = pyodbc.connect(
    f'Driver={{ODBC Driver 18 for SQL Server}};'
    f'Server=tcp:{os.getenv(\"SQL_SERVER\")},1433;'
    f'Database={os.getenv(\"SQL_DATABASE\")};'
    f'Uid={os.getenv(\"SQL_USERNAME\")};'
    f'Pwd={os.getenv(\"SQL_PASSWORD\")};'
    f'Encrypt=yes;TrustServerCertificate=no;'
)
cur = conn.cursor()
cur.execute('SELECT COUNT(*) FROM dbo.SalesTransactions')
print(f'SalesTransactions:   {cur.fetchone()[0]} rows')
cur.execute('SELECT COUNT(*) FROM dbo.HourlySalesSummary')
print(f'HourlySalesSummary:  {cur.fetchone()[0]} rows')
cur.execute('SELECT TOP 3 store_id, product_id, revenue, event_time FROM dbo.SalesTransactions ORDER BY id DESC')
for row in cur.fetchall():
    print(f'  {row[0]} | {row[1]} | ${row[2]:.2f} | {row[3]}')
conn.close()
"
```

## D3. Train Model ML

### Train local (nhanh, ~2 phút):
```powershell
python ml/train_model.py --output-dir ml/model_output --n-samples 50000
```

**Output:**
```
[TRAIN] Training Revenue model...
[TRAIN] Revenue model metrics:
  MAE:  4.23
  RMSE: 5.67
  R²:   0.92
[TRAIN] Training Quantity model...
[TRAIN] Artifacts saved to ml/model_output/
```

### Train trên Azure ML (production):
```powershell
python -m mlops.trigger_training_pipeline --n-samples 50000 --timeout 60
```

**Pipeline chạy 5 bước tự động:**
1. Submit job → Azure ML compute cluster
2. Đợi hoàn thành (~10–15 phút)
3. Đăng ký model mới vào Registry
4. So sánh với model production hiện tại
5. Nếu tốt hơn → tự động promote

## D4. Deploy Model lên Endpoint

```powershell
# Đăng ký model (nếu train local)
python -m mlops.model_registry register `
  --model-path ml/model_output `
  --metrics-file ml/model_output/model_metadata.json

# Xem danh sách model
python -m mlops.model_registry list

# Promote model tốt nhất
python -m mlops.model_registry promote --version 1 --stage production

# Deploy lên Online Endpoint (Blue/Green)
python -m mlops.deploy_to_endpoint
```

### Test endpoint:
```powershell
python -c "
import requests, os, json
from dotenv import load_dotenv
load_dotenv()
url = os.getenv('AML_ENDPOINT_URL')
key = os.getenv('AML_API_KEY')
data = {'data': [{'hour': 14, 'day_of_month': 1, 'month': 4, 'is_weekend': 0,
    'store_id': 'S01', 'product_id': 'COKE', 'category': 'Beverage',
    'temperature': 30.0, 'is_rainy': 0, 'holiday': 0}]}
resp = requests.post(url, json=data, headers={'Authorization': f'Bearer {key}', 'Content-Type': 'application/json'})
print(json.dumps(resp.json(), indent=2))
"
```

## D5. Đẩy dữ liệu lên Power BI

```powershell
# Chạy push script (loop mỗi 60 giây)
python powerbi/push_to_powerbi.py
```

**Yêu cầu:** Đã set `POWERBI_PUSH_URL` trong `.env` (xem Phần E).

## D6. Chạy Web App (Flask)

```powershell
python webapp/app.py
```

Mở browser: http://localhost:5000
- Form dự đoán: Nhập store, product, thời tiết → Nhấn Predict
- API: POST http://localhost:5000/api/predict (JSON)
- Health check: GET http://localhost:5000/api/health

## D7. Kiểm tra Drift & Retrain tự động

```powershell
# Chạy drift detector
python -m mlops.drift_detector
```

Nếu phát hiện drift (KS p-value < 0.01 hoặc R² giảm > 15%) → tự động trigger retrain.

---

# PHẦN E: XEM KẾT QUẢ & DASHBOARD

## E1. Xem dữ liệu trên Azure Portal

| Xem gì | Link |
|---|---|
| Toàn bộ Resources | [Resource Group](https://portal.azure.com/#@default/resource/subscriptions/34849ef9-3814-44df-ba32-a86ed9f2a69a/resourceGroups/rg-sales-analytics-dev/overview) |
| Event Hub metrics | [Event Hub](https://portal.azure.com/#@default/resource/subscriptions/34849ef9-3814-44df-ba32-a86ed9f2a69a/resourceGroups/rg-sales-analytics-dev/providers/Microsoft.EventHub/namespaces/evhns-sales-analytics-vebku5/overview) |
| Stream Analytics | [SA Job](https://portal.azure.com/#@default/resource/subscriptions/34849ef9-3814-44df-ba32-a86ed9f2a69a/resourceGroups/rg-sales-analytics-dev/providers/Microsoft.StreamAnalytics/streamingjobs/sa-sales-analytics-vebku5/overview) |
| SQL Query Editor | [Query Editor](https://portal.azure.com/#@default/resource/subscriptions/34849ef9-3814-44df-ba32-a86ed9f2a69a/resourceGroups/rg-sales-analytics-dev/providers/Microsoft.Sql/servers/sql-sales-analytics-vebku5/databases/SalesAnalyticsDB/queryEditor) |
| ML Models & Jobs | [ML Studio](https://ml.azure.com) |
| Function Logs | [Function App](https://portal.azure.com/#@default/resource/subscriptions/34849ef9-3814-44df-ba32-a86ed9f2a69a/resourceGroups/rg-sales-analytics-dev/providers/Microsoft.Web/sites/func-sales-validation-vebku5/overview) |
| Monitoring | [App Insights](https://portal.azure.com/#@default/resource/subscriptions/34849ef9-3814-44df-ba32-a86ed9f2a69a/resourceGroups/rg-sales-analytics-dev/providers/Microsoft.Insights/components/appi-sales-analytics-vebku5/overview) |
| Power BI | [app.powerbi.com](https://app.powerbi.com) |

## E2. Truy vấn SQL trực tiếp

Mở **Query Editor** trên Portal rồi chạy:

```sql
-- Xem 10 giao dịch gần nhất
SELECT TOP 10 * FROM dbo.SalesTransactions ORDER BY id DESC;

-- Xem aggregation 5 phút
SELECT TOP 10 * FROM dbo.HourlySalesSummary ORDER BY id DESC;

-- Tổng doanh thu theo cửa hàng
SELECT store_id, SUM(revenue) AS total_revenue, COUNT(*) AS tx_count
FROM dbo.SalesTransactions
GROUP BY store_id
ORDER BY total_revenue DESC;

-- Tổng doanh thu theo danh mục sản phẩm
SELECT category, SUM(revenue) AS total_revenue,
       SUM(units_sold) AS total_units, AVG(unit_price) AS avg_price
FROM dbo.SalesTransactions
GROUP BY category
ORDER BY total_revenue DESC;

-- Xem dự đoán ML
SELECT TOP 10 * FROM dbo.SalesForecast ORDER BY id DESC;
```

## E3. Cấu hình Power BI Dashboard

### Bước 1: Tạo Streaming Dataset
1. Vào [app.powerbi.com](https://app.powerbi.com) → Workspace → **+ New** → **Streaming dataset**
2. Chọn **API** → Nhập schema:

```json
{
  "name": "SalesRealtimeStream",
  "columns": [
    {"name": "timestamp", "dataType": "DateTime"},
    {"name": "store_id", "dataType": "String"},
    {"name": "category", "dataType": "String"},
    {"name": "transaction_count", "dataType": "Int64"},
    {"name": "total_quantity", "dataType": "Int64"},
    {"name": "total_revenue", "dataType": "Double"},
    {"name": "avg_unit_price", "dataType": "Double"},
    {"name": "avg_temperature", "dataType": "Double"}
  ]
}
```

3. Tick **Historic data analysis** → **Create**
4. Copy **Push URL** hiển thị

### Bước 2: Set Push URL vào .env
```bash
POWERBI_PUSH_URL=https://api.powerbi.com/beta/xxxxx/datasets/xxxxx/rows?key=xxxxx
```

### Bước 3: Chạy push script
```powershell
python powerbi/push_to_powerbi.py
```

### Bước 4: Tạo Dashboard tiles
1. Workspace → **+ New** → **Dashboard** → đặt tên "Sales Analytics"
2. Nhấn **+ Add tile** → **Custom Streaming Data**
3. Chọn dataset `SalesRealtimeStream`
4. Tạo từng tile:

| Tile | Visualization | Values | Filter |
|---|---|---|---|
| Tổng doanh thu | Card | total_revenue (SUM) | — |
| Doanh thu theo cửa hàng | Clustered bar | store_id × total_revenue | — |
| Doanh thu theo danh mục | Pie chart | category × total_revenue | — |
| Trend theo giờ | Line chart | timestamp × total_revenue | — |
| Chỉ HCM (S01) | Card | total_revenue (SUM) | store_id = S01 |
| Chỉ Hà Nội (S02) | Card | total_revenue (SUM) | store_id = S02 |
| Chỉ Đà Nẵng (S03) | Card | total_revenue (SUM) | store_id = S03 |

### Bước 5: Xem theo vùng (RLS)
Xem chi tiết setup RLS trong `powerbi/POWERBI_SETUP.md` → Mục 4.

---

# PHẦN F: TẮT HẠ TẦNG AZURE

> **QUAN TRỌNG:** Để tránh phát sinh chi phí, tắt hạ tầng khi không dùng.

## F1. Dừng các dịch vụ tính phí (giữ dữ liệu)

Nếu muốn **tạm dừng** (giữ dữ liệu, không mất cấu hình):

```powershell
# 1. Dừng Stream Analytics (đang tính phí theo SU)
az stream-analytics job stop `
  --resource-group rg-sales-analytics-dev `
  --name sa-sales-analytics-vebku5
Write-Host "Stream Analytics: Stopped" -ForegroundColor Yellow

# 2. Pause SQL Database (giảm DTU về 0)
# Lưu ý: Chỉ áp dụng cho tier vCore, S0/S1 không pause được
# Thay vào đó, scale xuống Basic ($5/tháng thấp nhất)
az sql db update `
  --server sql-sales-analytics-vebku5 `
  --resource-group rg-sales-analytics-dev `
  --name SalesAnalyticsDB `
  --service-objective Basic
Write-Host "SQL Database: Scaled to Basic" -ForegroundColor Yellow

# 3. Dừng ML Endpoint (nếu đang chạy)
az ml online-endpoint update `
  --name sales-forecast-endpoint `
  --resource-group rg-sales-analytics-dev `
  --workspace-name aml-sales-analytics-vebku5 `
  --traffic "" 2>$null
# Hoặc xóa endpoint hoàn toàn (tiết kiệm hơn):
az ml online-endpoint delete `
  --name sales-forecast-endpoint `
  --resource-group rg-sales-analytics-dev `
  --workspace-name aml-sales-analytics-vebku5 `
  --yes 2>$null
Write-Host "ML Endpoint: Deleted/Stopped" -ForegroundColor Yellow

# 4. Function App: Consumption plan — không tính phí khi không có event
# → Không cần dừng

# 5. Event Hub: Standard — tính phí $22/tháng dù idle
# → Xem F2 nếu muốn xóa hoàn toàn
```

**Chi phí khi tạm dừng:** ~$27/tháng (Event Hub $22 + SQL Basic $5)

## F2. Xóa hoàn toàn hạ tầng (XÓA HẾT)

### Cách 1: Terraform destroy (khuyên dùng)

```powershell
cd terraform

# Xem trước những gì sẽ xóa
terraform plan -destroy

# Xóa toàn bộ
terraform destroy
```

Nhập **yes** khi được hỏi. Terraform xóa tất cả 18 resources.

### Cách 2: Xóa Resource Group (nhanh nhất)

```powershell
# XÓA TOÀN BỘ — không thể hoàn tác!
az group delete --name rg-sales-analytics-dev --yes --no-wait
```

**Lệnh này xóa TẤT CẢ resources** trong resource group, bao gồm:
- Event Hub + tất cả messages
- SQL Database + tất cả dữ liệu
- Stream Analytics job
- ML Workspace + tất cả models
- Function App
- Key Vault (soft-delete 7 ngày, có thể recover)
- Storage Account + Blob data
- Log Analytics + Application Insights

### Xác minh đã xóa:

```powershell
# Kiểm tra resource group còn tồn tại không
az group exists --name rg-sales-analytics-dev
# Kết quả mong đợi: false
```

### Purge Key Vault (nếu cần tạo lại cùng tên):

```powershell
# Key Vault có soft-delete, cần purge nếu muốn tái sử dụng tên
az keyvault purge --name kv-sales-vebku5
```

## F3. Dọn dẹp local

```powershell
# Xóa file tạm
Remove-Item func_deploy.zip -ErrorAction SilentlyContinue
Remove-Item _sa_input.json, _sa_output1.json, _sa_query.json -ErrorAction SilentlyContinue
Remove-Item deployment_output.txt -ErrorAction SilentlyContinue

# Xóa terraform state (nếu đã destroy)
Remove-Item terraform/terraform.tfstate -ErrorAction SilentlyContinue
Remove-Item terraform/terraform.tfstate.backup -ErrorAction SilentlyContinue
Remove-Item terraform/tfplan -ErrorAction SilentlyContinue

# Giữ lại .env (có credentials) hoặc xóa nếu xong dự án
# Remove-Item .env
```

---

# BẢNG TỔNG HỢP: CÁC LỆNH QUAN TRỌNG

## Bật hạ tầng
```powershell
az login
cd terraform && terraform init && terraform apply -auto-approve && cd ..
```

## Cấu hình nhanh
```powershell
# Thêm firewall SQL
$MY_IP = (Invoke-RestMethod -Uri "https://api.ipify.org")
az sql server firewall-rule create --server sql-sales-analytics-vebku5 --resource-group rg-sales-analytics-dev --name AllowMyIP --start-ip-address $MY_IP --end-ip-address $MY_IP

# Tạo bảng SQL (dùng Portal Query Editor dễ hơn)

# Start Stream Analytics
az stream-analytics job start --resource-group rg-sales-analytics-dev --name sa-sales-analytics-vebku5 --output-start-mode JobStartTime
```

## Vận hành
```powershell
python data_generator/sales_generator.py          # Gửi events
python powerbi/push_to_powerbi.py                  # Push Power BI
python ml/train_model.py --output-dir ml/model_output  # Train ML
python -m mlops.trigger_training_pipeline          # Train trên cloud
python -m mlops.deploy_to_endpoint                 # Deploy endpoint
python webapp/app.py                               # Web app
```

## Kiểm tra
```powershell
az stream-analytics job show --name sa-sales-analytics-vebku5 --resource-group rg-sales-analytics-dev --query jobState -o tsv
az functionapp show --name func-sales-validation-vebku5 --resource-group rg-sales-analytics-dev --query state -o tsv
```

## Tắt hạ tầng
```powershell
# Tạm dừng (giữ data)
az stream-analytics job stop --resource-group rg-sales-analytics-dev --name sa-sales-analytics-vebku5

# Xóa hoàn toàn
cd terraform && terraform destroy && cd ..
# HOẶC
az group delete --name rg-sales-analytics-dev --yes --no-wait
```

---

# PHỤ LỤC: XỬ LÝ LỖI THƯỜNG GẶP

## Lỗi 1: "Login failed for user 'sqladmin'"
**Nguyên nhân:** Firewall chưa thêm IP  
**Fix:**
```powershell
$MY_IP = (Invoke-RestMethod -Uri "https://api.ipify.org")
az sql server firewall-rule create --server sql-sales-analytics-vebku5 --resource-group rg-sales-analytics-dev --name AllowMyIP --start-ip-address $MY_IP --end-ip-address $MY_IP
```

## Lỗi 2: "CBS handshake timed out" (Event Hub)
**Nguyên nhân:** Đồng hồ máy lệch > 5 phút  
**Fix:**
```powershell
w32tm /resync /force
```

## Lỗi 3: Stream Analytics "nvarchar(20)" error
**Nguyên nhân:** Compat level 1.2 chỉ hỗ trợ `nvarchar(max)`  
**Fix:** Sửa tất cả `CAST(... AS nvarchar(20))` thành `CAST(... AS nvarchar(max))` trong query.

## Lỗi 4: Function App "0 functions loaded"
**Nguyên nhân:** ZIP deploy thiếu dependencies  
**Fix:**
```powershell
cd azure_functions
pip install -r requirements.txt --target .python_packages/lib/site-packages
cd ..
Compress-Archive -Path azure_functions/* -DestinationPath func_deploy.zip -Force
az functionapp deployment source config-zip --name func-sales-validation-vebku5 --resource-group rg-sales-analytics-dev --src func_deploy.zip
```

## Lỗi 5: "TIMESTAMP BY not allowed on step"
**Nguyên nhân:** `TIMESTAMP BY` chỉ dùng trên input source, không dùng trong CTE  
**Fix:** Đặt `TIMESTAMP BY [timestamp]` ngay sau `FROM SalesInput`, không đặt trong CTE.

## Lỗi 6: Terraform "Key Vault already exists" khi tạo lại
**Nguyên nhân:** Key Vault soft-delete chưa purge  
**Fix:**
```powershell
az keyvault purge --name kv-sales-vebku5
```

## Lỗi 7: ML Endpoint "No healthy upstream"
**Nguyên nhân:** Model chưa deploy hoặc deployment unhealthy  
**Fix:**
```powershell
az ml online-deployment list --endpoint-name sales-forecast-endpoint --resource-group rg-sales-analytics-dev --workspace-name aml-sales-analytics-vebku5 -o table
# Kiểm tra provisioning_state phải là "Succeeded"
```

## Lỗi 8: Power BI push "POWERBI_PUSH_URL not configured"
**Nguyên nhân:** Chưa set biến POWERBI_PUSH_URL  
**Fix:** Tạo streaming dataset trên app.powerbi.com → copy Push URL → dán vào `.env`
