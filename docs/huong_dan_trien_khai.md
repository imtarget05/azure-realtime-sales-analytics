# Hướng Dẫn Triển Khai Lên Azure — Từ Đầu Đến Hoàn Thành

> **Dành cho**: Người mới bắt đầu với Azure  
> **Thời gian ước tính**: 3–5 giờ (lần đầu)  
> **Chi phí ước tính**: ~$5–15/tháng (tier sinh viên / Free Trial)

---

## Mục Lục

1. [Chuẩn bị môi trường](#1-chuẩn-bị-môi-trường)
2. [Tạo tài khoản Azure & đăng nhập](#2-tạo-tài-khoản-azure--đăng-nhập)
3. [Triển khai hạ tầng (Bước tự động)](#3-triển-khai-hạ-tầng-bước-tự-động)
4. [Tạo bảng trong Azure SQL](#4-tạo-bảng-trong-azure-sql)
5. [Cấu hình Stream Analytics](#5-cấu-hình-stream-analytics)
6. [Cấu hình biến môi trường (.env)](#6-cấu-hình-biến-môi-trường-env)
7. [Chạy Data Generator — test luồng dữ liệu](#7-chạy-data-generator--test-luồng-dữ-liệu)
8. [Train và deploy ML Model](#8-train-và-deploy-ml-model)
9. [Deploy Azure Functions](#9-deploy-azure-functions)
10. [Chạy Web App cục bộ](#10-chạy-web-app-cục-bộ)
11. [Cấu hình Power BI Dashboard](#11-cấu-hình-power-bi-dashboard)
12. [Thiết lập CI/CD với GitHub Actions](#12-thiết-lập-cicd-với-github-actions)
13. [Kiểm tra toàn bộ hệ thống](#13-kiểm-tra-toàn-bộ-hệ-thống)
14. [Xử lý sự cố thường gặp](#14-xử-lý-sự-cố-thường-gặp)

---

## 1. Chuẩn Bị Môi Trường

### 1.1 Phần mềm cần cài đặt trên máy tính

| Phần mềm | Phiên bản | Link tải | Kiểm tra |
|----------|-----------|----------|----------|
| Python | 3.10+ | https://python.org | `python --version` |
| Azure CLI | 2.55+ | https://aka.ms/installazurecliwindows | `az --version` |
| Terraform | 1.5+ | https://developer.hashicorp.com/terraform/downloads | `terraform --version` |
| Git | Bất kỳ | https://git-scm.com | `git --version` |
| VS Code | Bất kỳ | https://code.visualstudio.com | — |
| ODBC Driver 18 | 18.x | https://aka.ms/downloadmsodbcsql | Tìm trong Apps |

> **Windows**: Cài ODBC Driver 18 for SQL Server bắt buộc để kết nối Azure SQL từ Python.

### 1.2 Cài đặt Python dependencies

Mở PowerShell tại thư mục dự án:

```powershell
# Tạo virtual environment
python -m venv .venv

# Kích hoạt
.venv\Scripts\Activate.ps1

# Cài đặt tất cả thư viện
pip install -r requirements.txt
```

### 1.3 Kiểm tra cài đặt

```powershell
# Kiểm tra Azure CLI
az --version

# Kiểm tra Terraform
terraform --version

# Chạy test để đảm bảo code không bị lỗi
python -m pytest tests/ -v
# Kết quả mong đợi: 36 passed, 1 skipped
```

---

## 2. Tạo Tài Khoản Azure & Đăng Nhập

### 2.1 Tạo tài khoản Azure miễn phí

1. Vào https://azure.microsoft.com/free
2. Nhấn **Start free** → đăng ký bằng email Microsoft/Google
3. Nhập thông tin thẻ ngân hàng (chỉ để xác minh, **không bị trừ tiền** trong Free Trial)
4. Nhận **$200 credit** dùng trong 30 ngày + dịch vụ miễn phí 12 tháng

> **Lưu ý sinh viên**: Dùng https://azure.microsoft.com/free/students để nhận $100 credit không cần thẻ.

### 2.2 Đăng nhập Azure CLI

```powershell
# Đăng nhập — sẽ mở trình duyệt
az login

# Kiểm tra đăng nhập thành công — xem Subscription ID
az account show

# Nếu có nhiều subscription, chọn subscription muốn dùng
az account set --subscription "<SUBSCRIPTION_ID>"
```

Lấy và lưu lại **Subscription ID** (dạng: `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`).

### 2.3 Đăng ký Resource Providers cần thiết

```powershell
# Đăng ký các providers (chỉ cần làm 1 lần)
az provider register --namespace Microsoft.EventHub
az provider register --namespace Microsoft.StreamAnalytics
az provider register --namespace Microsoft.Sql
az provider register --namespace Microsoft.MachineLearningServices
az provider register --namespace Microsoft.Storage
az provider register --namespace Microsoft.KeyVault
az provider register --namespace Microsoft.DataFactory
az provider register --namespace Microsoft.Web
az provider register --namespace Microsoft.Insights

# Kiểm tra trạng thái (chờ đến khi tất cả là "Registered")
az provider show -n Microsoft.EventHub --query "registrationState"
```

---

## 3. Triển Khai Hạ Tầng (Bước Tự Động)

Bạn có **2 cách** để tạo hạ tầng: **Script PowerShell** (đơn giản hơn) hoặc **Terraform** (chuyên nghiệp hơn).

---

### CÁCH A — PowerShell Script (Khuyến nghị cho người mới)

Script này tạo tất cả dịch vụ Azure chỉ trong 1 lệnh.

```powershell
# Di chuyển vào thư mục infrastructure
cd infrastructure

# Chạy script
.\deploy_azure.ps1
```

Script sẽ tự động tạo:
- Resource Group
- Storage Account + 3 Blob Containers
- Event Hubs Namespace + 3 Event Hubs (sales-events, weather-events, stock-events)
- Azure SQL Server + Database `SalesAnalyticsDB`
- Stream Analytics Job
- Azure Machine Learning Workspace
- Azure Data Factory
- Key Vault + lưu tất cả secrets
- Application Insights + Log Analytics
- Azure Functions App

**Thời gian**: ~15–20 phút  
**Output**: File `deployment_output.txt` chứa tất cả connection strings.

> ⚠️ **Lưu lại file `deployment_output.txt`** — bạn sẽ cần các giá trị này ở bước 6.

---

### CÁCH B — Terraform (Nâng cao)

```powershell
# Di chuyển vào thư mục terraform
cd terraform

# Sao chép file cấu hình mẫu
cp terraform.tfvars.example terraform.tfvars
```

Mở file `terraform/terraform.tfvars` và điền thông tin:

```hcl
environment           = "dev"
location              = "eastus"
sql_admin_username    = "sqladmin"
sql_admin_password    = "P@ssw0rd2024!"   # ít nhất 12 ký tự
ml_training_vm_size   = "Standard_DS3_v2"
ml_endpoint_instance_type = "Standard_DS2_v2"
```

```powershell
# Khởi tạo Terraform
terraform init

# Xem trước những gì sẽ tạo
terraform plan

# Tạo hạ tầng (nhập "yes" khi được hỏi)
terraform apply

# Lấy các giá trị output
terraform output
```

Terraform sẽ hiển thị event_hub_connection_string, sql_server_fqdn, key_vault_uri — lưu lại hết.

---

## 4. Tạo Bảng Trong Azure SQL

Sau khi có SQL Server, cần tạo các bảng theo đúng schema của dự án.

### 4.1 Kết nối Azure SQL từ Azure Portal

1. Vào **portal.azure.com** → tìm **SQL databases**
2. Chọn database **SalesAnalyticsDB**
3. Nhấn **Query editor (preview)** ở menu trái
4. Đăng nhập bằng SQL authentication (user/pass từ deployment)

### 4.2 Chạy script tạo bảng

Trong Query Editor, copy toàn bộ nội dung file `sql/create_tables.sql` và nhấn **Run**.

Script sẽ tạo:
- Bảng `SalesTransactions` — chứa từng giao dịch raw từ Stream Analytics
- Bảng `HourlySalesSummary` — tổng hợp mỗi 5 phút
- Bảng `SalesForecast` — kết quả dự báo từ ML
- View `vw_RealtimeDashboard`, `vw_ForecastVsActual`
- Indexes cho hiệu năng query

Tiếp tục chạy file `sql/stored_procedures.sql` để tạo stored procedures.

### 4.3 Cho phép kết nối từ máy tính của bạn

```powershell
# Lấy IP hiện tại của bạn
$MY_IP = (Invoke-RestMethod -Uri "https://api.ipify.org")

# Thêm firewall rule (thay tên SQL server cho đúng)
az sql server firewall-rule create `
  --server "<TEN_SQL_SERVER>" `
  --resource-group "rg-sales-analytics" `
  --name "AllowMyIP" `
  --start-ip-address $MY_IP `
  --end-ip-address $MY_IP
```

---

## 5. Cấu Hình Stream Analytics

Stream Analytics là trái tim của hệ thống — nó đọc dữ liệu từ Event Hub, xử lý, và ghi vào SQL.

### 5.1 Tạo Input (Event Hub)

1. Vào **portal.azure.com** → tìm Stream Analytics job vừa tạo (tên dạng `sa-sales-analytics-...`)
2. Menu trái → **Inputs** → **+ Add stream input** → chọn **Event Hub**
3. Điền thông tin:
   - **Input alias**: `SalesInput` ← **PHẢI đúng tên này** (query dùng tên này)
   - **Subscription**: chọn subscription của bạn
   - **Event Hub namespace**: chọn namespace vừa tạo
   - **Event Hub name**: `sales-events`
   - **Consumer group**: `stream-analytics-cg`
   - **Authentication mode**: Connection string
   - **Serialization**: JSON, UTF-8
4. Nhấn **Save**

### 5.2 Tạo Output 1 — SalesTransactions (Azure SQL)

Menu **Outputs** → **+ Add output** → chọn **Azure SQL Database**

| Trường | Giá trị |
|--------|---------|
| Output alias | `SalesTransactionsOutput` |
| Database | `SalesAnalyticsDB` |
| Table | `dbo.SalesTransactions` |
| Authentication mode | SQL Server Authentication |
| Username | `sqladmin` |
| Password | (mật khẩu từ deployment) |

Nhấn **Save**.

### 5.3 Tạo Output 2 — HourlySalesSummary (Azure SQL)

**+ Add output** → **Azure SQL Database**

| Trường | Giá trị |
|--------|---------|
| Output alias | `HourlySalesSummaryOutput` |
| Database | `SalesAnalyticsDB` |
| Table | `dbo.HourlySalesSummary` |
| Authentication | SQL Server Authentication (giống trên) |

Nhấn **Save**.

### 5.4 Tạo Output 3 — Power BI Streaming (tùy chọn)

**+ Add output** → **Power BI**

| Trường | Giá trị |
|--------|---------|
| Output alias | `PowerBIOutput` |
| Dataset name | `SalesRealtimeDataset` |
| Table name | `RealtimeSales` |

Cần đăng nhập bằng tài khoản Power BI có workspace.

### 5.5 Upload Query

1. Menu **Query** → xoá query mặc định
2. Copy toàn bộ nội dung file `stream_analytics/stream_query.sql`
3. Dán vào khung Query
4. Nhấn **Save query**

### 5.6 Khởi động Stream Analytics Job

Menu **Overview** → nhấn **▶ Start** → chọn **Now** → nhấn **Start**

> Chờ ~2–3 phút để job khởi động (trạng thái từ "Starting" → "Running").

---

## 6. Cấu Hình Biến Môi Trường (.env)

File `.env` chứa tất cả connection strings và credentials — **KHÔNG commit file này lên Git**.

```powershell
# Tạo file .env từ mẫu
cp .env.example .env   # nếu có file .env.example
# hoặc tạo mới
notepad .env
```

Điền các giá trị từ file `deployment_output.txt`:

```env
# ── Event Hub ──────────────────────────────────────
EVENT_HUB_CONNECTION_STRING=Endpoint=sb://ehns-sales-XXXXXX.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=XXXXXXXXXX
EVENT_HUB_NAME=sales-events

# ── Azure SQL ───────────────────────────────────────
SQL_SERVER=sql-sales-analytics-XXXXXX.database.windows.net
SQL_DATABASE=SalesAnalyticsDB
SQL_USERNAME=sqladmin
SQL_PASSWORD=P@ssw0rdXXXXXX!

# ── Azure Machine Learning ──────────────────────────
AML_SUBSCRIPTION_ID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
AML_RESOURCE_GROUP=rg-sales-analytics
AML_WORKSPACE_NAME=aml-sales-forecast-XXXXXX
AML_ENDPOINT_URL=https://sales-forecast-endpoint.eastus.inference.ml.azure.com/score
AML_API_KEY=

# ── Blob Storage ────────────────────────────────────
BLOB_CONNECTION_STRING=DefaultEndpointsProtocol=https;AccountName=stsalesXXXXXX;AccountKey=XXXXXXXXXX

# ── Key Vault ───────────────────────────────────────
KEY_VAULT_URI=https://kv-sales-XXXXXX.vault.azure.net/

# ── Power BI ────────────────────────────────────────
POWERBI_WORKSPACE_ID=
POWERBI_DATASET_ID=
POWERBI_ACCESS_TOKEN=

# ── App Insights ────────────────────────────────────
APPLICATIONINSIGHTS_CONNECTION_STRING=InstrumentationKey=XXXXXXXX-...
```

> **Cách lấy giá trị khi quên**: Vào portal.azure.com → chọn resource → tìm mục "Connection strings" hoặc "Keys".

---

## 7. Chạy Data Generator — Test Luồng Dữ Liệu

### 7.1 Kiểm tra kết nối Event Hub

```powershell
# Kích hoạt .env
# Windows — đọc file .env và set env vars
Get-Content .env | ForEach-Object {
    if ($_ -match '^([^#][^=]*)=(.*)$') {
        [System.Environment]::SetEnvironmentVariable($matches[1].Trim(), $matches[2].Trim())
    }
}

# Chạy generator thử (gửi 10 events)
python -c "
from data_generator.sales_generator import generate_batch, build_sales_event
import json
events = generate_batch(10)
print(f'Generated {len(events)} events')
print(json.dumps(events[0], indent=2))
"
```

### 7.2 Chạy generator thật sự (gửi vào Event Hub)

```powershell
# Chạy sales generator — gửi dữ liệu liên tục vào Event Hub
python data_generator/sales_generator.py

# Để dừng: nhấn Ctrl+C
```

Theo dõi log — bạn sẽ thấy:
```
[INFO] Sent batch of 10 events to sales-events
[INFO] Sent batch of 10 events to sales-events
...
```

### 7.3 Kiểm tra dữ liệu đã vào SQL

Sau khi generator chạy ~2 phút và Stream Analytics đang Running, vào **Query Editor** trong Azure Portal:

```sql
-- Kiểm tra SalesTransactions có dữ liệu chưa
SELECT TOP 10 * FROM dbo.SalesTransactions ORDER BY event_time DESC

-- Kiểm tra HourlySalesSummary (cần chờ 5 phút)
SELECT TOP 5 * FROM dbo.HourlySalesSummary ORDER BY window_start DESC
```

Nếu thấy dữ liệu → **luồng hoạt động tốt!**

---

## 8. Train Và Deploy ML Model

### 8.1 Train model cục bộ

```powershell
# Train model (tạo file pkl trong ml/model_output/)
python ml/train_model.py

# Kết quả mong đợi:
# Revenue model R² = 0.86
# Quantity model R² = 0.79
# Files: model_output/revenue_model.pkl, quantity_model.pkl, label_encoders.pkl, model_metadata.json
```

### 8.2 Kiểm tra model hoạt động

```powershell
# Test scoring script cục bộ
python -c "
import json
from ml.score import init, run

init()

test_input = json.dumps({'data': [{
    'hour': 14,
    'day_of_month': 15,
    'month': 3,
    'is_weekend': 0,
    'store_id': 'S01',
    'product_id': 'COKE',
    'category': 'Beverage',
    'temperature': 28.5,
    'is_rainy': 0,
    'holiday': 0
}]})

result = run(test_input)
print('Prediction:', result)
"
```

### 8.3 Deploy model lên Azure ML

```powershell
# Đăng nhập Azure (nếu chưa)
az login

# Cài azure-ai-ml nếu chưa có
pip install azure-ai-ml azure-identity

# Deploy lên Azure ML Online Endpoint
python ml/deploy_model.py \
  --model-dir ml/model_output \
  --endpoint-name sales-forecast-endpoint
```

> **Lần đầu deploy**: mất 10–15 phút để tạo endpoint.

### 8.4 Lấy Endpoint URL và API Key

Sau khi deploy xong:

```powershell
# Lấy endpoint URL
az ml online-endpoint show \
  --name sales-forecast-endpoint \
  --resource-group rg-sales-analytics \
  --workspace-name aml-sales-forecast-XXXXXX \
  --query "scoring_uri"

# Lấy API key
az ml online-endpoint get-credentials \
  --name sales-forecast-endpoint \
  --resource-group rg-sales-analytics \
  --workspace-name aml-sales-forecast-XXXXXX \
  --query "primaryKey"
```

Điền 2 giá trị này vào `.env`:
```env
AML_ENDPOINT_URL=https://sales-forecast-endpoint.eastus.inference.ml.azure.com/score
AML_API_KEY=xxxxxxxxxxxxxxxxxxxxxx
```

### 8.5 So sánh 9 mô hình ML (tùy chọn — cho rubric)

```powershell
# Chạy so sánh 9 mô hình — tạo biểu đồ matplotlib + plotly
python ml/compare_models.py

# Kết quả lưu tại: benchmark_output/ml_comparison/
# - model_comparison_matplotlib.png
# - model_comparison_plotly.html
# - model_comparison_results.json
```

---

## 9. Deploy Azure Functions

Azure Functions dùng để validate mỗi event trước khi vào Event Hub.

### 9.1 Cài Azure Functions Core Tools

```powershell
# Cài qua npm (cần Node.js)
npm install -g azure-functions-core-tools@4 --unsafe-perm true

# Kiểm tra
func --version
```

### 9.2 Test Functions cục bộ

```powershell
cd azure_functions

# Cài dependency riêng của Functions
pip install -r requirements.txt

# Chạy cục bộ
func start

# Output sẽ hiện:
# Functions:
#   ValidateSalesEvent: eventHubTrigger
```

### 9.3 Deploy lên Azure

```powershell
# Deploy — thay tên Function App từ deployment_output.txt
func azure functionapp publish func-sales-validation-XXXXXX --python

# Kiểm tra trạng thái
az functionapp show \
  --name func-sales-validation-XXXXXX \
  --resource-group rg-sales-analytics \
  --query "state"
```

---

## 10. Chạy Web App Cục Bộ

Web app cung cấp giao diện dự báo bán hàng dùng ML model.

### 10.1 Chạy Flask app

```powershell
# Đảm bảo đang ở thư mục gốc dự án
cd c:\Users\Admin\azure-realtime-sales-analytics

# Set environment variables từ .env
# (dùng lệnh ở bước 7.1 để đọc .env)

# Chạy web app
python webapp/app.py

# Output:
# * Running on http://127.0.0.1:5000
```

### 10.2 Test các endpoints

Mở trình duyệt vào http://localhost:5000

| URL | Mô tả |
|-----|-------|
| http://localhost:5000 | Form dự báo doanh thu |
| http://localhost:5000/api/health | Kiểm tra app đang chạy |
| http://localhost:5000/api/predict | REST API dự báo (POST JSON) |

Test API bằng PowerShell:

```powershell
# Test /api/predict
$body = @{
    data = @(@{
        hour = 14
        day_of_month = 15
        month = 3
        is_weekend = 0
        store_id = "S01"
        product_id = "COKE"
        category = "Beverage"
        temperature = 28.5
        is_rainy = 0
        holiday = 0
    })
} | ConvertTo-Json -Depth 3

Invoke-RestMethod -Uri "http://localhost:5000/api/predict" `
  -Method POST `
  -ContentType "application/json" `
  -Body $body
```

---

## 11. Cấu Hình Power BI Dashboard

### 11.1 Điều kiện tiên quyết

- Tài khoản **Power BI Pro** hoặc **Premium Per User** (Miễn phí 60 ngày trial)
- Power BI Desktop: https://powerbi.microsoft.com/desktop
- Dữ liệu đã có trong bảng `SalesTransactions` (bước 7)

### 11.2 Lấy dữ liệu từ Azure SQL

1. Mở **Power BI Desktop**
2. **Get Data** → **Azure** → **Azure SQL Database**
3. Nhập:
   - **Server**: `sql-sales-analytics-XXXXXX.database.windows.net`
   - **Database**: `SalesAnalyticsDB`
4. Chọn **DirectQuery** (cập nhật real-time)
5. Đăng nhập bằng SQL authentication
6. Chọn bảng: `SalesTransactions`, `HourlySalesSummary`, `SalesForecast`
7. Nhấn **Load**

### 11.3 Import DAX Measures

1. Menu **Modeling** → **New Measure**
2. Copy từng measure từ file `powerbi/dax_measures.dax`
3. Các measures quan trọng:
   - `Total Revenue` = `SUMX(SalesTransactions, [units_sold] * [unit_price])`
   - `Revenue Growth %`
   - `Avg Transaction Value`

### 11.4 Cấu hình Row-Level Security (RLS)

1. Menu **Modeling** → **Manage Roles**
2. Tạo 3 roles từ file `powerbi/rls_config.dax`:
   - Role `Manager`: xem theo store_id
   - Role `Director`: xem tất cả
   - Role `Analyst`: xem tổng quan
3. Filter expression dùng `USERPRINCIPALNAME()`

### 11.5 Bố cục mobile

File `powerbi/mobile_layout.json` định nghĩa layout cho điện thoại (360×640).
Trong Power BI Desktop → **View** → **Mobile Layout** → thiết kế lại theo file này.

### 11.6 Publish lên Power BI Service

1. **File** → **Publish** → **Publish to Power BI**
2. Chọn workspace của bạn
3. Vào **app.powerbi.com** → tìm report vừa publish
4. **Settings** → **Scheduled refresh** → bật **Auto Page Refresh** (5 giây)

### 11.7 Đẩy dữ liệu real-time từ SQL vào Power BI Streaming

Sau khi lấy Workspace ID và Dataset ID từ Power BI Service:

```powershell
# Cập nhật .env:
# POWERBI_WORKSPACE_ID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
# POWERBI_DATASET_ID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx

# Lấy Access Token
$token = az account get-access-token --resource https://analysis.windows.net/powerbi/api --query accessToken --output tsv
# Thêm vào .env: POWERBI_ACCESS_TOKEN=<token>

# Chạy push script (tự động every 60 giây)
python powerbi/push_to_powerbi.py
```

---

## 12. Thiết Lập CI/CD Với GitHub Actions

### 12.1 Tạo GitHub repository

```powershell
# Khởi tạo git (nếu chưa)
git init
git add .
git commit -m "Initial commit"

# Tạo repo trên GitHub, sau đó:
git remote add origin https://github.com/<USERNAME>/azure-realtime-sales-analytics.git
git push -u origin main
```

> ⚠️ **QUAN TRỌNG**: Đảm bảo file `.env` và `terraform.tfvars` có trong `.gitignore` trước khi push.

### 12.2 Thêm GitHub Secrets

Vào **GitHub repo** → **Settings** → **Secrets and variables** → **Actions** → **New repository secret**

Thêm các secrets sau:

| Secret Name | Giá trị lấy từ |
|-------------|---------------|
| `AZURE_CREDENTIALS` | Service Principal JSON (xem bước 12.3) |
| `EVENT_HUB_CONNECTION_STRING` | deployment_output.txt |
| `SQL_SERVER` | deployment_output.txt |
| `SQL_DATABASE` | `SalesAnalyticsDB` |
| `SQL_USERNAME` | `sqladmin` |
| `SQL_PASSWORD` | deployment_output.txt |
| `AML_SUBSCRIPTION_ID` | az account show |
| `AML_RESOURCE_GROUP` | `rg-sales-analytics` |
| `AML_WORKSPACE_NAME` | deployment_output.txt |

### 12.3 Tạo Service Principal cho CI/CD

```powershell
# Lấy Subscription ID
$SUB_ID = az account show --query "id" --output tsv

# Tạo Service Principal
az ad sp create-for-rbac `
  --name "sp-sales-analytics-cicd" `
  --role Contributor `
  --scopes "/subscriptions/$SUB_ID" `
  --sdk-auth
```

Copy toàn bộ JSON output và thêm vào GitHub Secrets với tên `AZURE_CREDENTIALS`.

### 12.4 Kiểm tra workflows

Dự án đã có 6 workflows trong `.github/workflows/`:

| File | Trigger | Mục đích |
|------|---------|---------|
| `ci.yml` | Push/PR | Lint + Test tự động |
| `ci-cd-mlops.yml` | Push to main | Terraform + ML train/deploy |
| `deploy-functions.yml` | Manual | Deploy Azure Functions |
| `deploy-simulator.yml` | Manual | Deploy data generator |
| `deploy-ml-endpoint.yml` | Manual | Deploy ML endpoint |
| `drift-detection.yml` | Schedule (daily) | Kiểm tra model drift |

Vào **GitHub** → **Actions** tab → chọn workflow muốn chạy → **Run workflow**.

---

## 13. Kiểm Tra Toàn Bộ Hệ Thống

### 13.1 Checklist kiểm tra end-to-end

Chạy theo thứ tự để kiểm tra toàn bộ pipeline:

```powershell
# ── Bước 1: Test tất cả unit tests ──────────────────
python -m pytest tests/ -v
# Mong đợi: 36 passed

# ── Bước 2: Kiểm tra Event Hub nhận được events ─────
python data_generator/sales_generator.py &
# Để chạy 30 giây, sau đó Ctrl+C

# ── Bước 3: Kiểm tra SQL có dữ liệu ─────────────────
python -c "
import pyodbc, os
conn_str = (
    f'Driver={{ODBC Driver 18 for SQL Server}};'
    f'Server=tcp:{os.getenv(\"SQL_SERVER\")},1433;'
    f'Database={os.getenv(\"SQL_DATABASE\")};'
    f'Uid={os.getenv(\"SQL_USERNAME\")};'
    f'Pwd={os.getenv(\"SQL_PASSWORD\")};'
    f'Encrypt=yes;TrustServerCertificate=no;'
)
conn = pyodbc.connect(conn_str, timeout=30)
cursor = conn.cursor()
cursor.execute('SELECT COUNT(*) FROM dbo.SalesTransactions')
print(f'SalesTransactions rows: {cursor.fetchone()[0]}')
conn.close()
"

# ── Bước 4: Test ML prediction ───────────────────────
python -c "
import json
from ml.score import init, run
init()
r = run(json.dumps({'data': [{'hour':14,'day_of_month':15,'month':3,'is_weekend':0,'store_id':'S01','product_id':'COKE','category':'Beverage','temperature':28.5,'is_rainy':0,'holiday':0}]}))
print('ML OK:', r)
"

# ── Bước 5: Test web app ─────────────────────────────
python webapp/app.py &
python -c "
import requests, json, time
time.sleep(2)
r = requests.get('http://localhost:5000/api/health')
print('Web App health:', r.json())
"

# ── Bước 6: Chạy benchmark ───────────────────────────
python benchmarks/benchmark_latency.py
```

### 13.2 Kiểm tra trên Azure Portal

1. **Event Hubs** → Namespace → **Metrics** → xem "Incoming Messages"
2. **Stream Analytics** → Job → **Monitoring** → xem "Input Events" và "Output Events"
3. **SQL Database** → **Query Performance Insight** → xem query history
4. **Azure ML** → **Endpoints** → xem endpoint status và request logs
5. **Key Vault** → **Monitoring** → xem secret access logs

---

## 14. Xử Lý Sự Cố Thường Gặp

### Lỗi 1: `ModuleNotFoundError: No module named 'azure.eventhub'`

```powershell
pip install azure-eventhub azure-identity
```

### Lỗi 2: `pyodbc.Error: [ODBC Driver 18...]`

- Cài ODBC Driver 18: https://aka.ms/downloadmsodbcsql
- Hoặc đổi driver trong `.env`: `SQL_DRIVER={ODBC Driver 17 for SQL Server}`

### Lỗi 3: Stream Analytics — "No events received"

1. Kiểm tra generator đang chạy: `python data_generator/sales_generator.py`
2. Kiểm tra Event Hub connection string trong Stream Analytics Input
3. Vào Stream Analytics → **Test** query để debug

### Lỗi 4: `ResourceNotFoundError` khi deploy ML

```powershell
# Đảm bảo đã set đúng workspace
az ml workspace show \
  --name <AML_WORKSPACE_NAME> \
  --resource-group rg-sales-analytics
```

### Lỗi 5: SQL firewall block

```powershell
# Thêm IP mới (IP máy tính hay thay đổi)
$MY_IP = (Invoke-RestMethod -Uri "https://api.ipify.org")
az sql server firewall-rule create `
  --server "<SQL_SERVER_NAME>" `
  --resource-group "rg-sales-analytics" `
  --name "AllowMyIP2" `
  --start-ip-address $MY_IP `
  --end-ip-address $MY_IP
```

### Lỗi 6: Terraform `Error: A resource with the ID already exists`

```powershell
# Import resource vào Terraform state
terraform import azurerm_resource_group.main /subscriptions/<SUB>/resourceGroups/rg-sales-analytics-dev
```

### Lỗi 7: `az login` mở browser nhưng không đăng nhập được

```powershell
# Dùng device code login
az login --use-device-code
```

---

## Tổng Kết Kiến Trúc Sau Khi Triển Khai

```
[Python Generators]
       │  sales_generator.py / weather_generator.py
       ▼
[Azure Event Hubs]
       │  sales-events (4 partitions)
       ▼
[Azure Stream Analytics]           ← stream_query.sql
       │  ETL: validate, enrich, aggregate
       ├──── SalesTransactionsOutput ──► [Azure SQL: SalesTransactions]
       ├──── HourlySalesSummaryOutput ──► [Azure SQL: HourlySalesSummary]
       └──── PowerBIOutput ──────────── ► [Power BI Streaming Dataset]
                                                    │
[Azure ML Online Endpoint] ◄── /api/predict ── [Flask Web App]
       │  revenue_model.pkl / quantity_model.pkl
       └──────► [Azure SQL: SalesForecast]
                                                    │
                                               [Power BI]
                                                Dashboard + RLS + Mobile
```

---

## Chi Phí Ước Tính (USD/tháng)

| Dịch vụ | Tier | Chi phí |
|---------|------|---------|
| Event Hubs Standard | 1 TU | ~$10 |
| Azure SQL Database | S0 (10 DTU) | ~$15 |
| Stream Analytics | 1 SU | ~$80 |
| Azure ML (inference) | Standard_DS2_v2 | ~$90 |
| Azure Functions | Consumption | ~$0 (free tier) |
| Blob Storage | LRS | ~$1 |
| Key Vault | Standard | ~$0 (free tier) |
| **Tổng** | | **~$196/tháng** |

> **Tiết kiệm**: Tắt Stream Analytics và ML Endpoint khi không demo (giảm còn ~$26/tháng).
>
> ```powershell
> # Tắt Stream Analytics khi không dùng
> az stream-analytics job stop --name <JOB_NAME> --resource-group rg-sales-analytics
>
> # Bật lại khi cần
> az stream-analytics job start --name <JOB_NAME> --resource-group rg-sales-analytics --output-start-mode JobStartTime
> ```

---

## Dọn Dẹp Sau Khi Demo (Xoá Tất Cả)

```powershell
# XOÁ TOÀN BỘ resource group — KHÔNG THỂ HOÀN TÁC
az group delete --name rg-sales-analytics --yes --no-wait

# Hoặc nếu dùng Terraform
cd terraform
terraform destroy
```
