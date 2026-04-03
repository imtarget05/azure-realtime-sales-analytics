# HƯỚNG DẪN SỬ DỤNG ĐƠN GIẢN — Azure Real-time Sales Analytics
> Hướng dẫn từ đầu đến cuối, đơn giản, dễ hiểu, dễ làm theo

---

## MỤC LỤC
1. [Tổng quan kiến trúc](#1-tổng-quan-kiến-trúc)
2. [Khởi động hệ thống](#2-khởi-động-hệ-thống)
3. [Gửi dữ liệu bán hàng (Event Hub)](#3-gửi-dữ-liệu-bán-hàng-event-hub)
4. [Xem dữ liệu trong SQL Database](#4-xem-dữ-liệu-trong-sql-database)
5. [Stream Analytics — Xử lý realtime](#5-stream-analytics--xử-lý-realtime)
6. [Web App — Dự đoán doanh thu](#6-web-app--dự-đoán-doanh-thu)
7. [Azure ML — Quản lý model](#7-azure-ml--quản-lý-model)
8. [Power BI — Dashboard realtime](#8-power-bi--dashboard-realtime)
9. [Giám sát hệ thống (Monitoring)](#9-giám-sát-hệ-thống-monitoring)
10. [Xử lý lỗi thường gặp](#10-xử-lý-lỗi-thường-gặp)

---

## 1. Tổng quan kiến trúc

```
Data Generator → Event Hub → Stream Analytics → SQL Database → Power BI
                                    ↓
                              Azure Functions (validate)
                                    ↓
                              Azure ML (predict) ← Web App (Flask)
```

| Thành phần | Tên trên Azure | Vai trò |
|---|---|---|
| Event Hub | `evhns-sales-analytics-d9bt2m` | Nhận dữ liệu bán hàng realtime |
| Stream Analytics | `sa-sales-analytics-d9bt2m` | Xử lý, tổng hợp dữ liệu |
| SQL Database | `sql-sales-analytics-d9bt2m` | Lưu trữ kết quả |
| Azure ML | `aml-sales-analytics-d9bt2m` | Training & serving ML model |
| Function App | `func-sales-validation-d9bt2m` | Validate dữ liệu đầu vào |
| Key Vault | `kv-sales-d9bt2m` | Quản lý secrets |
| App Insights | `appi-sales-analytics-d9bt2m` | Monitoring |

---

## 2. Khởi động hệ thống

### Bước 1: Mở terminal
```powershell
cd c:\Users\Admin\azure-realtime-sales-analytics
.venv\Scripts\Activate.ps1
```

### Bước 2: Kiểm tra file `.env` đã có connection strings
```powershell
cat .env
```
File `.env` phải chứa: `EVENTHUB_CONNECTION_STR`, `SQL_SERVER`, `ML_ENDPOINT_URL`, `ML_API_KEY`

### Bước 3: Kiểm tra Stream Analytics đang chạy
```powershell
az stream-analytics job show --name sa-sales-analytics-d9bt2m --resource-group rg-sales-analytics-dev --query "jobState" -o tsv
```
Kết quả phải là **Running**. Nếu là **Stopped**:
```powershell
az stream-analytics job start --name sa-sales-analytics-d9bt2m --resource-group rg-sales-analytics-dev --output-start-mode JobStartTime
```

---

## 3. Gửi dữ liệu bán hàng (Event Hub)

### Cách 1: Chạy data generator (tự động, liên tục)
```powershell
python data_generator/sales_generator.py
```
Nhấn `Ctrl+C` để dừng.

### Cách 2: Gửi 1 event thủ công
```python
python -c "
from azure.eventhub import EventHubProducerClient, EventData
import json, os
from dotenv import load_dotenv
load_dotenv()

producer = EventHubProducerClient.from_connection_string(
    os.getenv('EVENTHUB_CONNECTION_STR'),
    eventhub_name=os.getenv('EVENTHUB_NAME')
)
batch = producer.create_batch()
batch.add(EventData(json.dumps({
    'event_id': 'TEST-001',
    'store_id': 'S01',
    'product_id': 'COKE',
    'category': 'Beverage',
    'units_sold': 5,
    'unit_price': 15.0,
    'revenue': 75.0,
    'temperature': 30.5,
    'event_time': '2025-04-02T14:00:00Z'
})))
producer.send_batch(batch)
producer.close()
print('Event sent!')
"
```

### Kiểm tra dữ liệu đã vào Event Hub
Azure Portal → Event Hub `evhns-sales-analytics-d9bt2m` → `sales-events` → Overview → biểu đồ **Incoming Messages**

---

## 4. Xem dữ liệu trong SQL Database

### Trên Azure Portal
1. Portal → search `sql-sales-analytics-d9bt2m`
2. Chọn `SalesAnalyticsDB` → **Query editor**
3. Đăng nhập: `sqladmin` / `SqlP@ssw0rd2026!`
4. Chạy:
```sql
-- 10 giao dịch gần nhất
SELECT TOP 10 * FROM dbo.SalesTransactions ORDER BY event_time DESC;

-- Tổng hợp theo giờ
SELECT TOP 10 * FROM dbo.HourlySalesSummary ORDER BY window_end DESC;

-- Thống kê
SELECT COUNT(*) AS total, SUM(revenue) AS revenue FROM dbo.SalesTransactions;
```

---

## 5. Stream Analytics — Xử lý realtime

### Luồng dữ liệu
```
Event Hub → Stream Analytics → SQL Database
                               ├── SalesTransactions (chi tiết từng giao dịch)
                               └── HourlySalesSummary (tổng hợp mỗi 5 phút)
```

### Xem trên Portal
Portal → `sa-sales-analytics-d9bt2m` → các tab: Overview, Query, Inputs, Outputs

### Lỗi "Session Expired"
**KHÔNG phải lỗi hệ thống.** Chỉ là Azure Portal hết phiên đăng nhập.
- **Fix**: Nhấn **F5** (refresh trang). Stream Analytics vẫn chạy bình thường.

---

## 6. Web App — Dự đoán doanh thu

### Khởi động
```powershell
python webapp/app.py
```
Mở trình duyệt → **http://localhost:5000**

### Sử dụng
1. Điền form: Giờ, Thứ, Tháng, Cửa hàng, Sản phẩm, Nhiệt độ, Mưa, Ngày lễ
2. Nhấn **Dự đoán**
3. Xem kết quả: doanh thu dự đoán, số lượng, khoảng tin cậy

### API (JSON)
```powershell
curl -X POST http://localhost:5000/api/predict -H "Content-Type: application/json" -d "{\"hour\":14,\"day_of_month\":15,\"month\":6,\"is_weekend\":0,\"store_id\":\"S01\",\"product_id\":\"COKE\",\"category\":\"Beverage\",\"temperature\":30,\"is_rainy\":0,\"holiday\":0}"
```

---

## 7. Azure ML — Quản lý model

### Thông tin model
- **Tên**: `sales-forecast-model` (version 1)
- **Metrics**: Revenue R² = 0.88, Quantity R² = 0.80
- **Endpoint**: `sales-forecast-endpoint` (deployment `blue`)

### Xem trên Azure ML Studio
1. Mở **https://ml.azure.com**
2. Chọn workspace `aml-sales-analytics-d9bt2m`
3. **Models** (menu trái) → `sales-forecast-model` — xem thông tin model
4. **Endpoints** (menu trái) → `sales-forecast-endpoint` — xem endpoint đang serve

> **Lưu ý quan trọng**: Trang **Models** có thể hiện "Not deployed to any endpoints". Đây chỉ là lỗi hiển thị khi deploy bằng SDK. Hãy vào **Endpoints** (menu trái) để kiểm tra — endpoint VẪN hoạt động bình thường.

### Test endpoint
```python
python -c "
import requests, json, os
from dotenv import load_dotenv
load_dotenv()

resp = requests.post(os.getenv('ML_ENDPOINT_URL'),
    json={'data': [{'hour':14,'day_of_month':15,'month':6,'is_weekend':0,
                    'store_id':'S01','product_id':'COKE','category':'Beverage',
                    'temperature':30,'is_rainy':0,'holiday':0}]},
    headers={'Authorization':f'Bearer {os.getenv(\"ML_API_KEY\")}',
             'Content-Type':'application/json'})
result = resp.json()
if isinstance(result, str): result = json.loads(result)
print(json.dumps(result, indent=2))
"
```

### Re-train model
```powershell
python ml/train_model.py
```

---

## 8. Power BI — Dashboard realtime

### Bước 1: Tạo Streaming Dataset
1. Mở **https://app.powerbi.com** → đăng nhập Microsoft 365
2. **Workspaces** → chọn hoặc tạo mới workspace
3. **+ New** → **Streaming dataset** → **API** → **Next**
4. Đặt tên: `SalesRealtimeStream`
5. Thêm các trường:

| Tên trường | Kiểu |
|---|---|
| `timestamp` | DateTime |
| `store_id` | Text |
| `category` | Text |
| `transaction_count` | Number |
| `total_quantity` | Number |
| `total_revenue` | Number |
| `avg_unit_price` | Number |
| `avg_temperature` | Number |

6. Bật **Historic data analysis** → **Create**
7. **Copy Push URL** → lưu lại

### Bước 2: Cấu hình
Thêm vào file `.env`:
```
POWERBI_PUSH_URL=https://api.powerbi.com/beta/...your-push-url...
```

### Bước 3: Push dữ liệu
```powershell
python powerbi/push_to_powerbi.py
```
Script đọc SQL mỗi 10 giây → push lên Power BI.

### Bước 4: Tạo Dashboard
1. Quay lại **app.powerbi.com** → workspace
2. Dataset `SalesRealtimeStream` đã xuất hiện
3. **+ New** → **Dashboard** → tên `Sales Realtime`
4. **+ Add tile** → **Custom Streaming Data** → chọn dataset
5. Tạo tile:
   - **Card**: total_revenue (Sum) — hiện tổng doanh thu
   - **Line chart**: timestamp (axis) + total_revenue (value) — biểu đồ theo thời gian
   - **Bar chart**: store_id (axis) + total_revenue (value) — so sánh cửa hàng
   - **Bar chart**: category (axis) + total_revenue (value) — so sánh danh mục
6. Dashboard tự động cập nhật realtime!

---

## 9. Giám sát hệ thống (Monitoring)

### Kiểm tra nhanh tất cả thành phần
```powershell
# Stream Analytics
az stream-analytics job show --name sa-sales-analytics-d9bt2m -g rg-sales-analytics-dev --query jobState -o tsv

# Function App
az functionapp show --name func-sales-validation-d9bt2m -g rg-sales-analytics-dev --query state -o tsv

# ML Endpoint
az ml online-endpoint show --name sales-forecast-endpoint -g rg-sales-analytics-dev --workspace-name aml-sales-analytics-d9bt2m --query provisioning_state -o tsv

# Web App health
curl http://localhost:5000/api/health
```

### Application Insights
Portal → `appi-sales-analytics-d9bt2m` → Live Metrics, Failures, Performance

---

## 10. Xử lý lỗi thường gặp

| Lỗi | Nguyên nhân | Cách fix |
|---|---|---|
| **Session Expired** (Portal) | Portal hết phiên | Nhấn F5 refresh |
| **'str' object has no attribute 'get'** | Double-serialize JSON | Đã fix trong `webapp/app.py` |
| **ML "Not deployed to endpoints"** | Deploy bằng SDK, UI hiển thị sai | Vào **Endpoints** menu trái |
| **Stream Analytics Stopped** | Job bị dừng | `az stream-analytics job start ...` |
| **SQL không kết nối** | Firewall block IP | Portal → SQL → Networking → thêm IP |
| **Power BI trống** | Chưa tạo streaming dataset | Làm theo Mục 8 ở trên |

---

## QUICK START — Demo trong 5 phút

```powershell
# Terminal 1: Gửi dữ liệu
cd c:\Users\Admin\azure-realtime-sales-analytics
.venv\Scripts\Activate.ps1
python data_generator/sales_generator.py
# Chạy ~30 giây rồi Ctrl+C

# Terminal 2: Web App
python webapp/app.py
# Mở http://localhost:5000 → điền form → Dự đoán

# Terminal 3: Kiểm tra SQL
python -c "import pyodbc; c=pyodbc.connect('Driver={ODBC Driver 18 for SQL Server};Server=tcp:sql-sales-analytics-d9bt2m.database.windows.net,1433;Database=SalesAnalyticsDB;Uid=sqladmin;Pwd=SqlP@ssw0rd2026!;Encrypt=yes;TrustServerCertificate=no;'); print('Transactions:', c.cursor().execute('SELECT COUNT(*) FROM dbo.SalesTransactions').fetchone()[0]); c.close()"
```
