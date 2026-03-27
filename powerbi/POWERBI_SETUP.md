# ============================================================
# Hướng dẫn cấu hình Power BI — Real-time Sales Analytics
# ============================================================

> **Kiến trúc**: Databricks Gold Layer (Delta Lake) → Serverless SQL Warehouse → Power BI DirectQuery
>
> **Files liên quan**:
> - `powerbi/dax_measures.dax` — Tất cả DAX measures (KPI, Time Intelligence, Top-N, Similarity, Access Right)
> - `powerbi/rls_config.dax` — Row-Level Security (Dynamic RLS + USERPRINCIPALNAME + AccessRightAdmin)
> - `powerbi/semantic_model.json` — Data model definition (8 tables, 10 relationships, 3 roles)
> - `powerbi/themes/SalesAnalytics_Dark.json` — JSON Theme (dark, drop shadow, bo góc)
> - `powerbi/dashboard_layout.json` — Desktop layout 5 pages + sidebar navigation
> - `powerbi/mobile_layout.json` — Mobile layout Z-Pattern (360×640, 5 pages)
> - `powerbi/auto_refresh_config.json` — Auto Page Refresh 1-5 giây + Change Detection
> - `databricks/sql/create_access_rights.sql` — Access Right dataset + monthly trend + views

---

## 1. Kết nối DirectQuery từ Power BI → Databricks SQL Warehouse

### Điều kiện tiên quyết
| Component | Yêu cầu |
|-----------|----------|
| Databricks Workspace | Premium / Enterprise tier |
| SQL Warehouse | **Serverless** (khuyến nghị) hoặc Classic |
| Unity Catalog | Đã enable, catalog `sales_analytics` đã tạo |
| Power BI Desktop | Phiên bản **February 2024** trở lên |
| ODBC Driver | [Simba Spark ODBC Driver](https://www.databricks.com/spark/odbc-drivers-download) |

### Bước 1: Lấy thông tin SQL Warehouse
1. Mở **Databricks Workspace** → **SQL Warehouses**
2. Chọn warehouse → tab **Connection Details**
3. Copy:
   - **Server hostname**: `adb-xxxx.xx.azuredatabricks.net`
   - **HTTP path**: `/sql/1.0/warehouses/<warehouse-id>`
   - **Port**: `443`

### Bước 2: Tạo Personal Access Token (PAT) hoặc dùng Azure AD
- **Option A — PAT** (nhanh nhất):
  - Databricks → User Settings → Access Tokens → Generate New Token
  - Lưu token an toàn (không share, không commit vào git)
- **Option B — Azure AD** (khuyến nghị cho Production):
  - Trong Power BI, chọn **Azure Active Directory** authentication
  - User phải có quyền `USE CATALOG`, `USE SCHEMA`, `SELECT` trên Gold tables

### Bước 3: Kết nối trong Power BI Desktop
1. **Get Data** → search "**Databricks**" → chọn **Azure Databricks**
2. Nhập:
   - **Server Hostname**: `adb-xxxx.xx.azuredatabricks.net`
   - **HTTP Path**: `/sql/1.0/warehouses/<warehouse-id>`
   - **Data Connectivity Mode**: ✅ **DirectQuery**
3. Authentication:
   - **Personal Access Token** → paste PAT
   - Hoặc **Azure Active Directory** → Sign in
4. Trong Navigator, mở `sales_analytics` → `gold` → chọn:

| Bảng / View | Mô tả | Mode |
|---|---|---|
| `hourly_summary` | Revenue/orders theo giờ × store × category | DirectQuery |
| `product_summary` | Hiệu suất sản phẩm + viral rate | DirectQuery |
| `customer_summary` | CLV, phân khúc, xếp hạng khách hàng | DirectQuery |
| `similarity_scores` | Similarity bins phân phối | DirectQuery |
| `access_rights` | Chi tiết quyền truy cập (role, dept, status) | DirectQuery |
| `access_rights_monthly` | Tổng hợp access rights theo tháng | DirectQuery |

5. Click **Load** (không Transform — Gold data đã clean)

### Bước 4: Thêm bảng SecurityMapping (Import mode)
1. **Get Data** → **Enter Data** (hoặc query từ `sales_analytics.gold.security_mapping`)
2. Tạo bảng với 4 cột: `user_email`, `display_name`, `role`, `allowed_region`
3. Nhập data theo mẫu trong `powerbi/rls_config.dax`
4. Mode: **Import** (bảng nhỏ, refresh daily)

### Bước 5: Tạo Calendar table (DAX Calculated Table)
1. **Modeling** → **New Table**
2. Paste DAX từ section cuối `powerbi/dax_measures.dax`
3. **Mark as Date Table**: Table Tools → Mark as Date Table → chọn `Date`

---

## 2. Thiết lập Relationships (Data Model)

> Chi tiết đầy đủ: `powerbi/semantic_model.json`

### Star Schema (8 tables, 10 relationships, 3 roles)
```
┌──────────────┐         ┌─────────────────────┐
│  Calendar    │ 1────M  │  hourly_summary     │
│  (Date dim)  │         │  (Fact — Revenue)   │
└──────┬───────┘         └──────────┬──────────┘
       │ 1:M                        │ category (M:M)
       │                 ┌──────────┴──────────┐
       │                 │  product_summary    │
       │                 │  (Dim — Product)    │
┌──────┴───────────┐     └─────────────────────┘
│ SecurityMapping  │
│ (RLS — Import)   │     ┌─────────────────────┐
│ allowed_region ──┼─M:M─│  customer_summary   │
│ allowed_region ──┼─M:M─│  (Dim — Customer)   │
│ allowed_region ──┼─M:M─│  access_rights      │
└──────────────────┘     │  (Fact — Quyền)     │
                         └─────────────────────┘
  Calendar ──1:M──►      ┌─────────────────────┐
                         │ access_rights_monthly│
                         │  (Fact — Trend)     │
                         └─────────────────────┘
                         ┌─────────────────────┐
                         │  similarity_scores  │
                         │  (Fact — NLP bins)  │
                         └─────────────────────┘
```

### Tạo Relationships trong Power BI Desktop
1. **Model view** → kéo thả:
   - `Calendar[Date]` → `hourly_summary[event_date]` (1:M, Both directions)
   - `hourly_summary[category]` → `product_summary[category]` (M:M, Both)
   - `hourly_summary[category]` → `similarity_scores[category]` (M:M, Single)
   - `SecurityMapping[allowed_region]` → `hourly_summary[region]` (M:M, Both, ✅ Apply security filter in both directions)
   - `SecurityMapping[allowed_region]` → `customer_summary[region]` (M:M, Both, ✅ Apply security filter in both directions)
   - `SecurityMapping[allowed_region]` → `access_rights[region]` (M:M, Both, ✅ Apply security filter in both directions)
   - `Calendar[Date]` → `access_rights[granted_date]` (1:M, Single)
   - `Calendar[Date]` → `access_rights_monthly[month_date]` (1:M, Single)

---

## 3. DAX Measures

> File đầy đủ: `powerbi/dax_measures.dax`

### Import measures vào Power BI
**Option A — Thủ công**: Modeling → New Measure → paste từng measure
**Option B — Tabular Editor 3** (khuyến nghị):
1. External Tools → Tabular Editor
2. Tạo Measure Group `_Measures`
3. Paste tất cả measures từ file

### Danh sách measures chính

| Measure | Mô tả |
|---------|--------|
| `Revenue Total` | SUM tổng doanh thu |
| `Order Total` | SUM tổng đơn hàng |
| `Average Order Value` | Revenue / Orders |
| `Revenue Today` | Doanh thu hôm nay |
| `Revenue DoD Growth %` | So sánh hôm nay vs hôm qua |
| `Revenue MTD` | Month-to-Date |
| `Revenue YTD` | Year-to-Date |
| `Revenue MoM Growth %` | So sánh tháng hiện tại vs tháng trước |
| `Revenue Rolling 7D` | Trung bình 7 ngày gần nhất |
| `Top 5 Customer Revenue` | Top 5 khách hàng theo doanh thu |
| `Customer Revenue Rank` | Xếp hạng khách hàng |
| `Similarity Bin Distribution %` | % phân phối similarity bins |
| `Avg Viral Rate` | Tỷ lệ viral trung bình từ ML |
| `Holiday Uplift %` | Tác động ngày lễ |
| `AR Total Users` | Tổng users Active (Access Right) |
| `AR Pending Count` | Requests chờ phê duyệt |
| `AR Monthly Growth` | Tăng trưởng user tháng này |
| `AR Users by Role` | Số users theo role (bar chart) |
| `AR Users by Department` | Phân bổ theo department (stacked column) |

---

## 4. Row-Level Security (RLS)

> File đầy đủ: `powerbi/rls_config.dax`

### Cơ chế Dynamic RLS
```
USERPRINCIPALNAME() → SecurityMapping[user_email]
    → allowed_region → filter hourly_summary[region]
                     → filter customer_summary[region]
```

### Setup
1. **Power BI Desktop** → Modeling → Manage Roles
2. Tạo role **RegionManager**:
   - Table: `SecurityMapping`
   - DAX Filter: `[user_email] = USERPRINCIPALNAME()`
3. Tạo role **Admin**: (không có filter — full access)
4. Tạo role **AccessRightAdmin** (IT admin quản lý quyền):
   - Table: `hourly_summary` → Filter: `FALSE()` (chặn xem sales)
   - Table: `customer_summary` → Filter: `FALSE()`
   - Kết quả: thấy toàn bộ access_rights, không thấy dữ liệu sales
5. Test: Modeling → View As → chọn Role + nhập email

### Assign Members (Power BI Service)
1. Publish report → Workspace → Dataset settings → **Security**
2. Thêm user/group vào roles:

| Role | Members |
|------|---------|
| RegionManager | manager_north@contoso.com, manager_south@contoso.com, analyst_01@contoso.com |
| AccessRightAdmin | sysadmin@contoso.com (xem tất cả access_rights, không xem sales) |
| Admin | admin@contoso.com |

### Kết quả
| User | Thấy dữ liệu |
|------|---------------|
| Manager_North | Chỉ region = North |
| Manager_South | Chỉ region = South |
| Director | North + South + East + West |
| AccessRightAdmin | Toàn bộ access_rights, không thấy sales |
| Admin | Toàn bộ |

---

## 5. Kết nối Real-time Streaming (Optional)

### 5.1 Streaming Dataset (Push API)

### Bước 1: Vào Power BI Service (app.powerbi.com)
- Chọn workspace → **+ New** → **Streaming dataset**

### Bước 2: Chọn **API** và nhập schema:

```json
{
  "name": "SalesRealtimeStream",
  "columns": [
    {"name": "timestamp", "dataType": "DateTime"},
    {"name": "region", "dataType": "String"},
    {"name": "category", "dataType": "String"},
    {"name": "transaction_count", "dataType": "Int64"},
    {"name": "total_quantity", "dataType": "Int64"},
    {"name": "total_revenue", "dataType": "Double"},
    {"name": "avg_order_value", "dataType": "Double"},
    {"name": "avg_rating", "dataType": "Double"}
  ]
}
```

### Bước 4: Copy **Push URL** và dán vào file `powerbi/push_to_powerbi.py`

### 5.2 Stream Analytics → Power BI

### Bước 1: Azure Portal → Stream Analytics Job
- **Output alias**: `PowerBIOutput`
- **Group workspace**: Chọn workspace của bạn
- **Dataset name**: `SalesAnalyticsStream`
- **Table name**: `RealtimeSales`
- **Authentication mode**: User token

---

## 6. UI/UX — Theme, Layout, Mobile, Auto Refresh (Phase 4)

> **Files chi tiết**:
> - `powerbi/themes/SalesAnalytics_Dark.json` — JSON Theme (dark, drop shadow, bo góc 12-16px)
> - `powerbi/dashboard_layout.json` — Desktop layout 4 pages + sidebar navigation
> - `powerbi/mobile_layout.json` — Mobile layout Z-Pattern (360×640)
> - `powerbi/auto_refresh_config.json` — Auto Page Refresh 1-5 giây + Change Detection

### 6.1 Import Theme
1. Power BI Desktop → **View** → **Themes** → **Browse for themes**
2. Chọn file `powerbi/themes/SalesAnalytics_Dark.json`
3. Theme áp dụng toàn bộ report: background dark (#0F172A), cards bo góc 12px + đổ bóng, accent teal (#00D4AA)

### 6.2 Dashboard Pages (Desktop 1280×720)

| Page | Tiêu đề | KPI Cards | Charts chính |
|------|---------|-----------|-------------|
| 01_Overview | Tổng quan thời gian thực | Revenue, Orders, AOV, Active Customers | Line (hourly trend), Bar (region), Donut (category), Top 5 Table |
| 02_Products | Sản phẩm & ML | Product Revenue, Unique Buyers | Treemap (product), Gauge (viral rate), Column (similarity bins), Scatter (sim vs viral) |
| 03_Customers | Khách hàng & CLV | Active Customers, Revenue/Customer, Top 5 Revenue | Customer table (rank + CLV tier), Donut (segment + CLV tier), Bar (payment method) |
| 04_Anomaly | Cảnh báo & Tác động | Holiday Uplift, Discount Rate, vs Target | Column (holiday vs non-holiday), Scatter (temperature), Line (rolling 7D vs 30D) |
| 05_AccessRights | Quyền truy cập | Total Users, Pending, Growth, Departments | Horizontal Bar (top roles), Line (monthly trend), Stacked Column (status×dept), Detail Table |

### 6.3 Navigation Sidebar
- **Desktop**: Sidebar 80px bên trái, 5 icon buttons (📊🏷️👥⚡🔐), chuyển page qua Bookmarks
- **Mobile**: Topbar 48px, 5 tabs ngang thay cho sidebar
- Chi tiết vị trí & cách tạo: xem `powerbi/dashboard_layout.json` → `navigation.howToCreate`

### 6.4 Mobile Layout (Z-Pattern)
- Canvas 360×640, vertical scroll
- KPI cards ở đầu (mắt nhìn đầu tiên) → Chart trend ở giữa → Detail table ở cuối
- Touch target ≥ 48px chiều cao
- Font: KPI value ≥ 20pt, label ≥ 10pt
- Chi tiết: `powerbi/mobile_layout.json`

### 6.5 Auto Page Refresh

| Page | Interval | Mode | Lý do |
|------|----------|------|-------|
| 01_Overview | **5 giây** | Change Detection | Real-time monitoring — detect revenue changes |
| 02_Products | 30 giây | Fixed interval | Product data ít thay đổi |
| 03_Customers | 60 giây | Fixed interval | CLV aggregation |
| 04_Anomaly | **10 giây** | Change Detection | Cần phát hiện anomaly nhanh |
| 05_AccessRights | 60 giây | Fixed interval | Access data ít thay đổi |

**Yêu cầu License**: Premium / Premium Per User / Fabric F2+ (Pro chỉ hỗ trợ ≥ 30 phút).

Setup:
1. Power BI Desktop → Format pane → Page → Auto page refresh → ON
2. Change Detection: chọn measure `[Revenue Total]`, polling 5 giây
3. Admin Portal: Capacity → Workloads → Datasets → minimum interval = 1 giây

Chi tiết: `powerbi/auto_refresh_config.json`

### 6.6 Design Tokens (Theme)

| Token | Giá trị | Sử dụng |
|-------|---------|---------|
| Background | `#0F172A` | Page canvas |
| Card BG | `#1E293B` | Visual containers |
| Border | `#334155` | Visual borders |
| Border Radius | `12px` (card: `16px`) | Bo góc |
| Drop Shadow | offset 4px, blur 12px, #000 60% | Đổ bóng cards |
| Accent | `#00D4AA` | Revenue, primary actions |
| Secondary | `#3B82F6` | Orders, bars |
| Warning | `#F59E0B` | AOV, neutral |
| Danger | `#EF4444` | Anomaly, bad |
| Text Primary | `#F8FAFC` | KPI values |
| Text Secondary | `#94A3B8` | Labels, axis |
| Button Radius | `24px` | Navigation buttons |

---

## 7. Publish & Chia sẻ

### Publish từ Power BI Desktop
1. Click **Publish** → chọn workspace
2. Đợi upload hoàn tất

### Chia sẻ Dashboard
1. Power BI Service → Workspace → Report/Dashboard
2. Click **Share** → nhập email người nhận
3. Chọn quyền: **Can view** hoặc **Can edit**
4. Với RLS: người nhận chỉ thấy dữ liệu thuộc role được gán

### Embed (tùy chọn)
- **Publish to Web**: cho demo nội bộ (không hỗ trợ RLS)
- **Embed API**: tích hợp vào web app (`webapp/`)
- **Power BI Embedded** (Azure): embed cho khách hàng bên ngoài

---

## Tổng hợp Files

| File | Nội dung |
|------|----------|
| `powerbi/POWERBI_SETUP.md` | Hướng dẫn tổng thể (file này) |
| `powerbi/dax_measures.dax` | 30+ DAX measures: KPI, Time Intelligence, Top-N, Similarity |
| `powerbi/rls_config.dax` | Dynamic RLS (USERPRINCIPALNAME + SecurityMapping) |
| `powerbi/semantic_model.json` | Data model: 8 tables, 10 relationships, 3 roles |
| `powerbi/themes/SalesAnalytics_Dark.json` | Dark theme: colors, fonts, drop shadow, bo góc |
| `powerbi/dashboard_layout.json` | Desktop layout: 5 pages + sidebar navigation |
| `powerbi/mobile_layout.json` | Mobile layout: Z-Pattern, topbar, 5 pages |
| `powerbi/auto_refresh_config.json` | Auto Page Refresh: intervals, Change Detection, license info |
| `powerbi/push_to_powerbi.py` | Push API streaming data |
| `databricks/sql/create_security_mapping.sql` | Tạo bảng SecurityMapping trên Unity Catalog |
| `databricks/sql/create_access_rights.sql` | Access Right dataset + monthly trend + views |

