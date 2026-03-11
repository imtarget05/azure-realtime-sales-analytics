# ============================================================
# Hướng dẫn cấu hình Power BI cho hệ thống phân tích bán hàng
# ============================================================

## 1. Kết nối Power BI với Azure SQL Database

### Bước 1: Mở Power BI Desktop
- Chọn **Get Data** → **Azure** → **Azure SQL Database**

### Bước 2: Nhập thông tin kết nối
- **Server**: `<your-server>.database.windows.net`
- **Database**: `SalesAnalyticsDB`
- **Data Connectivity Mode**: `DirectQuery` (cho real-time) hoặc `Import`

### Bước 3: Xác thực
- Chọn **Database** authentication
- Nhập username và password

### Bước 4: Chọn bảng/view
Chọn các bảng và views sau:
- `vw_SalesOverview` - Tổng quan bán hàng
- `vw_ForecastVsActual` - So sánh dự đoán vs thực tế
- `vw_RealtimeDashboard` - Dashboard thời gian thực
- `HourlySalesSummary` - Tổng hợp theo giờ
- `ProductSalesSummary` - Tổng hợp theo sản phẩm
- `SalesAlerts` - Cảnh báo bất thường
- `WeatherData` - Dữ liệu thời tiết
- `StockData` - Dữ liệu chứng khoán

---

## 2. Tạo Streaming Dataset (Real-time)

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

### Bước 3: Bật **Historic data analysis** → Tạo
### Bước 4: Copy **Push URL** và dán vào file `powerbi/push_to_powerbi.py`

---

## 3. Kết nối Stream Analytics Output đến Power BI

### Bước 1: Azure Portal → Stream Analytics Job
- Vào **Outputs** → **+ Add** → **Power BI**

### Bước 2: Cấu hình
- **Output alias**: `PowerBIOutput`
- **Group workspace**: Chọn workspace của bạn
- **Dataset name**: `SalesAnalyticsStream`
- **Table name**: `RealtimeSales`
- **Authentication mode**: User token

---

## 4. Các Dashboard được đề xuất

### Dashboard 1: Tổng quan thời gian thực
- **Card**: Tổng doanh thu hôm nay, Số giao dịch, Giá trị đơn trung bình
- **Line Chart**: Doanh thu theo thời gian (mỗi phút)
- **Bar Chart**: Doanh thu theo vùng
- **Pie Chart**: Phân bổ theo danh mục sản phẩm
- **Table**: 10 giao dịch gần nhất

### Dashboard 2: Phân tích sản phẩm
- **Treemap**: Doanh thu theo sản phẩm
- **Scatter Chart**: Giá vs Số lượng bán
- **Bar Chart**: Top 10 sản phẩm bán chạy
- **Gauge**: Rating trung bình

### Dashboard 3: Dự đoán nhu cầu
- **Line Chart**: Dự đoán vs Thực tế (với confidence interval)
- **Heatmap**: Dự đoán doanh thu theo giờ/vùng
- **KPI**: Sai số dự đoán (MAPE)
- **Table**: Chi tiết dự đoán 24h tới

### Dashboard 4: Cảnh báo & Bất thường
- **Table**: Danh sách cảnh báo mới nhất
- **Card**: Số cảnh báo Critical/High
- **Line Chart**: Xu hướng cảnh báo theo thời gian
- **Map**: Cảnh báo theo vùng

### Dashboard 5: Tác động thời tiết
- **Scatter Chart**: Nhiệt độ vs Doanh thu
- **Line Chart**: Doanh thu khi mưa vs không mưa
- **Bar Chart**: Doanh thu theo điều kiện thời tiết

---

## 5. Tự động refresh

### DirectQuery Mode:
- Dữ liệu tự động refresh khi người dùng tương tác

### Import Mode:
- Cấu hình **Scheduled Refresh**: 
  - Power BI Service → Dataset → Settings → Scheduled refresh
  - Đặt refresh mỗi 30 phút hoặc 1 giờ

### Streaming Dataset:
- Sử dụng script `powerbi/push_to_powerbi.py` hoặc Stream Analytics Output
- Dữ liệu update real-time (< 1 giây delay)

---

## 6. DAX Measures hữu ích

```dax
// Tổng doanh thu
Total Revenue = SUM(vw_SalesOverview[total_revenue])

// Doanh thu hôm nay
Today Revenue = 
CALCULATE(
    SUM(vw_SalesOverview[total_revenue]),
    vw_SalesOverview[sale_date] = TODAY()
)

// Tăng trưởng so với hôm qua
Revenue Growth = 
VAR TodayRev = [Today Revenue]
VAR YesterdayRev = 
    CALCULATE(
        SUM(vw_SalesOverview[total_revenue]),
        vw_SalesOverview[sale_date] = TODAY() - 1
    )
RETURN 
    DIVIDE(TodayRev - YesterdayRev, YesterdayRev, 0)

// Dự đoán chính xác (MAPE)
Forecast MAPE = 
AVERAGE(
    ABS(
        DIVIDE(
            vw_ForecastVsActual[actual_revenue] - vw_ForecastVsActual[predicted_revenue],
            vw_ForecastVsActual[actual_revenue],
            0
        )
    )
)

// Số giao dịch hôm nay
Today Transactions = 
CALCULATE(
    COUNTROWS(vw_SalesOverview),
    vw_SalesOverview[sale_date] = TODAY()
)

// Giá trị đơn trung bình
Avg Order Value = 
DIVIDE([Total Revenue], COUNTROWS(vw_SalesOverview), 0)

// Tổng số lượng bán
Total Quantity = SUM(vw_SalesOverview[total_quantity])
```

---

## 7. Row-Level Security (RLS) - Bảo mật cấp hàng

Row-Level Security cho phép giới hạn dữ liệu hiển thị theo vùng/vai trò của người dùng.

### Bước 1: Tạo Role trong Power BI Desktop

1. Mở Power BI Desktop → Tab **Modeling** → **Manage Roles**
2. Tạo các role sau:

| Role Name       | Table                | DAX Filter Expression                              |
|-----------------|----------------------|----------------------------------------------------|
| `NorthManager`  | `vw_SalesOverview`   | `[region] = "North"`                               |
| `SouthManager`  | `vw_SalesOverview`   | `[region] = "South"`                               |
| `EastManager`   | `vw_SalesOverview`   | `[region] = "East"`                                |
| `WestManager`   | `vw_SalesOverview`   | `[region] = "West"`                                |
| `CentralManager`| `vw_SalesOverview`   | `[region] = "Central"`                             |
| `AllRegions`    | `vw_SalesOverview`   | *(không có filter - xem tất cả)*                   |

3. Với mỗi role, nhập DAX filter:
```dax
// Ví dụ cho NorthManager
[region] = "North"
```

4. Click **Save**

### Bước 2: Kiểm tra RLS trong Desktop

1. Tab **Modeling** → **View as Roles**
2. Chọn role cần test (ví dụ: `NorthManager`)
3. Xác minh dashboard chỉ hiển thị dữ liệu vùng North
4. Thử từng role để confirm filter hoạt động đúng

### Bước 3: Gán RLS trong Power BI Service

1. Publish report lên Power BI Service
2. Vào **Workspace** → chọn **Dataset** → **Security** (biểu tượng ...)
3. Với mỗi role:
   - Chọn role (ví dụ: `NorthManager`)
   - Thêm email/group Azure AD của người quản lý vùng North
   - Click **Add** → **Save**

### Bước 4: Dynamic RLS (nâng cao)

Thay vì tạo nhiều role tĩnh, dùng Dynamic RLS với bảng `UserRegionMapping`:

1. Tạo bảng SQL:
```sql
CREATE TABLE UserRegionMapping (
    user_email NVARCHAR(200),
    region NVARCHAR(50)
);

INSERT INTO UserRegionMapping VALUES
('manager.north@company.com', 'North'),
('manager.south@company.com', 'South'),
('manager.east@company.com', 'East'),
('manager.west@company.com', 'West'),
('manager.central@company.com', 'Central'),
('director@company.com', 'North'),
('director@company.com', 'South'),
('director@company.com', 'East'),
('director@company.com', 'West'),
('director@company.com', 'Central');
```

2. Import bảng `UserRegionMapping` vào Power BI
3. Tạo relationship: `UserRegionMapping[region]` → `vw_SalesOverview[region]`
4. Tạo single role `DynamicRLS`:
```dax
[user_email] = USERPRINCIPALNAME()
```

5. Kết quả: Mỗi user chỉ thấy dữ liệu của region được gán cho email của họ

---

## 8. Mobile Layout - Bố cục cho thiết bị di động

### Bước 1: Bật Mobile Layout View

1. Mở Power BI Desktop → Tab **View** → **Mobile layout**
2. Giao diện chuyển sang chế độ thiết kế mobile (portrait phone)

### Bước 2: Thiết kế trang Mobile Dashboard

Kéo thả các visual từ desktop layout sang mobile canvas. Thứ tự đề xuất từ trên xuống:

```
┌─────────────────────────┐
│  KPI Cards (3 cái)      │
│  ┌───┐ ┌───┐ ┌───┐     │
│  │Rev│ │Txn│ │AOV│     │
│  └───┘ └───┘ └───┘     │
├─────────────────────────┤
│  Line Chart             │
│  Doanh thu theo giờ     │
│                         │
├─────────────────────────┤
│  Bar Chart              │
│  Doanh thu theo vùng    │
│                         │
├─────────────────────────┤
│  Donut Chart            │
│  Phân bổ danh mục       │
│                         │
├─────────────────────────┤
│  Table                  │
│  Top 5 sản phẩm         │
│                         │
├─────────────────────────┤
│  Card                   │
│  Forecast MAPE          │
│                         │
└─────────────────────────┘
```

### Bước 3: Tối ưu cho Mobile

- **Font size**: Tăng font chữ cho card KPI (>= 24pt)
- **Bỏ legend** nếu chiếm quá nhiều không gian
- **Giảm số cột** trong table (chỉ hiển thị 2-3 cột quan trọng)
- **Tắt tooltip** phức tạp
- **Dùng conditional formatting** thay vì quá nhiều visual

### Bước 4: Tạo thêm Mobile Pages

| Trang  | Nội dung                                    |
|--------|---------------------------------------------|
| Page 1 | Tổng quan: KPI + Line chart + Bar chart     |
| Page 2 | Sản phẩm: Treemap + Top 10 table            |
| Page 3 | Dự đoán: Forecast vs Actual + MAPE card     |
| Page 4 | Cảnh báo: Alert table + Status cards        |

### Bước 5: Publish & Kiểm tra trên điện thoại

1. **Publish** report lên Power BI Service
2. Cài app **Power BI** trên iOS/Android  
3. Đăng nhập → Mở workspace → Chọn report
4. Report sẽ tự động hiển thị mobile layout khi xem trên điện thoại
5. Kiểm tra:
   - Cuộn mượt không?
   - KPI card đọc được không?
   - Chart có bị cắt không?
   - Filter/Slicer dùng được trên mobile không?

### Bước 6: Alerts trên Mobile

1. Power BI Service → Dashboard → Pin một card (ví dụ: Total Revenue)
2. Click **...** trên card → **Manage alerts**
3. Thiết lập alert:
   - **Condition**: Above / Below threshold
   - **Threshold**: Ví dụ: Revenue < $1000 (cảnh báo doanh thu thấp)
   - **Notification**: Email + Push notification trên app
   - **Frequency**: At most every hour

---

## 9. Publish & Chia sẻ

### Publish từ Power BI Desktop
1. Click **Publish** → chọn workspace
2. Đợi upload hoàn tất

### Chia sẻ Dashboard
1. Power BI Service → Workspace → Report/Dashboard
2. Click **Share** → nhập email người nhận
3. Chọn quyền: **Can view** hoặc **Can edit**
4. Với RLS: người nhận chỉ thấy dữ liệu thuộc role được gán

### Embed (tùy chọn)
- Sử dụng **Publish to Web** cho nội bộ (không RLS)
- Sử dụng **Embed API** cho tích hợp vào web app
