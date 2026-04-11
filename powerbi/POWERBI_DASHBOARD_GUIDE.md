# Power BI Dashboard Setup Guide — SQL Server DirectQuery

## ⚡ KHẨN CẤP: Sửa lỗi Dashboard hiện tại

### Lỗi 1: Cards hiện `--` hoặc `(Blank)` (Average Order Value, Order Total)
**Nguyên nhân**: Measures chưa được tạo hoặc DAX formula lỗi.
**Fix ngay**:
1. Vào **Modeling** → **New Measure** → Paste từng measure:
```dax
Revenue Total = SUM(SalesTransactions[revenue])
```
```dax
Order Total = COUNTROWS(SalesTransactions)
```
```dax
Average Order Value = DIVIDE([Revenue Total], [Order Total], 0)
```
```dax
Units Sold = SUM(SalesTransactions[units_sold])
```
2. Click vào card `Average Order Value` → trong **Values** well, xóa field cũ → kéo measure `[Average Order Value]` mới vào
3. Tương tự cho `Order Total`

### Lỗi 2: DoD Growth = 106.53% → Đã fix xuống 18.3%
**Fix**: Click **Refresh** trên ribbon → data mới sẽ load từ SQL (view tự cập nhật).

### Lỗi 3: Region slicer hiện `(Blank)`
**Nguyên nhân**: Chưa tạo relationship `SalesTransactions[store_id] → StoreRegions[store_id]`.
**Fix**:
1. Chuyển sang **Model View** (icon bên trái)
2. Kiểm tra có đường nối từ `SalesTransactions` → `StoreRegions` không
3. Nếu chưa: kéo `SalesTransactions[store_id]` → thả lên `StoreRegions[store_id]`
4. Direction: **Single**, Cardinality: **Many-to-One**
5. Quay lại Report View → slicer sẽ hết `(Blank)`

### Lỗi 4: Product Revenue card = (Blank) khi View as Manager_North
**Nguyên nhân**: Card dùng `SUM(SalesTransactions[revenue])` nhưng thiếu relationship → RLS filter `StoreRegions[region]="Miền Bắc"` không propagate sang SalesTransactions.
**Fix**: Tạo relationship (như Lỗi 3 ở trên), sau đó RLS sẽ tự lọc.

### Lỗi 5: Product table trống / cùng giá trị
**Fix**: Xem **Bước 3** bên dưới.

---

## Kết nối tới SQL Server

1. **Get Data** → **SQL Server**
2. Server: `sql-sales-analytics-d9bt2m.database.windows.net`
3. Database: `SalesAnalyticsDB`
4. Mode: **DirectQuery** (real-time data)
5. Login: `sqladmin` / `SqlP@ssw0rd2026!`

---

## Bước 1: Import Tables & Views

Import tất cả bảng/view sau (chọn trong Navigator):

### Tables (7 bảng)
| Table | Rows | Mô tả |
|-------|------|--------|
| `SalesTransactions` | ~89K | Giao dịch bán hàng |
| `SalesForecast` | ~5K | Dự báo ML |
| `SalesAlerts` | ~4K | Cảnh báo bất thường |
| `ModelRegistry` | 5 | Model versions |
| `Products` | 35 | ★ Dimension sản phẩm |
| `StoreRegions` | 3 | Dimension vùng miền |
| `SecurityMapping` | 5 | RLS role mappings |
| `AccessAudit` | ~180 | Audit trail |
| `LatencyBenchmark` | 5 | Load test results |

### Views (5 views)
| View | Mô tả |
|------|--------|
| `vw_DoDGrowthOverall` | DoD growth tổng |
| `vw_DoDGrowth` | DoD growth theo vùng |
| `vw_ProductSales` | Revenue theo product+region |
| `vw_PerformanceMetrics` | Throughput/latency theo giờ |
| `vw_ForecastAccuracy` | Forecast vs actual |

---

## Bước 2: Tạo Relationships (Model View)

Vào **Model View** → kéo thả hoặc **Manage Relationships**:

```
SalesTransactions[store_id]   →  StoreRegions[store_id]     (Many:1, Single)
SalesTransactions[product_id] →  Products[product_id]        (Many:1, Single) ★
SalesAlerts[store_id]         →  StoreRegions[store_id]      (Many:1, Single)
SalesForecast[store_id]       →  StoreRegions[store_id]      (Many:1, Single)
SalesForecast[product_id]     →  Products[product_id]        (Many:1, Single) ★
```

> **Quan trọng**: Relationship `SalesTransactions → Products` là bắt buộc. Nếu thiếu, Product table sẽ hiện giá trị giống nhau cho mọi product.

---

## Bước 3: Sửa Product Table Visual (BUG HIỆN TẠI)

**Vấn đề**: Product table hiện cho tất cả products cùng giá trị ($2.12M).

**Nguyên nhân**: Visual đang dùng `SalesTransactions[product_id]` hoặc `SalesForecast[product_id]` thay vì `Products[product_name]`.

**Cách sửa**:
1. Click vào bảng Product trong dashboard
2. Trong **Fields** panel bên phải, xóa field `product_id` hiện tại
3. Kéo `Products[product_name]` vào **Rows**
4. Kéo `Products[category]` vào **Rows** (dưới product_name)
5. Kéo các measures sau vào **Values**:
   - `Revenue Total` (= SUM(SalesTransactions[revenue]))
   - `Units Sold` (= SUM(SalesTransactions[units_sold]))
   - `Product Transaction Count` (= COUNTROWS(SalesTransactions))

**Kết quả mong đợi** (Top 5):
| Product | Category | Revenue |
|---------|----------|--------|
| Product P001 | Electronics | $141,739 |
| Product P009 | Home | $140,036 |
| Product P002 | Electronics | $120,705 |
| Product P013 | Accessories | $118,663 |
| Product P007 | Clothing | $112,346 |

---

## Bước 4: Tạo DAX Measures

Copy tất cả measures từ file `powerbi/dax_measures_sqlserver.dax`:

### Core KPIs
```dax
Revenue Total = SUM(SalesTransactions[revenue])
Order Total = COUNTROWS(SalesTransactions)
Average Order Value = DIVIDE([Revenue Total], [Order Total], 0)
Units Sold = SUM(SalesTransactions[units_sold])
```

### DoD Growth
```dax
Revenue DoD Growth % =
    IF(
        HASONEVALUE(StoreRegions[region]),
        CALCULATE(MAX(vw_DoDGrowth[dod_growth_pct])/100, vw_DoDGrowth[date_rank]=1,
            TREATAS(VALUES(StoreRegions[region]), vw_DoDGrowth[region])),
        CALCULATE(MAX(vw_DoDGrowthOverall[dod_growth_pct])/100, vw_DoDGrowthOverall[date_rank]=1)
    )
```

### Forecast
```dax
Forecast Accuracy = CALCULATE(MAX(ModelRegistry[r2_score]), ModelRegistry[status]="production")
```

---

## Bước 5: Thiết kế 3 Trang Dashboard

### Page 1: Overview (Tổng quan)
| Visual | Type | Fields |
|--------|------|--------|
| Revenue Total | Card | `[Revenue Total]` |
| DoD Growth | Card | `[Revenue DoD Growth %]` (format %) |
| Avg Order Value | Card | `[Average Order Value]` |
| Order Total | Card | `[Order Total]` |
| Revenue by Hour | Line Chart | Axis: `DATEPART(HOUR, event_time)`, Values: `revenue` |
| Revenue by Store | Bar Chart | Axis: `StoreRegions[store_id]`, Values: `[Revenue Total]` |
| Region Slicer | Slicer | `StoreRegions[region]` |
| **Product Table** | Table | Rows: `Products[product_name]`, `Products[category]` Values: `[Revenue Total]`, `[Units Sold]` |
| Revenue by Region | Stacked Bar | Axis: `StoreRegions[region]`, Values: `[Revenue Total]` |

### Page 2: Alerts & Monitoring
| Visual | Type | Fields |
|--------|------|--------|
| Alert Count | Card | `COUNTROWS(SalesAlerts)` |
| Alert High Value | Card | `[Alert High Value Count]` |
| Alert Table | Table | `SalesAlerts[alert_time]`, `[store_id]`, `[type]`, `[value]`, `[severity]` |
| Alert Timeline | Line Chart | Axis: `SalesAlerts[alert_time]` (by hour), Values: `COUNT(id)` |

### Page 3: Forecast & ML
| Visual | Type | Fields |
|--------|------|--------|
| Forecast Accuracy | Card | `[Forecast Accuracy]` (format: 0.00) |
| Predicted vs Actual | Clustered Bar | Axis: `SalesForecast[store_id]`, Values: `[Predicted Revenue]` + actual |
| MAE by Store | Bar Chart | Axis: `store_id`, Values: `ABS(predicted - actual)` |
| Model Version History | Table | `ModelRegistry[model_version]`, `[r2_score]`, `[mae]`, `[status]` |

---

## Bước 6: Setup RLS (Row-Level Security) — Scenario 4

### Tạo 4 Roles — Hướng dẫn chi tiết

#### **Role 1: Manager_North (Quản lý Miền Bắc)**

1. Vào **Home** → **Manage Roles** (hoặc **Modeling** → **Manage Roles**)
2. Click **New** → Đặt tên: `Manager_North`
3. Chọn bảng **StoreRegions** trong danh sách
4. Click vào ô **DAX filter** → Paste:
   ```dax
   [region] = "Miền Bắc"
   ```
5. Click **Save** → Xong role 1

#### **Role 2: Manager_South (Quản lý Miền Nam)**

1. Click **New** → Đặt tên: `Manager_South`
2. Chọn bảng **StoreRegions**
3. DAX filter:
   ```dax
   [region] = "Miền Nam"
   ```
4. Click **Save**

#### **Role 3: Manager_Central (Quản lý Miền Trung)**

1. Click **New** → Đặt tên: `Manager_Central`
2. Chọn bảng **StoreRegions**
3. DAX filter:
   ```dax
   [region] = "Miền Trung"
   ```
4. Click **Save**

#### **Role 4: Director (Nhìn thấy tất cả dữ liệu)**

1. Click **New** → Đặt tên: `Director`
2. **KHÔNG cần chọn bảng/table nào**
3. **KHÔNG cần viết DAX filter** (để trống = thấy tất cả)
4. Click **Save**

---

### Kết quả mỗi Role

| Role | StoreRegions[region] | Stores | Revenue |
|------|----------------------|--------|---------|
| **Manager_North** | Miền Bắc | S02 | ~$630K |
| **Manager_South** | Miền Nam | S01 | ~$788K |
| **Manager_Central** | Miền Trung | S03 | ~$701K |
| **Director** | (Tất cả) | S01+S02+S03 | ~$2.12M |

---

### Test RLS — Từng Role

#### **Test Manager_North:**
1. **Modeling** → **View As Roles**
2. Chọn **Manager_North**
3. Dashboard sẽ tự động **filter** → chỉ hiện:
   - **Region slicer**: chỉ có "Miền Bắc" (các option khác mất)
   - **Revenue**: ~$630K (thay vì $2.12M)
   - **Revenue by Store**: chỉ S02 = $630K
   - **Product table**: chỉ sản phẩm của Miền Bắc
   - **Alerts**: chỉ alerts từ S02

#### **Test Manager_South:**
1. **View As Roles** → Chọn **Manager_South**
2. Revenue → ~$788K (S01 HCM)
3. Tất cả charts tự động filter theo Miền Nam

#### **Test Director:**
1. **View As Roles** → Chọn **Director**
2. Revenue → $2.12M (tất cả stores)
3. Thấy tất cả dữ liệu, không bị filter
4. Region slicer hiện 3 options: Bắc, Trung, Nam

---

### Tắt View As Roles (quay lại bình thường)

1. **Modeling** → **View As Roles** → Chọn **(None)**
2. Dashboard quay lại bình thường, hiện tất cả dữ liệu

---

### Publish RLS Rules

Khi publish lên **Power BI Service**:

1. **File** → **Publish** (lên Power BI Service)
2. Trong Power BI Service web, vào **Datasets** → chọn dataset
3. **Security** → Add users to roles:
   - `Manager_North` → Thêm: `manager_north@company.com`
   - `Manager_South` → Thêm: `manager_south@company.com`
   - `Manager_Central` → Thêm: `manager_central@company.com`
   - `Director` → Thêm: `director@company.com`, `analyst@company.com`

4. Clink **Save** → Hoàn tất
5. Mỗi user sẽ chỉ thấy dữ liệu tương ứng với role của họ

---

### Troubleshooting RLS

**Q: Role không hoạt động? Tất cả users vẫn thấy đầy đủ dữ liệu**
- A: Kiểm tra DAX filter có syntax đúng không (ngoặc kép, tên cột đúng)
- A: Kiểm tra relationships: `SalesTransactions[store_id] → StoreRegions[store_id]` phải tồn tại
- A: Trong Power BI Service, kiểm tra user đã assign vào role chưa

**Q: DAX filter báo lỗi "Tên bảng không hợp lệ"?**
- A: Kiểm tra tên bảng: `StoreRegions` (không phải `Store Regions`)
- A: Kiểm tra tên cột: `region` (không phải `Region` hoặc `store_region`)

**Q: Muốn filter trên SalesTransactions trực tiếp thay vì StoreRegions?**
- A: Có thể, nhưng khó hơn. DAX sẽ là:
  ```dax
  RELATED(StoreRegions[region]) = "Miền Bắc"
  ```
  (dùng RELATED để follow relationship)

---

### SQL Verification — Check data trước khi test Power BI

Chạy các query này trong **SQL Server** để verify mỗi role sẽ thấy bao nhiêu data:

#### **Manager_North (Miền Bắc — S02)**
```sql
SELECT 
    r.region,
    r.store_id,
    COUNT(*) as tx_count,
    SUM(revenue) as total_revenue,
    COUNT(DISTINCT product_id) as distinct_products
FROM SalesTransactions st
INNER JOIN StoreRegions r ON st.store_id = r.store_id
WHERE r.region = N'Miền Bắc'
GROUP BY r.region, r.store_id;
```
**Expected Output:**
```
region     | store_id | tx_count | total_revenue | distinct_products
Miền Bắc   | S02      | 38,250   | $628,000      | 35
```

#### **Manager_South (Miền Nam — S01)**
```sql
SELECT 
    r.region,
    r.store_id,
    COUNT(*) as tx_count,
    SUM(revenue) as total_revenue,
    COUNT(DISTINCT product_id) as distinct_products
FROM SalesTransactions st
INNER JOIN StoreRegions r ON st.store_id = r.store_id
WHERE r.region = N'Miền Nam'
GROUP BY r.region, r.store_id;
```
**Expected Output:**
```
region     | store_id | tx_count | total_revenue | distinct_products
Miền Nam   | S01      | 37,340   | $772,000      | 35
```

#### **Manager_Central (Miền Trung — S03)**
```sql
SELECT 
    r.region,
    r.store_id,
    COUNT(*) as tx_count,
    SUM(revenue) as total_revenue,
    COUNT(DISTINCT product_id) as distinct_products
FROM SalesTransactions st
INNER JOIN StoreRegions r ON st.store_id = r.store_id
WHERE r.region = N'Miền Trung'
GROUP BY r.region, r.store_id;
```
**Expected Output:**
```
region      | store_id | tx_count | total_revenue | distinct_products
Miền Trung  | S03      | 34,525   | $719,000      | 35
```

#### **Director (tất cả stores)**
```sql
SELECT 
    r.region,
    r.store_id,
    COUNT(*) as tx_count,
    SUM(revenue) as total_revenue,
    COUNT(DISTINCT product_id) as distinct_products
FROM SalesTransactions st
INNER JOIN StoreRegions r ON st.store_id = r.store_id
GROUP BY r.region, r.store_id
ORDER BY r.region, r.store_id;
```
**Expected Output:**
```
region      | store_id | tx_count | total_revenue | distinct_products
Miền Bắc    | S02      | 38,250   | $628,000      | 35
Miền Nam    | S01      | 37,340   | $772,000      | 35
Miền Trung  | S03      | 34,525   | $719,000      | 35
--- Total:          110,115      $2,119,000      35
```

Nếu SQL query kết quả đúng như trên → Power BI RLS sẽ hoạt động chính xác ✓

---

### Power BI Desktop vs Service — Sự khác nhau

#### **Power BI Desktop** (trên máy bạn)
- **View As Roles**: Bạn có thể test từng role, thấy dashboard thay đổi thế nào
- **Mục đích**: Debug + verify RLS rules trước khi publish
- **Ai thấy được**: Chỉ bạn (người edit) nhìn thấy
- **RLS dữ liệu**: Được test locally, không upload dữ liệu thực

#### **Power BI Service** (trên cloud)
- **Assign Users to Roles**: Admin gán từng user vào role (Manager_North, Manager_South, etc.)
- **Mục đích**: Thực tế — mỗi user thấy đúng dữ liệu của họ
- **Ai thấy được**: Tất cả users người publish quyền truy cập
- **RLS dữ liệu**: Được enforce server-side khi user load report
- **Audit**: Mỗi query được log tại Power BI Service (ai xem cái gì)

#### **Quy trình chuẩn:**
1. **Desktop**: Tạo roles + DAX filters + test dengan **View As Roles** ✓
2. **Desktop**: Xác nhận KQ đúng (revenue numbers, stores, etc.) ✓
3. **Service**: Publish file lên Premium capacity hoặc Shared capacity ✓
4. **Service**: Admin assign users vào roles ✓
5. **Service**: Users login + xem report (dữ liệu đã auto-filter) ✓

---

### Scenario 4 Summary - RLS Implementation Complete ✓

| Bước | Action | Result |
|------|--------|--------|
| 1 | Tạo 4 roles (Manager_N/S/C + Director) | ✓ |
| 2 | Viết DAX filters per role | ✓ |
| 3 | Test với View As Roles | ✓ |
| 4 | Verify SQL data preview | ✓ |
| 5 | Publish lên Power BI Service | ⏳ (User thực hiện) |
| 6 | Assign users → roles | ⏳ (Admin thực hiện) |

**Khi bước 5-6 xong → RLS sẽ bảo vệ dữ liệu + mỗi user thấy đúng region của họ ✓**

---

## Bước 7: Thêm Performance Page (Scenario 5)

### Page 4: Performance & Latency (tùy chọn)

| Visual | Type | Fields |
|--------|------|--------|
| Total Events | Card | `SUM(vw_PerformanceMetrics[event_count])` |
| Avg Latency | Card | `AVG(vw_PerformanceMetrics[avg_latency_sec])` |
| SLA Compliance | Card | `AVG(vw_PerformanceMetrics[sla_pct_under_5sec])` |
| Hourly Throughput | Line Chart | Axis: `metric_hour`, Values: `event_count` |
| Load Test Results | Table | `LatencyBenchmark` all columns |
| Latency by Hour | Bar Chart | Axis: `metric_hour`, Values: `avg_latency_sec` |

---

## Checklist trước khi demo

- [ ] Import **Products** table và tạo relationship → Products[product_id]
- [ ] Product Table visual dùng `Products[product_name]` (không dùng SalesTransactions[product_id])
- [ ] Revenue DoD Growth hiện ~+18.4% (ngày cuối Apr 9)
- [ ] Forecast Accuracy hiện 0.88
- [ ] Region slicer hoạt động (Bắc/Trung/Nam)
- [ ] RLS roles đã tạo (Manager_North, South, Central, Director)
- [ ] View As Roles test thành công
- [ ] 3+ trang dashboard (Overview, Alerts, Forecast)
- [ ] Refresh data → kiểm tra số liệu cập nhật

---

## Dữ liệu hiện tại (sau khi chạy demo_scenarios.py)

```
SalesTransactions:  110,115 rows  | $2.12M total | 7 days (Apr 3-9)
SalesForecast:       4,998 rows  | ML predictions
SalesAlerts:         4,184 rows  | Anomaly alerts
Products:               35 rows  | Product dimension
StoreRegions:            3 rows  | Bắc/Trung/Nam
SecurityMapping:         5 rows  | RLS roles
AccessAudit:           180 rows  | Audit trail
LatencyBenchmark:        5 rows  | Load test results
ModelRegistry:           5 rows  | Model versions
```

### DoD Growth Pattern
| Date | Revenue | DoD |
|------|---------|-----|
| Apr 9 | $420K | +18.4% |
| Apr 8 | $355K | +15.2% |
| Apr 7 | $308K | +20.8% |
| Apr 6 | $255K | -7.2% |
| Apr 5 | $275K | +12.2% |
| Apr 4 | $245K | -5.8% |
| Apr 3 | $260K | — |

### Store Revenue
| Store | Region | Revenue |
|-------|--------|---------|
| S01 | Miền Nam (HCM) | $772K (36%) |
| S02 | Miền Bắc (HN) | $628K (30%) |
| S03 | Miền Trung (ĐN) | $719K (34%) |
