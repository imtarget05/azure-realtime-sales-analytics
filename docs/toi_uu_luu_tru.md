# Giải pháp Tối ưu Lưu trữ & Xử lý Dữ liệu

> **Đồ án:** Hệ thống trực quan dữ liệu bán hàng thời gian thực trên Azure

---

## Mục lục

1. [Tối ưu Azure SQL Database](#1-tối-ưu-azure-sql-database)
2. [Tối ưu Event Hubs](#2-tối-ưu-event-hubs)
3. [Tối ưu Blob Storage](#3-tối-ưu-blob-storage)
4. [Tối ưu Stream Analytics](#4-tối-ưu-stream-analytics)
5. [Tối ưu Machine Learning](#5-tối-ưu-machine-learning)
6. [Tổng hợp & Ước tính chi phí](#6-tổng-hợp--ước-tính-chi-phí)

---

## 1. Tối ưu Azure SQL Database

### 1.1 Chiến lược Indexing

Đồ án tạo các index sau trên bảng `SalesTransactions` (xem `sql/create_tables.sql`):

```sql
-- Non-clustered indexes cho các query phổ biến
CREATE INDEX IX_SalesTransactions_Date     ON SalesTransactions(sale_date);
CREATE INDEX IX_SalesTransactions_Region   ON SalesTransactions(region);
CREATE INDEX IX_SalesTransactions_Category ON SalesTransactions(category);
CREATE INDEX IX_SalesTransactions_Product  ON SalesTransactions(product_id);
```

**Hiệu quả thực tế:**

| Query Pattern | Không có Index | Có Index | Cải thiện |
|---|---|---|---|
| `WHERE sale_date = '2024-06-01'` | Full table scan | Index seek | ~100× |
| `GROUP BY region` | Full scan + sort | Index scan | ~10× |
| `WHERE category = 'Electronics'` | Full scan | Index seek | ~50× |
| JOIN với `ProductSalesSummary` | Nested loop scan | Hash/Merge join | ~20× |

**Columnstore Index** cho bảng fact lớn (read-heavy):

```sql
-- Phù hợp cho analytical queries: SUM, AVG, GROUP BY
CREATE NONCLUSTERED COLUMNSTORE INDEX IX_CS_HourlySales
ON HourlySalesSummary (region, category, total_revenue, total_quantity);
```

> Columnstore lưu trữ theo cột → nén tốt hơn ~10× so với row-store.

### 1.2 Partitioning

Phân vùng bảng `SalesTransactions` theo tháng để tối ưu query theo thời gian:

```sql
-- Partition function: chia dữ liệu theo tháng
CREATE PARTITION FUNCTION pf_SalesByMonth (DATE)
AS RANGE RIGHT FOR VALUES (
    '2024-01-01', '2024-02-01', '2024-03-01', '2024-04-01',
    '2024-05-01', '2024-06-01', '2024-07-01', '2024-08-01',
    '2024-09-01', '2024-10-01', '2024-11-01', '2024-12-01'
);

-- Partition scheme
CREATE PARTITION SCHEME ps_SalesByMonth
AS PARTITION pf_SalesByMonth ALL TO ([PRIMARY]);
```

**Lợi ích:**
- Query `WHERE sale_date BETWEEN '2024-03-01' AND '2024-03-31'` chỉ scan 1 partition
- Partition elimination giảm I/O ~12× (12 tháng → 1 tháng)
- Có thể SWITCH partition cũ ra archive table

### 1.3 Data Compression

```sql
-- Row compression: giảm ~30–50% storage, phù hợp OLTP (write-heavy)
ALTER TABLE SalesTransactions
REBUILD WITH (DATA_COMPRESSION = ROW);

-- Page compression: giảm ~60–80%, phù hợp OLAP (read-heavy)
ALTER TABLE HourlySalesSummary
REBUILD WITH (DATA_COMPRESSION = PAGE);
```

| Compression | Storage giảm | CPU overhead | Phù hợp |
|---|---|---|---|
| None | 0% | 0% | Bảng nhỏ |
| Row | 30–50% | 5–10% | OLTP (write-heavy) |
| Page | 60–80% | 10–20% | OLAP (read-heavy) |
| Columnstore | 80–95% | 15–25% | Analytics/Warehouse |

### 1.4 Auto-Tuning

```sql
ALTER DATABASE SalesAnalyticsDB
SET AUTOMATIC_TUNING (
    FORCE_LAST_GOOD_PLAN = ON,   -- Tự dùng execution plan tốt nhất
    CREATE_INDEX         = ON,   -- Tự tạo index khi phát hiện cần thiết
    DROP_INDEX           = ON    -- Xóa index không dùng
);
```

---

## 2. Tối ưu Event Hubs

### 2.1 Partition Strategy

- **Số partitions:** 4 (mặc định, phù hợp throughput thấp–trung bình)
- **Partition key:** `region` — đảm bảo event cùng vùng vào cùng partition
- **Lợi ích:** Consumer xử lý tuần tự trong partition, đảm bảo thứ tự event

### 2.2 Batch Sending

```python
# ❌ Chậm — gửi từng event riêng lẻ
producer.send(EventData(json.dumps(event)))

# ✅ Nhanh — gửi theo batch (nhanh hơn 10–50×)
batch = await producer.create_batch()
for event in events:
    batch.add(EventData(json.dumps(event)))
await producer.send_batch(batch)
```

### 2.3 Throughput Units

| Workload | TU cần | Events/sec (ước tính) | Chi phí/tháng |
|---|---|---|---|
| Dev / Test | 1 | ~1 000 | ~$22 |
| Production nhỏ | 2–4 | ~4 000 | ~$44–88 |
| Production lớn | 10–20 | ~20 000 | ~$220–440 |
| Auto-inflate | 1–20 (tự scale) | Tự scale | Biến đổi |

> 💡 **Khuyến nghị:** Bật **Auto-Inflate** để tự động scale TU khi tải tăng đột biến.

---

## 3. Tối ưu Blob Storage

### 3.1 Access Tiers

| Tier | Trường hợp sử dụng | Storage cost | Access cost |
|---|---|---|---|
| **Hot** | Reference data (truy cập thường xuyên) | Cao nhất | Thấp nhất |
| **Cool** | Event Hub Capture (truy cập ít, lưu 30+ ngày) | Trung bình | Trung bình |
| **Archive** | Backup lâu dài (>180 ngày) | Thấp nhất | Cao nhất |

### 3.2 Lifecycle Management

Tự động chuyển tier theo thời gian để tiết kiệm chi phí:

```json
{
  "rules": [
    {
      "name": "MoveToCool",
      "definition": {
        "actions": {
          "baseBlob": { "tierToCool": { "daysAfterModificationGreaterThan": 30 } }
        },
        "filters": { "blobTypes": ["blockBlob"], "prefixMatch": ["sales-archive/"] }
      }
    },
    {
      "name": "MoveToArchive",
      "definition": {
        "actions": {
          "baseBlob": { "tierToArchive": { "daysAfterModificationGreaterThan": 180 } }
        },
        "filters": { "blobTypes": ["blockBlob"], "prefixMatch": ["sales-archive/"] }
      }
    }
  ]
}
```

---

## 4. Tối ưu Stream Analytics

### 4.1 Streaming Units (SU)

| Độ phức tạp | SU cần | Loại query |
|---|---|---|
| Pass-through đơn giản | 1–3 | SELECT INTO |
| Aggregation (GROUP BY) | 3–6 | Tumbling/Sliding window |
| JOIN + Multi-output | 6–12 | Reference JOIN + nhiều outputs |
| **Đồ án (9 queries)** | **6** | Aggregation + JOIN + multi-output |

### 4.2 Tối ưu Query

```sql
-- ❌ Chậm: SELECT * lấy hết cột
SELECT * FROM SalesInput

-- ✅ Nhanh: Chỉ lấy cột cần thiết
SELECT transaction_id, region, category, final_amount
FROM SalesInput

-- ❌ Chậm: Nested subquery
SELECT * FROM (SELECT ... FROM SalesInput GROUP BY ...) WHERE ...

-- ✅ Nhanh: Sử dụng WITH (CTE)
WITH Aggregated AS (
    SELECT region, SUM(final_amount) AS revenue
    FROM SalesInput
    GROUP BY region, TumblingWindow(hour, 1)
)
SELECT * FROM Aggregated WHERE revenue > 1000
```

---

## 5. Tối ưu Machine Learning

### 5.1 Compute theo Scenario

| Scenario | Compute type | Config | Chi phí ước tính |
|---|---|---|---|
| Training (dev) | Compute Instance | Standard_DS3_v2 (4 CPU, 14 GB) | ~$0.29/h |
| Training (prod) | Compute Cluster | Standard_DS4_v2 (auto 0–4 nodes) | ~$0.45/h × nodes |
| Inference | Online Endpoint | Standard_DS2_v2 (2 CPU, 7 GB) | ~$0.15/h |

### 5.2 Tối ưu Model

| Kỹ thuật | Mô tả |
|---|---|
| **Feature selection** | Loại bỏ features không quan trọng (dùng `feature_importances_`) |
| **Hyperparameter tuning** | GridSearchCV hoặc Azure ML HyperDrive |
| **Model compression** | Giảm `n_estimators` và `max_depth` để inference nhanh hơn |

---

## 6. Tổng hợp & Ước tính chi phí

### 6.1 Tổng hợp chiến lược

| Tầng | Chiến lược | Tác động |
|---|---|---|
| **Ingestion** | Batch sending + Auto-inflate TU | Giảm 50% latency & cost |
| **Processing** | Tối ưu query, đúng SU | Giảm 30% SU cost |
| **Storage** | Indexing + Partitioning + Compression | Giảm 60% storage, tăng 10× query speed |
| **ML** | Feature selection + right-size compute | Giảm 40% training cost |
| **Visualization** | DirectQuery thay Import cho real-time | Giảm dataset refresh cost |

### 6.2 Ước tính chi phí hàng tháng

| Dịch vụ | Tier / Config | Chi phí/tháng (USD) |
|---|---|---|
| Event Hubs | Standard, 1 TU | ~$22 |
| Stream Analytics | 6 SU | ~$110 |
| Azure SQL | S1 (20 DTU) | ~$30 |
| Blob Storage | Hot 10 GB + Cool 50 GB | ~$3 |
| Data Factory | 5 pipeline runs/ngày | ~$5 |
| Machine Learning | DS2_v2 endpoint, 8h/ngày | ~$36 |
| Power BI | Pro license × 1 user | ~$10 |
| **Tổng** | | **~$216/tháng** |

> 🎓 **Sinh viên/Free tier:** Sử dụng **Azure for Students** ($100 credit) hoặc Free tier để thử nghiệm:
> - Event Hubs: Free tier (1M events/tháng)
> - SQL: Free offer (100K vCore seconds/tháng)
> - Blob: 5 GB miễn phí
> - AML: Free tier (limited compute)
