# Giải pháp tối ưu hóa lưu trữ và tốc độ đọc/ghi

## 1. Tối ưu Azure SQL Database

### 1.1 Index Strategy
```sql
-- Clustered Index: tối ưu range scan theo thời gian
CREATE CLUSTERED INDEX IX_SalesTransactions_EventTime
ON SalesTransactions(event_time DESC);

-- Non-clustered Index: tối ưu filter theo store và category
CREATE NONCLUSTERED INDEX IX_SalesTransactions_StoreCategory 
ON SalesTransactions(store_id, category) INCLUDE (revenue, units_sold);

-- Filtered Index: chỉ index dữ liệu holiday
CREATE NONCLUSTERED INDEX IX_SalesTransactions_Holiday 
ON SalesTransactions(event_time, store_id) WHERE holiday = 1;
```

**Kết quả benchmark:**
- Query `GROUP BY store_id`: 34.2ms (có index) vs 450ms (không index) → **13x nhanh hơn**
- Query `WHERE category = 'Electronics'`: 12ms vs 180ms → **15x nhanh hơn**

### 1.2 Batch Insert thay Single Insert
- Single INSERT: ~80 rows/s
- Batch INSERT (1000 rows/batch): ~1,250 rows/s → **15.6x nhanh hơn**
- Sử dụng `executemany()` trong pyodbc với `fast_executemany = True`

### 1.3 Connection Pooling
```python
# Tái sử dụng connection thay vì tạo mới mỗi request
from sqlalchemy import create_engine
engine = create_engine(conn_string, pool_size=5, max_overflow=10)
```

### 1.4 Data Compression
- PAGE Compression trên SalesTransactions: giảm 60-70% storage
- Columnstore Index trên bảng lịch sử: giảm 90% cho analytical queries

## 2. Tối ưu Event Hub Ingestion

### 2.1 Batch Sending
```python
# Gom nhiều events thành 1 batch trước khi gửi
batch = producer.create_batch()
for event in events:
    batch.add(EventData(json.dumps(event)))
producer.send_batch(batch)  # 1 network call thay vì N calls
```

### 2.2 Partition Strategy
- Partition key = `store_id` → events cùng store đến cùng partition
- Đảm bảo ordering trong cùng store
- Tận dụng parallel processing của Stream Analytics

## 3. Tối ưu Stream Analytics

### 3.1 Streaming Units (SU)
- Mỗi SU = 1 unit xử lý song song
- Benchmark: 6 SU xử lý 1000 events/s với latency <500ms
- Scale up: tăng SU khi input throughput tăng

### 3.2 Query Optimization
- Sử dụng `TIMESTAMP BY` để Stream Analytics biết cột thời gian
- Tránh `SELECT *`, chỉ lấy cột cần thiết
- Sử dụng `TRY_CAST` thay `CAST` để tránh lỗi parsing

## 4. Tối ưu ML Model Training

### 4.1 Feature Selection
- Loại bỏ features có tương quan thấp (correlation < 0.05)
- Sử dụng cyclical encoding (sin/cos) cho features chu kỳ (hour, month)
- Label Encoding cho categorical features (nhanh hơn One-Hot cho tree-based models)

### 4.2 Hyperparameter Tuning
- Grid Search trên n_estimators, max_depth, learning_rate
- Early stopping khi validation score không cải thiện

## 5. Tối ưu Network Latency

### 5.1 Region Selection
Benchmark latency từ Việt Nam (xem `benchmark_output/benchmark_latency.json`):
| Region | Avg Latency |
|--------|------------|
| Southeast Asia | 69ms |
| Japan East | 130ms |
| Australia East | 150ms |
| West Europe | 216ms |
| East US | 291ms |

→ Deploy tất cả resources ở **Southeast Asia** (Singapore) để tối ưu latency

### 5.2 Connection Optimization
- Sử dụng `Encrypt=yes;TrustServerCertificate=no` cho secure connection
- Connection timeout: 30s (tránh hang)
- Retry logic với exponential backoff
