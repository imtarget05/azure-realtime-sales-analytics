# Cơ sở lý thuyết: Định dạng lưu trữ dữ liệu

## 1. Dữ liệu có cấu trúc (Structured Data)

### 1.1 Relational Database — Azure SQL Database
Hệ thống sử dụng **Azure SQL Database** (PaaS) để lưu trữ dữ liệu giao dịch bán hàng dạng có cấu trúc.

**Đặc điểm:**
- Lưu trữ dạng bảng (table) với schema cố định, hỗ trợ SQL chuẩn
- ACID Transactions đảm bảo toàn vẹn dữ liệu
- Hỗ trợ index (B-Tree, Columnstore) để tăng tốc truy vấn
- Tự động backup, geo-replication, encryption at rest

**Schema chính trong hệ thống:**
| Bảng | Mô tả | Dung lượng ước tính |
|------|--------|-------------------|
| SalesTransactions | Giao dịch bán hàng real-time | ~500MB/tháng |
| HourlySalesSummary | Tổng hợp theo giờ (từ Stream Analytics) | ~50MB/tháng |
| SalesForecast | Dự đoán từ ML model | ~10MB/tháng |
| Products | Bảng dimension sản phẩm | <1MB |
| StoreRegions | Bảng dimension cửa hàng | <1MB |

### 1.2 JSON (Semi-structured)
- **Event Hubs** nhận dữ liệu dạng JSON từ các generator (sales, weather, stock)
- **Model metadata** lưu dạng JSON (`model_metadata.json`, `comparison_report.json`)
- **Cấu hình pipeline** dạng JSON (`pipeline_definition.json`, ARM templates)

### 1.3 CSV (Flat File)
- Dữ liệu huấn luyện ML model xuất ra CSV cho offline processing
- Benchmark sinh dataset >4GB ở dạng CSV để đo hiệu năng

## 2. Dữ liệu truyền phát (Streaming Data)

### 2.1 Event Hub Message Format
```json
{
  "timestamp": "2026-04-09T10:30:00Z",
  "store_id": "S01",
  "product_id": "P001",
  "quantity": 3,
  "price": 25.50,
  "category": "Beverage"
}
```
- Mỗi message ≈ 200-500 bytes
- Throughput: lên đến 1 MB/s (Basic tier) hoặc 20 MB/s (Standard tier)
- Retention: 1-7 ngày (có thể mở rộng với Capture)

### 2.2 Stream Analytics Output Format
Stream Analytics chuyển đổi JSON → SQL rows qua window functions:
- **Tumbling Window** (5 phút): tổng hợp doanh thu theo cửa hàng
- **Sliding Window** (15 phút): phát hiện anomaly (doanh thu bất thường)
- **Session Window**: nhóm giao dịch liên tục của cùng một khách hàng

## 3. So sánh định dạng lưu trữ

| Tiêu chí | SQL Database | JSON/Event Hub | CSV |
|-----------|-------------|----------------|-----|
| Schema | Cố định (DDL) | Linh hoạt | Cố định (header) |
| Truy vấn | SQL (index) | Không hỗ trợ | Sequential scan |
| Real-time | ✅ | ✅ | ❌ |
| Kích thước | Compact + compression | Verbose | Medium |
| Sử dụng trong hệ thống | Lưu trữ chính | Truyền phát | Benchmark/Training |

## 4. Tối ưu lưu trữ áp dụng

1. **Columnstore Index** trên `SalesTransactions` cho query aggregation nhanh gấp 10x
2. **Partitioning** theo `event_time` (monthly) giảm scan không cần thiết
3. **Data Compression** (PAGE compression) giảm 60-80% dung lượng
4. **Index Strategy**: Clustered index trên `(event_time, store_id)`, Non-clustered trên `product_id`, `category`
