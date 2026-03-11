# Đề cương Nội dung Báo cáo (Word / PowerPoint)

> **Đồ án:** Hệ thống trực quan dữ liệu bán hàng thời gian thực trên Azure

---

## Mục lục

- [Báo cáo Word](#báo-cáo-word)
- [Slide PowerPoint](#slide-powerpoint)
- [Quản lý dự án (Rubric 5)](#quản-lý-dự-án-rubric-5)

---

## Báo cáo Word

### Trang bìa

- Tên trường / Khoa
- Tên đồ án: **Hệ thống trực quan dữ liệu bán hàng thời gian thực trên Azure**
- Giảng viên hướng dẫn
- Sinh viên thực hiện (Họ tên, MSSV)
- Năm học

### Phần mở đầu

- Mục lục
- Danh mục hình ảnh
- Danh mục bảng biểu
- Danh mục viết tắt

| Viết tắt | Đầy đủ |
|---|---|
| PaaS | Platform as a Service |
| SaaS | Software as a Service |
| IaaS | Infrastructure as a Service |
| ML | Machine Learning |
| ETL | Extract, Transform, Load |
| RLS | Row-Level Security |
| SU | Streaming Unit |
| TU | Throughput Unit |
| DTU | Database Transaction Unit |
| GBR | Gradient Boosting Regressor |
| MAPE | Mean Absolute Percentage Error |
| RMSE | Root Mean Square Error |
| API | Application Programming Interface |

---

### Chương 1 — Giới thiệu bài toán *(Rubric 1 — 1.5đ)*

#### 1.1 Đặt vấn đề

- **Bối cảnh:** Số hóa bán hàng, nhu cầu theo dõi real-time ngày càng cao
- **Thách thức:** Dữ liệu lớn, nhiều nguồn, cần xử lý nhanh
- **Giải pháp:** Hệ thống cloud-based trên Azure

#### 1.2 Loại bài toán *(Rubric 1.1 — 0.25đ)*

| Bài toán | Loại |
|---|---|
| Bài toán chính | Trực quan hóa dữ liệu (Data Visualization) |
| Bài toán phụ | Phân tích dữ liệu (Data Analytics) + Dự đoán (Prediction) |
| Phân loại kỹ thuật | Real-time streaming analytics + Batch prediction |

#### 1.3 Loại dữ liệu *(Rubric 1.2 — 0.25đ)*

| Nguồn | Format | Volume | Tần suất | Kênh |
|---|---|---|---|---|
| Sales | JSON → SQL | ~100 events/s | Continuous | Event Hub |
| Weather | JSON | ~5 events/30s | 30s interval | Event Hub |
| Stock | JSON → SQL | ~10 events/5s | 5s interval | Event Hub |
| Reference | JSON / CSV | ~50 KB | On-demand | Blob Storage |

**Trường dữ liệu bán hàng:** `transaction_id`, `product`, `quantity`, `price`, `region`, `payment`, `timestamp`

#### 1.4 Kích thước dữ liệu & Benchmark *(Rubric 1.3 — 0.75đ)*

- Sinh dataset >4 GB (CSV, ~15–20 triệu dòng)
- **Kết quả benchmark** (lấy từ `benchmark_output/benchmark_report.json`):
  - Local: thời gian đọc CSV, throughput (MB/s)
  - Cloud: thời gian insert, query aggregation
  - Nhận xét: Local nhanh hơn cho single-file batch; Cloud ưu việt cho concurrent/scale

*→ Chèn hình: Biểu đồ so sánh tốc độ local vs cloud*

#### 1.5 Phân loại dịch vụ *(Rubric 1.4 — 0.25đ)*

- Bảng phân loại IaaS/PaaS/SaaS → xem [`docs/ly_thuyet_va_phan_loai.md`](ly_thuyet_va_phan_loai.md)
- Sơ đồ phân tầng
- Giải thích lý do chọn PaaS/SaaS thay vì IaaS

---

### Chương 2 — Cơ sở lý thuyết *(Rubric 2 — 1.5đ)*

#### 2.1 Định dạng lưu trữ *(Rubric 2.1 — 0.5đ)*

- **JSON** — cấu trúc, ưu/nhược điểm, ứng dụng trong đồ án
- **CSV** — cấu trúc, ưu/nhược điểm, ứng dụng trong đồ án
- **Avro** — binary format, schema evolution, Event Hub Capture
- **SQL Tables** — relational model, normalization, ACID

*→ Bảng so sánh 4 format: Size, Speed, Schema, Use case*

#### 2.2 Thuật toán & Phương pháp *(Rubric 2.2 — 0.5đ)*

- **Gradient Boosting Regressor** — công thức, quy trình, hyperparameters
- **Streaming Processing** — Window functions (Tumbling, Sliding, Hopping)
- **Anomaly Detection** — Statistical threshold trong Stream Analytics

*→ Chèn hình: Minh họa Gradient Boosting iterative process*
*→ Chèn hình: Minh họa 3 loại window function*

#### 2.3 Dịch vụ Cloud chi tiết *(Rubric 2.3 — 0.5đ)*

Trình bày từng dịch vụ: Vai trò, Tier sử dụng, SLA, Chi phí

| Dịch vụ | Tier | SLA | Chi phí/tháng |
|---|---|---|---|
| Event Hubs | Standard 1TU | 99.95% | ~$22 |
| Stream Analytics | 6 SU | 99.9% | ~$110 |
| Azure SQL | S1 | 99.99% | ~$30 |
| Data Factory | Pay-as-you-go | 99.9% | ~$5 |
| Machine Learning | Pay-as-you-go | 99.9% | ~$36 |
| Power BI | Pro | 99.9% | ~$10 |

---

### Chương 3 — Mô hình dữ liệu *(Rubric 3 — 2đ)*

#### 3.1 Sơ đồ kiến trúc

- Flow: Data Sources → Event Hub → Stream Analytics → SQL + Power BI
- Flow: Data Sources → ML → SQL
- Sơ đồ: Data Factory orchestration

*→ Chèn hình: Architecture diagram*

#### 3.2 Thiết kế Database

- ERD cho 7 bảng chính
- Chi tiết schema: cột, kiểu dữ liệu, constraints
- 3 Views + mục đích sử dụng
- 3 Stored Procedures + logic

*→ Chèn hình: ERD diagram*

#### 3.3 Benchmark đọc/ghi *(Rubric 3.1 — 0.5đ)*

- INSERT: single vs batch (1K–50K rows)
- SELECT: 7 loại query với workload khác nhau
- Bảng tổng hợp throughput (rows/s, MB/s)

*→ Chèn bảng từ `benchmark_output/benchmark_read_write.json`*
*→ Chèn hình: Bar chart INSERT speed + SELECT speed*

#### 3.4 ETL Pipeline *(Rubric 3.2 — 0.5đ)*

**Stream Analytics — 9 queries:**

| # | Query | Mô tả |
|---|---|---|
| 1 | Direct insert | Ghi thẳng giao dịch vào SQL |
| 2 | Hourly aggregation | Tumbling Window 1h — tổng hợp theo giờ |
| 3 | Product summary | Tumbling Window 30m — tổng hợp theo sản phẩm |
| 4 | High-value anomaly | Phát hiện đơn hàng giá trị cao |
| 5 | Revenue spike | Sliding Window 5m — đột biến doanh thu |
| 6 | Weather store | Lưu dữ liệu thời tiết |
| 7 | Stock store | Lưu dữ liệu chứng khoán |
| 8 | Power BI stream | Đẩy dữ liệu real-time sang Power BI |
| 9 | Weather-Sales JOIN | Tương quan thời tiết × doanh thu |

**Data Factory — 2 pipelines:**

| Pipeline | Mô tả |
|---|---|
| `CopyStagingToSQL` | Copy từ Blob staging → SQL Database |
| `MLOrchestration` | Prepare Data → Train ML → Update Forecasts |

#### 3.5 Delay & Latency *(Rubric 3.3 — 0.5đ)*

- TCP latency đến các Azure regions
- SQL connection + query latency
- DNS resolution time
- Gợi ý region tối ưu cho Việt Nam

*→ Chèn bảng từ `benchmark_output/benchmark_latency.json`*
*→ Chèn hình: Bar chart latency theo region*

#### 3.6 Giải pháp tối ưu *(Rubric 3.4 — 0.5đ)*

- Indexing: 4 non-clustered indexes + columnstore index
- Partitioning: theo tháng
- Compression: Row (OLTP) / Page (OLAP)
- Event Hubs: batch sending + auto-inflate
- Blob: lifecycle management (Hot → Cool → Archive)
- Stream Analytics: query optimization

---

### Chương 4 — Hiện thực *(Rubric 4 — 3đ)*

#### 4A. Track Trực quan hóa dữ liệu *(1.5đ)*

##### 4A.1 Kết nối Power BI — Azure SQL *(0.25đ)*

- Setup DirectQuery
- *Screenshot: kết quả kết nối thành công*

##### 4A.2 Dashboard chính *(0.75đ)*

| Dashboard | Nội dung |
|---|---|
| 1 — Tổng quan real-time | KPI cards, line chart, bar chart |
| 2 — Phân tích sản phẩm | Treemap, scatter, top 10 |
| 3 — Dự đoán nhu cầu | Forecast vs actual, heatmap |
| 4 — Cảnh báo bất thường | Alert table, status cards |
| 5 — Tác động thời tiết | Correlation charts |

*→ Chèn screenshot: Mỗi dashboard 1 hình*

##### 4A.3 Row-Level Security *(0.25đ)*

- Roles: `NorthManager`, `SouthManager`, `EastManager`, `WestManager`, `CentralManager`
- Dynamic RLS với `UserRegionMapping` table
- *Screenshot: cùng report, khác role → dữ liệu khác vùng*

##### 4A.4 Mobile Layout *(0.25đ)*

- 4 trang mobile (portrait)
- *Screenshot: layout design + thực tế trên điện thoại*

---

#### 4B. Track Phân tích dữ liệu *(1.5đ)*

##### 4B.1 Xây dựng mô hình trên Azure ML *(0.25đ)*

- Pipeline: Data preparation → Feature engineering → Model training
- Environment: `conda_env.yml`
- Register model trên Azure ML Studio

##### 4B.2 Deploy ML Endpoint *(0.25đ)*

- Tạo Online Endpoint
- Deploy Managed Online Deployment
- Test với sample data

*→ Screenshot: Azure ML Studio — Endpoint + Test results*

##### 4B.3 So sánh mô hình *(0.5đ)*

9 mô hình được so sánh:

| # | Mô hình | MAE | RMSE | R² | MAPE | Training time |
|---|---|---|---|---|---|---|
| 1 | Linear Regression | — | — | — | — | — |
| 2 | Ridge | — | — | — | — | — |
| 3 | Lasso | — | — | — | — | — |
| 4 | Decision Tree | — | — | — | — | — |
| 5 | Random Forest | — | — | — | — | — |
| 6 | **Gradient Boosting** ✅ | — | — | — | — | — |
| 7 | AdaBoost | — | — | — | — | — |
| 8 | KNN | — | — | — | — | — |
| 9 | SVR | — | — | — | — | — |

*→ Chèn hình: 4 biểu đồ (MAE/RMSE, R², training time, MAPE)*
*→ Chèn hình: Radar chart so sánh tổng thể*
*→ Chèn hình: Actual vs Predicted (top 3 models)*

##### 4B.4 Web App gọi ML API *(0.5đ)*

- **Input:** Chọn sản phẩm, số lượng, giảm giá, thời gian, vùng
- **Output:** Predicted revenue + confidence interval + input summary
- **API endpoint:** `POST /api/predict`

*→ Screenshot: Trang input + Trang kết quả*

---

### Chương 5 — Kết luận

#### 5.1 Kết quả đạt được

- ✅ Hệ thống end-to-end từ data ingestion → visualization
- ✅ Real-time processing với latency < 5 giây
- ✅ ML prediction với MAPE < 10%
- ✅ Dashboard interactive trên Power BI (5 dashboards)
- ✅ Web app cho end-user prediction

#### 5.2 Hạn chế

- Chi phí Azure cao cho production quy mô lớn
- Chưa implement CI/CD pipeline đầy đủ
- ML model cần retrain định kỳ (data drift)

#### 5.3 Hướng phát triển

- Thêm Azure Cosmos DB cho NoSQL workload
- Implement CI/CD với Azure DevOps / GitHub Actions
- Sử dụng Azure Synapse Analytics cho data warehouse
- Thêm advanced ML (deep learning, time series forecasting)

### Tài liệu tham khảo

- Microsoft Azure Documentation
- Azure Architecture Center
- scikit-learn Documentation
- Power BI Documentation

### Phụ lục

- Link GitHub repository
- Hướng dẫn chạy (README.md)
- File `.env.example`

---

## Slide PowerPoint

> 15–20 slides, thời gian thuyết trình 15–20 phút

| Slide | Nội dung |
|---|---|
| 1 | Trang bìa: tên đồ án, SV, GVHD |
| 2 | Mục lục 5 phần chính |
| 3 | Đặt vấn đề: bối cảnh + thách thức + giải pháp |
| 4 | Kiến trúc hệ thống (architecture diagram) |
| 5 | Phân loại dịch vụ Cloud (IaaS/PaaS/SaaS) |
| 6 | Dữ liệu & Benchmark: 4 nguồn + kết quả >4GB |
| 7 | Gradient Boosting: công thức + diagram |
| 8 | Streaming Windows + Event Hub architecture |
| 9 | Database Schema (ERD, 7 tables + 3 views) |
| 10 | ETL Pipeline: Stream Analytics 9 queries + Data Factory |
| 11 | Benchmark kết quả: read/write + latency charts |
| 12 | Tối ưu lưu trữ: indexing + partitioning + compression |
| 13 | ML Model Comparison: bảng xếp hạng 9 models |
| 14 | Power BI Dashboard (screenshots) |
| 15 | RLS & Mobile Layout (screenshots) |
| 16 | Web App Demo: form input + kết quả prediction |
| 17 | Tổng kết chi phí (~$216/tháng, so sánh IaaS vs PaaS) |
| 18 | Kết luận: kết quả đạt được + hạn chế + hướng phát triển |
| 19 | Demo trực tiếp (URL Power BI / Web App / Azure Portal) |
| 20 | Q & A |

---

## Quản lý dự án (Rubric 5)

### 5.1 GitHub Repository *(Rubric 5.1 — 0.5đ)*

- [ ] Tạo repo trên GitHub
- [ ] Commit history rõ ràng (≥ 10–15 commits)
- [ ] Branch strategy: `main` + `develop`
- [ ] README.md đầy đủ
- [ ] `.gitignore` đã cấu hình

### 5.2 Quản lý chi phí & Quotas *(Rubric 5.3 — 0.5đ)*

- Azure Cost Management: Budget $50/tháng
- Alert khi chi phí > 80% budget
- Clean up resources khi không sử dụng
- Ưu tiên sử dụng Azure for Students credit ($100)

### 5.3 Cấu trúc thư mục dự án

```
azure-realtime-sales-analytics/
├── config/             # Cấu hình
├── data_generator/     # Sinh dữ liệu giả lập
├── blob_storage/       # Upload reference data
├── data_factory/       # ADF pipeline
├── stream_analytics/   # SA queries
├── sql/                # Database schema + SPs
├── ml/                 # ML training, deploy, compare
├── powerbi/            # Power BI setup guide
├── webapp/             # Flask web app
├── benchmarks/         # Benchmark scripts
├── docs/               # Tài liệu lý thuyết
├── infrastructure/     # Deploy scripts
├── .env.example
├── .gitignore
├── requirements.txt
└── README.md
```
