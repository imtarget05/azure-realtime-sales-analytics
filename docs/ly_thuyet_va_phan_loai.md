# Tài liệu Cơ sở Lý thuyết & Phân loại Dịch vụ Cloud

> **Đồ án:** Hệ thống trực quan dữ liệu bán hàng thời gian thực trên Azure

---

## Mục lục

1. [Phân loại dịch vụ Cloud (IaaS / PaaS / SaaS)](#1-phân-loại-dịch-vụ-cloud)
2. [Cơ sở lý thuyết](#2-cơ-sở-lý-thuyết)
   - [2.1 Định dạng lưu trữ dữ liệu](#21-định-dạng-lưu-trữ-dữ-liệu)
   - [2.2 Thuật toán Gradient Boosting](#22-thuật-toán-gradient-boosting-regressor)
   - [2.3 Streaming Data Processing](#23-streaming-data-processing)
   - [2.4 Chi tiết từng dịch vụ Cloud](#24-chi-tiết-dịch-vụ-cloud)

---

## 1. Phân loại dịch vụ Cloud

### 1.1 Tổng quan mô hình dịch vụ

| Mô hình | Mô tả | Người dùng quản lý | Provider quản lý |
|---|---|---|---|
| **IaaS** | Hạ tầng ảo hóa: VM, network, storage | OS, middleware, runtime, app, data | Hardware, virtualization, network |
| **PaaS** | Nền tảng phát triển ứng dụng | App, data | OS, middleware, runtime, hardware |
| **FaaS** | Thực thi hàm theo sự kiện | Code (functions) | Mọi thứ khác |
| **SaaS** | Phần mềm hoàn chỉnh qua cloud | Cấu hình, sử dụng | Toàn bộ hạ tầng + phần mềm |

### 1.2 Phân loại các dịch vụ Azure trong đồ án

| Dịch vụ Azure | Mô hình | Lý do |
|---|---|---|
| **Azure Event Hubs** | PaaS | Streaming được quản lý hoàn toàn — không cần quản lý server, Azure tự scale. |
| **Azure Blob Storage** | PaaS | Dịch vụ lưu trữ object managed — chỉ tương tác qua API (upload/download). |
| **Azure Stream Analytics** | PaaS | Viết query SQL-like, Azure chạy và scale tự động — không cần quản lý cluster. |
| **Azure SQL Database** | PaaS | Database-as-a-Service — không quản lý SQL Server/OS, có auto-tuning & backup tự động. |
| **Azure Data Factory** | PaaS | ETL/orchestration managed — thiết kế pipeline không cần quản lý compute cluster. |
| **Azure Machine Learning** | PaaS | ML platform managed — cung cấp compute, model registry, endpoint sẵn sàng. |
| **Power BI** | SaaS | Phần mềm BI hoàn chỉnh — người dùng chỉ tạo dashboard, không quản lý hạ tầng. |
| **Azure Resource Group** | N/A | Logical container để tổ chức tài nguyên, không phải dịch vụ compute/storage. |

### 1.3 Sơ đồ phân tầng

```
┌─────────────────────────────────────────────────────────────┐
│                          SaaS                                │
│   ┌─────────────┐                                           │
│   │  Power BI   │  ← Người dùng cuối chỉ tạo dashboard     │
│   └─────────────┘                                           │
├─────────────────────────────────────────────────────────────┤
│                          PaaS                                │
│   ┌──────────────┐ ┌──────────────┐ ┌──────────────────┐   │
│   │  Event Hubs  │ │   Stream     │ │  Azure SQL DB    │   │
│   └──────────────┘ │  Analytics   │ └──────────────────┘   │
│   ┌──────────────┐ └──────────────┘ ┌──────────────────┐   │
│   │ Blob Storage │ ┌──────────────┐ │  Machine Learning│   │
│   └──────────────┘ │Data Factory  │ └──────────────────┘   │
│                    └──────────────┘                         │
├─────────────────────────────────────────────────────────────┤
│                          IaaS                                │
│   (Không sử dụng — không cần quản lý VM trực tiếp)         │
├─────────────────────────────────────────────────────────────┤
│                      Infrastructure                          │
│   Azure tự quản lý: server vật lý, mạng, virtualization     │
└─────────────────────────────────────────────────────────────┘
```

### 1.4 Lý do chọn PaaS / SaaS thay vì IaaS

- **Giảm chi phí vận hành** — Không cần SysAdmin quản lý VM, patching, security updates
- **Auto-scaling** — Event Hubs, Stream Analytics, SQL tự scale theo tải
- **High Availability** — Built-in HA (99.95–99.99% SLA), không cần cấu hình thêm
- **Focus on data** — Tập trung vào logic xử lý dữ liệu thay vì quản lý hạ tầng
- **Faster time-to-market** — Deploy trong vài phút thay vì vài ngày

### 1.5 So sánh chi phí IaaS vs PaaS

| Thành phần | Giải pháp IaaS | Giải pháp PaaS (đồ án) | Tiết kiệm |
|---|---|---|---|
| Message Queue | VM + Apache Kafka (2 VMs) | Event Hubs (1 TU) | ~60% |
| Stream Processing | VM + Apache Flink (3 VMs) | Stream Analytics (6 SU) | ~50% |
| Database | VM + SQL Server (1 VM) | Azure SQL DB (S1) | ~40% |
| ML Training | VM + GPU (1 VM) | AML Compute Cluster | ~70% |
| Orchestration | VM + Apache Airflow | Data Factory | ~55% |
| **Tổng ước tính** | **~$800–1 200/tháng** | **~$200–400/tháng** | **~65%** |

---

## 2. Cơ sở lý thuyết

### 2.1 Định dạng lưu trữ dữ liệu

| Format | Sử dụng trong đồ án | Ưu điểm | Nhược điểm |
|---|---|---|---|
| **JSON** | Event Hubs messages, Blob reference data, API | Human-readable, schema-flexible, chuẩn streaming | File lớn hơn binary, parse chậm hơn |
| **CSV** | Reference data, benchmark dataset, export | Đơn giản, tương thích mọi tool, nhẹ | Không nested data, không schema enforcement |
| **Avro** | Event Hub Capture → Blob Storage | Binary hiệu quả, tự mang schema, nén tốt | Khó đọc trực tiếp, cần deserializer |
| **SQL Tables** | Azure SQL Database (lưu trữ chính) | ACID, query mạnh, indexing, JOIN | Schema cứng, scale ngang khó hơn NoSQL |

---

### 2.2 Thuật toán Gradient Boosting Regressor

#### Nguyên lý

Gradient Boosting xây dựng mô hình dự đoán bằng cách kết hợp nhiều **weak learners** (decision trees) theo trình tự:

$$F_m(x) = F_{m-1}(x) + \eta \cdot h_m(x)$$

| Ký hiệu | Ý nghĩa |
|---|---|
| $F_m(x)$ | Mô hình tại bước $m$ |
| $F_{m-1}(x)$ | Mô hình tích lũy từ các bước trước |
| $\eta$ | Learning rate (thường 0.01–0.1) |
| $h_m(x)$ | Weak learner mới, fit lên **residual** của bước trước |

#### Quy trình huấn luyện

1. Khởi tạo: $F_0(x) = \bar{y}$ (trung bình target)
2. Tại mỗi bước $m = 1, 2, \ldots, M$:
   - Tính residual: $r_i = y_i - F_{m-1}(x_i)$
   - Fit decision tree $h_m$ lên các residuals
   - Cập nhật: $F_m = F_{m-1} + \eta \cdot h_m$
3. Kết quả: $F_M(x) = F_0(x) + \eta \sum_{m=1}^{M} h_m(x)$

#### Hyperparameters trong đồ án

| Parameter | Giá trị | Ý nghĩa |
|---|---|---|
| `n_estimators` | 200 | Số lượng decision trees |
| `max_depth` | 5 | Độ sâu tối đa mỗi tree |
| `learning_rate` | 0.1 | Tốc độ học |
| `random_state` | 42 | Seed để kết quả tái lập được |

#### So sánh với các thuật toán khác

| Thuật toán | Ưu điểm | Nhược điểm |
|---|---|---|
| Linear Regression | Nhanh, dễ hiểu | Không capture nonlinear patterns |
| Random Forest | Ít overfitting, song song được | Kém hơn GB khi pattern phức tạp |
| **Gradient Boosting** ✅ | **Chính xác nhất, capture complex patterns** | **Chậm hơn RF, dễ overfit nếu tune sai** |
| SVR | Tốt cho small dataset | Rất chậm khi dataset lớn |
| KNN | Đơn giản, không cần training | Chậm khi predict, phải scale features |

---

### 2.3 Streaming Data Processing

#### Batch vs Stream

| Đặc điểm | Batch Processing | Stream Processing |
|---|---|---|
| Thời điểm xử lý | Theo lô, định kỳ | Liên tục khi dữ liệu đến |
| Độ trễ | Cao (phút → giờ) | Thấp (milliseconds → seconds) |
| Phù hợp | Báo cáo cuối ngày, ETL đêm | Alert real-time, dashboard trực tiếp |

#### Apache Kafka vs Azure Event Hubs

| Tiêu chí | Apache Kafka (Self-hosted) | Azure Event Hubs |
|---|---|---|
| Quản lý | Tự setup cluster + ZooKeeper | Fully managed PaaS |
| Protocol | Kafka protocol | AMQP, Kafka-compatible |
| Throughput | Rất cao (tùy config) | Lên đến millions events/sec |
| Chi phí | Server + DevOps cost | Pay-per-use (Throughput Units) |
| Trong đồ án | ❌ Không dùng | ✅ Sử dụng |

#### Window Functions trong Stream Analytics

Đồ án sử dụng 3 loại window:

| Loại | Mô tả | Sử dụng trong đồ án |
|---|---|---|
| **Tumbling Window** | Khoảng thời gian cố định, không overlap | Tổng hợp doanh thu theo giờ |
| **Sliding Window** | Cửa sổ trượt liên tục, có overlap | Phát hiện đột biến doanh thu (5 phút) |
| **Hopping Window** | Giống Tumbling nhưng hop tùy chỉnh | Smoothing dữ liệu |

```sql
-- Tumbling Window (1 giờ)
GROUP BY TumblingWindow(hour, 1)

-- Sliding Window (5 phút)
GROUP BY SlidingWindow(minute, 5)
```

---

### 2.4 Chi tiết dịch vụ Cloud

#### Azure Event Hubs

| Thuộc tính | Chi tiết |
|---|---|
| Vai trò | Ingestion layer — thu thập event bán hàng real-time |
| Kiến trúc | Namespace → Event Hub → Consumer Group → Partitions |
| Throughput | 1 TU = 1 MB/s ingress, 2 MB/s egress |
| Retention | 1–7 ngày (Standard), 90 ngày (Dedicated) |
| Capture | Tự động lưu events vào Blob Storage dạng Avro |

#### Azure Stream Analytics

| Thuộc tính | Chi tiết |
|---|---|
| Vai trò | ETL real-time — biến đổi và tính toán dữ liệu streaming |
| Input | Event Hubs, Blob Storage (reference data) |
| Output | Azure SQL, Power BI, Blob Storage |
| Query language | SQL-like (SAQL) |
| Scaling | 1 SU ≈ 1 MB/s processing capacity |

#### Azure SQL Database

| Thuộc tính | Chi tiết |
|---|---|
| Vai trò | Storage layer — lưu trữ kết quả đã xử lý |
| Tier | S1 Standard (20 DTU) |
| Features | Auto-tuning, threat detection, geo-replication |
| Backup | Tự động 7–35 ngày, long-term retention |

#### Azure Machine Learning

| Thuộc tính | Chi tiết |
|---|---|
| Vai trò | Prediction layer — dự đoán nhu cầu bán hàng |
| Components | Workspace, Compute, Model Registry, Endpoint |
| Workflow | Data → Training → Register model → Deploy endpoint |
| Endpoint | Online Endpoint cho real-time scoring (REST API) |

#### Azure Data Factory

| Thuộc tính | Chi tiết |
|---|---|
| Vai trò | Orchestration layer — tự động hóa pipeline |
| Activities | Copy Activity, Stored Procedure Activity, ML Pipeline |
| Trigger | Schedule (daily 02:00 UTC), Event-based |
| Monitoring | Built-in run monitoring & alerts |

#### Power BI

| Thuộc tính | Chi tiết |
|---|---|
| Vai trò | Visualization layer — dashboard và báo cáo |
| Chế độ kết nối | Import (nhanh), DirectQuery (real-time), Streaming |
| Features | DAX measures, Row-Level Security (RLS), Mobile layout |
| Chia sẻ | Workspace, Publish to Web, Embed API |
