# Cơ sở lý thuyết: Thuật toán xử lý dữ liệu

## 1. Xử lý luồng dữ liệu (Stream Processing)

### 1.1 Kiến trúc Lambda
Hệ thống sử dụng kiến trúc Lambda với 2 tầng xử lý song song:
- **Speed Layer** (real-time): Event Hub → Stream Analytics → Azure SQL
- **Batch Layer** (offline): Azure SQL → ML Training → Model Deployment

### 1.2 Window Functions trong Stream Analytics
Stream Analytics sử dụng các window function để xử lý dữ liệu theo thời gian:

**Tumbling Window (cửa sổ lăn):**
```sql
SELECT store_id, SUM(revenue) as total_revenue
FROM SalesInput
TIMESTAMP BY event_time
GROUP BY store_id, TumblingWindow(minute, 5)
```
- Chia thời gian thành các khoảng cố định, không chồng lấp
- Sử dụng: tổng hợp doanh thu mỗi 5 phút

**Sliding Window (cửa sổ trượt):**
```sql
SELECT store_id, AVG(revenue) as avg_revenue
FROM SalesInput
TIMESTAMP BY event_time
GROUP BY store_id, SlidingWindow(minute, 15)
HAVING AVG(revenue) > threshold
```
- Cửa sổ di chuyển liên tục, có chồng lấp
- Sử dụng: phát hiện anomaly khi doanh thu bất thường

**Hopping Window (cửa sổ nhảy):**
- Kết hợp giữa Tumbling và Sliding
- Sử dụng: tính rolling average với step cố định

### 1.3 Anomaly Detection
Stream Analytics tích hợp hàm `AnomalyDetection_SpikeAndDip`:
```sql
SELECT event_time, revenue,
       AnomalyDetection_SpikeAndDip(revenue, 95, 120, 'spikesanddips') 
       OVER(LIMIT DURATION(minute, 120))
FROM SalesInput
```
- Sử dụng thuật toán Spectral Residual + CNN
- Confidence level: 95% (có thể điều chỉnh)
- Phát hiện spike (tăng đột biến) và dip (giảm đột biến)

## 2. Machine Learning — Gradient Boosting Regressor

### 2.1 Thuật toán GradientBoostingRegressor
Hệ thống sử dụng **Gradient Boosting Regressor** từ scikit-learn để dự đoán doanh thu và số lượng bán.

**Nguyên lý hoạt động:**
1. Bắt đầu với một mô hình yếu (weak learner) — thường là decision tree nông
2. Tính residual (sai số) giữa dự đoán và giá trị thực
3. Huấn luyện decision tree mới để dự đoán residual
4. Cộng dồn dự đoán: $F_{m}(x) = F_{m-1}(x) + \eta \cdot h_m(x)$
5. Lặp lại bước 2-4 cho đến khi đạt n_estimators hoặc hội tụ

**Tham số quan trọng:**
| Tham số | Giá trị | Ý nghĩa |
|---------|---------|---------|
| n_estimators | 300 | Số lượng decision tree |
| max_depth | 5 | Độ sâu tối đa mỗi tree |
| learning_rate | 0.1 | Tốc độ học (shrinkage) |
| subsample | 0.8 | Tỷ lệ sampling mỗi iteration |
| min_samples_split | 10 | Số mẫu tối thiểu để split |

### 2.2 Feature Engineering
14 features được sử dụng:
- **Temporal**: hour, day_of_month, month, is_weekend
- **Cyclical encoding**: hour_sin, hour_cos, month_sin, month_cos (tránh discontinuity)
- **Categorical encoding**: store_id_enc, product_id_enc, category_enc (Label Encoding)
- **External**: temperature, is_rainy, holiday

### 2.3 Đánh giá Model
Sử dụng các metric:
- **R² Score**: $R^2 = 1 - \frac{SS_{res}}{SS_{tot}} = 1 - \frac{\sum(y_i - \hat{y}_i)^2}{\sum(y_i - \bar{y})^2}$
- **MAE** (Mean Absolute Error): $MAE = \frac{1}{n}\sum|y_i - \hat{y}_i|$
- **RMSE** (Root Mean Squared Error): $RMSE = \sqrt{\frac{1}{n}\sum(y_i - \hat{y}_i)^2}$
- **Cross-Validation**: 5-fold CV để đánh giá tính ổn định

### 2.4 MLOps Pipeline
```
Data Collection → Feature Engineering → Training → Evaluation → Deployment
     ↑                                                              |
     └──────────── Drift Detection → Retrain Trigger ←─────────────┘
```
- **Model Registry**: quản lý phiên bản model (v1.0, v2.0, ...)
- **A/B Testing**: shadow test model mới trước khi promote
- **Drift Detection**: theo dõi MAE, khi vượt threshold → tự động retrain

## 3. Luồng xử lý dữ liệu end-to-end

```
[Sales Generator] ──→ [Event Hub] ──→ [Stream Analytics] ──→ [Azure SQL]
[Weather Generator]──→ [Event Hub] ──→ [Stream Analytics] ──→     ↓
[Stock Generator] ──→ [Event Hub] ──→ [Stream Analytics]     [ML Training]
                                                                   ↓
                                              [Power BI] ←── [ML Endpoint]
                                              [Web App]  ←── [Model API]
```

1. **Thu thập**: 3 generator gửi JSON events đến 3 Event Hub topics
2. **Xử lý**: Stream Analytics JOIN 3 luồng, tính aggregation, phát hiện anomaly
3. **Lưu trữ**: Kết quả ghi vào Azure SQL Database (6+ bảng)
4. **Phân tích**: ML model huấn luyện trên dữ liệu lịch sử, deploy endpoint
5. **Trực quan**: Power BI dashboard real-time + Web app prediction
