# 5 KỊ CH BẢN DEMO - Azure Real-Time Sales Analytics
## Thuyết trình với Giáo Viên (20-25 phút)

---

## 📊 KỊCH BẢN 1: "Realtime Revenue Surge Detection" 
**⏱️ Thời gian: 5-7 phút**  
**🎯 Mục đích:** Hook mạnh - Thể hiện tính Real-time của hệ thống

### Câu chuyện:
Một chiến dịch flash sale bắt đầu lúc 12h trưa. Hệ thống cần phát hiện doanh thu tăng đột biến (300% trong 5 phút) và cảnh báo ngay tức thì tới các store managers.

### Demo Flow (Step-by-step):

1. **Khởi động data stream (Terminal 1):**
   ```bash
   cd C:\Users\Admin\azure-realtime-sales-analytics
   python data_generator/sales_generator.py
   ```
   - Nói: "Đây là hệ thống phát sinh dữ liệu bán hàng từ 3 cửa hàng, với đặc điểm theo thời gian, thời tiết, khách hàng..."
   - Chỉ ra: Sales events đang được gửi vào Azure Event Hub

2. **Mở Power BI Dashboard (DirectQuery to Azure SQL):**
   - Mở file: `demo.pbix` (hoặc tạo live dashboard mới)
   - Chỉ ra: KPI cards hiển thị Revenue, Transaction Count, Avg Order Value — tất cả đều ổn định
   - Bảng time-series hiển thị doanh thu theo giờ/phút

3. **Mô phỏng Flash Sale (Terminal 2 — Tăng tốc độ sinh data):**
   ```bash
   # Chỉnh file config:
   # RATE_PER_MINUTE = 1000 (tăng từ 60 events/minute lên 1000)
   # BURST_ENABLED = True
   # BURST_MULTIPLIER = 3x (3 lần doanh thu bình thường)
   python data_generator/sales_generator.py
   ```
   - Nói: "Bây giờ bắt đầu flash sale. Hệ thống sẽ sinh ra 3x nhiều đơn hàng so với bình thường..."

4. **Xem Event Hub Metrics (Azure Portal):**
   - Mở Azure Event Hubs → Metrics
   - Chỉ ra biểu đồ **Incoming Messages** vọt lên từ 100 → 500 messages/sec
   - Biểu đồ **Incoming Bytes** cũng tăng đạm đà

5. **Xem Stream Analytics xử lý (Azure Portal):**
   - Mở Azure Stream Analytics → Metrics
   - Chỉ ra: **Input Events** và **Output Events** tăng cân bằng (0 data loss)
   - Metric **Watermark Delay** < 3 seconds (latency rất thấp)

6. **Power BI tự refresh (15-30 giây sau khi data sinh ra):**
   - Nhấn Refresh hoặc để Auto-refresh (nếu cấu hình)
   - **Revenue KPI card nhảy từ $5,000 → $15,000** ⬆️⬆️⬆️
   - **Transaction Count tăng từ 200 → 600**
   - **Time-series chart vọt lên thẳng như dốc**
   - **Anomaly alert trong bảng SalesAlerts hiện lên: "SPIKE detected at S01-HCM"** (màu đỏ)

7. **Xem Stream Analytics Query (SQL):**
   - Mở file: `stream_analytics/stream_query.sql`
   - Giải thích:
     - **WITH Cleaned AS**: Dữ liệu được validate, type casting từ raw JSON
     - **WITH Enriched AS**: Tính toán revenue, category mapping từ product_id
     - **WITH Agg5m AS**: Tổng hợp theo 5-phút windows (tumbling window)
     - **AnomalySignals**: Dùng Azure hàm `AnomalyDetection_SpikeAndDip()` để detect anomaly
     - **3 outputs**: SalesTransactions (raw), HourlySalesSummary (agg), SalesAlerts (anomaly)

### Ấn tượng:
> **"Full end-to-end pipeline trong < 10 giây từ lúc khách quẹt thẻ đến lúc dashboard cập nhật. Đây là TRUE Real-time Analytics!"**

---

## 🚀 KỊCH BẢN 2: "ML-Powered Viral Product Prediction"
**⏱️ Thời gian: 5-7 phút**  
🎯 Mục đích: Thể hiện sức mạnh Machine Learning và MLOps

### Câu chuyện:
Brand manager muốn biết sản phẩm nào sẽ "viral" (bán chạy) tuần tới để chuẩn bị hàng và quảng cáo, thay vì dự đoán "mù quáng".

### Demo Flow:

1. **Mở mô hình ML Training (VS Code):**
   - File: `ml/train_model.py` (đọc code để giải thích)
   - Giải thích: Sử dụng **Gradient Boosting (XGBoost)** để dự đoán số lượng bán hàng dựa trên:
     - Historical features: Giá, discount, tồn kho, category
     - Temporal features: Giờ, ngày, holiday, promotion
     - Environmental features: Thời tiết, nhiệt độ
   - Show metrics mục tiêu: F1=0.87, AUC=0.92

2. **Chạy training live (hoặc show output đã saved):**
   ```bash
   cd C:\Users\Admin\azure-realtime-sales-analytics
   python ml/train_model.py --mode train
   ```
   - Terminal sẽ show: "Training Gradient Boosting model..."
   - Model được save vào: `ml/model_output/revenue_model.pkl`
   - Metrics được log vào: `ml/model_output/model_metadata.json`

3. **Chạy inference trên dữ liệu stream:**
   ```bash
   python ml/score.py
   ```
   - Nói: "Model đang chạy dự đoán trên stream data real-time, viết kết quả vào Azure SQL table `SalesForecast`"

4. **Mở Power BI - bảng "Forecast vs Actual":**
   - Tab: `vw_ForecastVsActual` (view từ SQL)
   - Cột: `predicted_quantity`, `actual_quantity`, `forecast_error`, `model_version`
   - Biểu đồ: Scatter plot (Predicted vs Actual) - phần lớn points nằm gần đường 45 độ = model tốt
   - KPI: **Forecast Accuracy: 87%** ✅

5. **So sánh Model Versions:**
   - File: `ml/retrain_and_compare.py`
   - Chạy A/B test: Model mới vs Model cũ
   - Output: Model mới có RMSE=12.5, Model cũ có RMSE=18.3 → Model mới tốt hơn 32% 🎉
   - Decision: **AUTO-PROMOTE** (tự động đưa model mới lên production)

6. **Power BI: "Top 10 Viral Products" table:**
   - Sắp xếp theo `viral_probability` (cao nhất)
   - Ví dụ: 
     - "P001 Electronics" → 92% khả năng viral → Recommended stock: 500 units
     - "P023 Snacks" → 88% → 300 units
     - "P015 Home Decor" → 79% → 200 units
   - Manager có thể dùng thông tin này để quyết định quảng cáo & tồn kho

### Ấn tượng:
> **"Model ML không chỉ train rồi ngồi để, mà được deploy vào production, chạy trên stream data thực tế, và tự động retrain/promote khi tốt hơn. Đây là MLOps đầy đủ, không phải chỉ notebook sinh viên!"**

---

## 🔄 KỊCH BẢN 3: "Data Drift Detection & Auto-Retrain"
**⏱️ Thời gian: 4-5 phút**  
🎯 Mục đích: Differentiator - Thể hiện sự khác biệt giữa student project và production-grade system

### Câu chuyện:
Sau 2 tuần chạy, mô hình ML bắt đầu dự đoán sai (accuracy drop từ 87% → 60%) vì hành vi khách hàng thay đổi (ví dụ: giá tăng đột ngột, hàng mới xuất hiện). Hệ thống cần **tự động phát hiện** vấn đề và **tự động sửa**.

### Demo Flow:

1. **Kiểm tra hiệu suất model hiện tại:**
   ```bash
   python monitoring/model_health_check.py --dry-run
   ```
   - Output: 
     ```
     Current Model: v2_trained_2025-04-02_10:30
     Metrics:
       - MAE: 15.2 (threshold: 12.0)  ❌ ABOVE THRESHOLD
       - MAPE: 22% (max: 15%)         ❌ ABOVE THRESHOLD
       - N samples: 5,430
     Status: UNHEALTHY — Drift detected!
     ```

2. **Mô phỏng drift trong dữ liệu (giá tăng 50%, category mới):**
   ```bash
   python scripts/simulate_drift.py --multiplier 1.5 --duration 300
   ```
   - Terminal: "Simulating price shock: avg price $10 → $15 (50% increase)"
   - Tạo dữ liệu mới để train model on

3. **Chạy drift monitor:**
   ```bash
   python ml/drift_monitor.py --check
   ```
   - Output:
     ```
     📊 Drift Monitor Report
     PSI (Population Stability Index): 0.35  ⚠️ DRIFT DETECTED (threshold: 0.20)
     KL-Divergence: 0.18  ⚠️ DISTRIBUTION CHANGED
     Recommendation: RETRAIN REQUIRED
     ```
   - Log file: `ml/model_output/drift_monitor_report.json`

4. **Tự động trigger retraining pipeline:**
   ```bash
   python mlops/trigger_training_pipeline.py
   ```
   - Terminal: "Detected significant drift. Triggering auto-retrain..."
   - Script sẽ:
     - Fetch dữ liệu mới từ Azure SQL (từ 2 tuần gần đây)
     - Train model mới (Model v3) trên dữ liệu này
     - So sánh Model v3 vs Model v2 (A/B test)

5. **A/B Shadow Test Result:**
   - File: `monitoring/ab_shadow_test.py` output
   ```
   Model v3 (NEW):
     - F1: 0.89
     - RMSE: 11.2
     - MAPE: 16%
   
   Model v2 (OLD):
     - F1: 0.62
     - RMSE: 18.5
     - MAPE: 28%
   
   🎉 Model v3 is 43% better! ✅ AUTO-PROMOTE TO PRODUCTION
   ```
   - Decision logic: F1 mới > F1 cũ + 0.15 → **PROMOTE**

6. **Model Rollback Mechanism (Backup Plan):**
   - Giải thích file: `monitoring/model_health_check.py`
   - Nếu Model v3 vẫn xấu, hệ thống tự động **rollback** về Model v2
   - Công nghệ: Automatic backup before deploy + health check after 2 hours grace period

7. **Power BI: Model Health Dashboard:**
   - Bảng "Model Performance Timeline":
     - v1: F1=0.85, deployed 2025-04-02
     - v2: F1=0.87, deployed 2025-04-05 ✅ Best
     - v3: F1=0.89, deployed 2025-04-09 ✅ Current (promoted by auto-pipeline)
   - Chart: Accuracy trend (showing recovery after drift)
   - 
