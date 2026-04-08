# Demo PBIX Template - Build Checklist

Tài liệu này thay thế file nhị phân .pbix trong git và giúp tạo nhanh bản demo Power BI.

## 1. Kết nối dữ liệu

1. Kết nối Azure SQL bằng DirectQuery.
2. Thêm các bảng/view:
- dbo.vw_RealtimeDashboard
- dbo.vw_ForecastVsActual
- dbo.HourlySalesSummary
- dbo.SalesAlerts
- dbo.WeatherSalesCorrelation

## 2. Theme và model

1. Import theme: powerbi/themes/SalesAnalytics_Dark.json
2. Import DAX measures: powerbi/dax_measures.dax
3. Apply RLS roles: powerbi/rls_config.dax

## 3. 5 dashboard pages (bắt buộc)

1. Executive Overview
- KPI: Revenue, Orders, Avg Order Value, Forecast Error
- Chart: Revenue trend theo giờ

2. Realtime Monitoring
- Table/line: stream realtime từ PowerBIRealtimeOutput
- Card: ingest lag, tx_count

3. Forecast vs Actual
- Line chart: predicted_revenue vs actual_revenue
- Bar: forecast_error theo store/category

4. Weather-Sales Correlation
- Scatter: avg_temperature vs total_revenue
- Slicer: store_id, weather
- Card: correlation_signal distribution

5. Alerts and Operations
- Table: SalesAlerts mới nhất
- Matrix: số alert theo type/store/time

## 4. Naming convention

- Report name: SalesAnalytics-Demo
- Dataset name: SalesAnalytics-DirectQuery
- Workspace: Sales Analytics Demo

## 5. Publish process (tóm tắt)

1. Save local: demo.pbix
2. Publish to target workspace
3. Configure dataset credentials (Azure SQL)
4. Configure gateway nếu cần
5. Test View as Role cho RLS
6. Pin key visuals lên dashboard

## 6. Demo readiness checklist

- [ ] Dashboard có đủ 5 pages
- [ ] RLS hoạt động với ít nhất 2 user test
- [ ] Auto page refresh hoạt động (1-5 giây)
- [ ] Realtime tile cập nhật khi có event mới
- [ ] Forecast page hiển thị dữ liệu 24h gần nhất
- [ ] Correlation page đọc được WeatherSalesCorrelation
- [ ] Export PDF chạy ổn
