# Stream Analytics Output Mapping Checklist (Azure Portal)

Muc tieu: copy-paste nhanh de cau hinh outputs dung alias va schema.

## 1) Outputs can tao trong Stream Analytics Job

Tao cac output sau trong Azure Portal -> Stream Analytics Job -> Outputs -> Add:

1. Alias: SalesTransactionsOutput
- Sink: Azure SQL Database
- Table: dbo.SalesTransactions

2. Alias: HourlySalesSummaryOutput
- Sink: Azure SQL Database
- Table: dbo.HourlySalesSummary

3. Alias: SalesAlertsOutput
- Sink: Azure SQL Database
- Table: dbo.SalesAlerts

4. Alias: WeatherSalesCorrelationOutput
- Sink: Azure SQL Database
- Table: dbo.WeatherSalesCorrelation

5. Alias: PowerBIRealtimeOutput
- Sink: Power BI
- Workspace: Sales Analytics Demo (hoac workspace ban dang dung)
- Dataset: SalesRealtimeStream
- Table: RealtimeSales

## 2) Copy-paste aliases phai khop query

Aliases trong query phai giong 100%:

- SalesTransactionsOutput
- HourlySalesSummaryOutput
- SalesAlertsOutput
- WeatherSalesCorrelationOutput
- PowerBIRealtimeOutput

Files query lien quan:
- stream_analytics/stream_query.sql
- stream_analytics/weather_sales_correlation.sql

## 3) Power BI streaming dataset schema (copy vao tao dataset API)

Neu tao Streaming dataset trong Power BI Service (API mode), dung schema:

```json
{
  "name": "SalesRealtimeStream",
  "columns": [
    {"name": "timestamp", "dataType": "DateTime"},
    {"name": "store_id", "dataType": "String"},
    {"name": "category", "dataType": "String"},
    {"name": "transaction_count", "dataType": "Int64"},
    {"name": "total_quantity", "dataType": "Int64"},
    {"name": "total_revenue", "dataType": "Double"},
    {"name": "avg_order_value", "dataType": "Double"},
    {"name": "avg_unit_price", "dataType": "Double"}
  ]
}
```

## 4) SQL prerequisites

Chay scripts truoc khi Start job:

1. sql/create_tables.sql
2. sql/create_monitoring_tables.sql (neu dung monitoring dashboard)

Cho test visual correlation nhanh:

3. sql/insert_mock_weather_sales_correlation.sql

## 5) Inputs checklist

Inputs de query chay dung:

- SalesInput (Event Hub: sales-events)
- WeatherInput (Event Hub: weather-events)
- StockInput (Event Hub: stock-events)

## 6) Verification checklist sau khi Start

1. Stream Analytics -> Monitoring -> no errors.
2. SQL check nhanh:

```sql
SELECT TOP 10 * FROM dbo.SalesTransactions ORDER BY event_time DESC;
SELECT TOP 10 * FROM dbo.SalesAlerts ORDER BY alert_time DESC;
SELECT TOP 10 * FROM dbo.WeatherSalesCorrelation ORDER BY window_end DESC;
```

3. Power BI Realtime page:
- Realtime cards/tables co du lieu moi
- timestamp cap nhat lien tuc

## 7) Troubleshooting nhanh

- Loi "Output alias not found": sai ten alias trong Outputs vs query.
- Power BI khong cap nhat: kiem tra authentication output Power BI va dataset schema.
- SQL insert fail: kiem tra firewall Azure SQL + credentials + table ton tai.
