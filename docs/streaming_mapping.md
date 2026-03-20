# Huong dan Streaming & Data Platform (28/03/2026)

Tai lieu nay cover day du cac muc:
1. Event Hubs ingestion setup
2. Stream Analytics input/output + query windowed
3. ETL validation, cast, null handling, enrichment
4. Mapping vao Azure SQL (raw + agg)
5. Checklist verify latency

## 1) Event Hubs setup

Thong so khuyen nghi:
- Namespace SKU: Standard
- Event Hub: sales-events
- Partition count: 4 (co the scale len 8 khi throughput tang)
- Retention: 3 days (phuc vu replay + debug)
- Policy cho ASA: asa-policy (Listen + Send)
- Consumer group cho ASA: stream-analytics-cg

CLI mau:

```bash
az eventhubs namespace create \
  --resource-group rg-sales-analytics \
  --name ehns-sales-prod \
  --location eastus \
  --sku Standard

az eventhubs eventhub create \
  --resource-group rg-sales-analytics \
  --namespace-name ehns-sales-prod \
  --name sales-events \
  --partition-count 4 \
  --message-retention 3

az eventhubs namespace authorization-rule create \
  --resource-group rg-sales-analytics \
  --namespace-name ehns-sales-prod \
  --name asa-policy \
  --rights Listen Send

az eventhubs eventhub consumer-group create \
  --resource-group rg-sales-analytics \
  --namespace-name ehns-sales-prod \
  --eventhub-name sales-events \
  --name stream-analytics-cg
```

## 2) Stream Analytics job setup

- Job policy:
  - Output error policy = Drop
  - Out-of-order policy = Adjust
  - Out-of-order max delay = 5 sec
  - Late arrival max delay = 30 sec
  - Streaming Units bat dau 3 SU

- Input:
  - SalesInput = Event Hub sales-events, JSON UTF-8

- Outputs:
  - SalesRawOutput -> Azure SQL table dbo.SalesRaw
  - SalesAggOutput -> Azure SQL table dbo.SalesAgg5m
  - PowerBIOutput -> Power BI dataset (optional)
  - BlobOutput (optional) -> archive/debug stream

ARM template:
- Su dung file infrastructure/arm_streaming_job.json

Luu y:
- Trong ARM template, field sharedAccessPolicyKey dang de rong de tranh hard-code secret.
- Sau khi deploy, cap nhat key trong Input datasource hoac deploy qua parameter file secure.

## 3) Stream query

- File query chinh: stream_analytics/stream_query.sql
- Features tao ra:
  - Tumbling 5 minutes: units_sold, revenue, avg_price, tx_count
  - Hopping 15 minutes step 5: rolling_15m_units, rolling_15m_revenue
  - Lag feature: prev_5m_revenue, revenue_delta_5m
- ETL logic:
  - Drop malformed rows: TRY_CAST(...) IS NOT NULL
  - Type cast: quantity bigint, price float, timestamp datetime
  - Null handling: weather -> unknown, holiday -> 0
  - Enrichment: category map theo product_id

## 4) Azure SQL schema va mapping

Chay script tao bang:
- sql/create_streaming_tables.sql

### Mapping input -> raw (dbo.SalesRaw)

| Input field | ETL expression | SQL column |
|---|---|---|
| timestamp | TRY_CAST([timestamp] AS datetime) | event_time |
| store_id | CAST(store_id AS nvarchar(20)) | store_id |
| product_id | CAST(product_id AS nvarchar(20)) | product_id |
| quantity | CAST(quantity AS bigint) | units_sold |
| price | CAST(price AS float) | unit_price |
| quantity * price | CAST(quantity * price AS float) | revenue |
| temperature | CAST(temperature AS float) | temperature |
| weather | CASE ... THEN 'unknown' ELSE LOWER(weather) | weather |
| holiday | CASE WHEN holiday IS NULL THEN 0 ... | holiday |
| product_id | CASE map category | category |
| EventEnqueuedUtcTime | direct map | enqueued_time |
| DATEDIFF(second, event_time, enqueued_time) | derived | ingest_lag_seconds |

### Mapping agg output -> dbo.SalesAgg5m

| Query field | SQL column |
|---|---|
| window_start | window_start |
| window_end | window_end |
| store_id | store_id |
| product_id | product_id |
| category | category |
| units_sold | units_sold |
| revenue | revenue |
| avg_price | avg_price |
| tx_count | tx_count |
| prev_5m_revenue | prev_5m_revenue |
| revenue_delta_5m | revenue_delta_5m |
| rolling_15m_units | rolling_15m_units |
| rolling_15m_revenue | rolling_15m_revenue |

## 5) Trinh tu chay de demo

1. Deploy infra (CLI/ARM).
2. Chay sql/create_streaming_tables.sql tren Azure SQL.
3. Tao input/output trong ASA dung ten nhu query.
4. Paste stream_analytics/stream_query.sql vao ASA Query.
5. Start ASA job.
6. Chay producer:

```bash
python data_generator/sales_generator.py
```

7. Kiem tra du lieu vao SQL:

```sql
SELECT TOP 20 * FROM dbo.SalesRaw ORDER BY id DESC;
SELECT TOP 20 * FROM dbo.SalesAgg5m ORDER BY id DESC;
```

## 6) Tieu chi nghiem thu va latency

Tieu chi pass:
- SalesAgg5m co dong moi theo moi cua so 5 phut
- Schema dung theo mapping
- ingest_lag_seconds trung binh < N giay

SQL do latency:

```sql
SELECT
    AVG(CAST(ingest_lag_seconds AS FLOAT)) AS avg_lag_seconds,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY ingest_lag_seconds) OVER () AS p95_lag_seconds,
    MAX(ingest_lag_seconds) AS max_lag_seconds
FROM dbo.SalesRaw
WHERE inserted_at >= DATEADD(minute, -15, SYSUTCDATETIME());
```

## 7) Danh sach screenshot can chup cho deliverable

1. Event Hubs namespace + sales-events config (partition, retention)
2. Authorization policy asa-policy va rights
3. Stream Analytics job overview (SU, policy)
4. ASA input SalesInput
5. ASA outputs (SalesRawOutput, SalesAggOutput, PowerBIOutput neu co)
6. ASA query editor voi stream_analytics/stream_query.sql
7. SQL query result cua SalesAgg5m (sample rows)
