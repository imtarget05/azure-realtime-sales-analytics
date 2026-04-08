-- ============================================================
-- Stream Analytics Query – SINGLE PRODUCTION QUERY
-- Pipeline: Data Generators → Event Hub → THIS QUERY → Azure SQL + Power BI
--
-- Input:  SalesInput (Event Hub: sales-events)
-- Output: SalesTransactionsOutput  → Azure SQL: dbo.SalesTransactions
--         HourlySalesSummaryOutput → Azure SQL: dbo.HourlySalesSummary
--         SalesAlertsOutput        → Azure SQL: dbo.SalesAlerts
-- ============================================================

WITH Cleaned AS (
    SELECT
        TRY_CAST([timestamp] AS datetime) AS event_time,
        CAST(store_id AS nvarchar(max)) AS store_id,
        CAST(product_id AS nvarchar(max)) AS product_id,
        TRY_CAST(quantity AS bigint) AS quantity,
        TRY_CAST(price AS float) AS price,
        TRY_CAST(temperature AS float) AS temperature,
        CAST(weather AS nvarchar(max)) AS weather,
        TRY_CAST(holiday AS bigint) AS holiday,
        EventEnqueuedUtcTime AS enqueued_time
    FROM SalesInput TIMESTAMP BY [timestamp]
    WHERE
        TRY_CAST([timestamp] AS datetime) IS NOT NULL
        AND store_id IS NOT NULL
        AND product_id IS NOT NULL
        AND TRY_CAST(quantity AS bigint) IS NOT NULL
        AND TRY_CAST(price AS float) IS NOT NULL
),

Enriched AS (
    SELECT
        event_time,
        store_id,
        product_id,
        CAST(quantity AS bigint) AS units_sold,
        CAST(price AS float) AS unit_price,
        CAST(quantity * price AS float) AS revenue,
        CAST(temperature AS float) AS temperature,
        CASE
            WHEN weather IS NULL OR LTRIM(RTRIM(weather)) = '' THEN 'unknown'
            ELSE LOWER(weather)
        END AS weather,
        CASE
            WHEN holiday IS NULL THEN 0
            ELSE CAST(holiday AS bigint)
        END AS holiday,
        enqueued_time,
        CASE
            WHEN product_id IN ('COKE', 'PEPSI', 'P016', 'P017') THEN 'Beverage'
            WHEN product_id IN ('MILK', 'P019', 'P020') THEN 'Dairy'
            WHEN product_id IN ('BREAD', 'P018') THEN 'Bakery'
            WHEN product_id IN ('P001', 'P002', 'P003', 'P004', 'P005', 'P014', 'P015') THEN 'Electronics'
            WHEN product_id IN ('P006', 'P007', 'P008') THEN 'Clothing'
            WHEN product_id IN ('P009', 'P010', 'P011') THEN 'Home'
            WHEN product_id IN ('P012', 'P013') THEN 'Accessories'
            WHEN product_id IN ('P021', 'P022', 'P023') THEN 'Snacks'
            WHEN product_id IN ('P024', 'P025', 'P026') THEN 'Health & Beauty'
            WHEN product_id IN ('P027', 'P028') THEN 'Sports'
            WHEN product_id IN ('P029', 'P030') THEN 'Stationery'
            WHEN product_id = 'P031' THEN 'Toys'
            ELSE 'Other'
        END AS category
    FROM Cleaned
),

Agg5m AS (
    SELECT
        DATEADD(minute, -5, System.Timestamp()) AS window_start,
        System.Timestamp() AS window_end,
        store_id,
        product_id,
        category,
        SUM(units_sold) AS units_sold,
        SUM(revenue) AS revenue,
        AVG(unit_price) AS avg_price,
        COUNT(*) AS tx_count
    FROM Enriched
    GROUP BY
        store_id,
        product_id,
        category,
        TumblingWindow(minute, 5)
),

AnomalySignals AS (
    SELECT
        event_time,
        store_id,
        product_id,
        revenue,
        AnomalyDetection_SpikeAndDip(
            CAST(revenue AS bigint),
            95,
            120,
            'spikesanddips'
        ) OVER (PARTITION BY store_id LIMIT DURATION(minute, 30)) AS anomaly_score
    FROM Enriched
),

Alerts AS (
    SELECT
        event_time AS alert_time,
        store_id,
        CASE
            WHEN anomaly_score > 0 THEN 'spike'
            ELSE 'dip'
        END AS type,
        CAST(revenue AS float) AS value
    FROM AnomalySignals
    WHERE anomaly_score <> 0
)

-- 1) Raw transactions → Azure SQL dbo.SalesTransactions
SELECT
    event_time,
    store_id,
    product_id,
    units_sold,
    unit_price,
    revenue,
    temperature,
    weather,
    holiday,
    category,
    enqueued_time,
    DATEDIFF(second, event_time, enqueued_time) AS ingest_lag_seconds
INTO SalesTransactionsOutput
FROM Enriched;

-- 2) Aggregated summary → Azure SQL dbo.HourlySalesSummary
SELECT
    window_start,
    window_end,
    store_id,
    product_id,
    category,
    units_sold,
    revenue,
    avg_price,
    tx_count,
    CAST(0 AS float) AS prev_5m_revenue,
    CAST(0 AS float) AS revenue_delta_5m,
    CAST(0 AS bigint) AS rolling_15m_units,
    CAST(0 AS float) AS rolling_15m_revenue
INTO HourlySalesSummaryOutput
FROM Agg5m;

-- 3) Realtime anomaly alerts → Azure SQL dbo.SalesAlerts
SELECT
    alert_time,
    store_id,
    type,
    value
INTO SalesAlertsOutput
FROM Alerts;

-- 4) Realtime tiles/cards → Power BI Streaming Dataset (PowerBIRealtimeOutput)
SELECT
    System.Timestamp() AS [timestamp],
    store_id,
    category,
    CAST(SUM(tx_count) AS bigint) AS transaction_count,
    CAST(SUM(units_sold) AS bigint) AS total_quantity,
    CAST(SUM(revenue) AS float) AS total_revenue,
    CAST(CASE WHEN SUM(tx_count) = 0 THEN 0 ELSE SUM(revenue) / SUM(tx_count) END AS float) AS avg_order_value,
    CAST(AVG(avg_price) AS float) AS avg_unit_price
INTO PowerBIRealtimeOutput
FROM Agg5m
GROUP BY store_id, category, TumblingWindow(minute, 1);
