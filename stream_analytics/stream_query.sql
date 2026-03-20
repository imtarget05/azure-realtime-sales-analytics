-- ============================================================
-- Stream Analytics Query (28/03/2026 milestone)
-- Input:  SalesInput (Event Hub: sales-events)
-- Output: SalesRawOutput (Azure SQL), SalesAggOutput (Azure SQL),
--         PowerBIOutput (Power BI optional)
-- ============================================================

WITH Cleaned AS (
    SELECT
        TRY_CAST([timestamp] AS datetime) AS event_time,
        CAST(store_id AS nvarchar(20)) AS store_id,
        CAST(product_id AS nvarchar(20)) AS product_id,
        TRY_CAST(quantity AS bigint) AS quantity,
        TRY_CAST(price AS float) AS price,
        TRY_CAST(temperature AS float) AS temperature,
        CAST(weather AS nvarchar(30)) AS weather,
        TRY_CAST(holiday AS bigint) AS holiday,
        EventEnqueuedUtcTime AS enqueued_time
    FROM SalesInput
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
            WHEN product_id IN ('COKE', 'PEPSI') THEN 'Beverage'
            WHEN product_id IN ('MILK') THEN 'Dairy'
            WHEN product_id IN ('BREAD') THEN 'Bakery'
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
    TIMESTAMP BY event_time
    GROUP BY
        store_id,
        product_id,
        category,
        TumblingWindow(minute, 5)
),

Rolling15m AS (
    SELECT
        DATEADD(minute, -15, System.Timestamp()) AS window_start,
        System.Timestamp() AS window_end,
        store_id,
        product_id,
        SUM(units_sold) AS rolling_15m_units,
        SUM(revenue) AS rolling_15m_revenue
    FROM Enriched
    TIMESTAMP BY event_time
    GROUP BY
        store_id,
        product_id,
        HoppingWindow(minute, 15, 5)
)

-- 1) Raw table output
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
INTO SalesRawOutput
FROM Enriched;

-- 2) Aggregated table output (windowed features + lag)
SELECT
    a.window_start,
    a.window_end,
    a.store_id,
    a.product_id,
    a.category,
    a.units_sold,
    a.revenue,
    a.avg_price,
    a.tx_count,
    ISNULL(
        LAG(a.revenue, 1) OVER (
            PARTITION BY a.store_id, a.product_id
            LIMIT DURATION(hour, 1)
        ),
        0
    ) AS prev_5m_revenue,
    a.revenue - ISNULL(
        LAG(a.revenue, 1) OVER (
            PARTITION BY a.store_id, a.product_id
            LIMIT DURATION(hour, 1)
        ),
        0
    ) AS revenue_delta_5m,
    ISNULL(r.rolling_15m_units, 0) AS rolling_15m_units,
    ISNULL(r.rolling_15m_revenue, 0) AS rolling_15m_revenue
INTO SalesAggOutput
FROM Agg5m a TIMESTAMP BY a.window_end
LEFT JOIN Rolling15m r TIMESTAMP BY r.window_end
    ON a.store_id = r.store_id
    AND a.product_id = r.product_id
    AND DATEDIFF(minute, a, r) BETWEEN 0 AND 0;

-- 3) Optional Power BI streaming dataset
SELECT
    a.window_end AS [timestamp],
    a.store_id,
    a.category,
    a.units_sold,
    a.revenue,
    a.avg_price,
    ISNULL(r.rolling_15m_revenue, 0) AS rolling_15m_revenue
INTO PowerBIOutput
FROM Agg5m a TIMESTAMP BY a.window_end
LEFT JOIN Rolling15m r TIMESTAMP BY r.window_end
    ON a.store_id = r.store_id
    AND a.product_id = r.product_id
    AND DATEDIFF(minute, a, r) BETWEEN 0 AND 0;
