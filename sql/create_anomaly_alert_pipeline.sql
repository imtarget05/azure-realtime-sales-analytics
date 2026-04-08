-- ============================================================
-- Anomaly Alert SQL Pipeline
-- Purpose: materialize simple anomaly signals from hourly revenue trend
-- and write standardized alerts into dbo.SalesAlerts for dashboarding.
-- ============================================================

IF OBJECT_ID('dbo.sp_GenerateAnomalyAlerts', 'P') IS NOT NULL
    DROP PROCEDURE dbo.sp_GenerateAnomalyAlerts;
GO

CREATE PROCEDURE dbo.sp_GenerateAnomalyAlerts
    @lookback_hours INT = 24,
    @z_threshold FLOAT = 2.5
AS
BEGIN
    SET NOCOUNT ON;

    IF OBJECT_ID('dbo.HourlySalesSummary', 'U') IS NULL
    BEGIN
        RAISERROR('HourlySalesSummary table not found.', 16, 1);
        RETURN;
    END;

    IF OBJECT_ID('dbo.SalesAlerts', 'U') IS NULL
    BEGIN
        RAISERROR('SalesAlerts table not found.', 16, 1);
        RETURN;
    END;

    ;WITH base AS (
        SELECT
            h.window_end,
            h.store_id,
            h.total_revenue,
            AVG(h.total_revenue) OVER (
                PARTITION BY h.store_id
                ORDER BY h.window_end
                ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING
            ) AS rolling_mean,
            STDEV(h.total_revenue) OVER (
                PARTITION BY h.store_id
                ORDER BY h.window_end
                ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING
            ) AS rolling_std
        FROM dbo.HourlySalesSummary h
        WHERE h.window_end >= DATEADD(HOUR, -@lookback_hours, SYSUTCDATETIME())
    ), scored AS (
        SELECT
            window_end,
            store_id,
            total_revenue,
            rolling_mean,
            rolling_std,
            CASE
                WHEN rolling_std IS NULL OR rolling_std = 0 THEN 0
                ELSE ABS((total_revenue - rolling_mean) / rolling_std)
            END AS z_score
        FROM base
    )
    INSERT INTO dbo.SalesAlerts (alert_time, store_id, type, value)
    SELECT
        s.window_end,
        s.store_id,
        CASE WHEN s.total_revenue > s.rolling_mean THEN 'spike' ELSE 'dip' END AS type,
        CAST(s.z_score AS FLOAT) AS value
    FROM scored s
    WHERE s.rolling_std IS NOT NULL
      AND s.z_score >= @z_threshold
      AND NOT EXISTS (
          SELECT 1
          FROM dbo.SalesAlerts a
          WHERE a.alert_time = s.window_end
            AND a.store_id = s.store_id
            AND a.type = CASE WHEN s.total_revenue > s.rolling_mean THEN 'spike' ELSE 'dip' END
      );

    SELECT @@ROWCOUNT AS inserted_alerts;
END;
GO

PRINT 'Created procedure: dbo.sp_GenerateAnomalyAlerts';
GO
