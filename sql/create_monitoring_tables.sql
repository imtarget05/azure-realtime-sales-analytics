-- ============================================================
-- Monitoring Events Table
-- Tracks drift detection, retrain triggers, model promotions
-- Used by Azure Function DriftMonitor + /dashboard UI
-- ============================================================

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'MonitoringEvents')
BEGIN
    CREATE TABLE dbo.MonitoringEvents (
        event_id      BIGINT IDENTITY(1,1) PRIMARY KEY,
        event_time    DATETIME2      DEFAULT SYSUTCDATETIME(),
        event_type    VARCHAR(50)    NOT NULL,
            -- Values: 'drift_check_ok', 'drift_detected',
            --         'retrain_started', 'retrain_completed',
            --         'model_promoted', 'model_rejected',
            --         'monitor_error'
        mae_value     FLOAT          NULL,
        threshold     FLOAT          NULL,
        model_version VARCHAR(20)    NULL,
        retrain_triggered BIT        DEFAULT 0,
        details       NVARCHAR(MAX)  NULL,   -- JSON payload
        created_at    DATETIME2      DEFAULT SYSUTCDATETIME()
    );
END;
GO

-- Indexes for dashboard queries
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'idx_monitoring_time')
    CREATE INDEX idx_monitoring_time ON dbo.MonitoringEvents(event_time DESC);
GO

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'idx_monitoring_type')
    CREATE INDEX idx_monitoring_type ON dbo.MonitoringEvents(event_type);
GO

-- View: Recent monitoring summary (last 7 days)
IF EXISTS (SELECT * FROM sys.views WHERE name = 'vw_MonitoringSummary')
    DROP VIEW dbo.vw_MonitoringSummary;
GO

CREATE VIEW dbo.vw_MonitoringSummary AS
SELECT
    event_type,
    COUNT(*)                           AS event_count,
    AVG(mae_value)                     AS avg_mae,
    MAX(mae_value)                     AS max_mae,
    MAX(event_time)                    AS last_event_time,
    SUM(CAST(retrain_triggered AS INT)) AS total_retrains
FROM dbo.MonitoringEvents
WHERE event_time >= DATEADD(DAY, -7, SYSUTCDATETIME())
GROUP BY event_type;
GO

-- ============================================================
-- Retention Policy: Auto-delete monitoring events older than 90 days
-- Run this as a scheduled SQL Agent Job or manually
-- ============================================================
IF NOT EXISTS (SELECT * FROM sys.procedures WHERE name = 'sp_CleanupMonitoringEvents')
BEGIN
    EXEC('
    CREATE PROCEDURE dbo.sp_CleanupMonitoringEvents
        @RetentionDays INT = 90
    AS
    BEGIN
        SET NOCOUNT ON;
        DECLARE @cutoff DATETIME2 = DATEADD(DAY, -@RetentionDays, SYSUTCDATETIME());
        DECLARE @deleted INT;

        DELETE FROM dbo.MonitoringEvents
        WHERE event_time < @cutoff;

        SET @deleted = @@ROWCOUNT;
        SELECT @deleted AS rows_deleted, @cutoff AS cutoff_date;
    END;
    ');
END;
GO
