-- ============================================================
-- Validation Logs Table & Stored Procedure
-- For tracking invalid events rejected by ValidateSalesEvent function
-- ============================================================

-- Table for logging invalid events
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'ValidationLogs')
BEGIN
    CREATE TABLE dbo.ValidationLogs (
        log_id INT IDENTITY(1,1) PRIMARY KEY,
        logged_at DATETIME2 DEFAULT GETUTCDATE(),
        error_reason NVARCHAR(500),
        raw_data NVARCHAR(MAX),
        event_timestamp DATETIME2 NULL,
        store_id NVARCHAR(50) NULL,
        product_id NVARCHAR(50) NULL
    );
    
    CREATE INDEX idx_logged_at ON dbo.ValidationLogs(logged_at DESC);
    CREATE INDEX idx_error_reason ON dbo.ValidationLogs(error_reason);
    
    PRINT 'Created table: dbo.ValidationLogs';
END
ELSE
BEGIN
    PRINT 'Table dbo.ValidationLogs already exists';
END

-- Stored procedure for batch insert of invalid events (used by Azure Function output binding)
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_NAME = 'sp_LogValidationError')
    DROP PROCEDURE sp_LogValidationError;
GO

CREATE PROCEDURE dbo.sp_LogValidationError
    @error NVARCHAR(500),
    @raw_data NVARCHAR(MAX)
AS
BEGIN
    INSERT INTO dbo.ValidationLogs (error_reason, raw_data)
    VALUES (@error, @raw_data);
END
GO

PRINT 'Created procedure: dbo.sp_LogValidationError';

-- View for quick analysis
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_NAME = 'vw_ValidationErrors')
    DROP VIEW vw_ValidationErrors;
GO

CREATE VIEW dbo.vw_ValidationErrors AS
SELECT
    log_id,
    logged_at,
    error_reason,
    COUNT(*) OVER (PARTITION BY error_reason) AS error_frequency,
    ROW_NUMBER() OVER (PARTITION BY error_reason ORDER BY logged_at DESC) AS rank_by_error
FROM dbo.ValidationLogs
GO

PRINT 'Created view: dbo.vw_ValidationErrors';
