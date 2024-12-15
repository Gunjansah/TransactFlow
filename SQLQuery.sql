CREATE DATABASE FinancialTransactionDB;
GO

USE FinancialTransactionDB;
GO

-- Users Table
CREATE TABLE Users (
	UserID INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
	Username VARCHAR(50) NOT NULL UNIQUE,
	PasswordHash VARCHAR(256) NOT NULL,
	RoleID INT NOT NULL,
	Email VARCHAR(100),
	LastLoginTime DATETIME,
	CreatedAt DATETIME DEFAULT CURRENT_TIMESTAMP,
	UpdatedAt DATETIME DEFAULT CURRENT_TIMESTAMP,
	IsActive BIT DEFAULT 1 
);

--Roles Table
CREATE TABLE Roles (
    RoleID INT IDENTITY(1,1) PRIMARY KEY,
    RoleName VARCHAR(50) NOT NULL UNIQUE,
    Description VARCHAR(200),
    CreatedAt DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Transactions Table
CREATE TABLE Transactions (
    TransactionID BIGINT IDENTITY(1,1) PRIMARY KEY,
    UserID INT NOT NULL,
    TransactionType VARCHAR(20) NOT NULL,  -- The kind of transactions, for example 'STOCK', 'BOND', 'FOREX'
    Amount DECIMAL(18,2) NOT NULL,
    CurrencyCode VARCHAR(3) NOT NULL,
    Status VARCHAR(20) NOT NULL,  -- 'PENDING', 'PROCESSING', 'COMPLETED', 'FAILED'
    CreatedAt DATETIME DEFAULT CURRENT_TIMESTAMP,
    ProcessedAt DATETIME,
    VersionNumber INT DEFAULT 1,  -- For optimistic locking
    CONSTRAINT FK_Transactions_Users FOREIGN KEY (UserID) REFERENCES Users(UserID) -- FK_Transactions_Users is just a random name for constraint property
);

-- Transaction locks tracking
CREATE TABLE TransactionLocks (
    LockID BIGINT IDENTITY(1,1) PRIMARY KEY,
    ResourceID BIGINT NOT NULL,  -- Could be TransactionID or other resource
    UserID INT NOT NULL,
    LockType VARCHAR(20) NOT NULL,  -- 'READ', 'WRITE'
    AcquiredAt DATETIME DEFAULT CURRENT_TIMESTAMP,
    ReleasedAt DATETIME,
    CONSTRAINT FK_TransactionLocks_Users FOREIGN KEY (UserID) REFERENCES Users(UserID)
);

-- Queue management table
CREATE TABLE TransactionQueue (
    QueueID BIGINT IDENTITY(1,1) PRIMARY KEY,
    TransactionID BIGINT NOT NULL,
    Priority INT DEFAULT 1,  -- Higher number = higher priority
    Status VARCHAR(20) DEFAULT 'PENDING',
    EnqueueTime DATETIME DEFAULT CURRENT_TIMESTAMP,
    ProcessStartTime DATETIME,
    ProcessEndTime DATETIME,
    RetryCount INT DEFAULT 0,
    CONSTRAINT FK_TransactionQueue_Transactions 
    FOREIGN KEY (TransactionID) REFERENCES Transactions(TransactionID)
);

-- Audit logging
CREATE TABLE AuditLog (
    AuditID BIGINT IDENTITY(1,1) PRIMARY KEY,
    UserID INT,
    Action VARCHAR(100) NOT NULL,
    Details VARCHAR(MAX),
    IPAddress VARCHAR(50),
    LoggedAt DATETIME DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT FK_AuditLog_Users FOREIGN KEY (UserID) REFERENCES Users(UserID)
);

-- Performance metrics tracking
CREATE TABLE PerformanceMetrics (
    MetricID BIGINT IDENTITY(1,1) PRIMARY KEY,
    MetricType VARCHAR(50) NOT NULL,  -- 'THROUGHPUT', 'LATENCY', 'LOCK_CONTENTION'
    MetricValue DECIMAL(18,2) NOT NULL,
    CollectedAt DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- indexes for frequently accessed columns
CREATE NONCLUSTERED INDEX IX_Transactions_UserID 
ON Transactions(UserID);

CREATE NONCLUSTERED INDEX IX_Transactions_Status 
ON Transactions(Status) 
INCLUDE (TransactionID, UserID, Amount);

CREATE NONCLUSTERED INDEX IX_Transactions_Type_Status 
ON Transactions(TransactionType, Status) 
INCLUDE (TransactionID, UserID, Amount);

CREATE NONCLUSTERED INDEX IX_TransactionQueue_Status_Priority 
ON TransactionQueue(Status, Priority) 
INCLUDE (TransactionID);

-- Procedure to initiate a new transaction
CREATE PROCEDURE InitiateTransaction
    @UserID INT,
    @TransactionType VARCHAR(20),
    @Amount DECIMAL(18,2),
    @CurrencyCode VARCHAR(3)
AS
BEGIN
    SET NOCOUNT ON;
    
    BEGIN TRY
        BEGIN TRANSACTION;
            
        -- Insert new transaction
        DECLARE @TransactionID BIGINT;
        
        INSERT INTO Transactions (UserID, TransactionType, Amount, CurrencyCode, Status)
        VALUES (@UserID, @TransactionType, @Amount, @CurrencyCode, 'PENDING');
        
        SET @TransactionID = SCOPE_IDENTITY();
        
        -- Add to queue
        INSERT INTO TransactionQueue (TransactionID, Priority)
        VALUES (@TransactionID,
            CASE @TransactionType
                WHEN 'STOCK' THEN 3
                WHEN 'BOND' THEN 2
                ELSE 1
            END);
        
        -- Log the action
        INSERT INTO AuditLog (UserID, Action, Details)
        VALUES (@UserID, 'TRANSACTION_INITIATED', 'Transaction ID: ' + CAST(@TransactionID AS VARCHAR(20)));   
        COMMIT TRANSACTION;
        
        SELECT @TransactionID AS NewTransactionID;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
            
        -- Log error
        INSERT INTO AuditLog (UserID, Action, Details)
        VALUES (@UserID, 'TRANSACTION_INITIATION_FAILED', 
                'Error: ' + ERROR_MESSAGE());
                
        THROW;
    END CATCH;
END;
GO

-- Procedure to process queued transactions
CREATE PROCEDURE ProcessTransactionQueue
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @TransactionID BIGINT;
    DECLARE @QueueID BIGINT;
    
    -- Get next transaction from queue
    SELECT TOP 1 
        @TransactionID = TransactionID,
        @QueueID = QueueID
    FROM TransactionQueue
    WHERE Status = 'PENDING'
    ORDER BY Priority DESC, EnqueueTime ASC;
    
    IF @TransactionID IS NULL
        RETURN;
        
    BEGIN TRY
        BEGIN TRANSACTION;
        
        UPDATE TransactionQueue
        SET Status = 'PROCESSING',
            ProcessStartTime = CURRENT_TIMESTAMP
        WHERE QueueID = @QueueID;
        
        UPDATE Transactions
        SET Status = 'PROCESSING'
        WHERE TransactionID = @TransactionID;
        
        WAITFOR DELAY '00:00:00.100';
        
        UPDATE TransactionQueue
        SET Status = 'COMPLETED',
            ProcessEndTime = CURRENT_TIMESTAMP
        WHERE QueueID = @QueueID;
        
        UPDATE Transactions
        SET Status = 'COMPLETED',
            ProcessedAt = CURRENT_TIMESTAMP
        WHERE TransactionID = @TransactionID;
        
        COMMIT TRANSACTION;
        
        INSERT INTO AuditLog (Action, Details)
        VALUES ('TRANSACTION_PROCESSED', 'Transaction ID: ' + CAST(@TransactionID AS VARCHAR(20)));
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
            
        -- Handle deadlock by requeueing
        IF ERROR_NUMBER() = 1205
        BEGIN
            UPDATE TransactionQueue
            SET RetryCount = RetryCount + 1,
                Status = 'PENDING'
            WHERE QueueID = @QueueID;
            
            INSERT INTO AuditLog (Action, Details)
            VALUES ('TRANSACTION_DEADLOCKED', 'Transaction ID: ' + CAST(@TransactionID AS VARCHAR(20)));
        END
        ELSE
        BEGIN
            -- Log other errors
            INSERT INTO AuditLog (Action, Details)
            VALUES ('TRANSACTION_PROCESSING_FAILED', 'Error: ' + ERROR_MESSAGE());
        END;
        
			THROW;
			END CATCH;
END;
GO

-- Monitoring and Performance Tracking
-- ====================================
-- Procedure to collect performance metrics
CREATE PROCEDURE CollectPerformanceMetrics
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO PerformanceMetrics (MetricType, MetricValue)
    SELECT 
        'THROUGHPUT',
        COUNT(*) / 
        CASE 
            WHEN DATEDIFF(SECOND, MIN(CreatedAt), MAX(CreatedAt)) = 0 THEN 1
            ELSE DATEDIFF(SECOND, MIN(CreatedAt), MAX(CreatedAt))
        END
    FROM Transactions
    WHERE CreatedAt >= DATEADD(MINUTE, -5, CURRENT_TIMESTAMP);
    
    INSERT INTO PerformanceMetrics (MetricType, MetricValue)
    SELECT 
        'AVG_PROCESSING_TIME',
        AVG(DATEDIFF(MILLISECOND, CreatedAt, ProcessedAt))
    FROM Transactions
    WHERE Status = 'COMPLETED'
    AND CreatedAt >= DATEADD(MINUTE, -5, CURRENT_TIMESTAMP);
    
    INSERT INTO PerformanceMetrics (MetricType, MetricValue)
    SELECT 
        'LOCK_CONTENTION_RATE',
        COUNT(*) * 100.0 / NULLIF(COUNT(DISTINCT ResourceID), 0)
    FROM TransactionLocks
    WHERE AcquiredAt >= DATEADD(MINUTE, -5, CURRENT_TIMESTAMP);
END;
GO

--Security Procedures
-- =============================================
-- Procedure to create new user
CREATE PROCEDURE CreateUser
    @Username VARCHAR(50),
    @PasswordHash VARCHAR(256),
    @RoleID INT,
    @Email VARCHAR(100)
AS
BEGIN
    SET NOCOUNT ON;
    
    BEGIN TRY
        INSERT INTO Users (Username, PasswordHash, RoleID, Email)
        VALUES (@Username, @PasswordHash, @RoleID, @Email);
        
        INSERT INTO AuditLog (Action, Details)
        VALUES ('USER_CREATED', 'Username: ' + @Username);
    END TRY
    BEGIN CATCH
        INSERT INTO AuditLog (Action, Details)
        VALUES ('USER_CREATION_FAILED', 'Username: ' + @Username + ', Error: ' + ERROR_MESSAGE());
        THROW;
    END CATCH;
END;
GO

-- Initial Data Setup
-- =============================================

INSERT INTO Roles (RoleName, Description)
VALUES 
    ('ADMIN', 'System Administrator'),
    ('TRADER', 'Regular Trader'),
    ('AUDITOR', 'System Auditor');

-- Create admin user
EXEC CreateUser 
    @Username = 'admin',
    @PasswordHash = 'HASHED_PASSWORD_HERE',
    @RoleID = 1,
    @Email = 'admin@example.com'; 

-- Table Partitioning
-- =============================================

-- partition function for date-based partitioning
CREATE PARTITION FUNCTION TransactionDateRangePF (DATETIME)
AS RANGE RIGHT FOR VALUES (
    '2024-01-01', 
    '2024-02-01', 
    '2024-03-01', 
    '2024-04-01'
);
GO

-- partition scheme
CREATE PARTITION SCHEME TransactionDateRangePS
AS PARTITION TransactionDateRangePF
ALL TO ([PRIMARY]);
GO

-- partitioned transaction history table
CREATE TABLE TransactionHistory (
    HistoryID BIGINT IDENTITY(1,1),
    TransactionID BIGINT,
    UserID INT,
    TransactionType VARCHAR(20),
    Amount DECIMAL(18,2),
    Status VARCHAR(20),
    CreatedAt DATETIME,
    ProcessedAt DATETIME,
    PartitionDate DATETIME,
    CONSTRAINT PK_TransactionHistory PRIMARY KEY (PartitionDate, HistoryID)
) ON TransactionDateRangePS(PartitionDate);
GO

-- Lock Management
-- =============================================

-- deadlock monitoring table
CREATE TABLE DeadlockEvents (
    DeadlockID BIGINT IDENTITY(1,1) PRIMARY KEY,
    DeadlockGraph XML,
    DetectedAt DATETIME DEFAULT CURRENT_TIMESTAMP,
    Resolution VARCHAR(100)
);
GO

-- Procedure to handle deadlock detection and resolution
ALTER TABLE Transactions
ADD Priority INT DEFAULT 1;
GO

CREATE OR ALTER PROCEDURE HandleDeadlockEvent
    @TransactionID1 BIGINT,
    @TransactionID2 BIGINT,
    @DeadlockGraph XML
AS
BEGIN
    SET NOCOUNT ON;
    
    BEGIN TRY
        BEGIN TRANSACTION;
            INSERT INTO DeadlockEvents (DeadlockGraph, Resolution)
            VALUES (@DeadlockGraph, 'Automatic Resolution Attempted');
            
            UPDATE TransactionLocks
            SET ReleasedAt = CURRENT_TIMESTAMP
            WHERE ResourceID IN (
                SELECT t.TransactionID 
                FROM Transactions t
                WHERE t.TransactionID IN (@TransactionID1, @TransactionID2)
                AND t.Priority = (
                    SELECT MIN(t2.Priority)
                    FROM Transactions t2
                    WHERE t2.TransactionID IN (@TransactionID1, @TransactionID2)
                )
            );
            
            UPDATE TransactionQueue
            SET Status = 'PENDING',
                RetryCount = RetryCount + 1,
                ProcessStartTime = NULL
            WHERE TransactionID IN (@TransactionID1, @TransactionID2)
            AND Status = 'PROCESSING';
            
        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
            
        INSERT INTO AuditLog (Action, Details)
        VALUES ('DEADLOCK_RESOLUTION_FAILED', 'Error: ' + ERROR_MESSAGE());
        
        THROW;
    END CATCH;
END;
GO

--Optimistic Locking Implementation
-- =============================================

CREATE PROCEDURE ProcessTransactionOptimistic
    @TransactionID BIGINT,
    @ExpectedVersion INT
AS
BEGIN
    SET NOCOUNT ON;
    
    BEGIN TRY
        BEGIN TRANSACTION;
        
        IF NOT EXISTS (
            SELECT 1 
            FROM Transactions 
            WHERE TransactionID = @TransactionID 
            AND VersionNumber = @ExpectedVersion
        )
        BEGIN
            THROW 51000, 'Optimistic locking failure - transaction modified by another user', 1;
        END
        
        UPDATE Transactions
        SET VersionNumber = @ExpectedVersion + 1,
            Status = 'PROCESSING'
        WHERE TransactionID = @TransactionID;
        
        COMMIT TRANSACTION;
        
        INSERT INTO AuditLog (Action, Details)
        VALUES ('TRANSACTION_PROCESSED_OPTIMISTIC', 'Transaction ID: ' + CAST(@TransactionID AS VARCHAR(20)));
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
            
        INSERT INTO AuditLog (Action, Details)
        VALUES ('TRANSACTION_PROCESSING_FAILED_OPTIMISTIC', 
                'Error: ' + ERROR_MESSAGE());
                
        THROW;
    END CATCH;
END;
GO

-- Performance Monitoring and Analysis
-- ====================================

CREATE TABLE PerformanceSnapshots (
    SnapshotID BIGINT IDENTITY(1,1) PRIMARY KEY,
    SnapshotTime DATETIME DEFAULT CURRENT_TIMESTAMP,
    TransactionsPerSecond DECIMAL(18,2),
    AvgResponseTime DECIMAL(18,2),
    ActiveTransactions INT,
    DeadlockCount INT,
    LockContentionRate DECIMAL(18,2)
);
GO

-- Procedure to capture performance snapshot
CREATE PROCEDURE CapturePerformanceSnapshot
AS
BEGIN
    SET NOCOUNT ON;
    
    INSERT INTO PerformanceSnapshots (TransactionsPerSecond, AvgResponseTime, ActiveTransactions, DeadlockCount, LockContentionRate)
    SELECT
        (SELECT COUNT(*) 
         FROM Transactions 
         WHERE CreatedAt >= DATEADD(SECOND, -1, CURRENT_TIMESTAMP)) AS TPS,
        
        (SELECT AVG(DATEDIFF(MILLISECOND, CreatedAt, ProcessedAt))
         FROM Transactions
         WHERE ProcessedAt >= DATEADD(MINUTE, -1, CURRENT_TIMESTAMP)) AS AvgResponse,
        
        (SELECT COUNT(*) 
         FROM Transactions 
         WHERE Status = 'PROCESSING') AS ActiveTrans,
        
        (SELECT COUNT(*) 
         FROM DeadlockEvents 
         WHERE DetectedAt >= DATEADD(MINUTE, -1, CURRENT_TIMESTAMP)) AS Deadlocks,
        
        (SELECT COUNT(*) * 100.0 / NULLIF(COUNT(DISTINCT ResourceID), 0)
         FROM TransactionLocks
         WHERE AcquiredAt >= DATEADD(MINUTE, -1, CURRENT_TIMESTAMP)) AS ContentionRate;
END;
GO

-- Optimized query procedure
CREATE PROCEDURE GetTransactionDetailsOptimized
    @StartDate DATETIME,
    @EndDate DATETIME,
    @TransactionType VARCHAR(20) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    SELECT 
        t.TransactionID,
        t.TransactionType,
        t.Amount,
        t.Status,
        u.Username,
        tq.Priority
    FROM Transactions t WITH (INDEX(IX_Transactions_Type_Status))
    INNER HASH JOIN Users u ON t.UserID = u.UserID
    LEFT JOIN TransactionQueue tq ON t.TransactionID = tq.TransactionID
    WHERE t.CreatedAt BETWEEN @StartDate AND @EndDate
    AND (@TransactionType IS NULL OR t.TransactionType = @TransactionType)
    OPTION (OPTIMIZE FOR UNKNOWN);
END;
GO



-- Procedure to clean up old data
CREATE PROCEDURE CleanupOldData
    @RetentionDays INT = 90
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @CutoffDate DATETIME = DATEADD(DAY, -@RetentionDays, CURRENT_TIMESTAMP);
    
    INSERT INTO TransactionHistory (
        TransactionID, UserID, TransactionType, 
        Amount, Status, CreatedAt, ProcessedAt, PartitionDate
    )
    SELECT 
        TransactionID, UserID, TransactionType,
        Amount, Status, CreatedAt, ProcessedAt, CreatedAt
    FROM Transactions
    WHERE CreatedAt < @CutoffDate
    AND Status IN ('COMPLETED', 'FAILED');
    
    -- Delete moved transactions
    DELETE FROM Transactions
    WHERE CreatedAt < @CutoffDate
    AND Status IN ('COMPLETED', 'FAILED');
    
    -- Clean up old audit logs
    DELETE FROM AuditLog
    WHERE LoggedAt < @CutoffDate;
    
    -- Clean up old performance metrics
    DELETE FROM PerformanceMetrics
    WHERE CollectedAt < @CutoffDate;
END;
GO



-- ***********************************************************
-- ***********************************************************


-- Insert 1000 users
USE FinancialTransactionDB;
INSERT INTO Users (Username, PasswordHash, RoleID, Email)
SELECT 
    CONCAT('User', ROW_NUMBER() OVER (ORDER BY (SELECT NULL))) AS Username, 
    HASHBYTES('SHA2_256', CONVERT(VARCHAR(36), NEWID())) AS PasswordHash,
    1 AS RoleID,  
    CONCAT('user', ROW_NUMBER() OVER (ORDER BY (SELECT NULL)), '@example.com') AS Email
FROM master.dbo.spt_values
WHERE type = 'P';  
DELETE FROM Users;
SELECT * FROM Users;

-- Insert 100,000 transactions
INSERT INTO Transactions (UserID, TransactionType, Amount, CurrencyCode, Status)
SELECT
    ABS(CHECKSUM(NEWID()) % 1000) + 1 AS UserID,  -- Random UserID between 1 and 1000
    CASE (ABS(CHECKSUM(NEWID())) % 3)
        WHEN 0 THEN 'STOCK'
        WHEN 1 THEN 'BOND'
        ELSE 'FOREX'
    END AS TransactionType,
    CAST(RAND(CHECKSUM(NEWID())) * 1000 + 100 AS DECIMAL(18, 2)) AS Amount, -- Random Amount
    'USD' AS CurrencyCode,
    'PENDING' AS Status
FROM master.dbo.spt_values v1
CROSS JOIN master.dbo.spt_values v2
WHERE v1.number BETWEEN 1 AND 100 AND v2.number BETWEEN 1 AND 1000;


SET STATISTICS TIME ON; 
SET STATISTICS IO ON;   





-- ***********************************************************
-- ***********************************************************




DELETE FROM Transactions;
DELETE FROM Users;

-- Insert 1000 users
INSERT INTO Users (Username, PasswordHash, RoleID, Email)
SELECT 
    CONCAT('User', RIGHT('000' + CAST(ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS VARCHAR(3)), 3)) AS Username,
    HASHBYTES('SHA2_256', CONVERT(VARCHAR(36), NEWID())) AS PasswordHash,
    2 AS RoleID,  -- Assuming 2 is the RoleID for 'TRADER'
    CONCAT('user', RIGHT('000' + CAST(ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS VARCHAR(3)), 3), '@example.com') AS Email
FROM 
    (SELECT TOP 1000 ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS RowNum
     FROM master.dbo.spt_values a
     CROSS JOIN master.dbo.spt_values b) AS Numbers;

-- Insert 100,000 transactions (100 per user)
INSERT INTO Transactions (UserID, TransactionType, Amount, CurrencyCode, Status)
SELECT
    UserID,
    CASE (ABS(CHECKSUM(NEWID())) % 3)
        WHEN 0 THEN 'STOCK'
        WHEN 1 THEN 'BOND'
        ELSE 'FOREX'
    END AS TransactionType,
    CAST(RAND(CHECKSUM(NEWID())) * 1000 + 100 AS DECIMAL(18, 2)) AS Amount,
    'USD' AS CurrencyCode,
    'PENDING' AS Status
FROM 
    (SELECT UserID
     FROM Users
     CROSS JOIN (SELECT TOP 100 ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS RowNum
                 FROM master.dbo.spt_values) AS Numbers) AS UserTransactions;

-- Verify the data
SELECT COUNT(*) AS UserCount FROM Users;
SELECT COUNT(*) AS TransactionCount FROM Transactions;

-- Enable statistics
SET STATISTICS XML ON;
SET STATISTICS PROFILE ON;
SET STATISTICS IO ON;
SET STATISTICS TIME ON;

-- 1. Analyze query performance
SELECT TOP 10
    qs.total_elapsed_time / qs.execution_count AS avg_elapsed_time,
    qs.total_logical_reads / qs.execution_count AS avg_logical_reads,
    qs.execution_count,
    SUBSTRING(qt.text, (qs.statement_start_offset/2)+1,
        ((CASE qs.statement_end_offset
            WHEN -1 THEN DATALENGTH(qt.text)
            ELSE qs.statement_end_offset
        END - qs.statement_start_offset)/2) + 1) AS query_text
FROM sys.dm_exec_query_stats qs
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) qt
ORDER BY avg_elapsed_time DESC;

-- 2. Check for missing indexes
SELECT TOP 10
    DB_NAME(database_id) AS DatabaseName,
    OBJECT_NAME(object_id) AS TableName,
    equality_columns, 
    inequality_columns, 
    included_columns,
    user_seeks,
    user_scans,
    last_user_seek,
    last_user_scan,
    avg_total_user_cost,
    avg_user_impact
FROM sys.dm_db_missing_index_details AS mid
CROSS APPLY sys.dm_db_missing_index_groups AS mig
CROSS APPLY sys.dm_db_missing_index_group_stats AS migs
WHERE mid.database_id = DB_ID()
ORDER BY avg_user_impact * user_seeks DESC;

-- 3. Examine lock contention
SELECT TOP 10
    tl.resource_type,
    tl.resource_associated_entity_id,
    tl.request_mode,
    tl.request_status,
    wt.blocking_session_id,
    wt.wait_duration_ms,
    es.program_name,
    COALESCE(
        (SELECT TEXT FROM sys.dm_exec_sql_text(er.sql_handle)),
        'SQL text not available'
    ) AS sql_text
FROM sys.dm_tran_locks tl
INNER JOIN sys.dm_os_waiting_tasks wt ON tl.lock_owner_address = wt.resource_address
INNER JOIN sys.dm_exec_sessions es ON tl.request_session_id = es.session_id
LEFT JOIN sys.dm_exec_requests er ON es.session_id = er.session_id
WHERE tl.request_status = 'WAIT'
ORDER BY wt.wait_duration_ms DESC;

-- 4. Monitor resource usage
SELECT TOP 10
    qs.total_worker_time / qs.execution_count AS avg_cpu_time,
    qs.total_elapsed_time / qs.execution_count AS avg_elapsed_time,
    qs.total_logical_reads / qs.execution_count AS avg_logical_reads,
    qs.total_physical_reads / qs.execution_count AS avg_physical_reads,
    qs.total_logical_writes / qs.execution_count AS avg_logical_writes,
    qs.execution_count,
    SUBSTRING(qt.text, (qs.statement_start_offset/2)+1,
        ((CASE qs.statement_end_offset
            WHEN -1 THEN DATALENGTH(qt.text)
            ELSE qs.statement_end_offset
        END - qs.statement_start_offset)/2) + 1) AS query_text
FROM sys.dm_exec_query_stats qs
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) qt
ORDER BY avg_cpu_time DESC;

-- 5. Testing a sample query
SELECT TOP 100
    t.TransactionID,
    t.TransactionType,
    t.Amount,
    t.Status,
    u.Username
FROM Transactions t
INNER JOIN Users u ON t.UserID = u.UserID
WHERE t.TransactionType = 'STOCK'
ORDER BY t.Amount DESC;

-- Disable statistics
SET STATISTICS XML OFF;
SET STATISTICS PROFILE OFF;
SET STATISTICS IO OFF;
SET STATISTICS TIME OFF;