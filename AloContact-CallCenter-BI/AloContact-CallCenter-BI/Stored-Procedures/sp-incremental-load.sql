/*
================================================================================
STORED PROCEDURE: Incremental Load - Call Center Metrics Summary
================================================================================
Purpose: 
    Incrementally updates call center metrics for the last 2 hours.
    Designed for real-time dashboard updates every 5 minutes.

Author: Jack Flores - BI Consultant
Date: 2024
Database: SQL Server
Performance: ~3-5 minutes execution time

Key Features:
    - Incremental processing (last 2 hours only)
    - DELETE + INSERT strategy to avoid duplicates
    - Indexed temporary tables for performance
    - Call abandonment analysis by time ranges
    - TMO (Average Handle Time) and ACW calculations
================================================================================
*/

USE [DatabaseName]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[SP_Incremental_CallMetrics_Update]
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Time window configuration
    DECLARE @HoursBack INT = 2;
    DECLARE @TimeThreshold DATETIME = DATEADD(HOUR, -@HoursBack, GETDATE());
    
    -- STEP 1: Create base data table (INCREMENTAL FILTER)
    DROP TABLE IF EXISTS #BaseMaster;

    SELECT 
        CASE WHEN t.agent_id <> '\N' AND t.answer_time <> '0000-00-00 00:00:00' 
             THEN 1 ELSE 0 END AS TotalAnsweredCalls,
        CASE WHEN (CASE WHEN t.agent_id <> '\N' AND t.answer_time <> '0000-00-00 00:00:00' 
             THEN 1 ELSE 0 END) = 1 THEN 0 ELSE 1 END AS TotalUnansweredCalls,
        c.name AS client_name,
        t.client_id,
        t.create_date,
        t.start_time,
        t.answer_time,
        t.end_time,
        t.queue_date,
        t.queue_answer,
        t.source_number,
        t.status,
        t.campaign_id,
        t.campaign_name,
        t.direction,
        t.agent_id,
        t.agent_name,
        t.talk_time,
        t.queue_time,
        t.ring_time,
        t.is_answered,
        t.marker_type,
        t.post_call_time,
        t.typification_code,
        t.typification_desc
    INTO #BaseMaster
    FROM [dbo].[call_tickets] t WITH(NOLOCK)
    LEFT JOIN [dbo].[clients] c ON t.client_id = c.client_id
    WHERE t.start_time >= @TimeThreshold;  -- 🔑 Incremental filter
    
    -- Check if there's data to process
    DECLARE @RowCount INT = @@ROWCOUNT;
    
    IF @RowCount = 0
    BEGIN
        PRINT 'INFO: No calls in the last ' + CAST(@HoursBack AS VARCHAR(10)) + ' hours. No update needed.';
        RETURN;
    END
    
    PRINT 'Processing ' + CAST(@RowCount AS VARCHAR(10)) + ' calls from the last ' + CAST(@HoursBack AS VARCHAR(10)) + ' hours.';
    
    CREATE INDEX IDX_create_date ON #BaseMaster (create_date, marker_type, agent_id, agent_name);

    -- STEP 2: Create summarized metrics table
    DROP TABLE IF EXISTS #Summary;
    
    SELECT
        b.client_id,
        b.direction,
        b.marker_type,
        b.agent_id,
        ISNULL(CAST(b.agent_name AS NVARCHAR(255)), 'N/A') AS agent_name,
        ISNULL(CAST(b.campaign_name AS NVARCHAR(255)), 'N/A') AS campaign_name,
        CAST(b.start_time AS DATE) AS call_date,
        DATEPART(HOUR, b.start_time) AS call_hour,
        
        -- Call volume metrics
        COUNT(1) AS total_calls,
        SUM(b.TotalAnsweredCalls) AS total_answered,
        SUM(b.TotalUnansweredCalls) AS total_unanswered,
        
        -- Abandonment analysis by time ranges (in seconds)
        SUM(CASE WHEN b.talk_time <= 5 THEN b.TotalUnansweredCalls ELSE 0 END) AS abandoned_0_5_sec,
        SUM(CASE WHEN b.talk_time >= 6 AND b.talk_time <= 10 THEN b.TotalUnansweredCalls ELSE 0 END) AS abandoned_6_10_sec,
        SUM(CASE WHEN b.talk_time >= 11 AND b.talk_time <= 15 THEN b.TotalUnansweredCalls ELSE 0 END) AS abandoned_11_15_sec,
        SUM(CASE WHEN b.talk_time >= 16 AND b.talk_time <= 20 THEN b.TotalUnansweredCalls ELSE 0 END) AS abandoned_16_20_sec,
        SUM(CASE WHEN b.talk_time >= 21 AND b.talk_time <= 30 THEN b.TotalUnansweredCalls ELSE 0 END) AS abandoned_21_30_sec,
        SUM(CASE WHEN b.talk_time >= 31 AND b.talk_time <= 60 THEN b.TotalUnansweredCalls ELSE 0 END) AS abandoned_31_60_sec,
        SUM(CASE WHEN b.talk_time >= 61 AND b.talk_time <= 120 THEN b.TotalUnansweredCalls ELSE 0 END) AS abandoned_61_120_sec,
        SUM(CASE WHEN b.talk_time >= 121 THEN b.TotalUnansweredCalls ELSE 0 END) AS abandoned_over_120_sec,
        
        -- Performance metrics
        AVG(CAST(DATEDIFF(SECOND, b.start_time, b.end_time) AS DECIMAL(10,2))) AS avg_handle_time_sec,
        AVG(CASE WHEN b.is_answered = 1 THEN CAST(b.post_call_time AS DECIMAL(10,2)) ELSE NULL END) AS avg_after_call_work_sec
    INTO #Summary
    FROM #BaseMaster b
    WHERE b.status <> 'RECORDING'
    GROUP BY 
        b.client_id, 
        b.direction, 
        b.marker_type, 
        b.agent_id,
        ISNULL(CAST(b.agent_name AS NVARCHAR(255)), 'N/A'),
        ISNULL(CAST(b.campaign_name AS NVARCHAR(255)), 'N/A'),
        CAST(b.start_time AS DATE),
        DATEPART(HOUR, b.start_time);

    CREATE INDEX IDX_Summary ON #Summary (
        client_id, direction, marker_type, agent_id,
        agent_name, campaign_name, call_date, call_hour
    );

    -- STEP 3: Incremental Update (DELETE + INSERT)
    
    -- 3a. DELETE: Remove old records for the time window
    DECLARE @DeleteCount INT;
    
    DELETE T
    FROM [dbo].[CallMetricsSummary] AS T
    INNER JOIN #Summary AS S ON
        T.client_id = S.client_id AND
        T.direction = S.direction AND
        T.marker_type = S.marker_type AND
        T.agent_id = S.agent_id AND
        T.call_date = S.call_date AND
        T.call_hour = S.call_hour AND
        ISNULL(T.agent_name, 'N/A') = S.agent_name AND
        ISNULL(T.campaign_name, 'N/A') = S.campaign_name;
    
    SET @DeleteCount = @@ROWCOUNT;
    PRINT 'Records deleted (old): ' + CAST(@DeleteCount AS VARCHAR(10));

    -- 3b. INSERT: Add updated data
    INSERT INTO [dbo].[CallMetricsSummary]
    (
        client_id, direction, marker_type, agent_id, agent_name, campaign_name,
        call_date, call_hour, total_calls, total_answered, total_unanswered,
        abandoned_0_5_sec, abandoned_6_10_sec, abandoned_11_15_sec, 
        abandoned_16_20_sec, abandoned_21_30_sec, abandoned_31_60_sec,
        abandoned_61_120_sec, abandoned_over_120_sec,
        avg_handle_time_sec, avg_after_call_work_sec
    )
    SELECT 
        client_id, direction, marker_type, agent_id, agent_name, campaign_name,
        call_date, call_hour, total_calls, total_answered, total_unanswered,
        abandoned_0_5_sec, abandoned_6_10_sec, abandoned_11_15_sec,
        abandoned_16_20_sec, abandoned_21_30_sec, abandoned_31_60_sec,
        abandoned_61_120_sec, abandoned_over_120_sec,
        avg_handle_time_sec, avg_after_call_work_sec
    FROM #Summary;
    
    DECLARE @InsertCount INT = @@ROWCOUNT;
    PRINT 'Records inserted (new): ' + CAST(@InsertCount AS VARCHAR(10));
    PRINT '✓ Incremental update completed successfully.';

END
GO
