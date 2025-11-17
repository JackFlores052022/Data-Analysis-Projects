/*
================================================================================
STORED PROCEDURE: Full Load - Call Center Metrics Summary
================================================================================
Purpose: 
    Complete historical reload of call center metrics.
    Used for initial setup or full data reconstruction.

Author: Jack Flores - BI Consultant
Date: 2024
Database: SQL Server
Performance: ~30-35 minutes execution time

Key Features:
    - Full historical data processing
    - TRUNCATE + INSERT strategy for complete refresh
    - Comprehensive call abandonment analysis
    - TMO (Average Handle Time) and ACW calculations
    - Optimized with indexed temporary tables

Usage:
    Execute manually for initial load or complete data refresh.
    For regular updates, use sp-incremental-load.sql instead.
================================================================================
*/

USE [DatabaseName]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[SP_FullLoad_CallMetrics_Update]
AS
BEGIN
    SET NOCOUNT ON;
    
    PRINT '*** Starting FULL LOAD - Processing all historical data ***';
    
    -- STEP 1: Create base data table (NO TIME FILTER - ALL DATA)
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
    LEFT JOIN [dbo].[clients] c ON t.client_id = c.client_id;
    -- NO WHERE CLAUSE - Processing all historical data
    
    DECLARE @TotalRows INT = @@ROWCOUNT;
    PRINT 'Total records to process: ' + CAST(@TotalRows AS VARCHAR(20));
    
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
        
        -- Detailed abandonment analysis by time ranges
        SUM(CASE WHEN b.talk_time <= 5 THEN b.TotalUnansweredCalls ELSE 0 END) AS abandoned_0_5_sec,
        SUM(CASE WHEN b.talk_time >= 6 AND b.talk_time <= 10 THEN b.TotalUnansweredCalls ELSE 0 END) AS abandoned_6_10_sec,
        SUM(CASE WHEN b.talk_time >= 11 AND b.talk_time <= 15 THEN b.TotalUnansweredCalls ELSE 0 END) AS abandoned_11_15_sec,
        SUM(CASE WHEN b.talk_time >= 16 AND b.talk_time <= 20 THEN b.TotalUnansweredCalls ELSE 0 END) AS abandoned_16_20_sec,
        SUM(CASE WHEN b.talk_time >= 21 AND b.talk_time <= 25 THEN b.TotalUnansweredCalls ELSE 0 END) AS abandoned_21_25_sec,
        SUM(CASE WHEN b.talk_time >= 26 AND b.talk_time <= 30 THEN b.TotalUnansweredCalls ELSE 0 END) AS abandoned_26_30_sec,
        SUM(CASE WHEN b.talk_time >= 31 AND b.talk_time <= 40 THEN b.TotalUnansweredCalls ELSE 0 END) AS abandoned_31_40_sec,
        SUM(CASE WHEN b.talk_time >= 41 AND b.talk_time <= 50 THEN b.TotalUnansweredCalls ELSE 0 END) AS abandoned_41_50_sec,
        SUM(CASE WHEN b.talk_time >= 51 AND b.talk_time <= 60 THEN b.TotalUnansweredCalls ELSE 0 END) AS abandoned_51_60_sec,
        SUM(CASE WHEN b.talk_time >= 61 AND b.talk_time <= 120 THEN b.TotalUnansweredCalls ELSE 0 END) AS abandoned_61_120_sec,
        SUM(CASE WHEN b.talk_time >= 121 AND b.talk_time <= 180 THEN b.TotalUnansweredCalls ELSE 0 END) AS abandoned_121_180_sec,
        SUM(CASE WHEN b.talk_time >= 181 AND b.talk_time <= 240 THEN b.TotalUnansweredCalls ELSE 0 END) AS abandoned_181_240_sec,
        SUM(CASE WHEN b.talk_time >= 241 AND b.talk_time <= 300 THEN b.TotalUnansweredCalls ELSE 0 END) AS abandoned_241_300_sec,
        SUM(CASE WHEN b.talk_time >= 301 THEN b.TotalUnansweredCalls ELSE 0 END) AS abandoned_over_300_sec,
        
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

    DECLARE @SummaryRows INT = @@ROWCOUNT;
    PRINT 'Summary records generated: ' + CAST(@SummaryRows AS VARCHAR(20));

    -- STEP 3: Full Load (TRUNCATE + INSERT)
    
    -- 3a. TRUNCATE: Clear all existing data
    TRUNCATE TABLE [dbo].[CallMetricsSummary];
    PRINT 'Existing data cleared from target table.';

    -- 3b. INSERT: Load all summarized data
    INSERT INTO [dbo].[CallMetricsSummary]
    (
        client_id, direction, marker_type, agent_id, agent_name, campaign_name,
        call_date, call_hour, total_calls, total_answered, total_unanswered,
        abandoned_0_5_sec, abandoned_6_10_sec, abandoned_11_15_sec,
        abandoned_16_20_sec, abandoned_21_25_sec, abandoned_26_30_sec,
        abandoned_31_40_sec, abandoned_41_50_sec, abandoned_51_60_sec,
        abandoned_61_120_sec, abandoned_121_180_sec, abandoned_181_240_sec,
        abandoned_241_300_sec, abandoned_over_300_sec,
        avg_handle_time_sec, avg_after_call_work_sec
    )
    SELECT 
        client_id, direction, marker_type, agent_id, agent_name, campaign_name,
        call_date, call_hour, total_calls, total_answered, total_unanswered,
        abandoned_0_5_sec, abandoned_6_10_sec, abandoned_11_15_sec,
        abandoned_16_20_sec, abandoned_21_25_sec, abandoned_26_30_sec,
        abandoned_31_40_sec, abandoned_41_50_sec, abandoned_51_60_sec,
        abandoned_61_120_sec, abandoned_121_180_sec, abandoned_181_240_sec,
        abandoned_241_300_sec, abandoned_over_300_sec,
        avg_handle_time_sec, avg_after_call_work_sec
    FROM #Summary;
    
    DECLARE @InsertCount INT = @@ROWCOUNT;
    PRINT 'Records inserted: ' + CAST(@InsertCount AS VARCHAR(20));
    PRINT '✓ FULL LOAD completed successfully.';

END
GO
