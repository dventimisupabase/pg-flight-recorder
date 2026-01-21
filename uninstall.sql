-- =============================================================================
-- Uninstall pg-flight-recorder (DATA PRESERVING)
-- =============================================================================
-- Stops cron jobs and removes functions/views, but KEEPS your data.
-- This allows you to reinstall or upgrade without losing historical data.
--
-- Run with: psql -f uninstall.sql
--
-- To completely remove including all data:
--   psql -f uninstall_full.sql
--   OR after this script: DROP SCHEMA flight_recorder CASCADE;
-- =============================================================================

\set ON_ERROR_STOP on

DO $$
DECLARE
    v_version TEXT;
    v_snapshot_count BIGINT;
    v_jobids BIGINT[];
    v_deleted_count INTEGER;
BEGIN
    -- Get current version for reporting
    BEGIN
        SELECT value INTO v_version
        FROM flight_recorder.config WHERE key = 'schema_version';
    EXCEPTION
        WHEN undefined_table THEN v_version := 'unknown';
    END;

    -- Count snapshots to show what's being preserved
    BEGIN
        SELECT count(*) INTO v_snapshot_count FROM flight_recorder.snapshots;
    EXCEPTION
        WHEN undefined_table THEN v_snapshot_count := 0;
    END;

    RAISE NOTICE E'\n=== Uninstalling Flight Recorder (v%) ===', COALESCE(v_version, 'unknown');
    RAISE NOTICE 'Preserving % snapshots and all historical data.', v_snapshot_count;
    RAISE NOTICE '';

    -- =========================================================================
    -- Step 1: Stop all cron jobs
    -- =========================================================================
    BEGIN
        SELECT array_agg(jobid) INTO v_jobids
        FROM cron.job
        WHERE jobname IN (
            'flight_recorder_snapshot',
            'flight_recorder_sample',
            'flight_recorder_flush',
            'flight_recorder_archive',
            'flight_recorder_cleanup'
        );

        PERFORM cron.unschedule('flight_recorder_snapshot')
        WHERE EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'flight_recorder_snapshot');

        PERFORM cron.unschedule('flight_recorder_sample')
        WHERE EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'flight_recorder_sample');

        PERFORM cron.unschedule('flight_recorder_flush')
        WHERE EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'flight_recorder_flush');

        PERFORM cron.unschedule('flight_recorder_archive')
        WHERE EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'flight_recorder_archive');

        PERFORM cron.unschedule('flight_recorder_cleanup')
        WHERE EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'flight_recorder_cleanup');

        IF v_jobids IS NOT NULL THEN
            DELETE FROM cron.job_run_details WHERE jobid = ANY(v_jobids);
            GET DIAGNOSTICS v_deleted_count = ROW_COUNT;
            IF v_deleted_count > 0 THEN
                RAISE NOTICE 'Cleaned up % cron job history records.', v_deleted_count;
            END IF;
        END IF;

        RAISE NOTICE 'Stopped all scheduled jobs.';
    EXCEPTION
        WHEN undefined_table THEN
            RAISE NOTICE 'pg_cron not found, skipping job cleanup.';
        WHEN undefined_function THEN
            RAISE NOTICE 'pg_cron functions not available, skipping job cleanup.';
    END;
END $$;

-- =========================================================================
-- Step 2: Drop views (these reference functions)
-- =========================================================================
DROP VIEW IF EXISTS flight_recorder.deltas CASCADE;
DROP VIEW IF EXISTS flight_recorder.recent_waits CASCADE;
DROP VIEW IF EXISTS flight_recorder.recent_activity CASCADE;
DROP VIEW IF EXISTS flight_recorder.recent_locks CASCADE;
DROP VIEW IF EXISTS flight_recorder.recent_replication CASCADE;
DROP VIEW IF EXISTS flight_recorder.capacity_dashboard CASCADE;

-- =========================================================================
-- Step 3: Drop all functions
-- =========================================================================
-- Internal functions
DROP FUNCTION IF EXISTS flight_recorder._get_config(TEXT) CASCADE;
DROP FUNCTION IF EXISTS flight_recorder._get_config_int(TEXT) CASCADE;
DROP FUNCTION IF EXISTS flight_recorder._get_config_bool(TEXT) CASCADE;
DROP FUNCTION IF EXISTS flight_recorder._get_config_interval(TEXT) CASCADE;
DROP FUNCTION IF EXISTS flight_recorder._circuit_breaker_check() CASCADE;
DROP FUNCTION IF EXISTS flight_recorder._circuit_breaker_record(TEXT, BIGINT, TEXT) CASCADE;
DROP FUNCTION IF EXISTS flight_recorder._check_schema_size() CASCADE;
DROP FUNCTION IF EXISTS flight_recorder._detect_pg_version() CASCADE;
DROP FUNCTION IF EXISTS flight_recorder._safe_query_preview(TEXT, INTEGER) CASCADE;

-- Core collection functions
DROP FUNCTION IF EXISTS flight_recorder.sample() CASCADE;
DROP FUNCTION IF EXISTS flight_recorder.snapshot() CASCADE;
DROP FUNCTION IF EXISTS flight_recorder.flush_ring_to_aggregates() CASCADE;
DROP FUNCTION IF EXISTS flight_recorder.archive_ring_samples() CASCADE;
DROP FUNCTION IF EXISTS flight_recorder.cleanup() CASCADE;

-- Configuration functions
DROP FUNCTION IF EXISTS flight_recorder.set_mode(TEXT) CASCADE;
DROP FUNCTION IF EXISTS flight_recorder.get_mode() CASCADE;
DROP FUNCTION IF EXISTS flight_recorder.apply_profile(TEXT) CASCADE;
DROP FUNCTION IF EXISTS flight_recorder.list_profiles() CASCADE;
DROP FUNCTION IF EXISTS flight_recorder.config_recommendations() CASCADE;

-- Analysis functions
DROP FUNCTION IF EXISTS flight_recorder.compare(TIMESTAMPTZ, TIMESTAMPTZ, TIMESTAMPTZ, TIMESTAMPTZ) CASCADE;
DROP FUNCTION IF EXISTS flight_recorder.wait_summary(TIMESTAMPTZ, TIMESTAMPTZ) CASCADE;
DROP FUNCTION IF EXISTS flight_recorder.table_hotspots(TIMESTAMPTZ, TIMESTAMPTZ) CASCADE;
DROP FUNCTION IF EXISTS flight_recorder.table_compare(TIMESTAMPTZ, TIMESTAMPTZ, TIMESTAMPTZ, TIMESTAMPTZ) CASCADE;
DROP FUNCTION IF EXISTS flight_recorder.index_efficiency(TIMESTAMPTZ, TIMESTAMPTZ) CASCADE;
DROP FUNCTION IF EXISTS flight_recorder.config_changes(TIMESTAMPTZ, TIMESTAMPTZ) CASCADE;
DROP FUNCTION IF EXISTS flight_recorder.db_role_config_changes(TIMESTAMPTZ, TIMESTAMPTZ) CASCADE;
DROP FUNCTION IF EXISTS flight_recorder.anomaly_report(TIMESTAMPTZ, TIMESTAMPTZ) CASCADE;
DROP FUNCTION IF EXISTS flight_recorder.export_markdown(TIMESTAMPTZ, TIMESTAMPTZ) CASCADE;
DROP FUNCTION IF EXISTS flight_recorder.export_for_upgrade() CASCADE;
DROP FUNCTION IF EXISTS flight_recorder.health_check() CASCADE;

-- Capacity planning functions
DROP FUNCTION IF EXISTS flight_recorder.capacity_status() CASCADE;
DROP FUNCTION IF EXISTS flight_recorder.growth_analysis(TEXT, INTEGER) CASCADE;
DROP FUNCTION IF EXISTS flight_recorder.forecast(TEXT, INTEGER) CASCADE;

-- Statement analysis functions
DROP FUNCTION IF EXISTS flight_recorder.statement_trends(TIMESTAMPTZ, TIMESTAMPTZ, INTEGER) CASCADE;
DROP FUNCTION IF EXISTS flight_recorder.top_queries(TIMESTAMPTZ, TIMESTAMPTZ, TEXT, INTEGER) CASCADE;

DO $$
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE '=== Flight Recorder Uninstalled ===';
    RAISE NOTICE '';
    RAISE NOTICE 'Removed:';
    RAISE NOTICE '  - All scheduled cron jobs';
    RAISE NOTICE '  - All functions and views';
    RAISE NOTICE '';
    RAISE NOTICE 'Preserved (in flight_recorder schema):';
    RAISE NOTICE '  - snapshots and all related tables';
    RAISE NOTICE '  - archive tables (activity, locks, waits)';
    RAISE NOTICE '  - aggregate tables';
    RAISE NOTICE '  - configuration settings';
    RAISE NOTICE '';
    RAISE NOTICE 'To reinstall: psql -f install.sql';
    RAISE NOTICE 'To remove data: DROP SCHEMA flight_recorder CASCADE;';
    RAISE NOTICE '';
END $$;
