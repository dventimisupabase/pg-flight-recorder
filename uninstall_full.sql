-- =============================================================================
-- FULL Uninstall pg-flight-recorder (DESTRUCTIVE)
-- =============================================================================
-- WARNING: This removes ALL data including historical snapshots!
-- Use this only when you want to completely remove flight_recorder.
--
-- For upgrades, use: psql -f migrations/upgrade.sql
-- To preserve data, use: psql -f uninstall.sql (stops jobs, keeps data)
--
-- Run with: psql -f uninstall_full.sql
-- =============================================================================

-- Remove all cron jobs and clean up job history
DO $$
DECLARE
    v_jobids BIGINT[];
    v_deleted_count INTEGER;
BEGIN
    -- Collect job IDs before unscheduling
    SELECT array_agg(jobid) INTO v_jobids
    FROM cron.job
    WHERE jobname IN ('flight_recorder_snapshot', 'flight_recorder_sample', 'flight_recorder_flush', 'flight_recorder_archive', 'flight_recorder_cleanup');

    -- Unschedule jobs
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

    -- Clean up job run history
    IF v_jobids IS NOT NULL THEN
        DELETE FROM cron.job_run_details WHERE jobid = ANY(v_jobids);
        GET DIAGNOSTICS v_deleted_count = ROW_COUNT;
        IF v_deleted_count > 0 THEN
            RAISE NOTICE 'Cleaned up % job run history records', v_deleted_count;
        END IF;
    END IF;
EXCEPTION
    WHEN undefined_table THEN NULL;  -- cron schema doesn't exist
    WHEN undefined_function THEN NULL;  -- cron.unschedule doesn't exist
END;
$$;

-- Drop schema and all objects
DROP SCHEMA IF EXISTS flight_recorder CASCADE;

DO $$
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE 'Flight Recorder uninstalled successfully.';
    RAISE NOTICE '';
    RAISE NOTICE 'Removed:';
    RAISE NOTICE '  - All flight recorder tables and data';
    RAISE NOTICE '  - All flight recorder functions and views';
    RAISE NOTICE '  - All scheduled cron jobs (snapshot, sample, flush, archive, cleanup)';
    RAISE NOTICE '  - All cron job execution history';
    RAISE NOTICE '';
END;
$$;
