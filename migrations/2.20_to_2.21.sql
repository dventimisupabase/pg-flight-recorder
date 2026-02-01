-- =============================================================================
-- Migration: 2.20 to 2.21
-- =============================================================================
-- Description: Remove auto-detect wrappers and related infrastructure
--
-- The automated storm/regression logging system has been removed to simplify
-- the project. The core detection functions remain for on-demand analysis:
--   - detect_query_storms() - shows current storm conditions
--   - detect_regressions()  - shows current performance regressions
--
-- For monitoring, call these functions directly or integrate with your
-- existing alerting infrastructure.
-- =============================================================================

\set ON_ERROR_STOP on

BEGIN;

DO $$
DECLARE
    v_current TEXT;
BEGIN
    SELECT value INTO v_current FROM flight_recorder.config WHERE key = 'schema_version';
    IF v_current != '2.20' THEN
        RAISE EXCEPTION 'Migration 2.20->2.21 requires version 2.20, found %', v_current;
    END IF;
    RAISE NOTICE 'Migrating from 2.20 to 2.21...';
END $$;

-- Drop storm-related automation functions
DROP FUNCTION IF EXISTS flight_recorder._compute_storm_correlation(interval);
DROP FUNCTION IF EXISTS flight_recorder.auto_detect_storms();
DROP FUNCTION IF EXISTS flight_recorder.enable_storm_detection();
DROP FUNCTION IF EXISTS flight_recorder.disable_storm_detection();
DROP FUNCTION IF EXISTS flight_recorder.storm_status(interval);
DROP FUNCTION IF EXISTS flight_recorder.resolve_storm(bigint, text);
DROP FUNCTION IF EXISTS flight_recorder.resolve_storms_by_queryid(bigint, text);
DROP FUNCTION IF EXISTS flight_recorder.resolve_all_storms(text);
DROP FUNCTION IF EXISTS flight_recorder.reopen_storm(bigint);
DROP FUNCTION IF EXISTS flight_recorder._notify_storm(text, bigint, bigint, text, text, bigint, bigint, numeric, text);

-- Drop regression-related automation functions
DROP FUNCTION IF EXISTS flight_recorder.auto_detect_regressions();
DROP FUNCTION IF EXISTS flight_recorder.enable_regression_detection();
DROP FUNCTION IF EXISTS flight_recorder.disable_regression_detection();
DROP FUNCTION IF EXISTS flight_recorder.regression_status(interval);
DROP FUNCTION IF EXISTS flight_recorder.resolve_regression(bigint, text);
DROP FUNCTION IF EXISTS flight_recorder.resolve_regressions_by_queryid(bigint, text);
DROP FUNCTION IF EXISTS flight_recorder.resolve_all_regressions(text);
DROP FUNCTION IF EXISTS flight_recorder.reopen_regression(bigint);
DROP FUNCTION IF EXISTS flight_recorder._notify_regression(text, bigint, bigint, text, numeric, numeric, numeric, text);

-- Drop dashboard views
DROP VIEW IF EXISTS flight_recorder.storm_dashboard;
DROP VIEW IF EXISTS flight_recorder.regression_dashboard;

-- Drop history tables (order matters if there were FK constraints)
DROP TABLE IF EXISTS flight_recorder.query_storms;
DROP TABLE IF EXISTS flight_recorder.query_regressions;

-- Remove auto-detect related config settings (keep detect_* tuning settings)
DELETE FROM flight_recorder.config WHERE key IN (
    'storm_detection_enabled',
    'storm_detection_interval_minutes',
    'storm_min_duration_minutes',
    'storm_notify_enabled',
    'storm_notify_channel',
    'retention_storms_days',
    'regression_detection_enabled',
    'regression_detection_interval_minutes',
    'regression_min_duration_minutes',
    'regression_notify_enabled',
    'regression_notify_channel',
    'retention_regressions_days'
);

-- Unschedule cron jobs if they exist
DO $$
BEGIN
    PERFORM cron.unschedule('flight_recorder_storm')
    WHERE EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'flight_recorder_storm');
EXCEPTION
    WHEN undefined_table THEN NULL;
    WHEN undefined_function THEN NULL;
END $$;

DO $$
BEGIN
    PERFORM cron.unschedule('flight_recorder_regression')
    WHERE EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'flight_recorder_regression');
EXCEPTION
    WHEN undefined_table THEN NULL;
    WHEN undefined_function THEN NULL;
END $$;

-- Update version
UPDATE flight_recorder.config SET value = '2.21', updated_at = now()
WHERE key = 'schema_version';

COMMIT;

DO $$
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE '=== Migration Complete ===';
    RAISE NOTICE 'Now at version: 2.21';
    RAISE NOTICE '';
    RAISE NOTICE 'The automated storm/regression logging system has been removed.';
    RAISE NOTICE 'For on-demand analysis, use:';
    RAISE NOTICE '  SELECT * FROM flight_recorder.detect_query_storms();';
    RAISE NOTICE '  SELECT * FROM flight_recorder.detect_regressions();';
    RAISE NOTICE '';
END $$;
