-- =============================================================================
-- Migration: 2.19 to 2.20
-- =============================================================================
-- Description: Remove canary queries feature
--
-- The canary queries feature has been removed to simplify the project.
-- For query performance monitoring, use statement_compare() on actual
-- application queries captured via pg_stat_statements.
-- =============================================================================

\set ON_ERROR_STOP on

BEGIN;

DO $$
DECLARE
    v_current TEXT;
BEGIN
    SELECT value INTO v_current FROM flight_recorder.config WHERE key = 'schema_version';
    IF v_current != '2.19' THEN
        RAISE EXCEPTION 'Migration 2.19->2.20 requires version 2.19, found %', v_current;
    END IF;
    RAISE NOTICE 'Migrating from 2.19 to 2.20...';
END $$;

-- Drop canary functions if they exist
DROP FUNCTION IF EXISTS flight_recorder.run_canaries();
DROP FUNCTION IF EXISTS flight_recorder.canary_status();
DROP FUNCTION IF EXISTS flight_recorder.enable_canaries();
DROP FUNCTION IF EXISTS flight_recorder.disable_canaries();

-- Drop canary tables if they exist (order matters due to FK constraint)
DROP TABLE IF EXISTS flight_recorder.canary_results;
DROP TABLE IF EXISTS flight_recorder.canaries;

-- Remove canary config settings
DELETE FROM flight_recorder.config WHERE key IN (
    'canary_enabled',
    'canary_interval_minutes',
    'canary_capture_plans',
    'retention_canary_days',
    'canary_comparison_metric'
);

-- Unschedule canary cron job if it exists
DO $$
BEGIN
    PERFORM cron.unschedule('flight_recorder_canary')
    WHERE EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'flight_recorder_canary');
EXCEPTION
    WHEN undefined_table THEN NULL;
    WHEN undefined_function THEN NULL;
END $$;

-- Update version
UPDATE flight_recorder.config SET value = '2.20', updated_at = now()
WHERE key = 'schema_version';

COMMIT;

DO $$
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE '=== Migration Complete ===';
    RAISE NOTICE 'Now at version: 2.20';
    RAISE NOTICE '';
    RAISE NOTICE 'The canary queries feature has been removed.';
    RAISE NOTICE 'For query performance monitoring, use:';
    RAISE NOTICE '  SELECT * FROM flight_recorder.statement_compare(start, end);';
    RAISE NOTICE '';
END $$;
