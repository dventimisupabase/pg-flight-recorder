-- =============================================================================
-- Migration: 2.18 to 2.19
-- =============================================================================
-- Description: Remove SQLite export function
--
-- The export_sql() function has been removed to simplify the project.
-- For database analysis, use: SELECT flight_recorder.report('1 hour');
-- =============================================================================

\set ON_ERROR_STOP on

BEGIN;

DO $$
DECLARE
    v_current TEXT;
BEGIN
    SELECT value INTO v_current FROM flight_recorder.config WHERE key = 'schema_version';
    IF v_current != '2.18' THEN
        RAISE EXCEPTION 'Migration 2.18->2.19 requires version 2.18, found %', v_current;
    END IF;
    RAISE NOTICE 'Migrating from 2.18 to 2.19...';
END $$;

-- Drop the SQLite export function if it exists
DROP FUNCTION IF EXISTS flight_recorder.export_sql(interval);

-- Update version
UPDATE flight_recorder.config SET value = '2.19', updated_at = now()
WHERE key = 'schema_version';

COMMIT;

DO $$
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE '=== Migration Complete ===';
    RAISE NOTICE 'Now at version: 2.19';
    RAISE NOTICE '';
    RAISE NOTICE 'The export_sql() function has been removed.';
    RAISE NOTICE 'For database analysis, use:';
    RAISE NOTICE '  SELECT flight_recorder.report(''1 hour'');';
    RAISE NOTICE '';
END $$;
