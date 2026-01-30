-- =============================================================================
-- Migration: 2.15 to 2.16
-- =============================================================================
-- Description: Blast Radius Analysis
--
-- Changes:
--   - No schema changes (functions only)
--
-- New functions added via install.sql reinstall:
--   - blast_radius() - Comprehensive incident impact assessment
--   - blast_radius_report() - Human-readable ASCII-formatted report
--
-- NOTE: After running this migration, reinstall install.sql to get the new functions.
-- =============================================================================

\set ON_ERROR_STOP on

BEGIN;

-- =============================================================================
-- Step 1: Version Guard
-- =============================================================================
DO $$
DECLARE
    v_current TEXT;
    v_expected TEXT := '2.15';
    v_target TEXT := '2.16';
BEGIN
    SELECT value INTO v_current
    FROM flight_recorder.config WHERE key = 'schema_version';

    IF v_current IS NULL THEN
        RAISE EXCEPTION 'schema_version not found. Is Flight Recorder installed?';
    END IF;

    IF v_current != v_expected THEN
        RAISE EXCEPTION 'Migration 2.15->2.16 requires version %, found %', v_expected, v_current;
    END IF;

    RAISE NOTICE 'Migrating from % to %...', v_expected, v_target;
END $$;

-- =============================================================================
-- Step 2: Update Version
-- =============================================================================
UPDATE flight_recorder.config
SET value = '2.16', updated_at = now()
WHERE key = 'schema_version';

COMMIT;

-- =============================================================================
-- Step 3: Post-migration verification
-- =============================================================================
DO $$
DECLARE
    v_version TEXT;
BEGIN
    SELECT value INTO v_version
    FROM flight_recorder.config WHERE key = 'schema_version';

    RAISE NOTICE '';
    RAISE NOTICE '=== Migration Complete ===';
    RAISE NOTICE 'Now at version: %', v_version;
    RAISE NOTICE '';
    RAISE NOTICE 'Added Blast Radius Analysis functions:';
    RAISE NOTICE '  - blast_radius(start_time, end_time)';
    RAISE NOTICE '  - blast_radius_report(start_time, end_time)';
    RAISE NOTICE '';
    RAISE NOTICE 'IMPORTANT: Reinstall install.sql to get the new functions:';
    RAISE NOTICE '  psql -f install.sql';
    RAISE NOTICE '';
    RAISE NOTICE 'Example usage after reinstall:';
    RAISE NOTICE '  -- Analyze blast radius of an incident';
    RAISE NOTICE '  SELECT * FROM flight_recorder.blast_radius(';
    RAISE NOTICE '      ''2024-01-15 10:23:00'',';
    RAISE NOTICE '      ''2024-01-15 10:35:00''';
    RAISE NOTICE '  );';
    RAISE NOTICE '';
    RAISE NOTICE '  -- Generate formatted report for postmortem';
    RAISE NOTICE '  SELECT flight_recorder.blast_radius_report(';
    RAISE NOTICE '      ''2024-01-15 10:23:00'',';
    RAISE NOTICE '      ''2024-01-15 10:35:00''';
    RAISE NOTICE '  );';
    RAISE NOTICE '';
END $$;
