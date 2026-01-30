-- =============================================================================
-- Migration: 2.14 to 2.15
-- =============================================================================
-- Description: Time-Travel Debugging
--
-- Changes:
--   - No schema changes (functions only)
--
-- New functions added via install.sql reinstall:
--   - _interpolate_metric() - Linear interpolation helper
--   - what_happened_at() - Forensic analysis at any timestamp
--   - incident_timeline() - Unified event timeline for incidents
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
    v_expected TEXT := '2.14';
    v_target TEXT := '2.15';
BEGIN
    SELECT value INTO v_current
    FROM flight_recorder.config WHERE key = 'schema_version';

    IF v_current IS NULL THEN
        RAISE EXCEPTION 'schema_version not found. Is Flight Recorder installed?';
    END IF;

    IF v_current != v_expected THEN
        RAISE EXCEPTION 'Migration 2.14->2.15 requires version %, found %', v_expected, v_current;
    END IF;

    RAISE NOTICE 'Migrating from % to %...', v_expected, v_target;
END $$;

-- =============================================================================
-- Step 2: Update Version
-- =============================================================================
UPDATE flight_recorder.config
SET value = '2.15', updated_at = now()
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
    RAISE NOTICE 'Added Time-Travel Debugging functions:';
    RAISE NOTICE '  - _interpolate_metric(before, time_before, after, time_after, target)';
    RAISE NOTICE '  - what_happened_at(timestamp, context_window)';
    RAISE NOTICE '  - incident_timeline(start_time, end_time)';
    RAISE NOTICE '';
    RAISE NOTICE 'IMPORTANT: Reinstall install.sql to get the new functions:';
    RAISE NOTICE '  psql -f install.sql';
    RAISE NOTICE '';
    RAISE NOTICE 'Example usage after reinstall:';
    RAISE NOTICE '  -- What happened at a specific moment?';
    RAISE NOTICE '  SELECT * FROM flight_recorder.what_happened_at(''2024-01-15 10:23:47'');';
    RAISE NOTICE '';
    RAISE NOTICE '  -- Reconstruct incident timeline';
    RAISE NOTICE '  SELECT * FROM flight_recorder.incident_timeline(';
    RAISE NOTICE '      now() - interval ''2 hours'',';
    RAISE NOTICE '      now() - interval ''1 hour''';
    RAISE NOTICE '  );';
    RAISE NOTICE '';
END $$;
