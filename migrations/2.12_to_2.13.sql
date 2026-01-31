-- =============================================================================
-- Migration: 2.12 to 2.13
-- =============================================================================
-- Description: Visual Performance Timeline (Sparklines and ASCII Charts)
--
-- Changes:
--   - No schema changes (functions only)
--
-- New functions added via install.sql reinstall:
--   - _sparkline() - Compact Unicode sparkline from numeric array
--   - _bar() - Horizontal progress bar
--   - timeline() - Full ASCII timeline chart for metrics
--   - sparkline_metrics() - Summary table with sparkline trends
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
    v_expected TEXT := '2.12';
    v_target TEXT := '2.13';
BEGIN
    SELECT value INTO v_current
    FROM flight_recorder.config WHERE key = 'schema_version';

    IF v_current IS NULL THEN
        RAISE EXCEPTION 'schema_version not found. Is Flight Recorder installed?';
    END IF;

    IF v_current != v_expected THEN
        RAISE EXCEPTION 'Migration 2.12->2.13 requires version %, found %', v_expected, v_current;
    END IF;

    RAISE NOTICE 'Migrating from % to %...', v_expected, v_target;
END $$;

-- =============================================================================
-- Step 2: Schema Changes
-- =============================================================================
-- No schema changes in this version.
-- Visual timeline functions (_sparkline, _bar, timeline, sparkline_metrics)
-- are pure functions that don't require any new tables or columns.

-- =============================================================================
-- Step 3: Update Version
-- =============================================================================
UPDATE flight_recorder.config
SET value = '2.13', updated_at = now()
WHERE key = 'schema_version';

COMMIT;

-- =============================================================================
-- Step 4: Post-migration verification
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
    RAISE NOTICE 'Added Visual Performance Timeline functions:';
    RAISE NOTICE '  - _sparkline(numeric[]) - Compact sparkline from array';
    RAISE NOTICE '  - _bar(value, max) - Horizontal progress bar';
    RAISE NOTICE '  - timeline(metric, duration) - ASCII timeline chart';
    RAISE NOTICE '  - sparkline_metrics(duration) - Summary with trends';
    RAISE NOTICE '';
    RAISE NOTICE 'IMPORTANT: Reinstall install.sql to get the new functions:';
    RAISE NOTICE '  psql -f install.sql';
    RAISE NOTICE '';
    RAISE NOTICE 'Example usage after reinstall:';
    RAISE NOTICE '  SELECT flight_recorder._sparkline(ARRAY[1,2,4,8,4,2,1]);';
    RAISE NOTICE '  SELECT flight_recorder._bar(75, 100);';
    RAISE NOTICE '  SELECT flight_recorder.timeline(''connections'', ''2 hours'');';
    RAISE NOTICE '  SELECT * FROM flight_recorder.sparkline_metrics(''1 hour'');';
    RAISE NOTICE '';
END $$;
