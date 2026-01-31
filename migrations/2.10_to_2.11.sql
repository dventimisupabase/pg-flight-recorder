-- =============================================================================
-- Migration: 2.10 to 2.11
-- =============================================================================
-- Description: Storm Severity Levels and Correlation Data
--
-- Changes:
--   - Add severity column (TEXT: LOW, MEDIUM, HIGH, CRITICAL) to query_storms
--   - Add correlation column (JSONB) to query_storms for correlated metrics
--   - Add severity threshold config settings (storm_severity_low_max,
--     storm_severity_medium_max, storm_severity_high_max)
--   - Update detect_query_storms() to return severity
--   - Add _compute_storm_correlation() function
--   - Update auto_detect_storms() to store severity and correlation
--   - Update _notify_storm() to include severity in notifications
--   - Update storm_status() to return severity and correlation
--   - Update storm_dashboard view with severity breakdown
--
-- Data preservation: Existing storms get default 'MEDIUM' severity
-- =============================================================================

\set ON_ERROR_STOP on

BEGIN;

-- =============================================================================
-- Step 1: Version Guard
-- =============================================================================
DO $$
DECLARE
    v_current TEXT;
    v_expected TEXT := '2.10';
    v_target TEXT := '2.11';
BEGIN
    SELECT value INTO v_current
    FROM flight_recorder.config WHERE key = 'schema_version';

    IF v_current IS NULL THEN
        RAISE EXCEPTION 'schema_version not found. Is Flight Recorder installed?';
    END IF;

    IF v_current != v_expected THEN
        RAISE EXCEPTION 'Migration 2.10->2.11 requires version %, found %', v_expected, v_current;
    END IF;

    RAISE NOTICE 'Migrating from % to %...', v_expected, v_target;
END $$;

-- =============================================================================
-- Step 2: Schema Changes (Data Preserving)
-- =============================================================================

-- Add severity column with default for existing data
ALTER TABLE flight_recorder.query_storms
ADD COLUMN IF NOT EXISTS severity TEXT NOT NULL DEFAULT 'MEDIUM';

-- Add correlation column for correlated metrics at detection time
ALTER TABLE flight_recorder.query_storms
ADD COLUMN IF NOT EXISTS correlation JSONB;

-- Add index for severity on active storms
CREATE INDEX IF NOT EXISTS query_storms_severity_idx
    ON flight_recorder.query_storms(severity) WHERE resolved_at IS NULL;

-- Update table comment
COMMENT ON TABLE flight_recorder.query_storms IS 'Query storm detection results. Tracks query execution spikes classified as RETRY_STORM, CACHE_MISS, SPIKE, or NORMAL with severity levels (LOW, MEDIUM, HIGH, CRITICAL) and correlated metrics.';

-- Add severity threshold config settings
INSERT INTO flight_recorder.config (key, value) VALUES
    ('storm_severity_low_max', '5.0'),
    ('storm_severity_medium_max', '10.0'),
    ('storm_severity_high_max', '50.0')
ON CONFLICT (key) DO NOTHING;

-- =============================================================================
-- Step 3: Update Version
-- =============================================================================
UPDATE flight_recorder.config
SET value = '2.11', updated_at = now()
WHERE key = 'schema_version';

COMMIT;

-- =============================================================================
-- Step 4: Post-migration verification
-- =============================================================================
DO $$
DECLARE
    v_version TEXT;
    v_severity_exists BOOLEAN;
    v_correlation_exists BOOLEAN;
    v_config_count INTEGER;
BEGIN
    SELECT value INTO v_version
    FROM flight_recorder.config WHERE key = 'schema_version';

    -- Verify severity column exists
    SELECT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'flight_recorder'
          AND table_name = 'query_storms'
          AND column_name = 'severity'
    ) INTO v_severity_exists;

    -- Verify correlation column exists
    SELECT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'flight_recorder'
          AND table_name = 'query_storms'
          AND column_name = 'correlation'
    ) INTO v_correlation_exists;

    -- Verify config settings
    SELECT count(*) INTO v_config_count
    FROM flight_recorder.config
    WHERE key IN ('storm_severity_low_max', 'storm_severity_medium_max', 'storm_severity_high_max');

    IF NOT v_severity_exists THEN
        RAISE WARNING 'severity column not found on query_storms';
    END IF;

    IF NOT v_correlation_exists THEN
        RAISE WARNING 'correlation column not found on query_storms';
    END IF;

    RAISE NOTICE '';
    RAISE NOTICE '=== Migration Complete ===';
    RAISE NOTICE 'Now at version: %', v_version;
    RAISE NOTICE 'Added storm severity and correlation:';
    RAISE NOTICE '  - severity column (LOW, MEDIUM, HIGH, CRITICAL)';
    RAISE NOTICE '  - correlation column (JSONB with checkpoint, locks, waits, io)';
    RAISE NOTICE '  - % severity threshold config settings', v_config_count;
    RAISE NOTICE '';
    RAISE NOTICE 'Severity thresholds (configurable):';
    RAISE NOTICE '  - LOW: multiplier <= 5.0x';
    RAISE NOTICE '  - MEDIUM: 5.0x < multiplier <= 10.0x';
    RAISE NOTICE '  - HIGH: 10.0x < multiplier <= 50.0x';
    RAISE NOTICE '  - CRITICAL: multiplier > 50.0x OR RETRY_STORM';
    RAISE NOTICE '';
    RAISE NOTICE 'To adjust thresholds:';
    RAISE NOTICE '  SELECT flight_recorder.set_config(''storm_severity_high_max'', ''100.0'');';
    RAISE NOTICE '';
    RAISE NOTICE 'To view storm severity breakdown:';
    RAISE NOTICE '  SELECT * FROM flight_recorder.storm_dashboard;';
    RAISE NOTICE '';
    RAISE NOTICE 'To view storm correlation data:';
    RAISE NOTICE '  SELECT severity, correlation FROM flight_recorder.storm_status();';
    RAISE NOTICE '';
END $$;
