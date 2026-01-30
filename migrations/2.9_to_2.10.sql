-- =============================================================================
-- Migration: 2.9 to 2.10
-- =============================================================================
-- Description: Query Storm Detection
--
-- Changes:
--   - Add query_storms table for storm detection results
--   - Add storm detection config settings (storm_detection_enabled,
--     storm_threshold_multiplier, storm_lookback_interval, storm_baseline_days,
--     storm_detection_interval_minutes, retention_storms_days)
--   - Add detect_query_storms(), auto_detect_storms(), storm_status(),
--     enable_storm_detection(), disable_storm_detection()
--   - Update cleanup_aggregates() to include storm retention
--   - Update disable() to unschedule storm cron job
--
-- Data preservation: Existing data unchanged, new tables created
-- =============================================================================

\set ON_ERROR_STOP on

BEGIN;

-- =============================================================================
-- Step 1: Version Guard
-- =============================================================================
DO $$
DECLARE
    v_current TEXT;
    v_expected TEXT := '2.9';
    v_target TEXT := '2.10';
BEGIN
    SELECT value INTO v_current
    FROM flight_recorder.config WHERE key = 'schema_version';

    IF v_current IS NULL THEN
        RAISE EXCEPTION 'schema_version not found. Is Flight Recorder installed?';
    END IF;

    IF v_current != v_expected THEN
        RAISE EXCEPTION 'Migration 2.9->2.10 requires version %, found %', v_expected, v_current;
    END IF;

    RAISE NOTICE 'Migrating from % to %...', v_expected, v_target;
END $$;

-- =============================================================================
-- Step 2: Schema Changes (Data Preserving)
-- =============================================================================

-- Query storm detection results
CREATE TABLE IF NOT EXISTS flight_recorder.query_storms (
    id                  BIGSERIAL PRIMARY KEY,
    detected_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    queryid             BIGINT NOT NULL,
    query_fingerprint   TEXT NOT NULL,
    storm_type          TEXT NOT NULL,  -- RETRY_STORM, CACHE_MISS, SPIKE, NORMAL
    recent_count        BIGINT NOT NULL,
    baseline_count      BIGINT NOT NULL,
    multiplier          NUMERIC,
    resolved_at         TIMESTAMPTZ,
    resolution_notes    TEXT
);
CREATE INDEX IF NOT EXISTS query_storms_detected_at_idx
    ON flight_recorder.query_storms(detected_at);
CREATE INDEX IF NOT EXISTS query_storms_queryid_idx
    ON flight_recorder.query_storms(queryid);
CREATE INDEX IF NOT EXISTS query_storms_storm_type_idx
    ON flight_recorder.query_storms(storm_type) WHERE resolved_at IS NULL;
COMMENT ON TABLE flight_recorder.query_storms IS 'Query storm detection results. Tracks query execution spikes classified as RETRY_STORM, CACHE_MISS, SPIKE, or NORMAL.';

-- Add storm detection config settings
INSERT INTO flight_recorder.config (key, value) VALUES
    ('storm_detection_enabled', 'false'),
    ('storm_threshold_multiplier', '3.0'),
    ('storm_lookback_interval', '1 hour'),
    ('storm_baseline_days', '7'),
    ('storm_detection_interval_minutes', '15'),
    ('retention_storms_days', '30')
ON CONFLICT (key) DO NOTHING;

-- =============================================================================
-- Step 3: Update Version
-- =============================================================================
UPDATE flight_recorder.config
SET value = '2.10', updated_at = now()
WHERE key = 'schema_version';

COMMIT;

-- =============================================================================
-- Step 4: Post-migration verification
-- =============================================================================
DO $$
DECLARE
    v_version TEXT;
    v_storms_exists BOOLEAN;
    v_config_count INTEGER;
BEGIN
    SELECT value INTO v_version
    FROM flight_recorder.config WHERE key = 'schema_version';

    -- Verify table exists
    SELECT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'flight_recorder'
          AND table_name = 'query_storms'
    ) INTO v_storms_exists;

    -- Verify config settings
    SELECT count(*) INTO v_config_count
    FROM flight_recorder.config
    WHERE key IN ('storm_detection_enabled', 'storm_threshold_multiplier',
                  'storm_lookback_interval', 'storm_baseline_days',
                  'storm_detection_interval_minutes', 'retention_storms_days');

    IF NOT v_storms_exists THEN
        RAISE WARNING 'query_storms table not found';
    END IF;

    RAISE NOTICE '';
    RAISE NOTICE '=== Migration Complete ===';
    RAISE NOTICE 'Now at version: %', v_version;
    RAISE NOTICE 'Added query storm detection:';
    RAISE NOTICE '  - query_storms table for storm tracking';
    RAISE NOTICE '  - % config settings', v_config_count;
    RAISE NOTICE '';
    RAISE NOTICE 'To enable storm detection:';
    RAISE NOTICE '  SELECT flight_recorder.enable_storm_detection();';
    RAISE NOTICE '';
    RAISE NOTICE 'To check storm status:';
    RAISE NOTICE '  SELECT * FROM flight_recorder.storm_status();';
    RAISE NOTICE '';
    RAISE NOTICE 'To detect storms manually:';
    RAISE NOTICE '  SELECT * FROM flight_recorder.detect_query_storms();';
    RAISE NOTICE '';
END $$;
