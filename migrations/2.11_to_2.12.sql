-- =============================================================================
-- Migration: 2.11 to 2.12
-- =============================================================================
-- Description: Performance Regression Detection
--
-- Changes:
--   - Add query_regressions table for regression detection results
--   - Add regression detection config settings (regression_detection_enabled,
--     regression_threshold_pct, regression_lookback_interval, regression_baseline_days,
--     regression_detection_interval_minutes, regression_min_duration_minutes,
--     regression_notify_enabled, regression_notify_channel, regression_severity_low_max,
--     regression_severity_medium_max, regression_severity_high_max, retention_regressions_days)
--
-- New functions added via install.sql reinstall:
--   - detect_regressions() - Core detection function
--   - _diagnose_regression_causes() - Root cause analysis
--   - _notify_regression() - pg_notify alerts
--   - auto_detect_regressions() - Scheduled detection
--   - regression_status() - Status monitoring
--   - enable/disable_regression_detection() - Feature toggle
--   - resolve_regression() - Resolution functions
--   - regression_dashboard view - At-a-glance dashboard
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
    v_expected TEXT := '2.11';
    v_target TEXT := '2.12';
BEGIN
    SELECT value INTO v_current
    FROM flight_recorder.config WHERE key = 'schema_version';

    IF v_current IS NULL THEN
        RAISE EXCEPTION 'schema_version not found. Is Flight Recorder installed?';
    END IF;

    IF v_current != v_expected THEN
        RAISE EXCEPTION 'Migration 2.11->2.12 requires version %, found %', v_expected, v_current;
    END IF;

    RAISE NOTICE 'Migrating from % to %...', v_expected, v_target;
END $$;

-- =============================================================================
-- Step 2: Schema Changes
-- =============================================================================

-- Create query_regressions table
CREATE TABLE IF NOT EXISTS flight_recorder.query_regressions (
    id                  BIGSERIAL PRIMARY KEY,
    detected_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    queryid             BIGINT NOT NULL,
    query_fingerprint   TEXT NOT NULL,
    severity            TEXT NOT NULL DEFAULT 'MEDIUM',  -- LOW, MEDIUM, HIGH, CRITICAL
    baseline_avg_ms     NUMERIC NOT NULL,
    current_avg_ms      NUMERIC NOT NULL,
    change_pct          NUMERIC NOT NULL,
    correlation         JSONB,  -- Correlated metrics at detection time
    probable_causes     TEXT[],
    resolved_at         TIMESTAMPTZ,
    resolution_notes    TEXT
);

-- Add indexes
CREATE INDEX IF NOT EXISTS query_regressions_detected_at_idx
    ON flight_recorder.query_regressions(detected_at);
CREATE INDEX IF NOT EXISTS query_regressions_queryid_idx
    ON flight_recorder.query_regressions(queryid);
CREATE INDEX IF NOT EXISTS query_regressions_severity_idx
    ON flight_recorder.query_regressions(severity) WHERE resolved_at IS NULL;

-- Add table comment
COMMENT ON TABLE flight_recorder.query_regressions IS 'Performance regression detection results. Tracks queries whose execution time has increased significantly compared to historical baseline with severity levels (LOW, MEDIUM, HIGH, CRITICAL) and correlated metrics.';

-- Add regression detection config settings
INSERT INTO flight_recorder.config (key, value) VALUES
    ('regression_detection_enabled', 'false'),
    ('regression_threshold_pct', '50.0'),
    ('regression_lookback_interval', '1 hour'),
    ('regression_baseline_days', '7'),
    ('regression_detection_interval_minutes', '60'),
    ('regression_min_duration_minutes', '30'),
    ('regression_notify_enabled', 'true'),
    ('regression_notify_channel', 'flight_recorder_regressions'),
    ('regression_severity_low_max', '200.0'),
    ('regression_severity_medium_max', '500.0'),
    ('regression_severity_high_max', '1000.0'),
    ('retention_regressions_days', '30')
ON CONFLICT (key) DO NOTHING;

-- =============================================================================
-- Step 3: Update Version
-- =============================================================================
UPDATE flight_recorder.config
SET value = '2.12', updated_at = now()
WHERE key = 'schema_version';

COMMIT;

-- =============================================================================
-- Step 4: Post-migration verification
-- =============================================================================
DO $$
DECLARE
    v_version TEXT;
    v_table_exists BOOLEAN;
    v_config_count INTEGER;
BEGIN
    SELECT value INTO v_version
    FROM flight_recorder.config WHERE key = 'schema_version';

    -- Verify table exists
    SELECT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'flight_recorder'
          AND table_name = 'query_regressions'
    ) INTO v_table_exists;

    -- Verify config settings
    SELECT count(*) INTO v_config_count
    FROM flight_recorder.config
    WHERE key LIKE 'regression_%' OR key = 'retention_regressions_days';

    IF NOT v_table_exists THEN
        RAISE WARNING 'query_regressions table not found';
    END IF;

    RAISE NOTICE '';
    RAISE NOTICE '=== Migration Complete ===';
    RAISE NOTICE 'Now at version: %', v_version;
    RAISE NOTICE '';
    RAISE NOTICE 'Added performance regression detection:';
    RAISE NOTICE '  - query_regressions table';
    RAISE NOTICE '  - % regression config settings', v_config_count;
    RAISE NOTICE '';
    RAISE NOTICE 'IMPORTANT: Reinstall install.sql to get the new functions:';
    RAISE NOTICE '  psql -f install.sql';
    RAISE NOTICE '';
    RAISE NOTICE 'Severity thresholds (percentage change):';
    RAISE NOTICE '  - LOW: <= 200%% change';
    RAISE NOTICE '  - MEDIUM: 200%% - 500%% change';
    RAISE NOTICE '  - HIGH: 500%% - 1000%% change';
    RAISE NOTICE '  - CRITICAL: > 1000%% change';
    RAISE NOTICE '';
    RAISE NOTICE 'To enable regression detection after reinstall:';
    RAISE NOTICE '  SELECT flight_recorder.enable_regression_detection();';
    RAISE NOTICE '';
    RAISE NOTICE 'To manually detect regressions:';
    RAISE NOTICE '  SELECT * FROM flight_recorder.detect_regressions();';
    RAISE NOTICE '';
    RAISE NOTICE 'To view regression dashboard:';
    RAISE NOTICE '  SELECT * FROM flight_recorder.regression_dashboard;';
    RAISE NOTICE '';
END $$;
