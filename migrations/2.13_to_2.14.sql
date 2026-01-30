-- =============================================================================
-- Migration: 2.13 to 2.14
-- =============================================================================
-- Description: Performance Forecasting
--
-- Changes:
--   - No schema changes (functions only)
--   - New config settings for forecasting
--
-- New functions added via install.sql reinstall:
--   - _linear_regression() - Least-squares linear regression helper
--   - forecast() - Single metric forecast with depletion prediction
--   - forecast_summary() - Multi-metric forecast dashboard
--   - _notify_forecast() - Internal pg_notify helper
--   - check_forecast_alerts() - Scheduled alert checker for pg_cron
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
    v_expected TEXT := '2.13';
    v_target TEXT := '2.14';
BEGIN
    SELECT value INTO v_current
    FROM flight_recorder.config WHERE key = 'schema_version';

    IF v_current IS NULL THEN
        RAISE EXCEPTION 'schema_version not found. Is Flight Recorder installed?';
    END IF;

    IF v_current != v_expected THEN
        RAISE EXCEPTION 'Migration 2.13->2.14 requires version %, found %', v_expected, v_current;
    END IF;

    RAISE NOTICE 'Migrating from % to %...', v_expected, v_target;
END $$;

-- =============================================================================
-- Step 2: Add Config Settings
-- =============================================================================
INSERT INTO flight_recorder.config (key, value) VALUES
    ('forecast_enabled', 'true'),
    ('forecast_lookback_days', '7'),
    ('forecast_window_days', '7'),
    ('forecast_alert_enabled', 'false'),
    ('forecast_alert_threshold', '3 days'),
    ('forecast_notify_channel', 'flight_recorder_forecasts'),
    ('forecast_disk_capacity_gb', '100'),
    ('forecast_min_samples', '10'),
    ('forecast_min_confidence', '0.5')
ON CONFLICT (key) DO NOTHING;

-- =============================================================================
-- Step 3: Update Version
-- =============================================================================
UPDATE flight_recorder.config
SET value = '2.14', updated_at = now()
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
    RAISE NOTICE 'Added Performance Forecasting functions:';
    RAISE NOTICE '  - _linear_regression(x[], y[]) - Linear regression helper';
    RAISE NOTICE '  - forecast(metric, lookback, window) - Single metric forecast';
    RAISE NOTICE '  - forecast_summary(lookback, window) - Multi-metric dashboard';
    RAISE NOTICE '  - check_forecast_alerts() - Scheduled alert checker';
    RAISE NOTICE '';
    RAISE NOTICE 'IMPORTANT: Reinstall install.sql to get the new functions:';
    RAISE NOTICE '  psql -f install.sql';
    RAISE NOTICE '';
    RAISE NOTICE 'Example usage after reinstall:';
    RAISE NOTICE '  SELECT * FROM flight_recorder.forecast(''db_size'');';
    RAISE NOTICE '  SELECT * FROM flight_recorder.forecast_summary();';
    RAISE NOTICE '';
    RAISE NOTICE 'To enable forecast alerts (optional):';
    RAISE NOTICE '  UPDATE flight_recorder.config SET value = ''true'' WHERE key = ''forecast_alert_enabled'';';
    RAISE NOTICE '  -- Then schedule via pg_cron:';
    RAISE NOTICE '  SELECT cron.schedule(''forecast-alerts'', ''0 */4 * * *'', ''SELECT flight_recorder.check_forecast_alerts()'');';
    RAISE NOTICE '';
END $$;
