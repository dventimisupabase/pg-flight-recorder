-- =============================================================================
-- Migration: 2.21 to 2.22
-- =============================================================================
-- Description: Remove visual timeline functions, forecasting system, and high_ddl profile
--
-- This migration simplifies the project by removing:
--   - Visual timeline functions (_sparkline, _bar, timeline, sparkline_metrics)
--   - Forecasting system (_linear_regression, forecast, forecast_summary,
--     _notify_forecast, check_forecast_alerts)
--   - high_ddl profile (overlaps with default profile)
--   - All forecast-related config settings
--
-- Core analysis functions remain available:
--   - report() - comprehensive health report
--   - detect_query_storms() - identifies query execution spikes
--   - detect_regressions() - finds performance regressions
-- =============================================================================

\set ON_ERROR_STOP on

BEGIN;

DO $$
DECLARE
    v_current TEXT;
BEGIN
    SELECT value INTO v_current FROM flight_recorder.config WHERE key = 'schema_version';
    IF v_current != '2.21' THEN
        RAISE EXCEPTION 'Migration 2.21->2.22 requires version 2.21, found %', v_current;
    END IF;
    RAISE NOTICE 'Migrating from 2.21 to 2.22...';
END $$;

-- Drop visual timeline functions
DROP FUNCTION IF EXISTS flight_recorder._sparkline(numeric[], integer);
DROP FUNCTION IF EXISTS flight_recorder._bar(numeric, numeric, integer);
DROP FUNCTION IF EXISTS flight_recorder.timeline(text, interval, integer, integer);
DROP FUNCTION IF EXISTS flight_recorder.sparkline_metrics(interval);

-- Drop forecasting functions
DROP FUNCTION IF EXISTS flight_recorder._linear_regression(numeric[], numeric[]);
DROP FUNCTION IF EXISTS flight_recorder.forecast(text, interval, interval);
DROP FUNCTION IF EXISTS flight_recorder.forecast_summary(interval, interval);
DROP FUNCTION IF EXISTS flight_recorder._notify_forecast(text, text, timestamptz, numeric, text);
DROP FUNCTION IF EXISTS flight_recorder.check_forecast_alerts();

-- Remove forecast-related config settings
DELETE FROM flight_recorder.config WHERE key IN (
    'forecast_enabled',
    'forecast_lookback_days',
    'forecast_window_days',
    'forecast_alert_enabled',
    'forecast_alert_threshold',
    'forecast_notify_channel',
    'forecast_disk_capacity_gb',
    'forecast_min_samples',
    'forecast_min_confidence',
    'capacity_forecast_window_days'
);

-- Update version
UPDATE flight_recorder.config SET value = '2.22', updated_at = now()
WHERE key = 'schema_version';

COMMIT;

DO $$
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE '=== Migration Complete ===';
    RAISE NOTICE 'Now at version: 2.22';
    RAISE NOTICE '';
    RAISE NOTICE 'Removed features:';
    RAISE NOTICE '  - Visual timeline functions (sparkline, bar, timeline, sparkline_metrics)';
    RAISE NOTICE '  - Forecasting system (forecast, forecast_summary, check_forecast_alerts)';
    RAISE NOTICE '  - high_ddl profile';
    RAISE NOTICE '';
    RAISE NOTICE 'For database analysis, use:';
    RAISE NOTICE '  SELECT flight_recorder.report(''1 hour'');';
    RAISE NOTICE '';
END $$;
