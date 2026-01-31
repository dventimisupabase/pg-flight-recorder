-- =============================================================================
-- pg-flight-recorder pgTAP Tests - Performance Forecasting
-- =============================================================================
-- Tests: _linear_regression, forecast, forecast_summary, check_forecast_alerts
-- Test count: 55
-- =============================================================================

BEGIN;
SELECT plan(55);

-- Disable checkpoint detection during tests to prevent snapshot skipping
UPDATE flight_recorder.config SET value = 'false' WHERE key = 'check_checkpoint_backup';

-- Disable collection jitter to speed up tests
UPDATE flight_recorder.config SET value = 'false' WHERE key = 'collection_jitter_enabled';

-- =============================================================================
-- 1. FUNCTION EXISTENCE (5 tests)
-- =============================================================================

SELECT has_function(
    'flight_recorder', '_linear_regression',
    ARRAY['numeric[]', 'numeric[]'],
    '_linear_regression function should exist'
);

SELECT has_function(
    'flight_recorder', 'forecast',
    ARRAY['text', 'interval', 'interval'],
    'forecast function should exist'
);

SELECT has_function(
    'flight_recorder', 'forecast_summary',
    ARRAY['interval', 'interval'],
    'forecast_summary function should exist'
);

SELECT has_function(
    'flight_recorder', '_notify_forecast',
    ARRAY['text', 'text', 'timestamptz', 'numeric', 'text'],
    '_notify_forecast function should exist'
);

SELECT has_function(
    'flight_recorder', 'check_forecast_alerts',
    '{}',
    'check_forecast_alerts function should exist'
);

-- =============================================================================
-- 2. CONFIG SETTINGS (9 tests)
-- =============================================================================

SELECT ok(
    EXISTS (SELECT 1 FROM flight_recorder.config WHERE key = 'forecast_enabled'),
    'forecast_enabled config setting should exist'
);

SELECT ok(
    EXISTS (SELECT 1 FROM flight_recorder.config WHERE key = 'forecast_lookback_days'),
    'forecast_lookback_days config setting should exist'
);

SELECT ok(
    EXISTS (SELECT 1 FROM flight_recorder.config WHERE key = 'forecast_window_days'),
    'forecast_window_days config setting should exist'
);

SELECT ok(
    EXISTS (SELECT 1 FROM flight_recorder.config WHERE key = 'forecast_alert_enabled'),
    'forecast_alert_enabled config setting should exist'
);

SELECT ok(
    EXISTS (SELECT 1 FROM flight_recorder.config WHERE key = 'forecast_alert_threshold'),
    'forecast_alert_threshold config setting should exist'
);

SELECT ok(
    EXISTS (SELECT 1 FROM flight_recorder.config WHERE key = 'forecast_notify_channel'),
    'forecast_notify_channel config setting should exist'
);

SELECT ok(
    EXISTS (SELECT 1 FROM flight_recorder.config WHERE key = 'forecast_disk_capacity_gb'),
    'forecast_disk_capacity_gb config setting should exist'
);

SELECT ok(
    EXISTS (SELECT 1 FROM flight_recorder.config WHERE key = 'forecast_min_samples'),
    'forecast_min_samples config setting should exist'
);

SELECT ok(
    EXISTS (SELECT 1 FROM flight_recorder.config WHERE key = 'forecast_min_confidence'),
    'forecast_min_confidence config setting should exist'
);

-- =============================================================================
-- 3. _LINEAR_REGRESSION TESTS (17 tests)
-- =============================================================================

-- Test perfect positive linear relationship (y = 2x)
SELECT ok(
    (SELECT slope FROM flight_recorder._linear_regression(
        ARRAY[1,2,3,4,5]::numeric[],
        ARRAY[2,4,6,8,10]::numeric[]
    )) = 2,
    '_linear_regression slope should be 2 for y = 2x'
);

SELECT ok(
    (SELECT intercept FROM flight_recorder._linear_regression(
        ARRAY[1,2,3,4,5]::numeric[],
        ARRAY[2,4,6,8,10]::numeric[]
    )) = 0,
    '_linear_regression intercept should be 0 for y = 2x (through origin)'
);

SELECT ok(
    (SELECT r_squared FROM flight_recorder._linear_regression(
        ARRAY[1,2,3,4,5]::numeric[],
        ARRAY[2,4,6,8,10]::numeric[]
    )) = 1.0,
    '_linear_regression R² should be 1.0 for perfect fit'
);

-- Test with intercept (y = x + 5)
SELECT ok(
    (SELECT slope FROM flight_recorder._linear_regression(
        ARRAY[1,2,3,4,5]::numeric[],
        ARRAY[6,7,8,9,10]::numeric[]
    )) = 1,
    '_linear_regression slope should be 1 for y = x + 5'
);

SELECT ok(
    (SELECT intercept FROM flight_recorder._linear_regression(
        ARRAY[1,2,3,4,5]::numeric[],
        ARRAY[6,7,8,9,10]::numeric[]
    )) = 5,
    '_linear_regression intercept should be 5 for y = x + 5'
);

-- Test negative slope (y = -x + 10)
SELECT ok(
    (SELECT slope FROM flight_recorder._linear_regression(
        ARRAY[1,2,3,4,5]::numeric[],
        ARRAY[9,8,7,6,5]::numeric[]
    )) = -1,
    '_linear_regression should handle negative slopes'
);

-- Test NULL input arrays
SELECT ok(
    (SELECT slope FROM flight_recorder._linear_regression(NULL, NULL)) IS NULL,
    '_linear_regression should return NULL for NULL inputs'
);

-- Test insufficient data (fewer than 3 points)
SELECT ok(
    (SELECT slope FROM flight_recorder._linear_regression(
        ARRAY[1,2]::numeric[],
        ARRAY[2,4]::numeric[]
    )) IS NULL,
    '_linear_regression should return NULL for fewer than 3 data points'
);

-- Test mismatched array lengths
SELECT ok(
    (SELECT slope FROM flight_recorder._linear_regression(
        ARRAY[1,2,3,4,5]::numeric[],
        ARRAY[2,4,6]::numeric[]
    )) IS NULL,
    '_linear_regression should return NULL for mismatched array lengths'
);

-- Test constant y values (horizontal line)
SELECT ok(
    (SELECT slope FROM flight_recorder._linear_regression(
        ARRAY[1,2,3,4,5]::numeric[],
        ARRAY[5,5,5,5,5]::numeric[]
    )) = 0,
    '_linear_regression slope should be 0 for constant y values'
);

SELECT ok(
    (SELECT r_squared FROM flight_recorder._linear_regression(
        ARRAY[1,2,3,4,5]::numeric[],
        ARRAY[5,5,5,5,5]::numeric[]
    )) = 1.0,
    '_linear_regression R² should be 1.0 for perfect horizontal fit'
);

-- Test constant x values (vertical line - undefined slope)
SELECT ok(
    (SELECT slope FROM flight_recorder._linear_regression(
        ARRAY[5,5,5,5,5]::numeric[],
        ARRAY[1,2,3,4,5]::numeric[]
    )) IS NULL,
    '_linear_regression should return NULL for constant x values (vertical line)'
);

-- Test real-world scenario: gradual increase with noise
SELECT ok(
    (SELECT r_squared FROM flight_recorder._linear_regression(
        ARRAY[1,2,3,4,5,6,7,8,9,10]::numeric[],
        ARRAY[10,12,11,14,16,15,18,19,21,22]::numeric[]
    )) BETWEEN 0.9 AND 1.0,
    '_linear_regression R² should be high for data with clear trend'
);

-- Test with decimal values
SELECT ok(
    (SELECT slope FROM flight_recorder._linear_regression(
        ARRAY[0.5,1.0,1.5,2.0,2.5]::numeric[],
        ARRAY[1.0,2.0,3.0,4.0,5.0]::numeric[]
    )) = 2,
    '_linear_regression should handle decimal values'
);

-- Test with large values (simulating bytes)
SELECT ok(
    (SELECT slope FROM flight_recorder._linear_regression(
        ARRAY[1000000000,2000000000,3000000000]::numeric[],
        ARRAY[1073741824,2147483648,3221225472]::numeric[]
    )) > 0,
    '_linear_regression should handle large values (bytes scale)'
);

-- Test R² bounds (should be between 0 and 1)
SELECT ok(
    (SELECT r_squared FROM flight_recorder._linear_regression(
        ARRAY[1,2,3,4,5]::numeric[],
        ARRAY[5,2,8,3,7]::numeric[]
    )) BETWEEN 0 AND 1,
    '_linear_regression R² should be between 0 and 1'
);

-- Test empty arrays
SELECT ok(
    (SELECT slope FROM flight_recorder._linear_regression(
        ARRAY[]::numeric[],
        ARRAY[]::numeric[]
    )) IS NULL,
    '_linear_regression should return NULL for empty arrays'
);

-- =============================================================================
-- 4. FORECAST FUNCTION TESTS (14 tests)
-- =============================================================================

-- Set up: reduce min_samples for testing
UPDATE flight_recorder.config SET value = '3' WHERE key = 'forecast_min_samples';

-- Insert test data with clear growth trend for db_size
INSERT INTO flight_recorder.snapshots (
    captured_at, pg_version, connections_active, connections_total, connections_max,
    blks_hit, blks_read, wal_bytes, temp_bytes, xact_commit, db_size_bytes
) VALUES
    (now() - interval '6 days', 160000, 10, 20, 100, 1000, 100, 1024000000, 0, 1000, 1073741824),
    (now() - interval '5 days', 160000, 12, 22, 100, 1200, 120, 1536000000, 0, 1200, 1610612736),
    (now() - interval '4 days', 160000, 14, 24, 100, 1400, 140, 2048000000, 0, 1400, 2147483648),
    (now() - interval '3 days', 160000, 16, 26, 100, 1600, 160, 2560000000, 0, 1600, 2684354560),
    (now() - interval '2 days', 160000, 18, 28, 100, 1800, 180, 3072000000, 0, 1800, 3221225472),
    (now() - interval '1 day', 160000, 20, 30, 100, 2000, 200, 3584000000, 0, 2000, 3758096384),
    (now(), 160000, 22, 32, 100, 2200, 220, 4096000000, 0, 2200, 4294967296);

-- Test forecast returns expected columns
SELECT ok(
    EXISTS (
        SELECT 1 FROM flight_recorder.forecast('db_size', '7 days', '7 days')
        WHERE metric IS NOT NULL
    ),
    'forecast should return metric column'
);

SELECT ok(
    EXISTS (
        SELECT 1 FROM flight_recorder.forecast('db_size', '7 days', '7 days')
        WHERE current_value IS NOT NULL
    ),
    'forecast should return current_value'
);

SELECT ok(
    EXISTS (
        SELECT 1 FROM flight_recorder.forecast('db_size', '7 days', '7 days')
        WHERE confidence IS NOT NULL AND confidence BETWEEN 0 AND 1
    ),
    'forecast confidence should be between 0 and 1'
);

-- Test supported metric aliases
SELECT ok(
    (SELECT metric FROM flight_recorder.forecast('storage', '7 days')) = 'storage',
    'forecast should accept "storage" alias for db_size'
);

SELECT ok(
    (SELECT metric FROM flight_recorder.forecast('wal', '7 days')) = 'wal',
    'forecast should accept "wal" alias for wal_bytes'
);

SELECT ok(
    (SELECT metric FROM flight_recorder.forecast('transactions', '7 days')) = 'transactions',
    'forecast should accept "transactions" alias for xact_commit'
);

SELECT ok(
    (SELECT metric FROM flight_recorder.forecast('temp', '7 days')) = 'temp',
    'forecast should accept "temp" alias for temp_bytes'
);

-- Test unknown metric error handling
SELECT ok(
    (SELECT current_display FROM flight_recorder.forecast('invalid_metric', '7 days')) LIKE '%Unknown metric%',
    'forecast should return error message for unknown metric'
);

-- Test depletion prediction for db_size
SELECT ok(
    EXISTS (
        SELECT 1 FROM flight_recorder.forecast('db_size', '7 days', '30 days')
        WHERE rate_per_day > 0
    ),
    'forecast should detect positive growth rate for db_size'
);

-- Test connections forecast
SELECT ok(
    EXISTS (
        SELECT 1 FROM flight_recorder.forecast('connections', '7 days')
        WHERE current_display LIKE '%/ 100'
    ),
    'forecast connections should show current/max format'
);

-- Test forecast with disabled forecasting
UPDATE flight_recorder.config SET value = 'false' WHERE key = 'forecast_enabled';
SELECT ok(
    (SELECT current_display FROM flight_recorder.forecast('db_size', '7 days')) = 'Forecasting disabled',
    'forecast should return disabled message when forecast_enabled is false'
);
UPDATE flight_recorder.config SET value = 'true' WHERE key = 'forecast_enabled';

-- Test forecast rate display format for db_size
SELECT ok(
    (SELECT rate_display FROM flight_recorder.forecast('db_size', '7 days')) LIKE '%/day',
    'forecast rate_display should end with /day'
);

-- Test forecast display values are human-readable
SELECT ok(
    (SELECT current_display FROM flight_recorder.forecast('db_size', '7 days')) ~ '(GB|MB|KB|B)$',
    'forecast current_display for db_size should use human-readable format'
);

-- Test insufficient data message
UPDATE flight_recorder.config SET value = '100' WHERE key = 'forecast_min_samples';
SELECT ok(
    (SELECT rate_display FROM flight_recorder.forecast('db_size', '1 hour')) LIKE '%Insufficient data%',
    'forecast should indicate insufficient data when below min_samples'
);
UPDATE flight_recorder.config SET value = '3' WHERE key = 'forecast_min_samples';

-- =============================================================================
-- 5. FORECAST_SUMMARY TESTS (6 tests)
-- =============================================================================

-- Test forecast_summary returns all metrics
SELECT ok(
    (SELECT count(DISTINCT metric) FROM flight_recorder.forecast_summary('7 days', '7 days')) = 5,
    'forecast_summary should return 5 metrics'
);

-- Test forecast_summary includes expected metrics
SELECT ok(
    EXISTS (SELECT 1 FROM flight_recorder.forecast_summary() WHERE metric = 'db_size'),
    'forecast_summary should include db_size metric'
);

SELECT ok(
    EXISTS (SELECT 1 FROM flight_recorder.forecast_summary() WHERE metric = 'connections'),
    'forecast_summary should include connections metric'
);

SELECT ok(
    EXISTS (SELECT 1 FROM flight_recorder.forecast_summary() WHERE metric = 'wal_bytes'),
    'forecast_summary should include wal_bytes metric'
);

-- Test forecast_summary status values
SELECT ok(
    (SELECT status FROM flight_recorder.forecast_summary() WHERE metric = 'db_size')
        IN ('critical', 'warning', 'attention', 'healthy', 'flat', 'insufficient_data'),
    'forecast_summary status should be a valid value'
);

-- Test forecast_summary recommendation not empty
SELECT ok(
    (SELECT recommendation FROM flight_recorder.forecast_summary() WHERE metric = 'db_size') IS NOT NULL
        AND length((SELECT recommendation FROM flight_recorder.forecast_summary() WHERE metric = 'db_size')) > 0,
    'forecast_summary should provide recommendations'
);

-- =============================================================================
-- 6. CHECK_FORECAST_ALERTS TESTS (4 tests)
-- =============================================================================

-- Test check_forecast_alerts returns integer
SELECT ok(
    pg_typeof(flight_recorder.check_forecast_alerts())::text = 'integer',
    'check_forecast_alerts should return integer (count of alerts)'
);

-- Test check_forecast_alerts with alerts disabled
UPDATE flight_recorder.config SET value = 'false' WHERE key = 'forecast_alert_enabled';
SELECT is(
    flight_recorder.check_forecast_alerts(),
    0,
    'check_forecast_alerts should return 0 when alerts are disabled'
);

-- Test check_forecast_alerts with forecasting disabled
UPDATE flight_recorder.config SET value = 'false' WHERE key = 'forecast_enabled';
SELECT is(
    flight_recorder.check_forecast_alerts(),
    0,
    'check_forecast_alerts should return 0 when forecasting is disabled'
);
UPDATE flight_recorder.config SET value = 'true' WHERE key = 'forecast_enabled';

-- Test check_forecast_alerts can be called without error
SELECT lives_ok(
    'SELECT flight_recorder.check_forecast_alerts()',
    'check_forecast_alerts should execute without error'
);

-- =============================================================================
-- CLEANUP
-- =============================================================================

-- Restore config settings
UPDATE flight_recorder.config SET value = 'true' WHERE key = 'check_checkpoint_backup';
UPDATE flight_recorder.config SET value = 'true' WHERE key = 'collection_jitter_enabled';
UPDATE flight_recorder.config SET value = '10' WHERE key = 'forecast_min_samples';
UPDATE flight_recorder.config SET value = 'false' WHERE key = 'forecast_alert_enabled';

SELECT * FROM finish();
ROLLBACK;
