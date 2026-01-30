-- =============================================================================
-- pg-flight-recorder pgTAP Tests - Performance Regression Detection
-- =============================================================================
-- Tests: Regression detection definitions, execution, status, resolution, dashboard, notifications, severity
-- Test count: 57
-- =============================================================================

BEGIN;
SELECT plan(57);

-- Disable checkpoint detection during tests to prevent snapshot skipping
UPDATE flight_recorder.config SET value = 'false' WHERE key = 'check_checkpoint_backup';

-- Disable collection jitter to speed up tests
UPDATE flight_recorder.config SET value = 'false' WHERE key = 'collection_jitter_enabled';

-- =============================================================================
-- 1. TABLE EXISTENCE (1 test)
-- =============================================================================

SELECT has_table(
    'flight_recorder', 'query_regressions',
    'query_regressions table should exist'
);

-- =============================================================================
-- 2. COLUMN EXISTENCE - query_regressions table (12 tests)
-- =============================================================================

SELECT has_column(
    'flight_recorder', 'query_regressions', 'id',
    'query_regressions table should have id column'
);

SELECT has_column(
    'flight_recorder', 'query_regressions', 'detected_at',
    'query_regressions table should have detected_at column'
);

SELECT has_column(
    'flight_recorder', 'query_regressions', 'queryid',
    'query_regressions table should have queryid column'
);

SELECT has_column(
    'flight_recorder', 'query_regressions', 'query_fingerprint',
    'query_regressions table should have query_fingerprint column'
);

SELECT has_column(
    'flight_recorder', 'query_regressions', 'severity',
    'query_regressions table should have severity column'
);

SELECT has_column(
    'flight_recorder', 'query_regressions', 'baseline_avg_ms',
    'query_regressions table should have baseline_avg_ms column'
);

SELECT has_column(
    'flight_recorder', 'query_regressions', 'current_avg_ms',
    'query_regressions table should have current_avg_ms column'
);

SELECT has_column(
    'flight_recorder', 'query_regressions', 'change_pct',
    'query_regressions table should have change_pct column'
);

SELECT has_column(
    'flight_recorder', 'query_regressions', 'correlation',
    'query_regressions table should have correlation column'
);

SELECT has_column(
    'flight_recorder', 'query_regressions', 'probable_causes',
    'query_regressions table should have probable_causes column'
);

SELECT has_column(
    'flight_recorder', 'query_regressions', 'resolved_at',
    'query_regressions table should have resolved_at column'
);

SELECT has_column(
    'flight_recorder', 'query_regressions', 'resolution_notes',
    'query_regressions table should have resolution_notes column'
);

-- =============================================================================
-- 3. CONFIG SETTINGS (12 tests)
-- =============================================================================

SELECT ok(
    EXISTS (SELECT 1 FROM flight_recorder.config WHERE key = 'regression_detection_enabled'),
    'regression_detection_enabled config setting should exist'
);

SELECT ok(
    EXISTS (SELECT 1 FROM flight_recorder.config WHERE key = 'regression_threshold_pct'),
    'regression_threshold_pct config setting should exist'
);

SELECT ok(
    EXISTS (SELECT 1 FROM flight_recorder.config WHERE key = 'regression_lookback_interval'),
    'regression_lookback_interval config setting should exist'
);

SELECT ok(
    EXISTS (SELECT 1 FROM flight_recorder.config WHERE key = 'regression_baseline_days'),
    'regression_baseline_days config setting should exist'
);

SELECT ok(
    EXISTS (SELECT 1 FROM flight_recorder.config WHERE key = 'regression_detection_interval_minutes'),
    'regression_detection_interval_minutes config setting should exist'
);

SELECT ok(
    EXISTS (SELECT 1 FROM flight_recorder.config WHERE key = 'regression_min_duration_minutes'),
    'regression_min_duration_minutes config setting should exist'
);

SELECT ok(
    EXISTS (SELECT 1 FROM flight_recorder.config WHERE key = 'regression_notify_enabled'),
    'regression_notify_enabled config setting should exist'
);

SELECT ok(
    EXISTS (SELECT 1 FROM flight_recorder.config WHERE key = 'regression_notify_channel'),
    'regression_notify_channel config setting should exist'
);

SELECT ok(
    EXISTS (SELECT 1 FROM flight_recorder.config WHERE key = 'regression_severity_low_max'),
    'regression_severity_low_max config setting should exist'
);

SELECT ok(
    EXISTS (SELECT 1 FROM flight_recorder.config WHERE key = 'regression_severity_medium_max'),
    'regression_severity_medium_max config setting should exist'
);

SELECT ok(
    EXISTS (SELECT 1 FROM flight_recorder.config WHERE key = 'regression_severity_high_max'),
    'regression_severity_high_max config setting should exist'
);

SELECT ok(
    EXISTS (SELECT 1 FROM flight_recorder.config WHERE key = 'retention_regressions_days'),
    'retention_regressions_days config setting should exist'
);

-- =============================================================================
-- 4. FUNCTION EXISTENCE - Detection (8 tests)
-- =============================================================================

SELECT has_function(
    'flight_recorder', 'detect_regressions', ARRAY['interval', 'numeric'],
    'detect_regressions(interval, numeric) function should exist'
);

SELECT has_function(
    'flight_recorder', 'auto_detect_regressions', ARRAY[]::TEXT[],
    'auto_detect_regressions() function should exist'
);

SELECT has_function(
    'flight_recorder', 'regression_status', ARRAY['interval'],
    'regression_status(interval) function should exist'
);

SELECT has_function(
    'flight_recorder', 'enable_regression_detection', ARRAY[]::TEXT[],
    'enable_regression_detection() function should exist'
);

SELECT has_function(
    'flight_recorder', 'disable_regression_detection', ARRAY[]::TEXT[],
    'disable_regression_detection() function should exist'
);

SELECT has_function(
    'flight_recorder', '_notify_regression', ARRAY['text', 'bigint', 'bigint', 'text', 'numeric', 'numeric', 'numeric', 'text'],
    '_notify_regression() function should exist'
);

SELECT has_function(
    'flight_recorder', '_diagnose_regression_causes', ARRAY['bigint'],
    '_diagnose_regression_causes(bigint) function should exist'
);

SELECT has_function(
    'flight_recorder', 'resolve_regression', ARRAY['bigint', 'text'],
    'resolve_regression(bigint, text) function should exist'
);

-- =============================================================================
-- 5. FUNCTION EXISTENCE - Resolution (4 tests)
-- =============================================================================

SELECT has_function(
    'flight_recorder', 'resolve_regressions_by_queryid', ARRAY['bigint', 'text'],
    'resolve_regressions_by_queryid(bigint, text) function should exist'
);

SELECT has_function(
    'flight_recorder', 'resolve_all_regressions', ARRAY['text'],
    'resolve_all_regressions(text) function should exist'
);

SELECT has_function(
    'flight_recorder', 'reopen_regression', ARRAY['bigint'],
    'reopen_regression(bigint) function should exist'
);

-- =============================================================================
-- 6. FUNCTION EXECUTION - Detection (4 tests)
-- =============================================================================

-- Test detect_regressions executes without error
SELECT lives_ok(
    $$SELECT * FROM flight_recorder.detect_regressions()$$,
    'detect_regressions() should execute without error'
);

-- Test regression_status executes without error
SELECT lives_ok(
    $$SELECT * FROM flight_recorder.regression_status()$$,
    'regression_status() should execute without error'
);

-- Test auto_detect_regressions executes without error (returns 0 when disabled)
SELECT lives_ok(
    $$SELECT flight_recorder.auto_detect_regressions()$$,
    'auto_detect_regressions() should execute without error'
);

-- Test _diagnose_regression_causes executes without error
SELECT lives_ok(
    $$SELECT flight_recorder._diagnose_regression_causes(12345)$$,
    '_diagnose_regression_causes() should execute without error'
);

-- =============================================================================
-- 7. RESOLUTION WORKFLOW (6 tests)
-- =============================================================================

-- Insert a test regression for resolution testing
INSERT INTO flight_recorder.query_regressions (queryid, query_fingerprint, severity, baseline_avg_ms, current_avg_ms, change_pct)
VALUES (12345, 'SELECT * FROM test_table', 'HIGH', 10.0, 100.0, 900.0);

-- Test resolve_regression returns success message
SELECT matches(
    flight_recorder.resolve_regression(
        (SELECT id FROM flight_recorder.query_regressions WHERE queryid = 12345),
        'Test resolution'
    ),
    'Regression .* resolved',
    'resolve_regression() should return success message'
);

-- Test regression is now resolved
SELECT ok(
    (SELECT resolved_at IS NOT NULL FROM flight_recorder.query_regressions WHERE queryid = 12345),
    'Regression should be marked as resolved'
);

-- Test resolution notes are saved
SELECT is(
    (SELECT resolution_notes FROM flight_recorder.query_regressions WHERE queryid = 12345),
    'Test resolution',
    'Resolution notes should be saved'
);

-- Test reopen_regression works
SELECT matches(
    flight_recorder.reopen_regression(
        (SELECT id FROM flight_recorder.query_regressions WHERE queryid = 12345)
    ),
    'Regression .* reopened',
    'reopen_regression() should return success message'
);

-- Test regression is now active again
SELECT ok(
    (SELECT resolved_at IS NULL FROM flight_recorder.query_regressions WHERE queryid = 12345),
    'Regression should be active after reopening'
);

-- Test resolve_all_regressions works
SELECT matches(
    flight_recorder.resolve_all_regressions('Bulk test resolution'),
    'Resolved .* regression',
    'resolve_all_regressions() should return success message'
);

-- Cleanup test data
DELETE FROM flight_recorder.query_regressions WHERE queryid = 12345;

-- =============================================================================
-- 8. AUTO-RESOLUTION (2 tests)
-- =============================================================================

-- Insert an active regression that should be auto-resolved (no matching current regression)
-- Back-date detected_at to bypass anti-flapping protection
INSERT INTO flight_recorder.query_regressions (queryid, query_fingerprint, severity, baseline_avg_ms, current_avg_ms, change_pct, detected_at)
VALUES (99999, 'SELECT * FROM auto_resolve_test', 'MEDIUM', 5.0, 50.0, 900.0, now() - interval '35 minutes');

-- Enable regression detection and run auto_detect_regressions
UPDATE flight_recorder.config SET value = 'true' WHERE key = 'regression_detection_enabled';

-- Run auto_detect_regressions - should auto-resolve the regression since there's no actual regression
SELECT lives_ok(
    $$SELECT flight_recorder.auto_detect_regressions()$$,
    'auto_detect_regressions() with auto-resolution should execute without error'
);

-- Verify the regression was auto-resolved (no current regression for queryid 99999)
SELECT ok(
    (SELECT resolved_at IS NOT NULL AND resolution_notes LIKE '%Auto-resolved%'
     FROM flight_recorder.query_regressions WHERE queryid = 99999),
    'Regression should be auto-resolved when performance normalizes (after min duration)'
);

-- Cleanup
DELETE FROM flight_recorder.query_regressions WHERE queryid = 99999;
UPDATE flight_recorder.config SET value = 'false' WHERE key = 'regression_detection_enabled';

-- =============================================================================
-- 9. REGRESSION DASHBOARD VIEW (4 tests)
-- =============================================================================

SELECT has_view(
    'flight_recorder', 'regression_dashboard',
    'regression_dashboard view should exist'
);

-- Test dashboard executes without error
SELECT lives_ok(
    $$SELECT * FROM flight_recorder.regression_dashboard$$,
    'regression_dashboard view should execute without error'
);

-- Test dashboard returns expected columns
SELECT ok(
    (SELECT active_count IS NOT NULL AND overall_status IS NOT NULL
     FROM flight_recorder.regression_dashboard),
    'regression_dashboard should return active_count and overall_status'
);

-- Test dashboard returns severity columns
SELECT ok(
    (SELECT active_low_severity IS NOT NULL
        AND active_medium_severity IS NOT NULL
        AND active_high_severity IS NOT NULL
        AND active_critical_severity IS NOT NULL
     FROM flight_recorder.regression_dashboard),
    'regression_dashboard should return severity breakdown columns'
);

-- =============================================================================
-- 10. SEVERITY CLASSIFICATION (4 tests)
-- =============================================================================

-- Test severity column has valid default
INSERT INTO flight_recorder.query_regressions (queryid, query_fingerprint, baseline_avg_ms, current_avg_ms, change_pct)
VALUES (77777, 'SELECT severity_test', 10.0, 50.0, 400.0);

SELECT is(
    (SELECT severity FROM flight_recorder.query_regressions WHERE queryid = 77777),
    'MEDIUM',
    'Default severity should be MEDIUM'
);

DELETE FROM flight_recorder.query_regressions WHERE queryid = 77777;

-- Test severity values are valid
INSERT INTO flight_recorder.query_regressions (queryid, query_fingerprint, severity, baseline_avg_ms, current_avg_ms, change_pct)
VALUES
    (77701, 'SELECT test_low', 'LOW', 10.0, 15.0, 50.0),
    (77702, 'SELECT test_medium', 'MEDIUM', 10.0, 40.0, 300.0),
    (77703, 'SELECT test_high', 'HIGH', 10.0, 80.0, 700.0),
    (77704, 'SELECT test_critical', 'CRITICAL', 10.0, 150.0, 1400.0);

SELECT is(
    (SELECT count(*) FROM flight_recorder.query_regressions
     WHERE queryid BETWEEN 77701 AND 77704
       AND severity IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')),
    4::BIGINT,
    'All severity values should be valid (LOW, MEDIUM, HIGH, CRITICAL)'
);

-- Test regression_status orders by severity
SELECT ok(
    (SELECT count(*) = 4 FROM flight_recorder.regression_status()
     WHERE queryid BETWEEN 77701 AND 77704),
    'regression_status() should return all test regressions'
);

-- Cleanup severity test data
DELETE FROM flight_recorder.query_regressions WHERE queryid BETWEEN 77701 AND 77704;

-- =============================================================================
-- 11. CORRELATION DATA (2 tests)
-- =============================================================================

-- Test correlation column can store JSONB data
INSERT INTO flight_recorder.query_regressions (
    queryid, query_fingerprint, severity, baseline_avg_ms, current_avg_ms, change_pct, correlation
)
VALUES (
    88888, 'SELECT correlation_test', 'MEDIUM', 10.0, 50.0, 400.0,
    '{"checkpoint": {"active": false}, "locks": {"blocked_count": 0}, "io": {"temp_bytes_delta": 0}}'::jsonb
);

SELECT ok(
    (SELECT correlation IS NOT NULL AND correlation ? 'checkpoint'
     FROM flight_recorder.query_regressions WHERE queryid = 88888),
    'correlation column should store and retrieve JSONB data correctly'
);

-- Test regression_status returns correlation data
SELECT ok(
    (SELECT correlation IS NOT NULL
     FROM flight_recorder.regression_status()
     WHERE queryid = 88888),
    'regression_status() should return correlation data'
);

-- Cleanup correlation test data
DELETE FROM flight_recorder.query_regressions WHERE queryid = 88888;

-- Disable regression detection after testing (should already be disabled)
UPDATE flight_recorder.config SET value = 'false' WHERE key = 'regression_detection_enabled';

SELECT * FROM finish();
ROLLBACK;
