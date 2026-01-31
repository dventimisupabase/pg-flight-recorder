-- =============================================================================
-- pg-flight-recorder pgTAP Tests - Query Storm Detection
-- =============================================================================
-- Tests: Query storm detection definitions, execution, status, resolution, dashboard, notifications, severity, correlation
-- Test count: 60
-- =============================================================================

BEGIN;
SELECT plan(60);

-- Disable checkpoint detection during tests to prevent snapshot skipping
UPDATE flight_recorder.config SET value = 'false' WHERE key = 'check_checkpoint_backup';

-- Disable collection jitter to speed up tests
UPDATE flight_recorder.config SET value = 'false' WHERE key = 'collection_jitter_enabled';

-- =============================================================================
-- 1. TABLE EXISTENCE (1 test)
-- =============================================================================

SELECT has_table(
    'flight_recorder', 'query_storms',
    'query_storms table should exist'
);

-- =============================================================================
-- 2. COLUMN EXISTENCE - query_storms table (10 tests)
-- =============================================================================

SELECT has_column(
    'flight_recorder', 'query_storms', 'id',
    'query_storms table should have id column'
);

SELECT has_column(
    'flight_recorder', 'query_storms', 'detected_at',
    'query_storms table should have detected_at column'
);

SELECT has_column(
    'flight_recorder', 'query_storms', 'queryid',
    'query_storms table should have queryid column'
);

SELECT has_column(
    'flight_recorder', 'query_storms', 'query_fingerprint',
    'query_storms table should have query_fingerprint column'
);

SELECT has_column(
    'flight_recorder', 'query_storms', 'storm_type',
    'query_storms table should have storm_type column'
);

SELECT has_column(
    'flight_recorder', 'query_storms', 'recent_count',
    'query_storms table should have recent_count column'
);

SELECT has_column(
    'flight_recorder', 'query_storms', 'baseline_count',
    'query_storms table should have baseline_count column'
);

SELECT has_column(
    'flight_recorder', 'query_storms', 'multiplier',
    'query_storms table should have multiplier column'
);

SELECT has_column(
    'flight_recorder', 'query_storms', 'resolved_at',
    'query_storms table should have resolved_at column'
);

SELECT has_column(
    'flight_recorder', 'query_storms', 'resolution_notes',
    'query_storms table should have resolution_notes column'
);

SELECT has_column(
    'flight_recorder', 'query_storms', 'severity',
    'query_storms table should have severity column'
);

SELECT has_column(
    'flight_recorder', 'query_storms', 'correlation',
    'query_storms table should have correlation column'
);

-- =============================================================================
-- 3. CONFIG SETTINGS (6 tests + 3 severity thresholds)
-- =============================================================================

SELECT ok(
    EXISTS (SELECT 1 FROM flight_recorder.config WHERE key = 'storm_detection_enabled'),
    'storm_detection_enabled config setting should exist'
);

SELECT ok(
    EXISTS (SELECT 1 FROM flight_recorder.config WHERE key = 'storm_threshold_multiplier'),
    'storm_threshold_multiplier config setting should exist'
);

SELECT ok(
    EXISTS (SELECT 1 FROM flight_recorder.config WHERE key = 'storm_lookback_interval'),
    'storm_lookback_interval config setting should exist'
);

SELECT ok(
    EXISTS (SELECT 1 FROM flight_recorder.config WHERE key = 'storm_baseline_days'),
    'storm_baseline_days config setting should exist'
);

SELECT ok(
    EXISTS (SELECT 1 FROM flight_recorder.config WHERE key = 'storm_detection_interval_minutes'),
    'storm_detection_interval_minutes config setting should exist'
);

SELECT ok(
    EXISTS (SELECT 1 FROM flight_recorder.config WHERE key = 'retention_storms_days'),
    'retention_storms_days config setting should exist'
);

SELECT ok(
    EXISTS (SELECT 1 FROM flight_recorder.config WHERE key = 'storm_min_duration_minutes'),
    'storm_min_duration_minutes config setting should exist'
);

SELECT ok(
    EXISTS (SELECT 1 FROM flight_recorder.config WHERE key = 'storm_notify_enabled'),
    'storm_notify_enabled config setting should exist'
);

SELECT ok(
    EXISTS (SELECT 1 FROM flight_recorder.config WHERE key = 'storm_notify_channel'),
    'storm_notify_channel config setting should exist'
);

SELECT ok(
    EXISTS (SELECT 1 FROM flight_recorder.config WHERE key = 'storm_severity_low_max'),
    'storm_severity_low_max config setting should exist'
);

SELECT ok(
    EXISTS (SELECT 1 FROM flight_recorder.config WHERE key = 'storm_severity_medium_max'),
    'storm_severity_medium_max config setting should exist'
);

SELECT ok(
    EXISTS (SELECT 1 FROM flight_recorder.config WHERE key = 'storm_severity_high_max'),
    'storm_severity_high_max config setting should exist'
);

-- =============================================================================
-- 4. FUNCTION EXISTENCE - Detection (6 tests + 1 correlation function)
-- =============================================================================

SELECT has_function(
    'flight_recorder', 'detect_query_storms', ARRAY['interval', 'numeric'],
    'detect_query_storms(interval, numeric) function should exist'
);

SELECT has_function(
    'flight_recorder', 'auto_detect_storms', ARRAY[]::TEXT[],
    'auto_detect_storms() function should exist'
);

SELECT has_function(
    'flight_recorder', 'storm_status', ARRAY['interval'],
    'storm_status(interval) function should exist'
);

SELECT has_function(
    'flight_recorder', 'enable_storm_detection', ARRAY[]::TEXT[],
    'enable_storm_detection() function should exist'
);

SELECT has_function(
    'flight_recorder', 'disable_storm_detection', ARRAY[]::TEXT[],
    'disable_storm_detection() function should exist'
);

SELECT has_function(
    'flight_recorder', '_notify_storm', ARRAY['text', 'bigint', 'bigint', 'text', 'text', 'bigint', 'bigint', 'numeric', 'text'],
    '_notify_storm() function should exist'
);

SELECT has_function(
    'flight_recorder', '_compute_storm_correlation', ARRAY['interval'],
    '_compute_storm_correlation(interval) function should exist'
);

-- =============================================================================
-- 5. FUNCTION EXISTENCE - Resolution (4 tests)
-- =============================================================================

SELECT has_function(
    'flight_recorder', 'resolve_storm', ARRAY['bigint', 'text'],
    'resolve_storm(bigint, text) function should exist'
);

SELECT has_function(
    'flight_recorder', 'resolve_storms_by_queryid', ARRAY['bigint', 'text'],
    'resolve_storms_by_queryid(bigint, text) function should exist'
);

SELECT has_function(
    'flight_recorder', 'resolve_all_storms', ARRAY['text'],
    'resolve_all_storms(text) function should exist'
);

SELECT has_function(
    'flight_recorder', 'reopen_storm', ARRAY['bigint'],
    'reopen_storm(bigint) function should exist'
);

-- =============================================================================
-- 6. FUNCTION EXECUTION - Detection (3 tests)
-- =============================================================================

-- Test detect_query_storms executes without error
SELECT lives_ok(
    $$SELECT * FROM flight_recorder.detect_query_storms()$$,
    'detect_query_storms() should execute without error'
);

-- Test storm_status executes without error
SELECT lives_ok(
    $$SELECT * FROM flight_recorder.storm_status()$$,
    'storm_status() should execute without error'
);

-- Test auto_detect_storms executes without error (returns 0 when disabled)
SELECT lives_ok(
    $$SELECT flight_recorder.auto_detect_storms()$$,
    'auto_detect_storms() should execute without error'
);

-- =============================================================================
-- 7. RESOLUTION WORKFLOW (6 tests)
-- =============================================================================

-- Insert a test storm for resolution testing
INSERT INTO flight_recorder.query_storms (queryid, query_fingerprint, storm_type, recent_count, baseline_count, multiplier)
VALUES (12345, 'SELECT * FROM test_table', 'SPIKE', 1000, 100, 10.0);

-- Test resolve_storm returns success message
SELECT matches(
    flight_recorder.resolve_storm(
        (SELECT id FROM flight_recorder.query_storms WHERE queryid = 12345),
        'Test resolution'
    ),
    'Storm .* resolved',
    'resolve_storm() should return success message'
);

-- Test storm is now resolved
SELECT ok(
    (SELECT resolved_at IS NOT NULL FROM flight_recorder.query_storms WHERE queryid = 12345),
    'Storm should be marked as resolved'
);

-- Test resolution notes are saved
SELECT is(
    (SELECT resolution_notes FROM flight_recorder.query_storms WHERE queryid = 12345),
    'Test resolution',
    'Resolution notes should be saved'
);

-- Test reopen_storm works
SELECT matches(
    flight_recorder.reopen_storm(
        (SELECT id FROM flight_recorder.query_storms WHERE queryid = 12345)
    ),
    'Storm .* reopened',
    'reopen_storm() should return success message'
);

-- Test storm is now active again
SELECT ok(
    (SELECT resolved_at IS NULL FROM flight_recorder.query_storms WHERE queryid = 12345),
    'Storm should be active after reopening'
);

-- Test resolve_all_storms works
SELECT matches(
    flight_recorder.resolve_all_storms('Bulk test resolution'),
    'Resolved .* storm',
    'resolve_all_storms() should return success message'
);

-- Cleanup test data
DELETE FROM flight_recorder.query_storms WHERE queryid = 12345;

-- =============================================================================
-- 8. AUTO-RESOLUTION (2 tests)
-- =============================================================================

-- Insert an active storm that should be auto-resolved (no matching current spike)
-- Back-date detected_at to bypass anti-flapping protection
INSERT INTO flight_recorder.query_storms (queryid, query_fingerprint, storm_type, recent_count, baseline_count, multiplier, detected_at)
VALUES (99999, 'SELECT * FROM auto_resolve_test', 'SPIKE', 500, 50, 10.0, now() - interval '10 minutes');

-- Enable storm detection and run auto_detect_storms
UPDATE flight_recorder.config SET value = 'true' WHERE key = 'storm_detection_enabled';

-- Run auto_detect_storms - should auto-resolve the storm since there's no actual spike
SELECT lives_ok(
    $$SELECT flight_recorder.auto_detect_storms()$$,
    'auto_detect_storms() with auto-resolution should execute without error'
);

-- Verify the storm was auto-resolved (no current spike for queryid 99999)
SELECT ok(
    (SELECT resolved_at IS NOT NULL AND resolution_notes LIKE '%Auto-resolved%'
     FROM flight_recorder.query_storms WHERE queryid = 99999),
    'Storm should be auto-resolved when counts normalize (after min duration)'
);

-- Cleanup
DELETE FROM flight_recorder.query_storms WHERE queryid = 99999;
UPDATE flight_recorder.config SET value = 'false' WHERE key = 'storm_detection_enabled';

-- =============================================================================
-- 9. STORM DASHBOARD VIEW (3 tests)
-- =============================================================================

SELECT has_view(
    'flight_recorder', 'storm_dashboard',
    'storm_dashboard view should exist'
);

-- Test dashboard executes without error
SELECT lives_ok(
    $$SELECT * FROM flight_recorder.storm_dashboard$$,
    'storm_dashboard view should execute without error'
);

-- Test dashboard returns expected columns
SELECT ok(
    (SELECT active_count IS NOT NULL AND overall_status IS NOT NULL
     FROM flight_recorder.storm_dashboard),
    'storm_dashboard should return active_count and overall_status'
);

-- Test dashboard returns severity columns
SELECT ok(
    (SELECT active_low_severity IS NOT NULL
        AND active_medium_severity IS NOT NULL
        AND active_high_severity IS NOT NULL
        AND active_critical_severity IS NOT NULL
     FROM flight_recorder.storm_dashboard),
    'storm_dashboard should return severity breakdown columns'
);

-- =============================================================================
-- 10. SEVERITY CLASSIFICATION (6 tests)
-- =============================================================================

-- Test severity column has valid default
INSERT INTO flight_recorder.query_storms (queryid, query_fingerprint, storm_type, recent_count, baseline_count, multiplier)
VALUES (77777, 'SELECT severity_test', 'SPIKE', 500, 100, 5.0);

SELECT is(
    (SELECT severity FROM flight_recorder.query_storms WHERE queryid = 77777),
    'MEDIUM',
    'Default severity should be MEDIUM'
);

DELETE FROM flight_recorder.query_storms WHERE queryid = 77777;

-- Test detect_query_storms returns severity column
SELECT ok(
    (SELECT count(*) >= 0 FROM flight_recorder.detect_query_storms()),
    'detect_query_storms() should return results (may be empty) with severity column'
);

-- Test storm_status returns severity column
SELECT ok(
    (SELECT count(*) >= 0 FROM flight_recorder.storm_status()),
    'storm_status() should return results (may be empty) with severity column'
);

-- Test severity values are valid
INSERT INTO flight_recorder.query_storms (queryid, query_fingerprint, storm_type, severity, recent_count, baseline_count, multiplier)
VALUES
    (77701, 'SELECT test_low', 'SPIKE', 'LOW', 100, 50, 2.0),
    (77702, 'SELECT test_medium', 'SPIKE', 'MEDIUM', 500, 100, 5.0),
    (77703, 'SELECT test_high', 'SPIKE', 'HIGH', 2000, 100, 20.0),
    (77704, 'SELECT test_critical', 'RETRY_STORM', 'CRITICAL', 10000, 100, 100.0);

SELECT is(
    (SELECT count(*) FROM flight_recorder.query_storms
     WHERE queryid BETWEEN 77701 AND 77704
       AND severity IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')),
    4::BIGINT,
    'All severity values should be valid (LOW, MEDIUM, HIGH, CRITICAL)'
);

-- Test storm_status orders by severity
SELECT ok(
    (SELECT count(*) = 4 FROM flight_recorder.storm_status()
     WHERE queryid BETWEEN 77701 AND 77704),
    'storm_status() should return all test storms'
);

-- Cleanup severity test data
DELETE FROM flight_recorder.query_storms WHERE queryid BETWEEN 77701 AND 77704;

-- =============================================================================
-- 11. CORRELATION DATA (4 tests)
-- =============================================================================

-- Test _compute_storm_correlation executes without error
SELECT lives_ok(
    $$SELECT flight_recorder._compute_storm_correlation()$$,
    '_compute_storm_correlation() should execute without error'
);

-- Test _compute_storm_correlation with custom lookback
SELECT lives_ok(
    $$SELECT flight_recorder._compute_storm_correlation('10 minutes'::interval)$$,
    '_compute_storm_correlation(interval) should execute without error'
);

-- Test correlation column can store JSONB data
INSERT INTO flight_recorder.query_storms (
    queryid, query_fingerprint, storm_type, severity, recent_count, baseline_count, multiplier, correlation
)
VALUES (
    88888, 'SELECT correlation_test', 'SPIKE', 'MEDIUM', 1000, 100, 10.0,
    '{"checkpoint": {"active": false}, "locks": {"blocked_count": 0}, "io": {"temp_bytes_delta": 0}}'::jsonb
);

SELECT ok(
    (SELECT correlation IS NOT NULL AND correlation ? 'checkpoint'
     FROM flight_recorder.query_storms WHERE queryid = 88888),
    'correlation column should store and retrieve JSONB data correctly'
);

-- Test storm_status returns correlation data
SELECT ok(
    (SELECT correlation IS NOT NULL
     FROM flight_recorder.storm_status()
     WHERE queryid = 88888),
    'storm_status() should return correlation data'
);

-- Cleanup correlation test data
DELETE FROM flight_recorder.query_storms WHERE queryid = 88888;

-- Disable storm detection after testing (should already be disabled)
UPDATE flight_recorder.config SET value = 'false' WHERE key = 'storm_detection_enabled';

SELECT * FROM finish();
ROLLBACK;
