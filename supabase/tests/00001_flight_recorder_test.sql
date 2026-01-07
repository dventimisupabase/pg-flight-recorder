-- =============================================================================
-- pg-flight-recorder pgTAP Tests
-- =============================================================================
-- Comprehensive test suite for pg-flight-recorder functionality
-- Run with: supabase test db
-- =============================================================================

BEGIN;
SELECT plan(131);  -- Total number of tests (73 + 15 P0 + 8 P1 + 12 P2 + 9 P3 + 10 P4 = 127 + 4 FK tests)

-- =============================================================================
-- 1. INSTALLATION VERIFICATION (16 tests)
-- =============================================================================

-- Test schema exists
SELECT has_schema('flight_recorder', 'Schema flight_recorder should exist');

-- Test all 12 tables exist (11 original + 1 collection_stats)
SELECT has_table('flight_recorder', 'snapshots', 'Table flight_recorder.snapshots should exist');
SELECT has_table('flight_recorder', 'tracked_tables', 'Table flight_recorder.tracked_tables should exist');
SELECT has_table('flight_recorder', 'table_snapshots', 'Table flight_recorder.table_snapshots should exist');
SELECT has_table('flight_recorder', 'replication_snapshots', 'Table flight_recorder.replication_snapshots should exist');
SELECT has_table('flight_recorder', 'statement_snapshots', 'Table flight_recorder.statement_snapshots should exist');
SELECT has_table('flight_recorder', 'samples', 'Table flight_recorder.samples should exist');
SELECT has_table('flight_recorder', 'wait_samples', 'Table flight_recorder.wait_samples should exist');
SELECT has_table('flight_recorder', 'activity_samples', 'Table flight_recorder.activity_samples should exist');
SELECT has_table('flight_recorder', 'progress_samples', 'Table flight_recorder.progress_samples should exist');
SELECT has_table('flight_recorder', 'lock_samples', 'Table flight_recorder.lock_samples should exist');
SELECT has_table('flight_recorder', 'config', 'Table flight_recorder.config should exist');
SELECT has_table('flight_recorder', 'collection_stats', 'P0 Safety: Table flight_recorder.collection_stats should exist');

-- Test Foreign Keys (Ensure partitioning support)
-- Using manual catalog check because pgTAP's fk_ok has issues with partitioned tables in this env
SELECT ok(
    EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'flight_recorder.wait_samples'::regclass
          AND confrelid = 'flight_recorder.samples'::regclass
          AND contype = 'f'
    ),
    'wait_samples should have FK to samples'
);

SELECT ok(
    EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'flight_recorder.activity_samples'::regclass
          AND confrelid = 'flight_recorder.samples'::regclass
          AND contype = 'f'
    ),
    'activity_samples should have FK to samples'
);

SELECT ok(
    EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'flight_recorder.progress_samples'::regclass
          AND confrelid = 'flight_recorder.samples'::regclass
          AND contype = 'f'
    ),
    'progress_samples should have FK to samples'
);

SELECT ok(
    EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'flight_recorder.lock_samples'::regclass
          AND confrelid = 'flight_recorder.samples'::regclass
          AND contype = 'f'
    ),
    'lock_samples should have FK to samples'
);

-- Test all 7 views exist
SELECT has_view('flight_recorder', 'deltas', 'View flight_recorder.deltas should exist');
SELECT has_view('flight_recorder', 'table_deltas', 'View flight_recorder.table_deltas should exist');
SELECT has_view('flight_recorder', 'recent_waits', 'View flight_recorder.recent_waits should exist');

-- =============================================================================
-- 2. FUNCTION EXISTENCE (24 tests)
-- =============================================================================

SELECT has_function('flight_recorder', '_pg_version', 'Function flight_recorder._pg_version should exist');
SELECT has_function('flight_recorder', '_get_config', 'Function flight_recorder._get_config should exist');
SELECT has_function('flight_recorder', '_has_pg_stat_statements', 'Function flight_recorder._has_pg_stat_statements should exist');
SELECT has_function('flight_recorder', '_pretty_bytes', 'Function flight_recorder._pretty_bytes should exist');
SELECT has_function('flight_recorder', '_check_circuit_breaker', 'P0 Safety: Function flight_recorder._check_circuit_breaker should exist');
SELECT has_function('flight_recorder', '_record_collection_start', 'P0 Safety: Function flight_recorder._record_collection_start should exist');
SELECT has_function('flight_recorder', '_record_collection_end', 'P0 Safety: Function flight_recorder._record_collection_end should exist');
SELECT has_function('flight_recorder', '_record_collection_skip', 'P0 Safety: Function flight_recorder._record_collection_skip should exist');
SELECT has_function('flight_recorder', '_check_schema_size', 'P1 Safety: Function flight_recorder._check_schema_size should exist');
SELECT has_function('flight_recorder', 'snapshot', 'Function flight_recorder.snapshot should exist');
SELECT has_function('flight_recorder', 'sample', 'Function flight_recorder.sample should exist');
SELECT has_function('flight_recorder', 'track_table', 'Function flight_recorder.track_table should exist');
SELECT has_function('flight_recorder', 'untrack_table', 'Function flight_recorder.untrack_table should exist');
SELECT has_function('flight_recorder', 'list_tracked_tables', 'Function flight_recorder.list_tracked_tables should exist');
SELECT has_function('flight_recorder', 'compare', 'Function flight_recorder.compare should exist');
SELECT has_function('flight_recorder', 'table_compare', 'Function flight_recorder.table_compare should exist');
SELECT has_function('flight_recorder', 'wait_summary', 'Function flight_recorder.wait_summary should exist');
SELECT has_function('flight_recorder', 'statement_compare', 'Function flight_recorder.statement_compare should exist');
SELECT has_function('flight_recorder', 'activity_at', 'Function flight_recorder.activity_at should exist');
SELECT has_function('flight_recorder', 'anomaly_report', 'Function flight_recorder.anomaly_report should exist');
SELECT has_function('flight_recorder', 'summary_report', 'Function flight_recorder.summary_report should exist');
SELECT has_function('flight_recorder', 'get_mode', 'Function flight_recorder.get_mode should exist');
SELECT has_function('flight_recorder', 'set_mode', 'Function flight_recorder.set_mode should exist');
SELECT has_function('flight_recorder', 'cleanup', 'Function flight_recorder.cleanup should exist');

-- =============================================================================
-- 3. CORE FUNCTIONALITY (10 tests)
-- =============================================================================

-- Test snapshot() function works
SELECT lives_ok(
    $$SELECT flight_recorder.snapshot()$$,
    'snapshot() function should execute without error'
);

-- Verify snapshot was captured
SELECT ok(
    (SELECT count(*) FROM flight_recorder.snapshots) >= 1,
    'At least one snapshot should be captured'
);

-- Test sample() function works
SELECT lives_ok(
    $$SELECT flight_recorder.sample()$$,
    'sample() function should execute without error'
);

-- Verify sample was captured
SELECT ok(
    (SELECT count(*) FROM flight_recorder.samples) >= 1,
    'At least one sample should be captured'
);

-- Test wait_samples captured
SELECT ok(
    (SELECT count(*) FROM flight_recorder.wait_samples) >= 1,
    'Wait samples should be captured'
);

-- Test activity_samples captured
SELECT ok(
    (SELECT count(*) FROM flight_recorder.activity_samples) >= 0,
    'Activity samples table should be queryable (may be empty)'
);

-- Test version detection works
SELECT ok(
    flight_recorder._pg_version() >= 15,
    'PostgreSQL version should be 15 or higher'
);

-- Test pg_stat_statements detection
SELECT ok(
    flight_recorder._has_pg_stat_statements() IS NOT NULL,
    'pg_stat_statements detection should work'
);

-- Test pretty bytes formatting
SELECT is(
    flight_recorder._pretty_bytes(1024),
    '1.00 KB',
    'Pretty bytes should format correctly'
);

-- Test config retrieval
SELECT is(
    flight_recorder._get_config('mode', 'normal'),
    'normal',
    'Config retrieval should work with defaults'
);

-- =============================================================================
-- 4. TABLE TRACKING (5 tests)
-- =============================================================================

-- Create a test table
CREATE TABLE public.flight_recorder_test_table (
    id serial PRIMARY KEY,
    data text
);

-- Test track_table()
SELECT lives_ok(
    $$SELECT flight_recorder.track_table('flight_recorder_test_table')$$,
    'track_table() should work'
);

-- Verify table is tracked
SELECT ok(
    EXISTS (SELECT 1 FROM flight_recorder.tracked_tables WHERE relname = 'flight_recorder_test_table'),
    'Table should be in tracked_tables'
);

-- Test list_tracked_tables()
SELECT ok(
    EXISTS (SELECT 1 FROM flight_recorder.list_tracked_tables() WHERE relname = 'flight_recorder_test_table'),
    'list_tracked_tables() should show tracked table'
);

-- Test untrack_table()
SELECT lives_ok(
    $$SELECT flight_recorder.untrack_table('flight_recorder_test_table')$$,
    'untrack_table() should work'
);

-- Verify table is untracked
SELECT ok(
    NOT EXISTS (SELECT 1 FROM flight_recorder.tracked_tables WHERE relname = 'flight_recorder_test_table'),
    'Table should be removed from tracked_tables'
);

-- Cleanup test table
DROP TABLE public.flight_recorder_test_table;

-- =============================================================================
-- 5. ANALYSIS FUNCTIONS (8 tests)
-- =============================================================================

-- Capture a second snapshot and sample for time-based queries
SELECT pg_sleep(1);
SELECT flight_recorder.snapshot();
SELECT flight_recorder.sample();

-- Get time range for queries
DO $$
DECLARE
    v_start_time TIMESTAMPTZ;
    v_end_time TIMESTAMPTZ;
BEGIN
    SELECT min(captured_at) INTO v_start_time FROM flight_recorder.samples;
    SELECT max(captured_at) INTO v_end_time FROM flight_recorder.samples;

    -- Store for later tests
    CREATE TEMP TABLE test_times (start_time TIMESTAMPTZ, end_time TIMESTAMPTZ);
    INSERT INTO test_times VALUES (v_start_time, v_end_time);
END;
$$;

-- Test compare() function
SELECT lives_ok(
    $$SELECT * FROM flight_recorder.compare(
        (SELECT start_time FROM test_times),
        (SELECT end_time FROM test_times)
    )$$,
    'compare() should execute without error'
);

-- Test wait_summary() function
SELECT lives_ok(
    $$SELECT * FROM flight_recorder.wait_summary(
        (SELECT start_time FROM test_times),
        (SELECT end_time FROM test_times)
    )$$,
    'wait_summary() should execute without error'
);

-- Test activity_at() function
SELECT lives_ok(
    $$SELECT * FROM flight_recorder.activity_at(now())$$,
    'activity_at() should execute without error'
);

-- Test anomaly_report() function
SELECT lives_ok(
    $$SELECT * FROM flight_recorder.anomaly_report(
        (SELECT start_time FROM test_times),
        (SELECT end_time FROM test_times)
    )$$,
    'anomaly_report() should execute without error'
);

-- Test summary_report() function
SELECT lives_ok(
    $$SELECT * FROM flight_recorder.summary_report(
        (SELECT start_time FROM test_times),
        (SELECT end_time FROM test_times)
    )$$,
    'summary_report() should execute without error'
);

-- Test statement_compare() function
SELECT lives_ok(
    $$SELECT * FROM flight_recorder.statement_compare(
        (SELECT start_time FROM test_times),
        (SELECT end_time FROM test_times)
    )$$,
    'statement_compare() should execute without error'
);

-- Test table_compare() (need a tracked table with activity)
CREATE TABLE public.flight_recorder_compare_test (id serial, data text);
SELECT flight_recorder.track_table('flight_recorder_compare_test');
SELECT flight_recorder.snapshot();
INSERT INTO public.flight_recorder_compare_test (data) VALUES ('test');
SELECT flight_recorder.snapshot();

SELECT lives_ok(
    $$SELECT * FROM flight_recorder.table_compare(
        'flight_recorder_compare_test',
        (SELECT start_time FROM test_times),
        now()
    )$$,
    'table_compare() should execute without error'
);

DROP TABLE public.flight_recorder_compare_test;

-- Test wait_summary returns data
SELECT ok(
    (SELECT count(*) FROM flight_recorder.wait_summary(
        (SELECT start_time FROM test_times),
        (SELECT end_time FROM test_times)
    )) > 0,
    'wait_summary() should return data'
);

-- =============================================================================
-- 6. CONFIGURATION FUNCTIONS (5 tests)
-- =============================================================================

-- Test get_mode()
SELECT lives_ok(
    $$SELECT * FROM flight_recorder.get_mode()$$,
    'get_mode() should execute without error'
);

-- Test default mode is normal
SELECT is(
    (SELECT mode FROM flight_recorder.get_mode()),
    'normal',
    'Default mode should be normal'
);

-- Test set_mode() to light
SELECT lives_ok(
    $$SELECT flight_recorder.set_mode('light')$$,
    'set_mode() should work'
);

-- Verify mode changed
SELECT is(
    (SELECT mode FROM flight_recorder.get_mode()),
    'light',
    'Mode should be changed to light'
);

-- Reset to normal
SELECT flight_recorder.set_mode('normal');

-- Test invalid mode throws error
SELECT throws_ok(
    $$SELECT flight_recorder.set_mode('invalid')$$,
    'Invalid mode: invalid. Must be normal, light, or emergency.',
    'set_mode() should reject invalid modes'
);

-- =============================================================================
-- 7. VIEWS FUNCTIONALITY (5 tests)
-- =============================================================================

-- Test deltas view
SELECT lives_ok(
    $$SELECT * FROM flight_recorder.deltas LIMIT 1$$,
    'deltas view should be queryable'
);

-- Test recent_waits view
SELECT lives_ok(
    $$SELECT * FROM flight_recorder.recent_waits LIMIT 1$$,
    'recent_waits view should be queryable'
);

-- Test recent_activity view
SELECT lives_ok(
    $$SELECT * FROM flight_recorder.recent_activity LIMIT 1$$,
    'recent_activity view should be queryable'
);

-- Test recent_locks view
SELECT lives_ok(
    $$SELECT * FROM flight_recorder.recent_locks LIMIT 1$$,
    'recent_locks view should be queryable'
);

-- Test recent_progress view
SELECT lives_ok(
    $$SELECT * FROM flight_recorder.recent_progress LIMIT 1$$,
    'recent_progress view should be queryable'
);

-- =============================================================================
-- 8. KILL SWITCH (6 tests)
-- =============================================================================

-- Test disable() function exists
SELECT has_function('flight_recorder', 'disable', 'Function flight_recorder.disable should exist');

-- Test enable() function exists
SELECT has_function('flight_recorder', 'enable', 'Function flight_recorder.enable should exist');

-- Test disable() stops collection
SELECT lives_ok(
    $$SELECT flight_recorder.disable()$$,
    'disable() should execute without error'
);

-- Verify jobs are unscheduled
SELECT ok(
    NOT EXISTS (SELECT 1 FROM cron.job WHERE jobname LIKE 'flight_recorder%'),
    'All telemetry cron jobs should be unscheduled after disable()'
);

-- Test enable() restarts collection
SELECT lives_ok(
    $$SELECT flight_recorder.enable()$$,
    'enable() should execute without error'
);

-- Verify jobs are rescheduled (4 jobs: snapshot, sample, cleanup, partition)
SELECT ok(
    (SELECT count(*) FROM cron.job WHERE jobname LIKE 'flight_recorder%') = 4,
    'All 4 telemetry cron jobs should be rescheduled after enable()'
);

-- =============================================================================
-- 9. P0 SAFETY FEATURES (10 tests)
-- =============================================================================

-- Test circuit breaker configuration exists
SELECT ok(
    EXISTS (SELECT 1 FROM flight_recorder.config WHERE key = 'circuit_breaker_enabled'),
    'P0 Safety: Circuit breaker config should exist'
);

SELECT ok(
    EXISTS (SELECT 1 FROM flight_recorder.config WHERE key = 'circuit_breaker_threshold_ms'),
    'P0 Safety: Circuit breaker threshold config should exist'
);

-- Test collection stats are recorded for sample()
SELECT lives_ok(
    $$SELECT flight_recorder.sample()$$,
    'P0 Safety: sample() with stats tracking should execute without error'
);

SELECT ok(
    EXISTS (SELECT 1 FROM flight_recorder.collection_stats WHERE collection_type = 'sample'),
    'P0 Safety: Collection stats should be recorded for sample()'
);

SELECT ok(
    (SELECT success FROM flight_recorder.collection_stats WHERE collection_type = 'sample' ORDER BY started_at DESC LIMIT 1) = true,
    'P0 Safety: Last sample collection should be marked as successful'
);

-- Test collection stats are recorded for snapshot()
SELECT lives_ok(
    $$SELECT flight_recorder.snapshot()$$,
    'P0 Safety: snapshot() with stats tracking should execute without error'
);

SELECT ok(
    EXISTS (SELECT 1 FROM flight_recorder.collection_stats WHERE collection_type = 'snapshot'),
    'P0 Safety: Collection stats should be recorded for snapshot()'
);

SELECT ok(
    (SELECT success FROM flight_recorder.collection_stats WHERE collection_type = 'snapshot' ORDER BY started_at DESC LIMIT 1) = true,
    'P0 Safety: Last snapshot collection should be marked as successful'
);

-- Test circuit breaker can be triggered
UPDATE flight_recorder.config SET value = '100' WHERE key = 'circuit_breaker_threshold_ms';

-- Clear existing sample collections to ensure our fake one is the most recent
DELETE FROM flight_recorder.collection_stats WHERE collection_type = 'sample';

-- Insert a fake long-running collection (most recent)
INSERT INTO flight_recorder.collection_stats (collection_type, started_at, completed_at, duration_ms, success)
VALUES ('sample', now(), now(), 10000, true);

-- Circuit breaker should now skip
SELECT ok(
    flight_recorder._check_circuit_breaker('sample') = true,
    'P0 Safety: Circuit breaker should trip after threshold exceeded'
);

-- Reset threshold
UPDATE flight_recorder.config SET value = '5000' WHERE key = 'circuit_breaker_threshold_ms';

-- Test circuit breaker can be disabled
UPDATE flight_recorder.config SET value = 'false' WHERE key = 'circuit_breaker_enabled';

SELECT ok(
    flight_recorder._check_circuit_breaker('sample') = false,
    'P0 Safety: Circuit breaker should not trip when disabled'
);

-- Re-enable
UPDATE flight_recorder.config SET value = 'true' WHERE key = 'circuit_breaker_enabled';

-- =============================================================================
-- 10. P1 SAFETY FEATURES (7 tests)
-- =============================================================================

-- Test schema size monitoring function exists
SELECT has_function('flight_recorder', '_check_schema_size', 'P1 Safety: Function flight_recorder._check_schema_size should exist');

-- Test schema size monitoring config exists
SELECT ok(
    EXISTS (SELECT 1 FROM flight_recorder.config WHERE key = 'schema_size_warning_mb'),
    'P1 Safety: Schema size warning config should exist'
);

SELECT ok(
    EXISTS (SELECT 1 FROM flight_recorder.config WHERE key = 'schema_size_critical_mb'),
    'P1 Safety: Schema size critical config should exist'
);

-- Test schema size monitoring returns results
SELECT ok(
    (SELECT count(*) FROM flight_recorder._check_schema_size()) = 1,
    'P1 Safety: Schema size check should return results'
);

-- Test schema size is below warning threshold (should be for fresh install)
SELECT ok(
    (SELECT status FROM flight_recorder._check_schema_size()) = 'OK',
    'P1 Safety: Fresh install should have OK schema size status'
);

-- Test cleanup() function now returns 3 columns including vacuumed_tables
SELECT lives_ok(
    $$SELECT * FROM flight_recorder.cleanup('1 day')$$,
    'P1 Safety: cleanup() with VACUUM should execute without error'
);

-- Verify cleanup returns vacuumed_tables column
SELECT ok(
    (SELECT vacuumed_tables FROM flight_recorder.cleanup('1 day')) >= 0,
    'P1 Safety: cleanup() should return vacuum count'
);

-- =============================================================================
-- 10. P2 SAFETY FEATURES (12 tests)
-- =============================================================================

-- Test P2: Automatic mode switching function exists
SELECT has_function(
    'flight_recorder', '_check_and_adjust_mode',
    'P2: Function flight_recorder._check_and_adjust_mode should exist'
);

-- Test P2: Auto mode config entries exist
SELECT ok(
    EXISTS (SELECT 1 FROM flight_recorder.config WHERE key = 'auto_mode_enabled'),
    'P2: Auto mode enabled config should exist'
);

SELECT ok(
    EXISTS (SELECT 1 FROM flight_recorder.config WHERE key = 'auto_mode_connections_threshold'),
    'P2: Auto mode connections threshold config should exist'
);

SELECT ok(
    EXISTS (SELECT 1 FROM flight_recorder.config WHERE key = 'auto_mode_trips_threshold'),
    'P2: Auto mode trips threshold config should exist'
);

-- Test P2: Auto mode defaults to enabled (A-GRADE safety improvement)
SELECT ok(
    (SELECT value FROM flight_recorder.config WHERE key = 'auto_mode_enabled') = 'true',
    'P2: Auto mode should be enabled by default (A-GRADE safety)'
);

-- Test P2: Configurable retention config entries exist
SELECT ok(
    EXISTS (SELECT 1 FROM flight_recorder.config WHERE key = 'retention_samples_days'),
    'P2: Samples retention config should exist'
);

SELECT ok(
    EXISTS (SELECT 1 FROM flight_recorder.config WHERE key = 'retention_snapshots_days'),
    'P2: Snapshots retention config should exist'
);

SELECT ok(
    EXISTS (SELECT 1 FROM flight_recorder.config WHERE key = 'retention_statements_days'),
    'P2: Statements retention config should exist'
);

-- Test P2: cleanup() now returns 4 columns (added deleted_statements)
SELECT lives_ok(
    $$SELECT deleted_snapshots, deleted_samples, deleted_statements, vacuumed_tables FROM flight_recorder.cleanup()$$,
    'P2: cleanup() should return 4 columns with configurable retention'
);

-- Test P2: Partition management functions exist
SELECT has_function(
    'flight_recorder', 'create_next_partition',
    'P2: Function flight_recorder.create_next_partition should exist'
);

SELECT has_function(
    'flight_recorder', 'drop_old_partitions',
    'P2: Function flight_recorder.drop_old_partitions should exist'
);

SELECT has_function(
    'flight_recorder', 'partition_status',
    'P2: Function flight_recorder.partition_status should exist'
);

-- =============================================================================
-- 11. P3 FEATURES - Self-Monitoring and Health Checks (9 tests)
-- =============================================================================

-- Test P3: Config entries exist
SELECT ok(
    EXISTS (SELECT 1 FROM flight_recorder.config WHERE key = 'self_monitoring_enabled'),
    'P3: Self-monitoring enabled config should exist'
);

SELECT ok(
    EXISTS (SELECT 1 FROM flight_recorder.config WHERE key = 'health_check_enabled'),
    'P3: Health check enabled config should exist'
);

-- Test P3: Health check function exists
SELECT has_function(
    'flight_recorder', 'health_check',
    'P3: Function flight_recorder.health_check should exist'
);

-- Test P3: Health check returns results
SELECT ok(
    (SELECT count(*) FROM flight_recorder.health_check()) >= 5,
    'P3: health_check() should return at least 5 components'
);

-- Test P3: Health check shows enabled status
SELECT ok(
    EXISTS (
        SELECT 1 FROM flight_recorder.health_check()
        WHERE component = 'Flight Recorder System'
          AND status = 'ENABLED'
    ),
    'P3: health_check() should show system as enabled'
);

-- Test P3: Performance report function exists
SELECT has_function(
    'flight_recorder', 'performance_report',
    'P3: Function flight_recorder.performance_report should exist'
);

-- Test P3: Performance report returns results
SELECT ok(
    (SELECT count(*) FROM flight_recorder.performance_report('24 hours')) >= 5,
    'P3: performance_report() should return at least 5 metrics'
);

-- Test P3: Performance report includes key metrics
SELECT ok(
    EXISTS (
        SELECT 1 FROM flight_recorder.performance_report('24 hours')
        WHERE metric = 'Schema Size'
    ),
    'P3: performance_report() should include schema size metric'
);

-- Test P3: Performance report includes assessment
SELECT ok(
    (
        SELECT count(*) FROM flight_recorder.performance_report('24 hours')
        WHERE assessment IS NOT NULL
    ) >= 5,
    'P3: performance_report() should include assessments for all metrics'
);

-- =============================================================================
-- 12. P4 FEATURES - Advanced Features (10 tests)
-- =============================================================================

-- Test P4: Alert config entries exist
SELECT ok(
    EXISTS (SELECT 1 FROM flight_recorder.config WHERE key = 'alert_enabled'),
    'P4: Alert enabled config should exist'
);

SELECT ok(
    EXISTS (SELECT 1 FROM flight_recorder.config WHERE key = 'alert_circuit_breaker_count'),
    'P4: Alert circuit breaker count config should exist'
);

SELECT ok(
    EXISTS (SELECT 1 FROM flight_recorder.config WHERE key = 'alert_schema_size_mb'),
    'P4: Alert schema size config should exist'
);

-- Test P4: Alert function exists
SELECT has_function(
    'flight_recorder', 'check_alerts',
    'P4: Function flight_recorder.check_alerts should exist'
);

-- Test P4: Alerts disabled by default
SELECT ok(
    (SELECT value FROM flight_recorder.config WHERE key = 'alert_enabled') = 'false',
    'P4: Alerts should be disabled by default'
);

-- Test P4: Export function exists
SELECT has_function(
    'flight_recorder', 'export_json',
    'P4: Function flight_recorder.export_json should exist'
);

-- Test P4: Export returns valid JSON
SELECT lives_ok(
    $$SELECT flight_recorder.export_json(now() - interval '1 hour', now())$$,
    'P4: export_json() should execute without error'
);

-- Test P4: Export includes metadata
SELECT ok(
    (SELECT flight_recorder.export_json(now() - interval '1 hour', now()) ? 'meta'),
    'P4: export_json() should include meta in result'
);

-- Test P4: Config recommendations function exists
SELECT has_function(
    'flight_recorder', 'config_recommendations',
    'P4: Function flight_recorder.config_recommendations should exist'
);

-- Test P4: Config recommendations returns results
SELECT ok(
    (SELECT count(*) FROM flight_recorder.config_recommendations()) >= 1,
    'P4: config_recommendations() should return at least one row'
);

SELECT * FROM finish();
ROLLBACK;
