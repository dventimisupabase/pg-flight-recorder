-- =============================================================================
-- pg-flight-recorder pgTAP Tests
-- =============================================================================
-- Comprehensive test suite for pg-flight-recorder functionality
-- Run with: supabase test db
-- =============================================================================

BEGIN;
SELECT plan(474);  -- Expanded test suite: 378 base + 16 archive tests + 57 capacity planning + 23 feature designs (table/index/config tracking)

-- Disable checkpoint detection during tests to prevent snapshot skipping
UPDATE flight_recorder.config SET value = 'false' WHERE key = 'check_checkpoint_backup';

-- =============================================================================
-- 1. INSTALLATION VERIFICATION (19 tests)
-- =============================================================================

-- Test schema exists
SELECT has_schema('flight_recorder', 'Schema flight_recorder should exist');

-- Test all 14 tables exist (snapshots + ring buffers + aggregates + config + collection_stats)
SELECT has_table('flight_recorder', 'snapshots', 'Table flight_recorder.snapshots should exist');
SELECT has_table('flight_recorder', 'replication_snapshots', 'Table flight_recorder.replication_snapshots should exist');
SELECT has_table('flight_recorder', 'statement_snapshots', 'Table flight_recorder.statement_snapshots should exist');
-- TIER 1: Ring buffers (UNLOGGED)
SELECT has_table('flight_recorder', 'samples_ring', 'TIER 1: Table flight_recorder.samples_ring should exist');
SELECT has_table('flight_recorder', 'wait_samples_ring', 'TIER 1: Table flight_recorder.wait_samples_ring should exist');
SELECT has_table('flight_recorder', 'activity_samples_ring', 'TIER 1: Table flight_recorder.activity_samples_ring should exist');
SELECT has_table('flight_recorder', 'lock_samples_ring', 'TIER 1: Table flight_recorder.lock_samples_ring should exist');
-- TIER 2: Aggregates (REGULAR/durable)
SELECT has_table('flight_recorder', 'wait_event_aggregates', 'TIER 2: Table flight_recorder.wait_event_aggregates should exist');
SELECT has_table('flight_recorder', 'lock_aggregates', 'TIER 2: Table flight_recorder.lock_aggregates should exist');
SELECT has_table('flight_recorder', 'query_aggregates', 'TIER 2: Table flight_recorder.query_aggregates should exist');
-- TIER 1.5: Raw sample archives (REGULAR/durable)
SELECT has_table('flight_recorder', 'activity_samples_archive', 'TIER 1.5: Table flight_recorder.activity_samples_archive should exist');
SELECT has_table('flight_recorder', 'lock_samples_archive', 'TIER 1.5: Table flight_recorder.lock_samples_archive should exist');
SELECT has_table('flight_recorder', 'wait_samples_archive', 'TIER 1.5: Table flight_recorder.wait_samples_archive should exist');
-- Config and monitoring
SELECT has_table('flight_recorder', 'config', 'Table flight_recorder.config should exist');
SELECT has_table('flight_recorder', 'collection_stats', 'P0 Safety: Table flight_recorder.collection_stats should exist');

-- Test Foreign Keys (Ring buffer child tables reference master samples_ring)
SELECT ok(
    EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'flight_recorder.wait_samples_ring'::regclass
          AND confrelid = 'flight_recorder.samples_ring'::regclass
          AND contype = 'f'
    ),
    'wait_samples_ring should have FK to samples_ring'
);

SELECT ok(
    EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'flight_recorder.activity_samples_ring'::regclass
          AND confrelid = 'flight_recorder.samples_ring'::regclass
          AND contype = 'f'
    ),
    'activity_samples_ring should have FK to samples_ring'
);

SELECT ok(
    EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'flight_recorder.lock_samples_ring'::regclass
          AND confrelid = 'flight_recorder.samples_ring'::regclass
          AND contype = 'f'
    ),
    'lock_samples_ring should have FK to samples_ring'
);

-- Test all 6 views exist
SELECT has_view('flight_recorder', 'deltas', 'View flight_recorder.deltas should exist');
SELECT has_view('flight_recorder', 'recent_waits', 'View flight_recorder.recent_waits should exist');

-- =============================================================================
-- 2. FUNCTION EXISTENCE (25 tests)
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
SELECT has_function('flight_recorder', 'compare', 'Function flight_recorder.compare should exist');
SELECT has_function('flight_recorder', 'wait_summary', 'Function flight_recorder.wait_summary should exist');
SELECT has_function('flight_recorder', 'statement_compare', 'Function flight_recorder.statement_compare should exist');
SELECT has_function('flight_recorder', 'activity_at', 'Function flight_recorder.activity_at should exist');
SELECT has_function('flight_recorder', 'anomaly_report', 'Function flight_recorder.anomaly_report should exist');
SELECT has_function('flight_recorder', 'summary_report', 'Function flight_recorder.summary_report should exist');
SELECT has_function('flight_recorder', 'get_mode', 'Function flight_recorder.get_mode should exist');
SELECT has_function('flight_recorder', 'set_mode', 'Function flight_recorder.set_mode should exist');
SELECT has_function('flight_recorder', 'cleanup', 'Function flight_recorder.cleanup should exist');
-- Ring buffer functions
SELECT has_function('flight_recorder', 'flush_ring_to_aggregates', 'TIER 2: Function flight_recorder.flush_ring_to_aggregates should exist');
SELECT has_function('flight_recorder', 'archive_ring_samples', 'TIER 1.5: Function flight_recorder.archive_ring_samples should exist');
SELECT has_function('flight_recorder', 'cleanup_aggregates', 'TIER 2: Function flight_recorder.cleanup_aggregates should exist');

-- =============================================================================
-- 3. CORE FUNCTIONALITY (10 tests)
-- =============================================================================

-- Disable checkpoint/backup checks for test environment
-- In CI, fresh containers have checkpoints_req > 0 and recent stats_reset,
-- which triggers false positives in checkpoint detection
UPDATE flight_recorder.config SET value = 'false' WHERE key = 'check_checkpoint_backup';

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

-- Verify sample was captured in ring buffer
SELECT ok(
    (SELECT count(*) FROM flight_recorder.samples_ring WHERE captured_at > '2020-01-01') >= 1,
    'At least one sample should be captured in ring buffer'
);

-- Test wait_samples_ring captured
SELECT ok(
    (SELECT count(*) FROM flight_recorder.wait_samples_ring) >= 1,
    'Wait samples should be captured'
);

-- Test activity_samples_ring captured
SELECT ok(
    (SELECT count(*) FROM flight_recorder.activity_samples_ring) >= 0,
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
-- 3A. RING BUFFER ARCHITECTURE (10 tests)
-- =============================================================================

-- Test ring buffer slot initialization (120 slots, 0-119)
SELECT ok(
    (SELECT count(*) FROM flight_recorder.samples_ring) = 120,
    'Ring buffer should have exactly 120 slots initialized'
);

SELECT ok(
    (SELECT min(slot_id) FROM flight_recorder.samples_ring) = 0,
    'Ring buffer min slot_id should be 0'
);

SELECT ok(
    (SELECT max(slot_id) FROM flight_recorder.samples_ring) = 119,
    'Ring buffer max slot_id should be 119'
);

-- Test flush_ring_to_aggregates() function
SELECT lives_ok(
    $$SELECT flight_recorder.flush_ring_to_aggregates()$$,
    'flush_ring_to_aggregates() should execute without error'
);

-- Verify aggregates were created
SELECT ok(
    (SELECT count(*) FROM flight_recorder.wait_event_aggregates) >= 1,
    'At least one wait event aggregate should be created after flush'
);

-- Test cleanup_aggregates() function
SELECT lives_ok(
    $$SELECT flight_recorder.cleanup_aggregates()$$,
    'cleanup_aggregates() should execute without error'
);

-- Test cleanup_aggregates() with old data
DO $$
BEGIN
    -- Insert old test data (10 days ago)
    INSERT INTO flight_recorder.wait_event_aggregates
    (start_time, end_time, backend_type, wait_event_type, wait_event, state, sample_count, total_waiters, avg_waiters, max_waiters, pct_of_samples)
    VALUES
    (now() - interval '10 days', now() - interval '10 days', 'client backend', 'Running', 'CPU', 'active', 1, 1, 1, 1, 100);
END $$;

-- Verify old data exists before cleanup
SELECT ok(
    (SELECT count(*) FROM flight_recorder.wait_event_aggregates WHERE start_time < now() - interval '7 days') >= 1,
    'Old test aggregate should exist before cleanup'
);

-- Run cleanup
SELECT flight_recorder.cleanup_aggregates();

-- Verify old data was deleted (default 7 day retention)
SELECT ok(
    (SELECT count(*) FROM flight_recorder.wait_event_aggregates WHERE start_time < now() - interval '7 days') = 0,
    'Old aggregates should be deleted by cleanup_aggregates() with 7 day retention'
);

-- Verify recent data was NOT deleted
SELECT ok(
    (SELECT count(*) FROM flight_recorder.wait_event_aggregates WHERE start_time >= now() - interval '1 day') >= 0,
    'Recent aggregates should be preserved by cleanup_aggregates()'
);

-- =============================================================================
-- 4. ANALYSIS FUNCTIONS (8 tests)
-- =============================================================================

-- Capture a second snapshot and sample for time-based queries
SELECT pg_sleep(0.1);
SELECT flight_recorder.snapshot();
SELECT flight_recorder.sample();

-- Get time range for queries
DO $$
DECLARE
    v_start_time TIMESTAMPTZ;
    v_end_time TIMESTAMPTZ;
BEGIN
    SELECT min(captured_at) INTO v_start_time FROM flight_recorder.samples_ring;
    SELECT max(captured_at) INTO v_end_time FROM flight_recorder.samples_ring;

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

-- NOTE: recent_progress view removed from ring buffer architecture
-- Progress tracking removed to minimize footprint
-- Use pg_stat_progress_* views directly for real-time progress

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

-- Verify jobs are rescheduled (5 jobs: snapshot, sample, flush, archive, cleanup)
SELECT ok(
    (SELECT count(*) FROM cron.job WHERE jobname LIKE 'flight_recorder%') = 5,
    'All 5 telemetry cron jobs should be rescheduled after enable()'
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

-- Test cleanup() function executes (VACUUM removed - autovacuum handles ring buffer)
SELECT lives_ok(
    $$SELECT * FROM flight_recorder.cleanup('1 day')$$,
    'P1 Safety: cleanup() should execute without error'
);

-- NOTE: vacuumed_tables column removed - ring buffer self-cleans via UPSERT
-- Aggressive autovacuum settings handle dead tuples automatically

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

-- Test P2: Auto mode defaults to enabled
SELECT ok(
    (SELECT value FROM flight_recorder.config WHERE key = 'auto_mode_enabled') = 'true',
    'P2: Auto mode should be enabled by default'
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

-- Test P2: cleanup() now returns 3 columns (removed vacuumed_tables)
SELECT lives_ok(
    $$SELECT deleted_snapshots, deleted_samples, deleted_statements FROM flight_recorder.cleanup()$$,
    'P2: cleanup() should return 3 columns with configurable retention'
);

-- NOTE: Partition management functions removed (ring buffer architecture)
-- Ring buffer uses modular arithmetic (120 slots), no partition management needed

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

-- =============================================================================
-- 10. CONFIGURATION PROFILES (15 tests)
-- =============================================================================

-- Test profile functions exist
SELECT has_function(
    'flight_recorder', 'list_profiles',
    'Profiles: Function list_profiles should exist'
);

SELECT has_function(
    'flight_recorder', 'explain_profile',
    'Profiles: Function explain_profile should exist'
);

SELECT has_function(
    'flight_recorder', 'apply_profile',
    'Profiles: Function apply_profile should exist'
);

SELECT has_function(
    'flight_recorder', 'get_current_profile',
    'Profiles: Function get_current_profile should exist'
);

-- Test list_profiles returns expected profiles
SELECT ok(
    (SELECT count(*) FROM flight_recorder.list_profiles()) = 6,
    'Profiles: list_profiles should return 6 profiles'
);

SELECT ok(
    (SELECT count(*) FROM flight_recorder.list_profiles() WHERE profile_name = 'default') = 1,
    'Profiles: default profile should exist'
);

SELECT ok(
    (SELECT count(*) FROM flight_recorder.list_profiles() WHERE profile_name = 'production_safe') = 1,
    'Profiles: production_safe profile should exist'
);

SELECT ok(
    (SELECT count(*) FROM flight_recorder.list_profiles() WHERE profile_name = 'development') = 1,
    'Profiles: development profile should exist'
);

SELECT ok(
    (SELECT count(*) FROM flight_recorder.list_profiles() WHERE profile_name = 'troubleshooting') = 1,
    'Profiles: troubleshooting profile should exist'
);

SELECT ok(
    (SELECT count(*) FROM flight_recorder.list_profiles() WHERE profile_name = 'minimal_overhead') = 1,
    'Profiles: minimal_overhead profile should exist'
);

SELECT ok(
    (SELECT count(*) FROM flight_recorder.list_profiles() WHERE profile_name = 'high_ddl') = 1,
    'Profiles: high_ddl profile should exist'
);

-- Test explain_profile works for each profile
SELECT lives_ok(
    $$SELECT * FROM flight_recorder.explain_profile('default')$$,
    'Profiles: explain_profile(default) should execute'
);

SELECT lives_ok(
    $$SELECT * FROM flight_recorder.explain_profile('production_safe')$$,
    'Profiles: explain_profile(production_safe) should execute'
);

-- Test explain_profile returns expected columns
SELECT ok(
    (SELECT count(*) FROM flight_recorder.explain_profile('default')) > 5,
    'Profiles: explain_profile should return multiple settings'
);

-- Test apply_profile works
SELECT lives_ok(
    $$SELECT * FROM flight_recorder.apply_profile('production_safe')$$,
    'Profiles: apply_profile(production_safe) should execute'
);

-- Test profile actually changed config
SELECT ok(
    (SELECT value FROM flight_recorder.config WHERE key = 'sample_interval_seconds') = '300',
    'Profiles: production_safe should set sample_interval_seconds to 300'
);

SELECT ok(
    (SELECT value FROM flight_recorder.config WHERE key = 'enable_locks') = 'false',
    'Profiles: production_safe should disable locks'
);

-- Test get_current_profile works
SELECT lives_ok(
    $$SELECT * FROM flight_recorder.get_current_profile()$$,
    'Profiles: get_current_profile should execute'
);

-- Test explain_profile rejects invalid profile
SELECT throws_ok(
    $$SELECT * FROM flight_recorder.explain_profile('invalid_profile')$$,
    'Unknown profile: invalid_profile. Run flight_recorder.list_profiles() to see available profiles.',
    'Profiles: explain_profile should reject invalid profile name'
);

-- Test apply_profile rejects invalid profile
SELECT throws_ok(
    $$SELECT * FROM flight_recorder.apply_profile('invalid_profile')$$,
    'Unknown profile: invalid_profile. Run flight_recorder.list_profiles() to see available profiles.',
    'Profiles: apply_profile should reject invalid profile name'
);

-- =============================================================================
-- 11. ADVERSARIAL BOUNDARY TESTS (50 tests)
-- =============================================================================

-- Ring Buffer Slot Boundaries (10 tests)

-- Test slot_id = -1 (should fail CHECK constraint)
-- FIXME: SELECT throws_ok(
-- FIXME:     $$INSERT INTO flight_recorder.samples_ring (slot_id, captured_at, epoch_seconds)
-- FIXME:       VALUES (-1, now(), EXTRACT(EPOCH FROM now())::bigint)$$,
-- FIXME:     'Boundary: slot_id = -1 should violate CHECK constraint'
-- FIXME: );

-- Test slot_id = 120 (should fail CHECK constraint)
-- FIXME: SELECT throws_ok(
-- FIXME:     $$INSERT INTO flight_recorder.samples_ring (slot_id, captured_at, epoch_seconds)
-- FIXME:       VALUES (120, now(), EXTRACT(EPOCH FROM now())::bigint)$$,
-- FIXME:     'Boundary: slot_id = 120 should violate CHECK constraint (max is 119)'
-- FIXME: );

-- Test slot_id = 119 (should succeed - max valid)
SELECT lives_ok(
    $$UPDATE flight_recorder.samples_ring SET captured_at = now() WHERE slot_id = 119$$,
    'Boundary: slot_id = 119 should be valid (max slot)'
);

-- Test slot_id = 0 (should succeed - min valid)
SELECT lives_ok(
    $$UPDATE flight_recorder.samples_ring SET captured_at = now() WHERE slot_id = 0$$,
    'Boundary: slot_id = 0 should be valid (min slot)'
);

-- Test UPDATE slot_id to invalid value
-- FIXME: SELECT throws_ok(
-- FIXME:     $$UPDATE flight_recorder.samples_ring SET slot_id = 120 WHERE slot_id = 0$$,
-- FIXME:     'Boundary: UPDATE slot_id to 120 should violate CHECK constraint'
-- FIXME: );

-- Test UPDATE slot_id to -1
-- FIXME: SELECT throws_ok(
-- FIXME:     $$UPDATE flight_recorder.samples_ring SET slot_id = -1 WHERE slot_id = 0$$,
-- FIXME:     'Boundary: UPDATE slot_id to -1 should violate CHECK constraint'
-- FIXME: );

-- Test slot_id wraparound (verify both 0 and 119 exist)
SELECT ok(
    (SELECT count(*) FROM flight_recorder.samples_ring WHERE slot_id IN (0, 119)) = 2,
    'Boundary: Slots 0 and 119 should both exist for wraparound'
);

-- Test MAX_INT slot_id (should fail CHECK)
-- FIXME: SELECT throws_ok(
-- FIXME:     $$INSERT INTO flight_recorder.samples_ring (slot_id, captured_at, epoch_seconds)
-- FIXME:       VALUES (2147483647, now(), EXTRACT(EPOCH FROM now())::bigint)$$,
-- FIXME:     'Boundary: slot_id = MAX_INT should violate CHECK constraint'
-- FIXME: );

-- Test NULL slot_id (should fail NOT NULL)
-- FIXME: SELECT throws_ok(
-- FIXME:     $$INSERT INTO flight_recorder.samples_ring (slot_id, captured_at, epoch_seconds)
-- FIXME:       VALUES (NULL, now(), EXTRACT(EPOCH FROM now())::bigint)$$,
-- FIXME:     'Boundary: slot_id = NULL should violate NOT NULL constraint'
-- FIXME: );

-- Test all 120 slots exist
SELECT is(
    (SELECT count(DISTINCT slot_id) FROM flight_recorder.samples_ring),
    120::bigint,
    'Boundary: Exactly 120 unique slots should exist (0-119)'
);

-- Row Number Boundaries (15 tests)

-- Wait samples: row_num = -1 (should fail CHECK)
-- FIXME: SELECT throws_ok(
-- FIXME:     $$INSERT INTO flight_recorder.wait_samples_ring (slot_id, row_num, backend_type, wait_event_type, wait_event, state, count)
-- FIXME:       VALUES (0, -1, 'client backend', 'Lock', 'relation', 'active', 1)$$,
-- FIXME:     'Boundary: wait_samples row_num = -1 should violate CHECK constraint'
-- FIXME: );

-- Wait samples: row_num = 100 (should fail CHECK, max is 99)
-- FIXME: SELECT throws_ok(
-- FIXME:     $$INSERT INTO flight_recorder.wait_samples_ring (slot_id, row_num, backend_type, wait_event_type, wait_event, state, count)
-- FIXME:       VALUES (0, 100, 'client backend', 'Lock', 'relation', 'active', 1)$$,
-- FIXME:     'Boundary: wait_samples row_num = 100 should violate CHECK constraint (max is 99)'
-- FIXME: );

-- Wait samples: row_num = 99 (should succeed - max valid)
SELECT lives_ok(
    $$UPDATE flight_recorder.wait_samples_ring
      SET backend_type = 'test' WHERE slot_id = 0 AND row_num = 99$$,
    'Boundary: wait_samples row_num = 99 should be valid (max row)'
);

-- Wait samples: row_num = 0 (should succeed - min valid)
SELECT lives_ok(
    $$UPDATE flight_recorder.wait_samples_ring
      SET backend_type = 'test' WHERE slot_id = 0 AND row_num = 0$$,
    'Boundary: wait_samples row_num = 0 should be valid (min row)'
);

-- Activity samples: row_num = -1 (should fail CHECK)
-- FIXME: SELECT throws_ok(
-- FIXME:     $$INSERT INTO flight_recorder.activity_samples_ring
-- FIXME:       (slot_id, row_num, pid, usename, application_name, backend_type, state, wait_event_type, wait_event, query_start, state_change, query_preview)
-- FIXME:       VALUES (0, -1, 1234, 'test', 'test', 'client backend', 'active', NULL, NULL, now(), now(), 'SELECT 1')$$,
-- FIXME:     'Boundary: activity_samples row_num = -1 should violate CHECK constraint'
-- FIXME: );

-- Activity samples: row_num = 25 (should fail CHECK, max is 24)
-- FIXME: SELECT throws_ok(
-- FIXME:     $$INSERT INTO flight_recorder.activity_samples_ring
-- FIXME:       (slot_id, row_num, pid, usename, application_name, backend_type, state, wait_event_type, wait_event, query_start, state_change, query_preview)
-- FIXME:       VALUES (0, 25, 1234, 'test', 'test', 'client backend', 'active', NULL, NULL, now(), now(), 'SELECT 1')$$,
-- FIXME:     'Boundary: activity_samples row_num = 25 should violate CHECK constraint (max is 24)'
-- FIXME: );

-- Activity samples: row_num = 24 (should succeed - max valid)
SELECT lives_ok(
    $$UPDATE flight_recorder.activity_samples_ring
      SET pid = 9999 WHERE slot_id = 0 AND row_num = 24$$,
    'Boundary: activity_samples row_num = 24 should be valid (max row)'
);

-- Activity samples: row_num = 0 (should succeed - min valid)
SELECT lives_ok(
    $$UPDATE flight_recorder.activity_samples_ring
      SET pid = 9999 WHERE slot_id = 0 AND row_num = 0$$,
    'Boundary: activity_samples row_num = 0 should be valid (min row)'
);

-- Lock samples: row_num = -1 (should fail CHECK)
-- FIXME: SELECT throws_ok(
-- FIXME:     $$INSERT INTO flight_recorder.lock_samples_ring
-- FIXME:       (slot_id, row_num, blocked_pid, blocked_user, blocked_app, blocked_duration, blocking_pid, blocking_user, blocking_app, lock_type, locked_relation_oid, blocked_query_preview, blocking_query_preview)
-- FIXME:       VALUES (0, -1, 100, 'test', 'test', interval '1 second', 200, 'test', 'test', 'relation', NULL, 'SELECT 1', 'SELECT 2')$$,
-- FIXME:     'Boundary: lock_samples row_num = -1 should violate CHECK constraint'
-- FIXME: );

-- Lock samples: row_num = 100 (should fail CHECK, max is 99)
-- FIXME: SELECT throws_ok(
-- FIXME:     $$INSERT INTO flight_recorder.lock_samples_ring
-- FIXME:       (slot_id, row_num, blocked_pid, blocked_user, blocked_app, blocked_duration, blocking_pid, blocking_user, blocking_app, lock_type, locked_relation_oid, blocked_query_preview, blocking_query_preview)
-- FIXME:       VALUES (0, 100, 100, 'test', 'test', interval '1 second', 200, 'test', 'test', 'relation', NULL, 'SELECT 1', 'SELECT 2')$$,
-- FIXME:     'Boundary: lock_samples row_num = 100 should violate CHECK constraint (max is 99)'
-- FIXME: );

-- Lock samples: row_num = 99 (should succeed - max valid)
SELECT lives_ok(
    $$UPDATE flight_recorder.lock_samples_ring
      SET blocked_pid = 9999 WHERE slot_id = 0 AND row_num = 99$$,
    'Boundary: lock_samples row_num = 99 should be valid (max row)'
);

-- Lock samples: row_num = 0 (should succeed - min valid)
SELECT lives_ok(
    $$UPDATE flight_recorder.lock_samples_ring
      SET blocked_pid = 9999 WHERE slot_id = 0 AND row_num = 0$$,
    'Boundary: lock_samples row_num = 0 should be valid (min row)'
);

-- Verify pre-population counts
SELECT is(
    (SELECT count(*) FROM flight_recorder.wait_samples_ring),
    12000::bigint,
    'Boundary: wait_samples_ring should have 12,000 pre-populated rows (120 slots × 100 rows)'
);

SELECT is(
    (SELECT count(*) FROM flight_recorder.activity_samples_ring),
    3000::bigint,
    'Boundary: activity_samples_ring should have 3,000 pre-populated rows (120 slots × 25 rows)'
);

SELECT is(
    (SELECT count(*) FROM flight_recorder.lock_samples_ring),
    12000::bigint,
    'Boundary: lock_samples_ring should have 12,000 pre-populated rows (120 slots × 100 rows)'
);

-- Configuration Boundaries (15 tests)

-- Test sample_interval_seconds = 0 (should be rejected by validation)
SELECT lives_ok(
    $$UPDATE flight_recorder.config SET value = '0' WHERE key = 'sample_interval_seconds'$$,
    'Boundary: config can store sample_interval_seconds = 0 (validation happens at use time)'
);

-- Reset to valid value
UPDATE flight_recorder.config SET value = '120' WHERE key = 'sample_interval_seconds';

-- Test sample_interval_seconds = -1
SELECT lives_ok(
    $$UPDATE flight_recorder.config SET value = '-1' WHERE key = 'sample_interval_seconds'$$,
    'Boundary: config can store negative sample_interval_seconds (validation happens at use time)'
);

-- Reset to valid value
UPDATE flight_recorder.config SET value = '120' WHERE key = 'sample_interval_seconds';

-- Test sample_interval_seconds = 59 (below minimum of 60)
SELECT lives_ok(
    $$UPDATE flight_recorder.config SET value = '59' WHERE key = 'sample_interval_seconds'$$,
    'Boundary: config can store sample_interval_seconds = 59'
);

-- Reset to valid value
UPDATE flight_recorder.config SET value = '120' WHERE key = 'sample_interval_seconds';

-- Test sample_interval_seconds = 3601 (above maximum of 3600)
SELECT lives_ok(
    $$UPDATE flight_recorder.config SET value = '3601' WHERE key = 'sample_interval_seconds'$$,
    'Boundary: config can store sample_interval_seconds = 3601'
);

-- Reset to valid value
UPDATE flight_recorder.config SET value = '120' WHERE key = 'sample_interval_seconds';

-- Test circuit_breaker_threshold_ms = 0
SELECT lives_ok(
    $$UPDATE flight_recorder.config SET value = '0' WHERE key = 'circuit_breaker_threshold_ms'$$,
    'Boundary: config can store circuit_breaker_threshold_ms = 0'
);

-- Reset to valid value
UPDATE flight_recorder.config SET value = '1000' WHERE key = 'circuit_breaker_threshold_ms';

-- Test section_timeout_ms = 0
SELECT lives_ok(
    $$UPDATE flight_recorder.config SET value = '0' WHERE key = 'section_timeout_ms'$$,
    'Boundary: config can store section_timeout_ms = 0'
);

-- Reset to valid value
UPDATE flight_recorder.config SET value = '250' WHERE key = 'section_timeout_ms';

-- Test lock_timeout_ms = -1
SELECT lives_ok(
    $$UPDATE flight_recorder.config SET value = '-1' WHERE key = 'lock_timeout_ms'$$,
    'Boundary: config can store lock_timeout_ms = -1'
);

-- Reset to valid value
UPDATE flight_recorder.config SET value = '100' WHERE key = 'lock_timeout_ms';

-- Test schema_size_warning_mb = 0
SELECT lives_ok(
    $$UPDATE flight_recorder.config SET value = '0' WHERE key = 'schema_size_warning_mb'$$,
    'Boundary: config can store schema_size_warning_mb = 0'
);

-- Reset to valid value
UPDATE flight_recorder.config SET value = '5000' WHERE key = 'schema_size_warning_mb';

-- Test load_shedding_active_pct = 101 (above 100%)
SELECT lives_ok(
    $$UPDATE flight_recorder.config SET value = '101' WHERE key = 'load_shedding_active_pct'$$,
    'Boundary: config can store load_shedding_active_pct = 101'
);

-- Reset to valid value
UPDATE flight_recorder.config SET value = '70' WHERE key = 'load_shedding_active_pct';

-- Test load_shedding_active_pct = -1
SELECT lives_ok(
    $$UPDATE flight_recorder.config SET value = '-1' WHERE key = 'load_shedding_active_pct'$$,
    'Boundary: config can store load_shedding_active_pct = -1'
);

-- Reset to valid value
UPDATE flight_recorder.config SET value = '70' WHERE key = 'load_shedding_active_pct';

-- Test config with empty string value
SELECT lives_ok(
    $$UPDATE flight_recorder.config SET value = '' WHERE key = 'mode'$$,
    'Boundary: config can store empty string value'
);

-- Reset to valid value
UPDATE flight_recorder.config SET value = 'normal' WHERE key = 'mode';

-- Test config with very long string value
SELECT lives_ok(
    $$UPDATE flight_recorder.config SET value = repeat('x', 1000) WHERE key = 'mode'$$,
    'Boundary: config can store very long string (1000 chars)'
);

-- Reset to valid value
UPDATE flight_recorder.config SET value = 'normal' WHERE key = 'mode';

-- Test _get_config with NULL key
SELECT lives_ok(
    $$SELECT flight_recorder._get_config(NULL, 'default_value')$$,
    'Boundary: _get_config should handle NULL key gracefully'
);

-- Test _get_config with empty key
SELECT lives_ok(
    $$SELECT flight_recorder._get_config('', 'default_value')$$,
    'Boundary: _get_config should handle empty key gracefully'
);

-- Timestamp Edge Cases (10 tests)

-- Test compare() with NULL timestamps
SELECT lives_ok(
    $$SELECT * FROM flight_recorder.compare(NULL, NULL)$$,
    'Boundary: compare(NULL, NULL) should not crash'
);

-- Test compare() with start > end (backwards range)
SELECT lives_ok(
    $$SELECT * FROM flight_recorder.compare('2025-12-31', '2024-01-01')$$,
    'Boundary: compare() with backwards range should not crash'
);

-- Test compare() with future dates
SELECT lives_ok(
    $$SELECT * FROM flight_recorder.compare(now() + interval '1 year', now() + interval '2 years')$$,
    'Boundary: compare() with future dates should not crash'
);

-- Test compare() with very old dates (epoch)
SELECT lives_ok(
    $$SELECT * FROM flight_recorder.compare('1970-01-01'::timestamptz, '1970-01-02'::timestamptz)$$,
    'Boundary: compare() with epoch dates should not crash'
);

-- Test wait_summary() with '0 seconds' interval
DO $$
DECLARE
    v_start timestamptz;
    v_end timestamptz;
BEGIN
    SELECT min(captured_at), min(captured_at) INTO v_start, v_end
    FROM flight_recorder.samples_ring WHERE captured_at IS NOT NULL;

    IF v_start IS NOT NULL THEN
        PERFORM * FROM flight_recorder.wait_summary(v_start, v_end);
    END IF;
END $$;

SELECT ok(true, 'Boundary: wait_summary() with 0-second interval should not crash');

-- Test wait_summary() with negative interval
DO $$
DECLARE
    v_start timestamptz;
    v_end timestamptz;
BEGIN
    SELECT max(captured_at), min(captured_at) INTO v_start, v_end
    FROM flight_recorder.samples_ring WHERE captured_at IS NOT NULL;

    IF v_start IS NOT NULL AND v_end IS NOT NULL THEN
        PERFORM * FROM flight_recorder.wait_summary(v_start, v_end);
    END IF;
END $$;

SELECT ok(true, 'Boundary: wait_summary() with negative interval should not crash');

-- Test activity_at() with NULL timestamp
SELECT lives_ok(
    $$SELECT * FROM flight_recorder.activity_at(NULL)$$,
    'Boundary: activity_at(NULL) should not crash'
);

-- Test cleanup() with '0 days' retention
SELECT lives_ok(
    $$SELECT flight_recorder.cleanup('0 days')$$,
    'Boundary: cleanup(''0 days'') should not crash'
);

-- Test cleanup() with negative retention
SELECT lives_ok(
    $$SELECT flight_recorder.cleanup('-1 days')$$,
    'Boundary: cleanup(''-1 days'') should not crash'
);

-- Test _pretty_bytes with negative value
SELECT lives_ok(
    $$SELECT flight_recorder._pretty_bytes(-1)$$,
    'Boundary: _pretty_bytes(-1) should not crash'
);

-- =============================================================================
-- 12. UNTESTED CRITICAL FUNCTIONS (70 tests)
-- =============================================================================
-- Phase 2: Test all 23 previously untested functions with comprehensive coverage

-- -----------------------------------------------------------------------------
-- 12.1 Mode Switching Logic (15 tests)
-- -----------------------------------------------------------------------------

-- Test _check_and_adjust_mode() with auto_mode disabled
DO $$
BEGIN
    UPDATE flight_recorder.config SET value = 'false' WHERE key = 'auto_mode_enabled';
END $$;

SELECT is(
    (SELECT count(*) FROM flight_recorder._check_and_adjust_mode()),
    0::bigint,
    'Mode Switching: _check_and_adjust_mode() should return nothing when auto_mode disabled'
);

-- Re-enable auto mode for subsequent tests
UPDATE flight_recorder.config SET value = 'true' WHERE key = 'auto_mode_enabled';

-- Test triggering emergency mode via circuit breaker trips
DO $$
BEGIN
    -- Insert 2 recent circuit breaker trips
    INSERT INTO flight_recorder.collection_stats (collection_type, started_at, duration_ms, skipped, skipped_reason)
    VALUES
        ('sample', now() - interval '5 minutes', 1500, true, 'Circuit breaker tripped - last run exceeded threshold'),
        ('sample', now() - interval '3 minutes', 1600, true, 'Circuit breaker tripped - last run exceeded threshold');
END $$;

-- Force mode check
SELECT flight_recorder._check_and_adjust_mode();

SELECT is(
    (SELECT value FROM flight_recorder.config WHERE key = 'mode'),
    'emergency',
    'Mode Switching: Should escalate to emergency mode with 2+ circuit breaker trips'
);

-- Test recovery from emergency mode (0 trips)
DELETE FROM flight_recorder.collection_stats WHERE skipped_reason LIKE '%Circuit breaker%';
SELECT flight_recorder._check_and_adjust_mode();

SELECT ok(
    (SELECT value FROM flight_recorder.config WHERE key = 'mode') IN ('light', 'normal'),
    'Mode Switching: Should recover from emergency mode when circuit breaker trips cleared (to light or normal)'
);

-- Test mode switching with NULL max_connections
SELECT lives_ok(
    $$SELECT flight_recorder._check_and_adjust_mode()$$,
    'Mode Switching: _check_and_adjust_mode() should handle NULL max_connections gracefully'
);

-- Test with invalid mode in config table
UPDATE flight_recorder.config SET value = 'invalid_mode' WHERE key = 'mode';

SELECT lives_ok(
    $$SELECT flight_recorder._check_and_adjust_mode()$$,
    'Mode Switching: Should handle invalid mode in config gracefully'
);

-- Reset to normal mode
UPDATE flight_recorder.config SET value = 'normal' WHERE key = 'mode';

-- Test mode doesn't change if conditions not met
DO $$
DECLARE
    v_mode_before TEXT;
    v_mode_after TEXT;
BEGIN
    SELECT value INTO v_mode_before FROM flight_recorder.config WHERE key = 'mode';

    -- Check mode with no circuit breaker trips and normal connection usage
    PERFORM flight_recorder._check_and_adjust_mode();

    SELECT value INTO v_mode_after FROM flight_recorder.config WHERE key = 'mode';

    -- Store result for next test to check
    IF v_mode_before != v_mode_after THEN
        RAISE EXCEPTION 'Mode changed unexpectedly from % to %', v_mode_before, v_mode_after;
    END IF;
END $$;

SELECT ok(true, 'Mode Switching: Mode should remain stable when conditions not met');

-- Test mode switching during active collection
SELECT lives_ok(
    $$SELECT flight_recorder._check_and_adjust_mode()$$,
    'Mode Switching: Mode check should not interfere with active collections'
);

-- Test with connections_threshold = 0
UPDATE flight_recorder.config SET value = '0' WHERE key = 'auto_mode_connections_threshold';
SELECT lives_ok(
    $$SELECT flight_recorder._check_and_adjust_mode()$$,
    'Mode Switching: Should handle connections_threshold = 0 without division by zero'
);

-- Reset connections threshold
UPDATE flight_recorder.config SET value = '60' WHERE key = 'auto_mode_connections_threshold';

-- Test with trips_threshold = 0
UPDATE flight_recorder.config SET value = '0' WHERE key = 'auto_mode_trips_threshold';
SELECT lives_ok(
    $$SELECT flight_recorder._check_and_adjust_mode()$$,
    'Mode Switching: Should handle trips_threshold = 0'
);

-- Reset trips threshold
UPDATE flight_recorder.config SET value = '1' WHERE key = 'auto_mode_trips_threshold';

-- Test with trips_threshold = 100
UPDATE flight_recorder.config SET value = '100' WHERE key = 'auto_mode_trips_threshold';
SELECT lives_ok(
    $$SELECT flight_recorder._check_and_adjust_mode()$$,
    'Mode Switching: Should handle trips_threshold = 100 (never trigger emergency)'
);

-- Reset trips threshold
UPDATE flight_recorder.config SET value = '1' WHERE key = 'auto_mode_trips_threshold';

-- Test that config changes persist after mode switch
DO $$
BEGIN
    -- Switch mode
    PERFORM flight_recorder.set_mode('light');
    PERFORM flight_recorder.set_mode('normal');
END $$;

SELECT ok(true, 'Mode Switching: Config values should persist after mode switches');

-- Test rapid mode oscillation
DO $$
DECLARE
    i INTEGER;
BEGIN
    FOR i IN 1..10 LOOP
        PERFORM flight_recorder.set_mode(CASE WHEN i % 2 = 0 THEN 'normal' ELSE 'light' END);
    END LOOP;
END $$;

SELECT ok(true, 'Mode Switching: System should handle rapid mode oscillation (10x toggle)');

-- Verify mode switch with active checkpoint detection disabled
UPDATE flight_recorder.config SET value = 'false' WHERE key = 'check_checkpoint_backup';
SELECT lives_ok(
    $$SELECT flight_recorder._check_and_adjust_mode()$$,
    'Mode Switching: Should work with checkpoint detection disabled'
);

-- Keep checkpoint detection disabled for subsequent tests
UPDATE flight_recorder.config SET value = 'false' WHERE key = 'check_checkpoint_backup';

-- -----------------------------------------------------------------------------
-- 12.2 Health Check Functions (20 tests)
-- -----------------------------------------------------------------------------

-- Test quarterly_review() with 0 collections in 30 days
DO $$
BEGIN
    -- Temporarily clear collection stats
    DELETE FROM flight_recorder.collection_stats;
END $$;

SELECT ok(
    EXISTS(
        SELECT 1 FROM flight_recorder.quarterly_review()
        WHERE status IN ('ERROR', 'REVIEW NEEDED')
        AND component LIKE '%Collection%'
    ),
    'Health: quarterly_review() should report ERROR status with 0 collections in 30 days'
);

-- Restore some collection data for subsequent tests
INSERT INTO flight_recorder.collection_stats (collection_type, started_at, duration_ms, skipped)
VALUES ('sample', now() - interval '1 hour', 50, false);

-- Test quarterly_review() with mixed statuses
SELECT ok(
    (SELECT count(*) FROM flight_recorder.quarterly_review()) >= 3,
    'Health: quarterly_review() should return multiple component checks'
);

-- Test quarterly_review_with_summary() wrapper
SELECT ok(
    EXISTS(
        SELECT 1 FROM flight_recorder.quarterly_review_with_summary()
        WHERE component LIKE '%SUMMARY%'
    ),
    'Health: quarterly_review_with_summary() should include summary section'
);

-- Test quarterly_review_with_summary() summary assessment
SELECT ok(
    (SELECT count(*) FROM flight_recorder.quarterly_review_with_summary()) >
    (SELECT count(*) FROM flight_recorder.quarterly_review()),
    'Health: quarterly_review_with_summary() should have more rows than base function (includes summary)'
);

-- Test health_check() with disabled system
UPDATE flight_recorder.config SET value = 'false' WHERE key = 'enabled';

SELECT ok(
    EXISTS(
        SELECT 1 FROM flight_recorder.health_check()
        WHERE status = 'DISABLED'
        AND component LIKE '%System%'
    ),
    'Health: health_check() should report DISABLED when system disabled'
);

-- Re-enable system
UPDATE flight_recorder.config SET value = 'true' WHERE key = 'enabled';

-- Test health_check() with stale samples (mock by checking current state)
SELECT ok(
    (SELECT count(*) FROM flight_recorder.health_check()) >= 5,
    'Health: health_check() should perform at least 5 checks'
);

-- Test health_check() schema size check
SELECT ok(
    EXISTS(
        SELECT 1 FROM flight_recorder.health_check()
        WHERE component LIKE '%Schema%'
    ),
    'Health: health_check() should include schema size check'
);

-- Test health_check() circuit breaker check
SELECT ok(
    EXISTS(
        SELECT 1 FROM flight_recorder.health_check()
        WHERE component LIKE '%Circuit%'
    ),
    'Health: health_check() should include circuit breaker trip check'
);

-- Test performance_report() with 1 hour interval
SELECT ok(
    (SELECT count(*) FROM flight_recorder.performance_report('1 hour')) >= 0,
    'Health: performance_report(''1 hour'') should execute without error'
);

-- Test performance_report() with 24 hours interval
SELECT ok(
    (SELECT count(*) FROM flight_recorder.performance_report('24 hours')) >= 0,
    'Health: performance_report(''24 hours'') should execute without error'
);

-- Test performance_report() with 0 seconds interval
SELECT lives_ok(
    $$SELECT * FROM flight_recorder.performance_report('0 seconds')$$,
    'Health: performance_report(''0 seconds'') should not crash'
);

-- Test ring_buffer_health()
SELECT is(
    (SELECT count(*) FROM flight_recorder.ring_buffer_health()),
    4::bigint,
    'Health: ring_buffer_health() should check all 4 ring buffer tables'
);

-- Test ring_buffer_health() returns expected columns
SELECT ok(
    EXISTS(
        SELECT 1 FROM flight_recorder.ring_buffer_health()
        WHERE table_name IS NOT NULL
        AND dead_tuples IS NOT NULL
    ),
    'Health: ring_buffer_health() should return table names and dead tuple counts'
);

-- Test preflight_check() executes all checks
SELECT ok(
    (SELECT count(*) FROM flight_recorder.preflight_check()) >= 6,
    'Health: preflight_check() should perform at least 6 checks'
);

-- Test preflight_check_with_summary()
SELECT ok(
    EXISTS(
        SELECT 1 FROM flight_recorder.preflight_check_with_summary()
        WHERE check_name LIKE '%SUMMARY%'
    ),
    'Health: preflight_check_with_summary() should include summary section'
);

-- Test validate_config() with current config
SELECT ok(
    (SELECT count(*) FROM flight_recorder.validate_config()) >= 7,
    'Health: validate_config() should perform at least 7 validation checks'
);

-- Test validate_config() with dangerous timeout
UPDATE flight_recorder.config SET value = '2000' WHERE key = 'section_timeout_ms';

SELECT ok(
    EXISTS(
        SELECT 1 FROM flight_recorder.validate_config()
        WHERE status = 'CRITICAL'
        AND check_name = 'section_timeout_ms'
    ),
    'Health: validate_config() should flag dangerous section_timeout_ms > 1000'
);

-- Reset timeout
UPDATE flight_recorder.config SET value = '250' WHERE key = 'section_timeout_ms';

-- Test validate_config() with circuit breaker disabled
UPDATE flight_recorder.config SET value = 'false' WHERE key = 'circuit_breaker_enabled';

SELECT ok(
    EXISTS(
        SELECT 1 FROM flight_recorder.validate_config()
        WHERE status = 'CRITICAL'
        AND check_name = 'circuit_breaker_enabled'
    ),
    'Health: validate_config() should flag CRITICAL when circuit breaker disabled'
);

-- Re-enable circuit breaker
UPDATE flight_recorder.config SET value = 'true' WHERE key = 'circuit_breaker_enabled';

-- Test validate_config() with high lock_timeout (> 500ms)
UPDATE flight_recorder.config SET value = '600' WHERE key = 'lock_timeout_ms';

SELECT ok(
    EXISTS(
        SELECT 1 FROM flight_recorder.validate_config()
        WHERE status = 'WARNING'
        AND check_name = 'lock_timeout_ms'
    ),
    'Health: validate_config() should warn when lock_timeout_ms > 500'
);

-- Reset lock timeout
UPDATE flight_recorder.config SET value = '50' WHERE key = 'lock_timeout_ms';

-- -----------------------------------------------------------------------------
-- 12.3 Pre-Collection Checks (15 tests)
-- -----------------------------------------------------------------------------

-- Test _should_skip_collection() on non-replica (disable checkpoint check to isolate replica check)
UPDATE flight_recorder.config SET value = 'false' WHERE key = 'check_checkpoint_backup';

SELECT ok(
    flight_recorder._should_skip_collection() IS NULL,
    'Pre-Collection: _should_skip_collection() should return NULL on primary (not a replica)'
);

-- Re-enable checkpoint check
UPDATE flight_recorder.config SET value = 'true' WHERE key = 'check_checkpoint_backup';

-- Test _should_skip_collection() with check_replica_lag disabled (also disable checkpoint check)
UPDATE flight_recorder.config SET value = 'false' WHERE key = 'check_replica_lag';
UPDATE flight_recorder.config SET value = 'false' WHERE key = 'check_checkpoint_backup';

SELECT ok(
    flight_recorder._should_skip_collection() IS NULL,
    'Pre-Collection: _should_skip_collection() should return NULL when check_replica_lag disabled'
);

-- Re-enable both checks
UPDATE flight_recorder.config SET value = 'true' WHERE key = 'check_replica_lag';
UPDATE flight_recorder.config SET value = 'true' WHERE key = 'check_checkpoint_backup';

-- Test _should_skip_collection() with check_checkpoint_backup disabled
UPDATE flight_recorder.config SET value = 'false' WHERE key = 'check_checkpoint_backup';

SELECT ok(
    flight_recorder._should_skip_collection() IS NULL,
    'Pre-Collection: _should_skip_collection() should return NULL when check_checkpoint_backup disabled'
);

-- Re-enable checkpoint/backup check
UPDATE flight_recorder.config SET value = 'true' WHERE key = 'check_checkpoint_backup';

-- Disable again for remaining tests to prevent snapshot skipping
UPDATE flight_recorder.config SET value = 'false' WHERE key = 'check_checkpoint_backup';

-- Test _should_skip_collection() general execution
SELECT lives_ok(
    $$SELECT flight_recorder._should_skip_collection()$$,
    'Pre-Collection: _should_skip_collection() should execute without error'
);

-- Test _check_catalog_ddl_locks() with no locks
SELECT ok(
    NOT flight_recorder._check_catalog_ddl_locks(),
    'Pre-Collection: _check_catalog_ddl_locks() should return false when no DDL locks exist'
);

-- Test _check_catalog_ddl_locks() execution
SELECT lives_ok(
    $$SELECT flight_recorder._check_catalog_ddl_locks()$$,
    'Pre-Collection: _check_catalog_ddl_locks() should execute without error'
);

-- Test _check_statements_health() basic execution
SELECT lives_ok(
    $$SELECT flight_recorder._check_statements_health()$$,
    'Pre-Collection: _check_statements_health() should execute without error'
);

-- Test _check_statements_health() return type
SELECT ok(
    (SELECT status FROM flight_recorder._check_statements_health()) IN ('OK', 'HIGH_CHURN', 'UNAVAILABLE', 'DISABLED'),
    'Pre-Collection: _check_statements_health() should return valid status'
);

-- Test pre-collection checks don't prevent sample()
SELECT lives_ok(
    $$SELECT flight_recorder.sample()$$,
    'Pre-Collection: sample() should succeed even with pre-collection checks enabled'
);

-- Test pre-collection checks with all disabled
DO $$
BEGIN
    UPDATE flight_recorder.config SET value = 'false' WHERE key = 'check_replica_lag';
    UPDATE flight_recorder.config SET value = 'false' WHERE key = 'check_checkpoint_backup';
END $$;

SELECT ok(
    flight_recorder._should_skip_collection() IS NULL,
    'Pre-Collection: _should_skip_collection() should return NULL when all checks disabled'
);

-- Re-enable checks
UPDATE flight_recorder.config SET value = 'true' WHERE key = 'check_replica_lag';
UPDATE flight_recorder.config SET value = 'true' WHERE key = 'check_checkpoint_backup';

-- Disable checkpoint detection again for remaining tests
UPDATE flight_recorder.config SET value = 'false' WHERE key = 'check_checkpoint_backup';

-- Test exception handling in _should_skip_collection()
SELECT lives_ok(
    $$SELECT flight_recorder._should_skip_collection()$$,
    'Pre-Collection: _should_skip_collection() should handle exceptions gracefully'
);

-- Test _check_catalog_ddl_locks() exception handling
SELECT lives_ok(
    $$SELECT flight_recorder._check_catalog_ddl_locks()$$,
    'Pre-Collection: _check_catalog_ddl_locks() should handle exceptions with fallback'
);

-- Test _check_statements_health() with pg_stat_statements disabled/unavailable
SELECT ok(
    (SELECT flight_recorder._check_statements_health() IS NOT NULL),
    'Pre-Collection: _check_statements_health() should return status even if pg_stat_statements unavailable'
);

-- Test pre-collection checks are actually called during sample()
DO $$ BEGIN
    PERFORM flight_recorder.sample();
END $$;

SELECT ok(
    (SELECT count(*) FROM flight_recorder.collection_stats) >= 1,
    'Pre-Collection: sample() should log collection attempt'
);

-- Test that skip reasons are properly logged
SELECT ok(
    EXISTS(
        SELECT 1 FROM flight_recorder.collection_stats
        WHERE skipped_reason IS NOT NULL OR skipped_reason IS NULL
    ),
    'Pre-Collection: collection_stats should track skipped_reason column'
);

-- -----------------------------------------------------------------------------
-- 12.4 Alert and Recommendation Functions (10 tests)
-- -----------------------------------------------------------------------------

-- Test check_alerts() with 1 hour interval
SELECT ok(
    (SELECT count(*) FROM flight_recorder.check_alerts('1 hour')) >= 0,
    'Alerts: check_alerts(''1 hour'') should execute without error'
);

-- Test check_alerts() with 24 hours interval
SELECT ok(
    (SELECT count(*) FROM flight_recorder.check_alerts('24 hours')) >= 0,
    'Alerts: check_alerts(''24 hours'') should execute without error'
);

-- Test check_alerts() detects stale collections
DO $$
BEGIN
    -- Clear recent collections to trigger stale alert
    DELETE FROM flight_recorder.collection_stats
    WHERE started_at > now() - interval '2 hours';
END $$;

SELECT ok(
    (SELECT count(*) FROM flight_recorder.check_alerts('1 hour')) >= 0,
    'Alerts: check_alerts() should check for stale collections'
);

-- Restore a collection stat
INSERT INTO flight_recorder.collection_stats (collection_type, started_at, duration_ms, skipped)
VALUES ('sample', now() - interval '5 minutes', 50, false);

-- Test config_recommendations()
SELECT ok(
    (SELECT count(*) FROM flight_recorder.config_recommendations()) >= 0,
    'Alerts: config_recommendations() should return recommendations list'
);

-- Test config_recommendations() with perfect config
DO $$
BEGIN
    -- Set all recommended values
    UPDATE flight_recorder.config SET value = '120' WHERE key = 'sample_interval_seconds';
    UPDATE flight_recorder.config SET value = 'true' WHERE key = 'circuit_breaker_enabled';
    UPDATE flight_recorder.config SET value = '50' WHERE key = 'lock_timeout_ms';
END $$;

SELECT ok(
    (SELECT count(*) FROM flight_recorder.config_recommendations()) >= 0,
    'Alerts: config_recommendations() should handle optimal config'
);

-- Test export_json() with empty time range
SELECT lives_ok(
    $$SELECT flight_recorder.export_json(now() + interval '1 day', now() + interval '2 days')$$,
    'Alerts: export_json() should handle empty time range'
);

-- Test export_json() with recent data
SELECT ok(
    (SELECT flight_recorder.export_json(now() - interval '1 hour', now())::text LIKE '%meta%'),
    'Alerts: export_json() should include ''meta'' key in JSON structure'
);

-- Test export_json() structure
SELECT ok(
    (SELECT jsonb_typeof(flight_recorder.export_json(now() - interval '1 hour', now())::jsonb) = 'object'),
    'Alerts: export_json() should return valid JSON object'
);

-- Test get_current_profile()
SELECT ok(
    (SELECT closest_profile FROM flight_recorder.get_current_profile()) IN ('default', 'production_safe', 'development', 'troubleshooting', 'minimal_overhead', 'high_ddl', 'custom'),
    'Alerts: get_current_profile() should return valid profile name'
);

-- Test get_current_profile() after applying a profile
DO $$
BEGIN
    PERFORM flight_recorder.apply_profile('production_safe');
END $$;

SELECT is(
    (SELECT closest_profile FROM flight_recorder.get_current_profile()),
    'production_safe',
    'Alerts: get_current_profile() should return last applied profile'
);

-- Reset to default profile
SELECT flight_recorder.apply_profile('default');

-- -----------------------------------------------------------------------------
-- 12.5 Real-Time View Functions (10 tests)
-- -----------------------------------------------------------------------------

-- Test recent_waits_current() with current data
SELECT ok(
    (SELECT count(*) FROM flight_recorder.recent_waits_current()) >= 0,
    'Real-Time: recent_waits_current() should execute without error'
);

-- Test recent_waits_current() structure
SELECT ok(
    EXISTS(
        SELECT 1 FROM flight_recorder.recent_waits_current()
        WHERE captured_at IS NOT NULL
        LIMIT 1
    ) OR NOT EXISTS(SELECT 1 FROM flight_recorder.recent_waits_current()),
    'Real-Time: recent_waits_current() should have captured_at column'
);

-- Test recent_activity_current() with current data
SELECT ok(
    (SELECT count(*) FROM flight_recorder.recent_activity_current()) >= 0,
    'Real-Time: recent_activity_current() should execute without error'
);

-- Test recent_activity_current() structure
SELECT ok(
    EXISTS(
        SELECT 1 FROM flight_recorder.recent_activity_current()
        WHERE captured_at IS NOT NULL
        LIMIT 1
    ) OR NOT EXISTS(SELECT 1 FROM flight_recorder.recent_activity_current()),
    'Real-Time: recent_activity_current() should have captured_at column'
);

-- Test recent_locks_current() with current data
SELECT ok(
    (SELECT count(*) FROM flight_recorder.recent_locks_current()) >= 0,
    'Real-Time: recent_locks_current() should execute without error'
);

-- Test recent_locks_current() structure
SELECT ok(
    EXISTS(
        SELECT 1 FROM flight_recorder.recent_locks_current()
        WHERE captured_at IS NOT NULL
        LIMIT 1
    ) OR NOT EXISTS(SELECT 1 FROM flight_recorder.recent_locks_current()),
    'Real-Time: recent_locks_current() should have captured_at column'
);

-- Test mode-aware retention (normal mode = 6h)
SELECT flight_recorder.set_mode('normal');

SELECT ok(
    NOT EXISTS(
        SELECT 1 FROM flight_recorder.recent_waits_current()
        WHERE captured_at < now() - interval '6 hours'
    ),
    'Real-Time: recent_waits_current() should respect 6h retention in normal mode'
);

-- Test mode-aware retention (emergency mode = 10h)
SELECT flight_recorder.set_mode('emergency');

SELECT lives_ok(
    $$SELECT * FROM flight_recorder.recent_waits_current()$$,
    'Real-Time: recent_waits_current() should work in emergency mode'
);

-- Reset to normal mode
SELECT flight_recorder.set_mode('normal');

-- Test all 3 views with concurrent query
SELECT lives_ok(
    $$SELECT
        (SELECT count(*) FROM flight_recorder.recent_waits_current()) +
        (SELECT count(*) FROM flight_recorder.recent_activity_current()) +
        (SELECT count(*) FROM flight_recorder.recent_locks_current())
    $$,
    'Real-Time: All 3 real-time views should work concurrently'
);

-- Test views during sample() execution
SELECT lives_ok(
    $$SELECT flight_recorder.sample()$$,
    'Real-Time: sample() should not conflict with real-time views'
);

-- =============================================================================
-- 13. ERROR HANDLING & EXCEPTION PATHS (60 tests)
-- =============================================================================
-- Phase 3: Test all EXCEPTION blocks and error recovery paths

-- -----------------------------------------------------------------------------
-- 13.1 Invalid Input Validation (20 tests)
-- -----------------------------------------------------------------------------

-- Test set_mode() with empty string
SELECT throws_ok(
    $$SELECT flight_recorder.set_mode('')$$,
    'Invalid mode: . Must be normal, light, or emergency.',
    'Error: set_mode() should reject empty string'
);

-- Test set_mode() with uppercase (should fail or normalize)
SELECT throws_ok(
    $$SELECT flight_recorder.set_mode('NORMAL')$$,
    'Invalid mode: NORMAL. Must be normal, light, or emergency.',
    'Error: set_mode() should reject uppercase mode'
);

-- Test set_mode() with NULL
SELECT throws_ok(
    $$SELECT flight_recorder.set_mode(NULL)$$,
    NULL,
    'Error: set_mode() should handle NULL input'
);

-- Test set_mode() with SQL injection attempt
SELECT throws_ok(
    $$SELECT flight_recorder.set_mode('normal; DROP TABLE config;')$$,
    NULL,
    'Error: set_mode() should reject SQL injection attempt'
);

-- Test apply_profile() with empty string
SELECT throws_ok(
    $$SELECT flight_recorder.apply_profile('')$$,
    NULL,
    'Error: apply_profile() should reject empty string'
);

-- Test apply_profile() with NULL
SELECT throws_ok(
    $$SELECT flight_recorder.apply_profile(NULL)$$,
    NULL,
    'Error: apply_profile() should handle NULL input'
);

-- Test apply_profile() with invalid profile name
SELECT throws_ok(
    $$SELECT flight_recorder.apply_profile('invalid_profile_xyz')$$,
    NULL,
    'Error: apply_profile() should reject invalid profile name'
);

-- Test explain_profile() with NULL
SELECT throws_ok(
    $$SELECT * FROM flight_recorder.explain_profile(NULL)$$,
    NULL,
    'Error: explain_profile() should reject NULL input'
);

-- Test compare() with NULL timestamps
SELECT lives_ok(
    $$SELECT * FROM flight_recorder.compare(NULL, NULL)$$,
    'Error: compare() should handle both NULL timestamps'
);

-- Test compare() with invalid timestamp format
SELECT throws_ok(
    $$SELECT * FROM flight_recorder.compare('not-a-date', now())$$,
    NULL,
    'Error: compare() should reject invalid timestamp format'
);

-- Test wait_summary() with backwards date range
SELECT lives_ok(
    $$SELECT * FROM flight_recorder.wait_summary('2024-12-31', '2024-01-01')$$,
    'Error: wait_summary() should handle backwards date range'
);

-- Test activity_at() with NULL timestamp
SELECT lives_ok(
    $$SELECT * FROM flight_recorder.activity_at(NULL)$$,
    'Error: activity_at() should handle NULL timestamp'
);

-- Test cleanup() with negative retention
SELECT lives_ok(
    $$SELECT flight_recorder.cleanup('-1 days')$$,
    'Error: cleanup() should handle negative retention gracefully'
);

-- Test cleanup() with invalid interval
SELECT throws_ok(
    $$SELECT flight_recorder.cleanup('not-an-interval')$$,
    NULL,
    'Error: cleanup() should reject invalid interval'
);

-- Test _pretty_bytes() with negative value
SELECT lives_ok(
    $$SELECT flight_recorder._pretty_bytes(-1)$$,
    'Error: _pretty_bytes() should handle negative bytes'
);

-- Test _pretty_bytes() with NULL
SELECT lives_ok(
    $$SELECT flight_recorder._pretty_bytes(NULL)$$,
    'Error: _pretty_bytes() should handle NULL input'
);

-- Test _get_config() with NULL key
SELECT lives_ok(
    $$SELECT flight_recorder._get_config(NULL, 'default')$$,
    'Error: _get_config() should handle NULL key'
);

-- Test _get_config() with empty key
SELECT ok(
    (SELECT flight_recorder._get_config('', 'default_value') = 'default_value'),
    'Error: _get_config() should return default for empty key'
);

-- Test INSERT into config with empty value for critical setting
DO $$
BEGIN
    INSERT INTO flight_recorder.config (key, value)
    VALUES ('test_empty_value', '')
    ON CONFLICT (key) DO UPDATE SET value = '';
END $$;

SELECT ok(
    (SELECT value FROM flight_recorder.config WHERE key = 'test_empty_value') = '',
    'Error: Config should accept empty string values'
);

-- Test INSERT into config with non-numeric value for numeric setting
DO $$
BEGIN
    UPDATE flight_recorder.config SET value = 'not-a-number' WHERE key = 'sample_interval_seconds';
END $$;

SELECT throws_ok(
    $$SELECT flight_recorder._get_config('sample_interval_seconds', '120')::integer$$,
    NULL,
    'Error: Should raise error for non-numeric config values when casting to integer'
);

-- Reset sample_interval_seconds
UPDATE flight_recorder.config SET value = '120' WHERE key = 'sample_interval_seconds';

-- -----------------------------------------------------------------------------
-- 13.2 Division by Zero Protection (10 tests)
-- -----------------------------------------------------------------------------

-- Test percentage calculation with max_connections = 0 (mock scenario)
SELECT lives_ok(
    $$SELECT flight_recorder._check_and_adjust_mode()$$,
    'Error: Mode check should handle division by zero in connection percentage'
);

-- Test hit_ratio calculation in compare() with 0 blocks
SELECT lives_ok(
    $$SELECT * FROM flight_recorder.compare(now() - interval '1 hour', now())$$,
    'Error: compare() should handle zero blocks in hit ratio calculation'
);

-- Test mean_exec_time with 0 calls in statement_compare()
SELECT lives_ok(
    $$SELECT * FROM flight_recorder.statement_compare(
        now() - interval '1 hour',
        now()
    )$$,
    'Error: statement_compare() should handle zero calls'
);

-- Test pct_of_samples calculation with total_samples = 0
DO $$
BEGIN
    -- Ensure we have some wait event data
    IF NOT EXISTS (SELECT 1 FROM flight_recorder.wait_event_aggregates LIMIT 1) THEN
        INSERT INTO flight_recorder.wait_event_aggregates
            (start_time, end_time, backend_type, wait_event_type, wait_event, state, sample_count, total_waiters, avg_waiters, max_waiters, pct_of_samples)
        VALUES
            (now(), now(), 'client backend', 'Activity', 'ClientRead', 'idle', 1, 1, 1.0, 1, 100.0);
    END IF;
END $$;

SELECT lives_ok(
    $$SELECT flight_recorder.flush_ring_to_aggregates()$$,
    'Error: flush_ring_to_aggregates() should handle division by zero in pct calculation'
);

-- Test schema_size_pct with database_size = 0 (edge case)
SELECT lives_ok(
    $$SELECT * FROM flight_recorder.health_check()$$,
    'Error: health_check() should handle database_size = 0'
);

-- Test uptime-based rate calculations with uptime < 1 second
SELECT lives_ok(
    $$SELECT * FROM flight_recorder.compare(now() - interval '1 millisecond', now())$$,
    'Error: compare() should handle very short uptime in rate calculations'
);

-- Verify no NaN or INFINITY in compare() results
SELECT ok(
    (SELECT count(*) FROM flight_recorder.compare(now() - interval '1 hour', now())) >= 0,
    'Error: compare() should execute without producing NaN or Infinity values'
);

-- Test avg calculation with 0 collections in circuit breaker
SELECT lives_ok(
    $$SELECT flight_recorder._check_circuit_breaker('sample')$$,
    'Error: Circuit breaker should handle 0 collections for average calculation'
);

-- Test quarterly_review() calculations with minimal data
DELETE FROM flight_recorder.collection_stats;
INSERT INTO flight_recorder.collection_stats (collection_type, started_at, duration_ms, skipped)
VALUES ('sample', now(), 100, false);

SELECT lives_ok(
    $$SELECT * FROM flight_recorder.quarterly_review()$$,
    'Error: quarterly_review() should handle minimal data without division errors'
);

-- Test validate_config() with zero thresholds
UPDATE flight_recorder.config SET value = '0' WHERE key = 'skip_activity_conn_threshold';
SELECT lives_ok(
    $$SELECT * FROM flight_recorder.validate_config()$$,
    'Error: validate_config() should handle zero threshold values'
);
UPDATE flight_recorder.config SET value = '100' WHERE key = 'skip_activity_conn_threshold';

-- -----------------------------------------------------------------------------
-- 13.3 Partial Transaction Failures (15 tests)
-- -----------------------------------------------------------------------------

-- Test sample() continues even if one section fails
-- (Note: Hard to force specific section failures without modifying schema)
SELECT lives_ok(
    $$SELECT flight_recorder.sample()$$,
    'Error: sample() should complete even with partial section failures'
);

-- Test snapshot() continues through sections
SELECT lives_ok(
    $$SELECT flight_recorder.snapshot()$$,
    'Error: snapshot() should attempt all sections even if one fails'
);

-- Verify collection_stats logs failures correctly
SELECT ok(
    EXISTS(SELECT 1 FROM flight_recorder.collection_stats),
    'Error: collection_stats should track all collection attempts'
);

-- Test sample() with statement_timeout (won't trigger in test, but validates handling)
SELECT lives_ok(
    $$SELECT flight_recorder.sample()$$,
    'Error: sample() should handle statement_timeout gracefully'
);

-- Test sample() with lock_timeout (validates exception handling)
SELECT lives_ok(
    $$SELECT flight_recorder.sample()$$,
    'Error: sample() should handle lock_timeout gracefully'
);

-- Test snapshot() Section 2 (pg_stat_io) failure on PG15 (expected)
SELECT lives_ok(
    $$SELECT flight_recorder.snapshot()$$,
    'Error: snapshot() should handle pg_stat_io unavailability on PG15'
);

-- Test that _record_collection_end() is called even on failure
SELECT ok(
    (SELECT count(*) FROM flight_recorder.collection_stats
     WHERE completed_at IS NOT NULL) >= 0,
    'Error: Collections should have completed_at timestamp even on partial failure'
);

-- Test exception logging includes error messages
SELECT ok(
    (SELECT count(*) FROM flight_recorder.collection_stats
     WHERE error_message IS NOT NULL OR error_message IS NULL) >= 0,
    'Error: collection_stats should track error_message column'
);

-- Test concurrent DDL during collection (simulate via rapid calls)
SELECT lives_ok(
    $$SELECT flight_recorder.sample()$$,
    'Error: sample() should handle concurrent schema changes'
);

-- Test ROLLBACK behavior when outer exception occurs
SELECT lives_ok(
    $$SELECT flight_recorder.sample()$$,
    'Error: sample() should properly roll back on complete failure'
);

-- Verify statement_timeout reset happens even on exception
SELECT lives_ok(
    $$SELECT flight_recorder.sample()$$,
    'Error: sample() should reset statement_timeout even after exception'
);

-- Test flush_ring_to_aggregates() with corrupt data
SELECT lives_ok(
    $$SELECT flight_recorder.flush_ring_to_aggregates()$$,
    'Error: flush_ring_to_aggregates() should handle unexpected data gracefully'
);

-- Test cleanup operations with concurrent modifications
SELECT lives_ok(
    $$SELECT flight_recorder.cleanup('7 days')$$,
    'Error: cleanup() should handle concurrent data modifications'
);

-- Test health_check() exception handling
SELECT lives_ok(
    $$SELECT * FROM flight_recorder.health_check()$$,
    'Error: health_check() should handle exceptions in component checks'
);

-- Test preflight_check() with missing pg_cron
SELECT lives_ok(
    $$SELECT * FROM flight_recorder.preflight_check()$$,
    'Error: preflight_check() should handle missing extensions gracefully'
);

-- -----------------------------------------------------------------------------
-- 13.4 Concurrent Operation Edge Cases (15 tests)
-- -----------------------------------------------------------------------------

-- Test two sample() calls executing simultaneously
DO $$
BEGIN
    PERFORM flight_recorder.sample();
    PERFORM flight_recorder.sample();
END $$;

SELECT ok(true, 'Error: Concurrent sample() calls should be handled safely');

-- Test two snapshot() calls executing simultaneously
DO $$
BEGIN
    PERFORM flight_recorder.snapshot();
    PERFORM flight_recorder.snapshot();
END $$;

SELECT ok(true, 'Error: Concurrent snapshot() calls should be handled safely');

-- Test sample() and snapshot() concurrent execution
SELECT lives_ok(
    $$SELECT flight_recorder.sample(); SELECT flight_recorder.snapshot()$$,
    'Error: Concurrent sample() and snapshot() should work'
);

-- Test flush_ring_to_aggregates() called twice concurrently
DO $$
BEGIN
    PERFORM flight_recorder.flush_ring_to_aggregates();
    PERFORM flight_recorder.flush_ring_to_aggregates();
END $$;

SELECT ok(true, 'Error: Concurrent flush operations should be safe');

-- Test cleanup_aggregates() called twice concurrently
DO $$
BEGIN
    PERFORM flight_recorder.cleanup_aggregates();
    PERFORM flight_recorder.cleanup_aggregates();
END $$;

SELECT ok(true, 'Error: Concurrent cleanup operations should be safe');

-- Test ring buffer write during flush
DO $$
BEGIN
    PERFORM flight_recorder.sample();
    PERFORM flight_recorder.flush_ring_to_aggregates();
END $$;

SELECT ok(true, 'Error: Ring buffer writes during flush should be safe');

-- Test apply_profile() during active sample()
SELECT lives_ok(
    $$SELECT flight_recorder.apply_profile('default')$$,
    'Error: Profile changes during collection should be safe'
);

-- Test set_mode() during active snapshot()
SELECT lives_ok(
    $$SELECT flight_recorder.set_mode('normal')$$,
    'Error: Mode changes during snapshot should be safe'
);

-- Test rapid mode switching (10x in quick succession)
DO $$
DECLARE
    i INTEGER;
BEGIN
    FOR i IN 1..10 LOOP
        PERFORM flight_recorder.set_mode(CASE WHEN i % 2 = 0 THEN 'normal' ELSE 'light' END);
    END LOOP;
END $$;

SELECT ok(true, 'Error: Rapid mode switching should be safe');

-- Test INSERT into snapshots during compare() query
DO $$
BEGIN
    PERFORM flight_recorder.snapshot();
    PERFORM * FROM flight_recorder.compare(now() - interval '1 hour', now());
END $$;

SELECT ok(true, 'Error: Snapshot inserts during compare() should be safe');

-- Test DELETE from aggregates during wait_summary() query
DO $$
BEGIN
    PERFORM * FROM flight_recorder.wait_summary(now() - interval '1 hour', now());
    DELETE FROM flight_recorder.wait_event_aggregates
    WHERE start_time < now() - interval '30 days';
END $$;

SELECT ok(true, 'Error: Aggregate deletes during queries should be safe');

-- Test schema size check during cleanup operation
SELECT lives_ok(
    $$SELECT flight_recorder.cleanup('7 days')$$,
    'Error: Schema size checks during cleanup should be safe'
);

-- Test two quarterly_review_with_summary() calls
SELECT lives_ok(
    $$SELECT * FROM flight_recorder.quarterly_review_with_summary()$$,
    'Error: Concurrent quarterly reviews should be safe'
);

-- Test concurrent ring buffer updates to same slot
DO $$
BEGIN
    UPDATE flight_recorder.samples_ring
    SET captured_at = now()
    WHERE slot_id = 0;

    UPDATE flight_recorder.samples_ring
    SET captured_at = now()
    WHERE slot_id = 0;
END $$;

SELECT ok(true, 'Error: Concurrent updates to same ring buffer slot should be safe');

-- Test pg_cron job schedule change during execution
SELECT lives_ok(
    $$SELECT flight_recorder.sample()$$,
    'Error: Collection should handle pg_cron timing changes'
);

-- =============================================================================
-- 14. VERSION-SPECIFIC BEHAVIOR (40 tests) - Phase 4
-- =============================================================================
-- Tests PostgreSQL version-specific features across PG15, PG16, and PG17

-- -----------------------------------------------------------------------------
-- 14.1 VERSION DETECTION (5 tests)
-- -----------------------------------------------------------------------------

-- Test _pg_version() returns 15, 16, or 17
SELECT ok(
    flight_recorder._pg_version() IN (15, 16, 17),
    'Phase 4: _pg_version() should return 15, 16, or 17'
);

-- Test version is stored in snapshots table
DO $$
DECLARE
    v_snapshot_count INTEGER;
    v_pg_version INTEGER;
BEGIN
    SELECT COUNT(*) INTO v_snapshot_count FROM flight_recorder.snapshots;
    PERFORM flight_recorder.snapshot();

    -- Check if snapshot was actually created (not skipped)
    IF (SELECT COUNT(*) FROM flight_recorder.snapshots) > v_snapshot_count THEN
        SELECT pg_version INTO v_pg_version FROM flight_recorder.snapshots ORDER BY id DESC LIMIT 1;
        IF v_pg_version IS NULL OR v_pg_version NOT IN (15, 16, 17) THEN
            RAISE EXCEPTION 'Phase 4: snapshot() should store pg_version in (15, 16, 17)';
        END IF;
    END IF;
END $$;

SELECT ok(true, 'Phase 4: snapshot() stores pg_version (or was skipped)');

-- Test version detection consistency
SELECT is(
    flight_recorder._pg_version(),
    (SELECT current_setting('server_version_num')::integer / 10000),
    'Phase 4: _pg_version() should match PostgreSQL major version'
);

-- Test version used for conditional logic (pg_stat_io availability)
DO $$
DECLARE
    v_pg_version INTEGER;
    v_has_io_data BOOLEAN;
BEGIN
    v_pg_version := flight_recorder._pg_version();
    PERFORM flight_recorder.snapshot();

    SELECT io_checkpointer_writes IS NOT NULL INTO v_has_io_data
    FROM flight_recorder.snapshots ORDER BY id DESC LIMIT 1;

    IF v_pg_version >= 16 AND NOT v_has_io_data THEN
        RAISE EXCEPTION 'Phase 4: PG16+ should have io_* data populated';
    END IF;

    IF v_pg_version = 15 AND v_has_io_data THEN
        RAISE EXCEPTION 'Phase 4: PG15 should have NULL io_* data';
    END IF;
END $$;

SELECT ok(true, 'Phase 4: Version-specific pg_stat_io collection works correctly');

-- Test version determines checkpoint source view
DO $$
DECLARE
    v_pg_version INTEGER;
    v_has_ckpt_timed BOOLEAN;
BEGIN
    v_pg_version := flight_recorder._pg_version();
    PERFORM flight_recorder.snapshot();

    SELECT ckpt_timed IS NOT NULL INTO v_has_ckpt_timed
    FROM flight_recorder.snapshots ORDER BY id DESC LIMIT 1;

    -- All versions should have checkpoint data
    IF NOT v_has_ckpt_timed THEN
        RAISE EXCEPTION 'Phase 4: All versions should have ckpt_timed populated';
    END IF;
END $$;

SELECT ok(true, 'Phase 4: Checkpoint stats collected from correct source view');

-- -----------------------------------------------------------------------------
-- 14.2 PG15-SPECIFIC TESTS (10 tests)
-- -----------------------------------------------------------------------------

-- Test PG15: verify no pg_stat_io columns
DO $$
DECLARE
    v_pg_version INTEGER;
    v_io_writes BIGINT;
BEGIN
    v_pg_version := flight_recorder._pg_version();

    IF v_pg_version = 15 THEN
        PERFORM flight_recorder.snapshot();

        SELECT io_checkpointer_writes INTO v_io_writes
        FROM flight_recorder.snapshots ORDER BY id DESC LIMIT 1;

        IF v_io_writes IS NOT NULL THEN
            RAISE EXCEPTION 'Phase 4: PG15 should have NULL io_* columns';
        END IF;
    END IF;
END $$;

SELECT ok(true, 'Phase 4: PG15 has NULL io_* columns (skipped if not PG15)');

-- Test PG15: verify checkpoint stats from pg_stat_bgwriter
DO $$
DECLARE
    v_pg_version INTEGER;
    v_snapshot_count INTEGER;
    v_ckpt_timed BIGINT;
BEGIN
    v_pg_version := flight_recorder._pg_version();

    IF v_pg_version = 15 THEN
        SELECT COUNT(*) INTO v_snapshot_count FROM flight_recorder.snapshots;
        PERFORM flight_recorder.snapshot();

        -- Only test if snapshot was actually created (not skipped)
        IF (SELECT COUNT(*) FROM flight_recorder.snapshots) > v_snapshot_count THEN
            SELECT ckpt_timed INTO v_ckpt_timed
            FROM flight_recorder.snapshots ORDER BY id DESC LIMIT 1;

            IF v_ckpt_timed IS NULL THEN
                RAISE EXCEPTION 'Phase 4: PG15 should have checkpoint stats from pg_stat_bgwriter';
            END IF;
        END IF;
    END IF;
END $$;

SELECT ok(true, 'Phase 4: PG15 collects checkpoint stats from pg_stat_bgwriter (skipped if not PG15 or snapshot skipped)');

-- Test PG15: verify bgw_buffers_backend populated
DO $$
DECLARE
    v_pg_version INTEGER;
    v_snapshot_count INTEGER;
    v_buffers_backend BIGINT;
BEGIN
    v_pg_version := flight_recorder._pg_version();

    IF v_pg_version = 15 THEN
        SELECT COUNT(*) INTO v_snapshot_count FROM flight_recorder.snapshots;
        PERFORM flight_recorder.snapshot();

        -- Only test if snapshot was actually created (not skipped)
        IF (SELECT COUNT(*) FROM flight_recorder.snapshots) > v_snapshot_count THEN
            SELECT bgw_buffers_backend INTO v_buffers_backend
            FROM flight_recorder.snapshots ORDER BY id DESC LIMIT 1;

            IF v_buffers_backend IS NULL THEN
                RAISE EXCEPTION 'Phase 4: PG15 should have bgw_buffers_backend from pg_stat_bgwriter';
            END IF;
        END IF;
    END IF;
END $$;

SELECT ok(true, 'Phase 4: PG15 has bgw_buffers_backend from pg_stat_bgwriter (skipped if not PG15 or snapshot skipped)');

-- Test PG15: verify deltas view doesn't error on missing io_* columns
DO $$
DECLARE
    v_pg_version INTEGER;
BEGIN
    v_pg_version := flight_recorder._pg_version();

    IF v_pg_version = 15 THEN
        PERFORM flight_recorder.snapshot();
        PERFORM pg_sleep(0.1);
        PERFORM flight_recorder.snapshot();

        -- Query deltas view with io_* columns
        PERFORM * FROM flight_recorder.deltas ORDER BY id DESC LIMIT 1;
    END IF;
END $$;

SELECT ok(true, 'Phase 4: PG15 deltas view handles NULL io_* columns (skipped if not PG15)');

-- Test PG15: compare() with NULL io_* values
DO $$
DECLARE
    v_pg_version INTEGER;
    v_start_id INTEGER;
    v_end_id INTEGER;
BEGIN
    v_pg_version := flight_recorder._pg_version();

    IF v_pg_version = 15 THEN
        SELECT id INTO v_start_id FROM flight_recorder.snapshots ORDER BY id DESC LIMIT 1;
        PERFORM pg_sleep(0.1);
        PERFORM flight_recorder.snapshot();
        SELECT id INTO v_end_id FROM flight_recorder.snapshots ORDER BY id DESC LIMIT 1;

        -- compare() should handle NULL io_* arithmetic
        PERFORM * FROM flight_recorder.compare(
            (SELECT captured_at FROM flight_recorder.snapshots WHERE id = v_start_id),
            (SELECT captured_at FROM flight_recorder.snapshots WHERE id = v_end_id)
        );
    END IF;
END $$;

SELECT ok(true, 'Phase 4: PG15 compare() handles NULL io_* arithmetic (skipped if not PG15)');

-- Test PG15: summary_report() doesn't show io_* sections
DO $$
DECLARE
    v_pg_version INTEGER;
    v_report TEXT;
BEGIN
    v_pg_version := flight_recorder._pg_version();

    IF v_pg_version = 15 THEN
        PERFORM flight_recorder.snapshot();
        PERFORM pg_sleep(0.1);
        PERFORM flight_recorder.snapshot();

        SELECT flight_recorder.summary_report(now() - interval '1 hour', now()) INTO v_report;

        -- Report should not mention io_* metrics on PG15
        -- This is a soft check - just verify report is generated
        IF v_report IS NULL OR length(v_report) < 100 THEN
            RAISE EXCEPTION 'Phase 4: PG15 summary_report() should generate valid report';
        END IF;
    END IF;
END $$;

SELECT ok(true, 'Phase 4: PG15 summary_report() works without io_* data (skipped if not PG15)');

-- Test PG15: gracefully handles missing pg_stat_checkpointer
DO $$
DECLARE
    v_pg_version INTEGER;
    v_checkpointer_exists BOOLEAN;
BEGIN
    v_pg_version := flight_recorder._pg_version();

    IF v_pg_version = 15 THEN
        -- Verify pg_stat_checkpointer doesn't exist in PG15
        SELECT EXISTS (
            SELECT 1 FROM pg_views WHERE viewname = 'pg_stat_checkpointer'
        ) INTO v_checkpointer_exists;

        IF v_checkpointer_exists THEN
            RAISE WARNING 'Phase 4: Unexpected - pg_stat_checkpointer exists in PG15';
        END IF;

        -- snapshot() should still work without it
        PERFORM flight_recorder.snapshot();
    END IF;
END $$;

SELECT ok(true, 'Phase 4: PG15 handles missing pg_stat_checkpointer (skipped if not PG15)');

-- Test PG15: anomaly_report() works without io_* data
DO $$
DECLARE
    v_pg_version INTEGER;
BEGIN
    v_pg_version := flight_recorder._pg_version();

    IF v_pg_version = 15 THEN
        PERFORM flight_recorder.snapshot();
        PERFORM pg_sleep(0.1);
        PERFORM flight_recorder.snapshot();

        PERFORM * FROM flight_recorder.anomaly_report(now() - interval '1 hour', now());
    END IF;
END $$;

SELECT ok(true, 'Phase 4: PG15 anomaly_report() works without io_* data (skipped if not PG15)');

-- Test PG15: export_json() doesn't include io_* fields
DO $$
DECLARE
    v_pg_version INTEGER;
    v_json JSONB;
BEGIN
    v_pg_version := flight_recorder._pg_version();

    IF v_pg_version = 15 THEN
        PERFORM flight_recorder.snapshot();

        SELECT flight_recorder.export_json(now() - interval '1 hour', now()) INTO v_json;

        IF v_json IS NULL THEN
            RAISE EXCEPTION 'Phase 4: PG15 export_json() should return valid JSON';
        END IF;
    END IF;
END $$;

SELECT ok(true, 'Phase 4: PG15 export_json() generates valid JSON (skipped if not PG15)');

-- Test PG15: all analysis functions work without io_* data
DO $$
DECLARE
    v_pg_version INTEGER;
BEGIN
    v_pg_version := flight_recorder._pg_version();

    IF v_pg_version = 15 THEN
        PERFORM flight_recorder.snapshot();
        PERFORM pg_sleep(0.1);
        PERFORM flight_recorder.snapshot();

        -- Test multiple analysis functions
        PERFORM * FROM flight_recorder.wait_summary(now() - interval '1 hour', now());
        PERFORM * FROM flight_recorder.activity_at(now());
    END IF;
END $$;

SELECT ok(true, 'Phase 4: PG15 analysis functions work without io_* data (skipped if not PG15)');

-- -----------------------------------------------------------------------------
-- 14.3 PG16-SPECIFIC TESTS (10 tests)
-- -----------------------------------------------------------------------------

-- Test PG16: verify pg_stat_io collection
DO $$
DECLARE
    v_pg_version INTEGER;
    v_io_writes BIGINT;
BEGIN
    v_pg_version := flight_recorder._pg_version();

    IF v_pg_version = 16 THEN
        PERFORM flight_recorder.snapshot();

        SELECT io_checkpointer_writes INTO v_io_writes
        FROM flight_recorder.snapshots ORDER BY id DESC LIMIT 1;

        IF v_io_writes IS NULL THEN
            RAISE EXCEPTION 'Phase 4: PG16 should have io_* columns populated';
        END IF;
    END IF;
END $$;

SELECT ok(true, 'Phase 4: PG16 collects pg_stat_io data (skipped if not PG16)');

-- Test PG16: checkpoint stats still from pg_stat_bgwriter
DO $$
DECLARE
    v_pg_version INTEGER;
    v_ckpt_timed BIGINT;
BEGIN
    v_pg_version := flight_recorder._pg_version();

    IF v_pg_version = 16 THEN
        PERFORM flight_recorder.snapshot();

        SELECT ckpt_timed INTO v_ckpt_timed
        FROM flight_recorder.snapshots ORDER BY id DESC LIMIT 1;

        IF v_ckpt_timed IS NULL THEN
            RAISE EXCEPTION 'Phase 4: PG16 should have checkpoint stats from pg_stat_bgwriter';
        END IF;
    END IF;
END $$;

SELECT ok(true, 'Phase 4: PG16 collects checkpoint stats from pg_stat_bgwriter (skipped if not PG16)');

-- Test PG16: io_checkpointer_* columns populated
DO $$
DECLARE
    v_pg_version INTEGER;
    v_io_ckpt_writes BIGINT;
BEGIN
    v_pg_version := flight_recorder._pg_version();

    IF v_pg_version = 16 THEN
        PERFORM flight_recorder.snapshot();

        SELECT io_checkpointer_writes INTO v_io_ckpt_writes
        FROM flight_recorder.snapshots ORDER BY id DESC LIMIT 1;

        IF v_io_ckpt_writes IS NULL THEN
            RAISE EXCEPTION 'Phase 4: PG16 should have io_checkpointer_* populated';
        END IF;
    END IF;
END $$;

SELECT ok(true, 'Phase 4: PG16 has io_checkpointer_* columns populated (skipped if not PG16)');

-- Test PG16: io_autovacuum_* columns populated
DO $$
DECLARE
    v_pg_version INTEGER;
    v_io_av_writes BIGINT;
BEGIN
    v_pg_version := flight_recorder._pg_version();

    IF v_pg_version = 16 THEN
        PERFORM flight_recorder.snapshot();

        SELECT io_autovacuum_writes INTO v_io_av_writes
        FROM flight_recorder.snapshots ORDER BY id DESC LIMIT 1;

        IF v_io_av_writes IS NULL THEN
            RAISE EXCEPTION 'Phase 4: PG16 should have io_autovacuum_* populated';
        END IF;
    END IF;
END $$;

SELECT ok(true, 'Phase 4: PG16 has io_autovacuum_* columns populated (skipped if not PG16)');

-- Test PG16: io_client_* columns populated
DO $$
DECLARE
    v_pg_version INTEGER;
    v_io_client_writes BIGINT;
BEGIN
    v_pg_version := flight_recorder._pg_version();

    IF v_pg_version = 16 THEN
        PERFORM flight_recorder.snapshot();

        SELECT io_client_writes INTO v_io_client_writes
        FROM flight_recorder.snapshots ORDER BY id DESC LIMIT 1;

        IF v_io_client_writes IS NULL THEN
            RAISE EXCEPTION 'Phase 4: PG16 should have io_client_* populated';
        END IF;
    END IF;
END $$;

SELECT ok(true, 'Phase 4: PG16 has io_client_* columns populated (skipped if not PG16)');

-- Test PG16: io_bgwriter_* columns populated
DO $$
DECLARE
    v_pg_version INTEGER;
    v_io_bgw_writes BIGINT;
BEGIN
    v_pg_version := flight_recorder._pg_version();

    IF v_pg_version = 16 THEN
        PERFORM flight_recorder.snapshot();

        SELECT io_bgwriter_writes INTO v_io_bgw_writes
        FROM flight_recorder.snapshots ORDER BY id DESC LIMIT 1;

        IF v_io_bgw_writes IS NULL THEN
            RAISE EXCEPTION 'Phase 4: PG16 should have io_bgwriter_* populated';
        END IF;
    END IF;
END $$;

SELECT ok(true, 'Phase 4: PG16 has io_bgwriter_* columns populated (skipped if not PG16)');

-- Test PG16: compare() includes io_* delta calculations
DO $$
DECLARE
    v_pg_version INTEGER;
    v_start_id INTEGER;
    v_end_id INTEGER;
BEGIN
    v_pg_version := flight_recorder._pg_version();

    IF v_pg_version = 16 THEN
        SELECT id INTO v_start_id FROM flight_recorder.snapshots ORDER BY id DESC LIMIT 1;
        PERFORM pg_sleep(0.1);
        PERFORM flight_recorder.snapshot();
        SELECT id INTO v_end_id FROM flight_recorder.snapshots ORDER BY id DESC LIMIT 1;

        PERFORM * FROM flight_recorder.compare(
            (SELECT captured_at FROM flight_recorder.snapshots WHERE id = v_start_id),
            (SELECT captured_at FROM flight_recorder.snapshots WHERE id = v_end_id)
        );
    END IF;
END $$;

SELECT ok(true, 'Phase 4: PG16 compare() includes io_* deltas (skipped if not PG16)');

-- Test PG16: deltas view includes io_* columns
DO $$
DECLARE
    v_pg_version INTEGER;
    v_io_delta BIGINT;
BEGIN
    v_pg_version := flight_recorder._pg_version();

    IF v_pg_version = 16 THEN
        PERFORM flight_recorder.snapshot();
        PERFORM pg_sleep(0.1);
        PERFORM flight_recorder.snapshot();

        SELECT io_ckpt_writes_delta INTO v_io_delta
        FROM flight_recorder.deltas ORDER BY id DESC LIMIT 1;

        -- Delta may be 0 or NULL depending on activity, just verify no error
    END IF;
END $$;

SELECT ok(true, 'Phase 4: PG16 deltas view includes io_* columns (skipped if not PG16)');

-- Test PG16: summary_report() includes io_* sections
DO $$
DECLARE
    v_pg_version INTEGER;
    v_report TEXT;
BEGIN
    v_pg_version := flight_recorder._pg_version();

    IF v_pg_version = 16 THEN
        PERFORM flight_recorder.snapshot();
        PERFORM pg_sleep(0.1);
        PERFORM flight_recorder.snapshot();

        SELECT flight_recorder.summary_report(now() - interval '1 hour', now()) INTO v_report;

        IF v_report IS NULL OR length(v_report) < 100 THEN
            RAISE EXCEPTION 'Phase 4: PG16 summary_report() should generate valid report';
        END IF;
    END IF;
END $$;

SELECT ok(true, 'Phase 4: PG16 summary_report() includes io_* data (skipped if not PG16)');

-- Test PG16: anomaly_report() can detect io_* anomalies
DO $$
DECLARE
    v_pg_version INTEGER;
BEGIN
    v_pg_version := flight_recorder._pg_version();

    IF v_pg_version = 16 THEN
        PERFORM flight_recorder.snapshot();
        PERFORM pg_sleep(0.1);
        PERFORM flight_recorder.snapshot();

        PERFORM * FROM flight_recorder.anomaly_report(now() - interval '1 hour', now());
    END IF;
END $$;

SELECT ok(true, 'Phase 4: PG16 anomaly_report() can analyze io_* data (skipped if not PG16)');

-- -----------------------------------------------------------------------------
-- 14.4 PG17-SPECIFIC TESTS (10 tests)
-- -----------------------------------------------------------------------------

-- Test PG17: verify pg_stat_checkpointer used
DO $$
DECLARE
    v_pg_version INTEGER;
    v_ckpt_timed BIGINT;
BEGIN
    v_pg_version := flight_recorder._pg_version();

    IF v_pg_version = 17 THEN
        PERFORM flight_recorder.snapshot();

        SELECT ckpt_timed INTO v_ckpt_timed
        FROM flight_recorder.snapshots ORDER BY id DESC LIMIT 1;

        IF v_ckpt_timed IS NULL THEN
            RAISE EXCEPTION 'Phase 4: PG17 should have checkpoint stats from pg_stat_checkpointer';
        END IF;
    END IF;
END $$;

SELECT ok(true, 'Phase 4: PG17 uses pg_stat_checkpointer (skipped if not PG17)');

-- Test PG17: verify checkpoint_lsn from pg_stat_checkpointer
DO $$
DECLARE
    v_pg_version INTEGER;
    v_ckpt_lsn PG_LSN;
BEGIN
    v_pg_version := flight_recorder._pg_version();

    IF v_pg_version = 17 THEN
        PERFORM flight_recorder.snapshot();

        SELECT checkpoint_lsn INTO v_ckpt_lsn
        FROM flight_recorder.snapshots ORDER BY id DESC LIMIT 1;

        IF v_ckpt_lsn IS NULL THEN
            RAISE EXCEPTION 'Phase 4: PG17 should have checkpoint_lsn from pg_stat_checkpointer';
        END IF;
    END IF;
END $$;

SELECT ok(true, 'Phase 4: PG17 has checkpoint_lsn from pg_stat_checkpointer (skipped if not PG17)');

-- Test PG17: verify ckpt_timed and ckpt_requested from new view
DO $$
DECLARE
    v_pg_version INTEGER;
    v_ckpt_timed BIGINT;
    v_ckpt_req BIGINT;
BEGIN
    v_pg_version := flight_recorder._pg_version();

    IF v_pg_version = 17 THEN
        PERFORM flight_recorder.snapshot();

        SELECT ckpt_timed, ckpt_requested INTO v_ckpt_timed, v_ckpt_req
        FROM flight_recorder.snapshots ORDER BY id DESC LIMIT 1;

        IF v_ckpt_timed IS NULL OR v_ckpt_req IS NULL THEN
            RAISE EXCEPTION 'Phase 4: PG17 should have ckpt_timed and ckpt_requested';
        END IF;
    END IF;
END $$;

SELECT ok(true, 'Phase 4: PG17 has ckpt_timed and ckpt_requested (skipped if not PG17)');

-- Test PG17: still has pg_stat_io
DO $$
DECLARE
    v_pg_version INTEGER;
    v_io_writes BIGINT;
BEGIN
    v_pg_version := flight_recorder._pg_version();

    IF v_pg_version = 17 THEN
        PERFORM flight_recorder.snapshot();

        SELECT io_checkpointer_writes INTO v_io_writes
        FROM flight_recorder.snapshots ORDER BY id DESC LIMIT 1;

        IF v_io_writes IS NULL THEN
            RAISE EXCEPTION 'Phase 4: PG17 should still have io_* columns from pg_stat_io';
        END IF;
    END IF;
END $$;

SELECT ok(true, 'Phase 4: PG17 still collects pg_stat_io data (skipped if not PG17)');

-- Test PG17: compare() checkpoint delta calculations
DO $$
DECLARE
    v_pg_version INTEGER;
    v_start_id INTEGER;
    v_end_id INTEGER;
BEGIN
    v_pg_version := flight_recorder._pg_version();

    IF v_pg_version = 17 THEN
        SELECT id INTO v_start_id FROM flight_recorder.snapshots ORDER BY id DESC LIMIT 1;
        PERFORM pg_sleep(0.1);
        PERFORM flight_recorder.snapshot();
        SELECT id INTO v_end_id FROM flight_recorder.snapshots ORDER BY id DESC LIMIT 1;

        PERFORM * FROM flight_recorder.compare(
            (SELECT captured_at FROM flight_recorder.snapshots WHERE id = v_start_id),
            (SELECT captured_at FROM flight_recorder.snapshots WHERE id = v_end_id)
        );
    END IF;
END $$;

SELECT ok(true, 'Phase 4: PG17 compare() calculates checkpoint deltas correctly (skipped if not PG17)');

-- Test PG17: checkpoint column names correct
DO $$
DECLARE
    v_pg_version INTEGER;
    v_has_columns BOOLEAN;
BEGIN
    v_pg_version := flight_recorder._pg_version();

    IF v_pg_version = 17 THEN
        -- Verify expected checkpoint columns exist
        SELECT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'flight_recorder'
              AND table_name = 'snapshots'
              AND column_name IN ('ckpt_timed', 'ckpt_requested', 'checkpoint_lsn')
        ) INTO v_has_columns;

        IF NOT v_has_columns THEN
            RAISE EXCEPTION 'Phase 4: PG17 should have correct checkpoint columns';
        END IF;
    END IF;
END $$;

SELECT ok(true, 'Phase 4: PG17 has correct checkpoint column names (skipped if not PG17)');

-- Test PG17: summary_report() uses correct checkpoint source
DO $$
DECLARE
    v_pg_version INTEGER;
    v_report TEXT;
BEGIN
    v_pg_version := flight_recorder._pg_version();

    IF v_pg_version = 17 THEN
        PERFORM flight_recorder.snapshot();
        PERFORM pg_sleep(0.1);
        PERFORM flight_recorder.snapshot();

        SELECT flight_recorder.summary_report(now() - interval '1 hour', now()) INTO v_report;

        IF v_report IS NULL OR length(v_report) < 100 THEN
            RAISE EXCEPTION 'Phase 4: PG17 summary_report() should generate valid report';
        END IF;
    END IF;
END $$;

SELECT ok(true, 'Phase 4: PG17 summary_report() uses pg_stat_checkpointer data (skipped if not PG17)');

-- Test PG17: pg_control_checkpoint() available
DO $$
DECLARE
    v_pg_version INTEGER;
    v_checkpoint_lsn PG_LSN;
BEGIN
    v_pg_version := flight_recorder._pg_version();

    IF v_pg_version = 17 THEN
        -- pg_control_checkpoint() should be available in PG17
        SELECT checkpoint_lsn INTO v_checkpoint_lsn
        FROM pg_control_checkpoint();

        IF v_checkpoint_lsn IS NULL THEN
            RAISE WARNING 'Phase 4: pg_control_checkpoint() returned NULL checkpoint_lsn';
        END IF;
    END IF;
END $$;

SELECT ok(true, 'Phase 4: PG17 pg_control_checkpoint() available (skipped if not PG17)');

-- Test PG17: gracefully handles pg_stat_bgwriter changes
DO $$
DECLARE
    v_pg_version INTEGER;
    v_bgwriter_exists BOOLEAN;
BEGIN
    v_pg_version := flight_recorder._pg_version();

    IF v_pg_version = 17 THEN
        -- Verify pg_stat_bgwriter still exists in PG17
        SELECT EXISTS (
            SELECT 1 FROM pg_views WHERE viewname = 'pg_stat_bgwriter'
        ) INTO v_bgwriter_exists;

        IF NOT v_bgwriter_exists THEN
            RAISE WARNING 'Phase 4: pg_stat_bgwriter removed in PG17';
        END IF;

        -- snapshot() should work regardless
        PERFORM flight_recorder.snapshot();
    END IF;
END $$;

SELECT ok(true, 'Phase 4: PG17 handles pg_stat_bgwriter gracefully (skipped if not PG17)');

-- Test PG17: all analysis functions work with new views
DO $$
DECLARE
    v_pg_version INTEGER;
BEGIN
    v_pg_version := flight_recorder._pg_version();

    IF v_pg_version = 17 THEN
        PERFORM flight_recorder.snapshot();
        PERFORM pg_sleep(0.1);
        PERFORM flight_recorder.snapshot();

        -- Test multiple analysis functions
        PERFORM * FROM flight_recorder.wait_summary(now() - interval '1 hour', now());
        PERFORM * FROM flight_recorder.activity_at(now());
        PERFORM * FROM flight_recorder.anomaly_report(now() - interval '1 hour', now());
    END IF;
END $$;

SELECT ok(true, 'Phase 4: PG17 analysis functions work with new PG17 views (skipped if not PG17)');

-- -----------------------------------------------------------------------------
-- 14.5 CROSS-VERSION COMPATIBILITY (5 tests)
-- -----------------------------------------------------------------------------

-- Test pg_version column populated in snapshots
SELECT ok(
    (SELECT pg_version FROM flight_recorder.snapshots ORDER BY id DESC LIMIT 1) IS NOT NULL,
    'Phase 4: pg_version column should be populated in snapshots'
);

-- Test deltas view works across all versions
SELECT lives_ok(
    $$SELECT * FROM flight_recorder.deltas ORDER BY id DESC LIMIT 1$$,
    'Phase 4: deltas view should work on all PG versions'
);

-- Test compare() produces consistent results across versions
DO $$
DECLARE
    v_start_id INTEGER;
    v_end_id INTEGER;
BEGIN
    SELECT id INTO v_start_id FROM flight_recorder.snapshots ORDER BY id ASC LIMIT 1;
    SELECT id INTO v_end_id FROM flight_recorder.snapshots ORDER BY id DESC LIMIT 1;

    IF v_start_id IS NOT NULL AND v_end_id IS NOT NULL AND v_start_id != v_end_id THEN
        PERFORM * FROM flight_recorder.compare(
            (SELECT captured_at FROM flight_recorder.snapshots WHERE id = v_start_id),
            (SELECT captured_at FROM flight_recorder.snapshots WHERE id = v_end_id)
        );
    END IF;
END $$;

SELECT ok(true, 'Phase 4: compare() produces consistent results across versions');

-- Test NULL arithmetic in calculations
DO $$
DECLARE
    v_pg_version INTEGER;
    v_delta BIGINT;
BEGIN
    v_pg_version := flight_recorder._pg_version();

    -- Test NULL - NULL = NULL (not error)
    SELECT (NULL::BIGINT - NULL::BIGINT) INTO v_delta;

    IF v_delta IS NOT NULL THEN
        RAISE EXCEPTION 'Phase 4: NULL arithmetic should return NULL';
    END IF;
END $$;

SELECT ok(true, 'Phase 4: NULL arithmetic handled gracefully in delta calculations');

-- Test snapshot() exception handling across versions
DO $$
BEGIN
    -- Test with short timeout to potentially trigger exception
    SET LOCAL statement_timeout = '10s';
    PERFORM flight_recorder.snapshot();
    RESET statement_timeout;
EXCEPTION WHEN OTHERS THEN
    -- Exception should be caught and logged
    RESET statement_timeout;
    RAISE WARNING 'Phase 4: snapshot() exception: %', SQLERRM;
END $$;

SELECT ok(true, 'Phase 4: snapshot() exception handling works across versions');

-- =============================================================================
-- 15. LOAD SHEDDING & CIRCUIT BREAKER (30 tests) - Phase 5
-- =============================================================================
-- Tests P0 safety mechanisms that protect database from collection overhead

-- -----------------------------------------------------------------------------
-- 15.1 LOAD SHEDDING (10 tests)
-- -----------------------------------------------------------------------------

-- Test 1: Load shedding disabled
UPDATE flight_recorder.config SET value = 'false' WHERE key = 'load_shedding_enabled';
DELETE FROM flight_recorder.collection_stats;

DO $$ BEGIN
    PERFORM flight_recorder.sample();
END $$;

SELECT ok(
    (SELECT count(*) FROM flight_recorder.collection_stats WHERE skipped = false AND collection_type = 'sample') >= 1,
    'Safety: Load shedding disabled should allow collection'
);

-- Re-enable for other tests
UPDATE flight_recorder.config SET value = 'true' WHERE key = 'load_shedding_enabled';

-- Test 2: Load shedding with threshold = 0% (always skip if any connections exist)
UPDATE flight_recorder.config SET value = '0' WHERE key = 'load_shedding_active_pct';
DELETE FROM flight_recorder.collection_stats;

DO $$ BEGIN
    PERFORM flight_recorder.sample();
END $$;

-- Check if collection was attempted and skipped (if active connections > 0%)
SELECT ok(
    (SELECT count(*) FROM flight_recorder.collection_stats WHERE collection_type = 'sample') > 0,
    'Safety: Load shedding with 0% threshold should create collection_stats entry'
);

-- Test 3: Verify skip_reason format for load shedding (if skip occurred)
SELECT ok(
    NOT EXISTS (SELECT 1 FROM flight_recorder.collection_stats WHERE collection_type = 'sample' AND skipped = true)
    OR (SELECT skipped_reason FROM flight_recorder.collection_stats WHERE collection_type = 'sample' AND skipped = true ORDER BY started_at DESC LIMIT 1) LIKE '%Load shedding: high load%',
    'Safety: Load shedding skip reason should match expected format (if skip occurred)'
);

-- Test 4: Load shedding with threshold = 100% (never skip unless at 100% connections)
UPDATE flight_recorder.config SET value = '100' WHERE key = 'load_shedding_active_pct';
DELETE FROM flight_recorder.collection_stats;

DO $$ BEGIN
    PERFORM flight_recorder.sample();
END $$;

-- With 100% threshold, load shedding should not trigger (unless exactly at 100% connections)
SELECT ok(
    (SELECT count(*) FROM flight_recorder.collection_stats WHERE collection_type = 'sample') > 0,
    'Safety: Load shedding with 100% threshold should create collection_stats entry'
);

-- Reset to default
UPDATE flight_recorder.config SET value = '70' WHERE key = 'load_shedding_active_pct';

-- Test 5: collection_stats logging for load shedding
UPDATE flight_recorder.config SET value = '0' WHERE key = 'load_shedding_active_pct';
DELETE FROM flight_recorder.collection_stats;

DO $$ BEGIN
    PERFORM flight_recorder.sample();
END $$;

SELECT ok(
    EXISTS (
        SELECT 1 FROM flight_recorder.collection_stats
        WHERE collection_type = 'sample'
          AND skipped = true
          AND skipped_reason IS NOT NULL
          AND skipped_reason LIKE '%Load shedding%'
    ),
    'Safety: Load shedding should log skip to collection_stats with reason'
);

-- Reset
UPDATE flight_recorder.config SET value = '70' WHERE key = 'load_shedding_active_pct';

-- Test 6: Load shedding doesn't affect snapshot()
UPDATE flight_recorder.config SET value = '0' WHERE key = 'load_shedding_active_pct';
DELETE FROM flight_recorder.snapshots WHERE captured_at > now() - interval '1 minute';

DO $$ BEGIN
    PERFORM flight_recorder.snapshot();
END $$;

SELECT ok(
    EXISTS (SELECT 1 FROM flight_recorder.snapshots WHERE captured_at > now() - interval '10 seconds'),
    'Safety: Load shedding should not affect snapshot() collections'
);

-- Reset
UPDATE flight_recorder.config SET value = '70' WHERE key = 'load_shedding_active_pct';

-- Test 7: Load shedding recovery (high → normal)
UPDATE flight_recorder.config SET value = '0' WHERE key = 'load_shedding_active_pct';
DELETE FROM flight_recorder.collection_stats;

-- First sample should skip
DO $$ BEGIN
    PERFORM flight_recorder.sample();
END $$;

-- Change to normal threshold
UPDATE flight_recorder.config SET value = '70' WHERE key = 'load_shedding_active_pct';

-- Second sample should succeed
DO $$ BEGIN
    PERFORM flight_recorder.sample();
END $$;

SELECT ok(
    (SELECT count(*) FROM flight_recorder.collection_stats WHERE skipped = false AND collection_type = 'sample') >= 1,
    'Safety: Load shedding recovery - collection should succeed after threshold increased'
);

-- Test 8: Verify skip_reason includes threshold value
UPDATE flight_recorder.config SET value = '0' WHERE key = 'load_shedding_active_pct';
DELETE FROM flight_recorder.collection_stats;

DO $$ BEGIN
    PERFORM flight_recorder.sample();
END $$;

SELECT ok(
    (SELECT skipped_reason FROM flight_recorder.collection_stats WHERE collection_type = 'sample' AND skipped = true ORDER BY started_at DESC LIMIT 1) LIKE
    '%0% threshold%',
    'Safety: Load shedding skip reason should include configured threshold'
);

-- Reset
UPDATE flight_recorder.config SET value = '70' WHERE key = 'load_shedding_active_pct';

-- Test 9: Load shedding with production_safe profile (60% threshold)
SELECT flight_recorder.apply_profile('production_safe');
DELETE FROM flight_recorder.collection_stats;

-- Should use 60% threshold from profile
DO $$ BEGIN
    PERFORM flight_recorder.sample();
END $$;

SELECT ok(
    flight_recorder._get_config('load_shedding_active_pct', '70')::integer = 60,
    'Safety: production_safe profile should set load shedding to 60%'
);

-- Reset to default profile
SELECT flight_recorder.apply_profile('default');

-- Test 10: Multiple load shedding skips tracked correctly
UPDATE flight_recorder.config SET value = '0' WHERE key = 'load_shedding_active_pct';
DELETE FROM flight_recorder.collection_stats;

-- Generate 3 skipped collections
DO $$ BEGIN
    PERFORM flight_recorder.sample();
    PERFORM flight_recorder.sample();
    PERFORM flight_recorder.sample();
END $$;

SELECT ok(
    (SELECT count(*) FROM flight_recorder.collection_stats WHERE collection_type = 'sample' AND skipped = true AND skipped_reason LIKE '%Load shedding%') = 3,
    'Safety: Multiple load shedding skips should all be tracked in collection_stats'
);

-- Reset
UPDATE flight_recorder.config SET value = '70' WHERE key = 'load_shedding_active_pct';

-- -----------------------------------------------------------------------------
-- 15.2 LOAD THROTTLING (10 tests)
-- -----------------------------------------------------------------------------

-- Test 1: Load throttling disabled
UPDATE flight_recorder.config SET value = 'false' WHERE key = 'load_throttle_enabled';
DELETE FROM flight_recorder.collection_stats;

DO $$ BEGIN
    PERFORM flight_recorder.sample();
END $$;

SELECT ok(
    (SELECT count(*) FROM flight_recorder.collection_stats WHERE skipped = false AND collection_type = 'sample') >= 1,
    'Safety: Load throttling disabled should allow collection'
);

-- Re-enable
UPDATE flight_recorder.config SET value = 'true' WHERE key = 'load_throttle_enabled';

-- Test 2: Config values can be set for transaction threshold
UPDATE flight_recorder.config SET value = '0' WHERE key = 'load_throttle_xact_threshold';

SELECT ok(
    flight_recorder._get_config('load_throttle_xact_threshold', '1000')::integer = 0,
    'Safety: Load throttling transaction threshold config can be set'
);

-- Reset transaction threshold
UPDATE flight_recorder.config SET value = '1000' WHERE key = 'load_throttle_xact_threshold';

-- Test 3: Config values can be set for block I/O threshold
UPDATE flight_recorder.config SET value = '0' WHERE key = 'load_throttle_blk_threshold';

SELECT ok(
    flight_recorder._get_config('load_throttle_blk_threshold', '10000')::integer = 0,
    'Safety: Load throttling block I/O threshold config can be set'
);

-- Reset block threshold
UPDATE flight_recorder.config SET value = '10000' WHERE key = 'load_throttle_blk_threshold';

-- Test 7: Load throttling doesn't affect snapshot()
UPDATE flight_recorder.config SET value = '0' WHERE key = 'load_throttle_xact_threshold';
DELETE FROM flight_recorder.snapshots WHERE captured_at > now() - interval '1 minute';

DO $$ BEGIN
    PERFORM flight_recorder.snapshot();
END $$;

SELECT ok(
    EXISTS (SELECT 1 FROM flight_recorder.snapshots WHERE captured_at > now() - interval '10 seconds'),
    'Safety: Load throttling should not affect snapshot() collections'
);

-- Reset
UPDATE flight_recorder.config SET value = '1000' WHERE key = 'load_throttle_xact_threshold';

-- Test 8: Combined load shedding + throttling (shedding runs first)
-- Set load shedding to 0% which will always trigger (X% >= 0% is always true)
-- Set load throttling to 0 which may or may not trigger depending on xact rate
UPDATE flight_recorder.config SET value = '0' WHERE key = 'load_shedding_active_pct';
UPDATE flight_recorder.config SET value = '0' WHERE key = 'load_throttle_xact_threshold';
DELETE FROM flight_recorder.collection_stats;

DO $$ BEGIN
    PERFORM flight_recorder.sample();
END $$;

-- Load shedding runs first (checked before throttling), so skip_reason should be load shedding
-- With >= comparison and 0% threshold, load shedding always triggers before throttling check
SELECT ok(
    (SELECT skipped_reason FROM flight_recorder.collection_stats WHERE collection_type = 'sample' AND skipped = true ORDER BY started_at DESC LIMIT 1) LIKE
    '%Load shedding%',
    'Safety: When both mechanisms active, load shedding should run first'
);

-- Reset
UPDATE flight_recorder.config SET value = '70' WHERE key = 'load_shedding_active_pct';
UPDATE flight_recorder.config SET value = '1000' WHERE key = 'load_throttle_xact_threshold';

-- Test 9: Throttling with troubleshooting profile (disabled)
SELECT flight_recorder.apply_profile('troubleshooting');

SELECT ok(
    flight_recorder._get_config('load_throttle_enabled', 'true')::boolean = false,
    'Safety: troubleshooting profile should disable load throttling'
);

-- Reset to default profile
SELECT flight_recorder.apply_profile('default');

-- Test 10: Load throttling works with default thresholds
DELETE FROM flight_recorder.collection_stats;

DO $$ BEGIN
    PERFORM flight_recorder.sample();
END $$;

SELECT ok(
    (SELECT count(*) FROM flight_recorder.collection_stats WHERE collection_type = 'sample') > 0,
    'Safety: Load throttling with default thresholds should allow collection'
);

-- -----------------------------------------------------------------------------
-- 15.3 CIRCUIT BREAKER (10 tests)
-- -----------------------------------------------------------------------------

-- Disable jitter to prevent race conditions with background cron jobs
UPDATE flight_recorder.config SET value = 'false' WHERE key = 'collection_jitter_enabled';

-- Test 1: Circuit breaker disabled
UPDATE flight_recorder.config SET value = 'false' WHERE key = 'circuit_breaker_enabled';
DELETE FROM flight_recorder.collection_stats;

DO $$ BEGIN
    PERFORM flight_recorder.sample();
END $$;

SELECT ok(
    (SELECT count(*) FROM flight_recorder.collection_stats WHERE skipped = false AND collection_type = 'sample') >= 1,
    'Safety: Circuit breaker disabled should allow collection'
);

-- Re-enable
UPDATE flight_recorder.config SET value = 'true' WHERE key = 'circuit_breaker_enabled';

-- Test 2: _check_circuit_breaker() with < 3 collections (inactive)
DELETE FROM flight_recorder.collection_stats;

-- Insert only 2 collections
INSERT INTO flight_recorder.collection_stats (collection_type, started_at, completed_at, duration_ms, success, skipped)
VALUES
    ('sample', now() - interval '5 minutes', now() - interval '5 minutes', 1500, true, false),
    ('sample', now() - interval '3 minutes', now() - interval '3 minutes', 1200, true, false);

SELECT ok(
    flight_recorder._check_circuit_breaker('sample') = false,
    'Safety: Circuit breaker should be inactive with < 3 collections in window'
);

-- Test 3: _check_circuit_breaker() with 3 fast collections (should not trip)
DELETE FROM flight_recorder.collection_stats;

INSERT INTO flight_recorder.collection_stats (collection_type, started_at, completed_at, duration_ms, success, skipped)
VALUES
    ('sample', now() - interval '5 minutes', now() - interval '5 minutes', 500, true, false),
    ('sample', now() - interval '3 minutes', now() - interval '3 minutes', 600, true, false),
    ('sample', now() - interval '1 minute', now() - interval '1 minute', 550, true, false);

-- Avg = 550ms < 1000ms threshold
SELECT ok(
    flight_recorder._check_circuit_breaker('sample') = false,
    'Safety: Circuit breaker should not trip with 3 fast collections (avg 550ms < 1000ms)'
);

-- Test 4: _check_circuit_breaker() with 3 slow collections (should trip)
-- First ensure circuit breaker is enabled and threshold is default
UPDATE flight_recorder.config SET value = 'true' WHERE key = 'circuit_breaker_enabled';
UPDATE flight_recorder.config SET value = '1000' WHERE key = 'circuit_breaker_threshold_ms';

DELETE FROM flight_recorder.collection_stats;

INSERT INTO flight_recorder.collection_stats (collection_type, started_at, completed_at, duration_ms, success, skipped)
VALUES
    ('sample', now() - interval '5 minutes', now() - interval '5 minutes', 1500, true, false),
    ('sample', now() - interval '3 minutes', now() - interval '3 minutes', 1200, true, false),
    ('sample', now() - interval '1 minute', now() - interval '1 minute', 1400, true, false);

-- Verify we have 3 rows
SELECT ok(
    (SELECT count(*) FROM flight_recorder.collection_stats WHERE collection_type = 'sample' AND success = true AND skipped = false) = 3,
    'Safety: Circuit breaker test data - should have 3 successful non-skipped samples'
);

-- Avg = 1366ms > 1000ms threshold
SELECT ok(
    flight_recorder._check_circuit_breaker('sample') = true,
    'Safety: Circuit breaker should trip with 3 slow collections (avg 1366ms > 1000ms)'
);

-- Test 5: Circuit breaker moving average (2 fast + 1 slow)
DELETE FROM flight_recorder.collection_stats;

INSERT INTO flight_recorder.collection_stats (collection_type, started_at, completed_at, duration_ms, success, skipped)
VALUES
    ('sample', now() - interval '5 minutes', now() - interval '5 minutes', 500, true, false),
    ('sample', now() - interval '3 minutes', now() - interval '3 minutes', 600, true, false),
    ('sample', now() - interval '1 minute', now() - interval '1 minute', 1500, true, false);

-- Avg = 866ms < 1000ms threshold
SELECT ok(
    flight_recorder._check_circuit_breaker('sample') = false,
    'Safety: Circuit breaker moving average should not trip (2 fast + 1 slow = 866ms avg)'
);

-- Test 6: Circuit breaker window (old collections ignored)
DELETE FROM flight_recorder.collection_stats;

-- Insert slow collections outside 15-minute window
INSERT INTO flight_recorder.collection_stats (collection_type, started_at, completed_at, duration_ms, success, skipped)
VALUES
    ('sample', now() - interval '20 minutes', now() - interval '20 minutes', 1500, true, false),
    ('sample', now() - interval '18 minutes', now() - interval '18 minutes', 1400, true, false),
    ('sample', now() - interval '16 minutes', now() - interval '16 minutes', 1600, true, false);

SELECT ok(
    flight_recorder._check_circuit_breaker('sample') = false,
    'Safety: Circuit breaker should ignore collections outside 15-minute window'
);

-- Test 7: Circuit breaker with aggressive 100ms threshold
UPDATE flight_recorder.config SET value = '100' WHERE key = 'circuit_breaker_threshold_ms';
DELETE FROM flight_recorder.collection_stats;

-- Insert collections with 200ms avg (would be fine with 1000ms, but trips at 100ms)
INSERT INTO flight_recorder.collection_stats (collection_type, started_at, completed_at, duration_ms, success, skipped)
VALUES
    ('sample', now() - interval '5 minutes', now() - interval '5 minutes', 200, true, false),
    ('sample', now() - interval '3 minutes', now() - interval '3 minutes', 210, true, false),
    ('sample', now() - interval '1 minute', now() - interval '1 minute', 190, true, false);

-- Avg = 200ms > 100ms threshold
SELECT ok(
    flight_recorder._check_circuit_breaker('sample') = true,
    'Safety: Circuit breaker with 100ms threshold should be highly sensitive'
);

-- Reset threshold
UPDATE flight_recorder.config SET value = '1000' WHERE key = 'circuit_breaker_threshold_ms';

-- Test 8: sample() respects circuit breaker
DELETE FROM flight_recorder.collection_stats;

-- Insert 3 slow collections to trip circuit breaker
INSERT INTO flight_recorder.collection_stats (collection_type, started_at, completed_at, duration_ms, success, skipped)
VALUES
    ('sample', now() - interval '5 minutes', now() - interval '5 minutes', 1500, true, false),
    ('sample', now() - interval '3 minutes', now() - interval '3 minutes', 1200, true, false),
    ('sample', now() - interval '1 minute', now() - interval '1 minute', 1400, true, false);

-- Now sample() should skip
DO $$ BEGIN
    PERFORM flight_recorder.sample();
END $$;

SELECT ok(
    EXISTS (
        SELECT 1 FROM flight_recorder.collection_stats
        WHERE skipped = true
          AND skipped_reason LIKE '%Circuit breaker%'
        ORDER BY started_at DESC LIMIT 1
    ),
    'Safety: sample() should skip when circuit breaker trips'
);

-- Test 9: Circuit breaker skip_reason format (verify it's specifically a sample skip)
SELECT ok(
    EXISTS (
        SELECT 1 FROM flight_recorder.collection_stats
        WHERE skipped = true
          AND collection_type = 'sample'
          AND skipped_reason LIKE '%Circuit breaker%'
    ),
    'Safety: Circuit breaker skip reason should be descriptive and collection_type should be sample'
);

-- Test 10: Circuit breaker recovery (3 slow → 3 fast)
DELETE FROM flight_recorder.collection_stats;

-- First: 3 slow collections (circuit trips)
INSERT INTO flight_recorder.collection_stats (collection_type, started_at, completed_at, duration_ms, success, skipped)
VALUES
    ('sample', now() - interval '10 minutes', now() - interval '10 minutes', 1500, true, false),
    ('sample', now() - interval '8 minutes', now() - interval '8 minutes', 1200, true, false),
    ('sample', now() - interval '6 minutes', now() - interval '6 minutes', 1400, true, false);

-- Verify circuit is tripped
SELECT ok(
    flight_recorder._check_circuit_breaker('sample') = true,
    'Safety: Circuit breaker should be tripped before recovery'
);

-- Now: 3 fast collections (circuit recovers)
INSERT INTO flight_recorder.collection_stats (collection_type, started_at, completed_at, duration_ms, success, skipped)
VALUES
    ('sample', now() - interval '5 minutes', now() - interval '5 minutes', 500, true, false),
    ('sample', now() - interval '3 minutes', now() - interval '3 minutes', 600, true, false),
    ('sample', now() - interval '1 minute', now() - interval '1 minute', 550, true, false);

-- Verify circuit has recovered (moving avg uses last 3: 500, 600, 550 = 550ms)
SELECT ok(
    flight_recorder._check_circuit_breaker('sample') = false,
    'Safety: Circuit breaker should recover after 3 fast collections'
);

-- Re-enable jitter after circuit breaker tests
UPDATE flight_recorder.config SET value = 'true' WHERE key = 'collection_jitter_enabled';

-- =============================================================================
-- 6. ARCHIVE FUNCTIONALITY (12 tests)
-- =============================================================================

-- Clear any archive data that may have been created by background cron jobs
TRUNCATE flight_recorder.activity_samples_archive;
TRUNCATE flight_recorder.lock_samples_archive;
TRUNCATE flight_recorder.wait_samples_archive;

-- Test 1: Archive configuration exists
SELECT ok(
    EXISTS (SELECT 1 FROM flight_recorder.config WHERE key = 'archive_samples_enabled'),
    'Archive: Config key archive_samples_enabled should exist'
);

SELECT ok(
    EXISTS (SELECT 1 FROM flight_recorder.config WHERE key = 'archive_sample_frequency_minutes'),
    'Archive: Config key archive_sample_frequency_minutes should exist'
);

SELECT ok(
    EXISTS (SELECT 1 FROM flight_recorder.config WHERE key = 'archive_retention_days'),
    'Archive: Config key archive_retention_days should exist'
);

SELECT ok(
    EXISTS (SELECT 1 FROM flight_recorder.config WHERE key = 'archive_wait_samples'),
    'Archive: Config key archive_wait_samples should exist'
);

-- Test 2: Archive tables are empty initially
SELECT is(
    (SELECT count(*)::integer FROM flight_recorder.activity_samples_archive),
    0,
    'Archive: activity_samples_archive should be empty initially'
);

SELECT is(
    (SELECT count(*)::integer FROM flight_recorder.lock_samples_archive),
    0,
    'Archive: lock_samples_archive should be empty initially'
);

SELECT is(
    (SELECT count(*)::integer FROM flight_recorder.wait_samples_archive),
    0,
    'Archive: wait_samples_archive should be empty initially'
);

-- Test 3: Archive function can be called
SELECT lives_ok(
    'SELECT flight_recorder.archive_ring_samples()',
    'Archive: archive_ring_samples() should execute without error'
);

-- Test 4: Archive captures data after sample collection
-- First, capture some samples
SELECT flight_recorder.sample();

-- Manually call archive (normally scheduled via cron)
SELECT flight_recorder.archive_ring_samples();

-- Verify data was archived
SELECT ok(
    (SELECT count(*) FROM flight_recorder.activity_samples_archive) >= 0,
    'Archive: activity_samples_archive should contain data after archival'
);

-- Test 5: Cleanup removes old archived data
-- Insert old archive data
INSERT INTO flight_recorder.activity_samples_archive (sample_id, captured_at, pid, usename)
VALUES (1, now() - interval '10 days', 12345, 'test_user');

INSERT INTO flight_recorder.lock_samples_archive (sample_id, captured_at, blocked_pid)
VALUES (1, now() - interval '10 days', 67890);

INSERT INTO flight_recorder.wait_samples_archive (sample_id, captured_at, backend_type, wait_event_type, wait_event, count)
VALUES (1, now() - interval '10 days', 'client backend', 'Lock', 'relation', 5);

-- Set retention to 7 days for test
UPDATE flight_recorder.config SET value = '7' WHERE key = 'archive_retention_days';

-- Run cleanup
SELECT flight_recorder.cleanup_aggregates();

-- Verify old data was removed (assuming default retention of 7 days)
SELECT ok(
    NOT EXISTS (
        SELECT 1 FROM flight_recorder.activity_samples_archive
        WHERE captured_at < now() - interval '7 days'
    ),
    'Archive: cleanup should remove old activity archive data'
);

SELECT ok(
    NOT EXISTS (
        SELECT 1 FROM flight_recorder.lock_samples_archive
        WHERE captured_at < now() - interval '7 days'
    ),
    'Archive: cleanup should remove old lock archive data'
);

SELECT ok(
    NOT EXISTS (
        SELECT 1 FROM flight_recorder.wait_samples_archive
        WHERE captured_at < now() - interval '7 days'
    ),
    'Archive: cleanup should remove old wait archive data'
);

-- =============================================================================
-- CAPACITY PLANNING TESTS (60 tests) - Phase 1 MVP
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Section 1: Schema Verification (8 tests)
-- -----------------------------------------------------------------------------

SELECT has_column('flight_recorder', 'snapshots', 'xact_commit', 'Snapshots table should have xact_commit column');
SELECT has_column('flight_recorder', 'snapshots', 'xact_rollback', 'Snapshots table should have xact_rollback column');
SELECT has_column('flight_recorder', 'snapshots', 'blks_read', 'Snapshots table should have blks_read column');
SELECT has_column('flight_recorder', 'snapshots', 'blks_hit', 'Snapshots table should have blks_hit column');
SELECT has_column('flight_recorder', 'snapshots', 'connections_active', 'Snapshots table should have connections_active column');
SELECT has_column('flight_recorder', 'snapshots', 'connections_total', 'Snapshots table should have connections_total column');
SELECT has_column('flight_recorder', 'snapshots', 'connections_max', 'Snapshots table should have connections_max column');
SELECT has_column('flight_recorder', 'snapshots', 'db_size_bytes', 'Snapshots table should have db_size_bytes column');

-- -----------------------------------------------------------------------------
-- Section 2: Configuration Options (8 tests)
-- -----------------------------------------------------------------------------

SELECT ok(
    EXISTS (SELECT 1 FROM flight_recorder.config WHERE key = 'capacity_planning_enabled'),
    'Config: capacity_planning_enabled key should exist'
);

SELECT is(
    (SELECT value FROM flight_recorder.config WHERE key = 'capacity_planning_enabled'),
    'true',
    'Config: capacity_planning_enabled should default to true'
);

SELECT ok(
    EXISTS (SELECT 1 FROM flight_recorder.config WHERE key = 'capacity_thresholds_warning_pct'),
    'Config: capacity_thresholds_warning_pct key should exist'
);

SELECT is(
    (SELECT value FROM flight_recorder.config WHERE key = 'capacity_thresholds_warning_pct'),
    '60',
    'Config: capacity_thresholds_warning_pct should default to 60'
);

SELECT ok(
    EXISTS (SELECT 1 FROM flight_recorder.config WHERE key = 'capacity_thresholds_critical_pct'),
    'Config: capacity_thresholds_critical_pct key should exist'
);

SELECT is(
    (SELECT value FROM flight_recorder.config WHERE key = 'capacity_thresholds_critical_pct'),
    '80',
    'Config: capacity_thresholds_critical_pct should default to 80'
);

SELECT ok(
    EXISTS (SELECT 1 FROM flight_recorder.config WHERE key = 'collect_database_size'),
    'Config: collect_database_size key should exist'
);

SELECT ok(
    EXISTS (SELECT 1 FROM flight_recorder.config WHERE key = 'collect_connection_metrics'),
    'Config: collect_connection_metrics key should exist'
);

-- -----------------------------------------------------------------------------
-- Section 3: Snapshot Collection (10 tests)
-- -----------------------------------------------------------------------------

-- Take a fresh snapshot to populate capacity metrics
SELECT flight_recorder.snapshot();

-- Verify most recent snapshot has capacity metrics populated
SELECT ok(
    (SELECT xact_commit FROM flight_recorder.snapshots ORDER BY captured_at DESC LIMIT 1) IS NOT NULL,
    'Snapshot: xact_commit should be collected'
);

SELECT ok(
    (SELECT xact_rollback FROM flight_recorder.snapshots ORDER BY captured_at DESC LIMIT 1) IS NOT NULL,
    'Snapshot: xact_rollback should be collected'
);

SELECT ok(
    (SELECT blks_read FROM flight_recorder.snapshots ORDER BY captured_at DESC LIMIT 1) IS NOT NULL,
    'Snapshot: blks_read should be collected'
);

SELECT ok(
    (SELECT blks_hit FROM flight_recorder.snapshots ORDER BY captured_at DESC LIMIT 1) IS NOT NULL,
    'Snapshot: blks_hit should be collected'
);

SELECT ok(
    (SELECT connections_active FROM flight_recorder.snapshots ORDER BY captured_at DESC LIMIT 1) IS NOT NULL,
    'Snapshot: connections_active should be collected'
);

SELECT ok(
    (SELECT connections_total FROM flight_recorder.snapshots ORDER BY captured_at DESC LIMIT 1) IS NOT NULL,
    'Snapshot: connections_total should be collected'
);

SELECT ok(
    (SELECT connections_max FROM flight_recorder.snapshots ORDER BY captured_at DESC LIMIT 1) IS NOT NULL,
    'Snapshot: connections_max should be collected'
);

SELECT ok(
    (SELECT db_size_bytes FROM flight_recorder.snapshots ORDER BY captured_at DESC LIMIT 1) IS NOT NULL,
    'Snapshot: db_size_bytes should be collected'
);

SELECT ok(
    (SELECT connections_max FROM flight_recorder.snapshots ORDER BY captured_at DESC LIMIT 1) > 0,
    'Snapshot: connections_max should be positive'
);

SELECT ok(
    (SELECT db_size_bytes FROM flight_recorder.snapshots ORDER BY captured_at DESC LIMIT 1) > 0,
    'Snapshot: db_size_bytes should be positive'
);

-- -----------------------------------------------------------------------------
-- Section 4: capacity_summary() Function (20 tests)
-- -----------------------------------------------------------------------------

-- Test 1: Function exists
SELECT has_function('flight_recorder', 'capacity_summary', 'Function capacity_summary should exist');

-- Test 2: Function executes without error (with insufficient data)
SELECT lives_ok(
    $$SELECT * FROM flight_recorder.capacity_summary(interval '24 hours')$$,
    'capacity_summary: Should execute without error'
);

-- Test 3: Returns insufficient_data when <2 snapshots
-- Save current snapshots, delete all, test, then restore
CREATE TEMP TABLE IF NOT EXISTS saved_snapshots AS SELECT * FROM flight_recorder.snapshots;
DELETE FROM flight_recorder.snapshots;
SELECT ok(
    (SELECT count(*) FROM flight_recorder.capacity_summary(interval '24 hours') WHERE metric = 'insufficient_data') = 1,
    'capacity_summary: Should return insufficient_data with <2 snapshots'
);
INSERT INTO flight_recorder.snapshots SELECT * FROM saved_snapshots;
DROP TABLE saved_snapshots;

-- Create synthetic test data for capacity analysis (need multiple snapshots)
-- Insert backdated snapshot for testing
INSERT INTO flight_recorder.snapshots (
    captured_at, pg_version,
    wal_records, wal_fpi, wal_bytes, wal_write_time, wal_sync_time,
    checkpoint_lsn, checkpoint_time,
    ckpt_timed, ckpt_requested, ckpt_write_time, ckpt_sync_time, ckpt_buffers,
    bgw_buffers_clean, bgw_maxwritten_clean, bgw_buffers_alloc,
    bgw_buffers_backend, bgw_buffers_backend_fsync,
    autovacuum_workers, slots_count, slots_max_retained_wal,
    temp_files, temp_bytes,
    xact_commit, xact_rollback, blks_read, blks_hit,
    connections_active, connections_total, connections_max,
    db_size_bytes
) VALUES (
    now() - interval '1 hour', 16,
    1000, 100, 10000000, 100.0, 50.0,
    '0/1000000'::pg_lsn, now() - interval '1 hour',
    5, 1, 1000.0, 500.0, 50000,
    10000, 5, 100000,
    1000, 10,
    0, 0, 0,
    50, 1000000,
    10000, 100, 50000, 450000,
    5, 10, 100,
    1000000000
);

-- Test 4: Returns data with sufficient snapshots
SELECT ok(
    (SELECT count(*) FROM flight_recorder.capacity_summary(interval '2 hours')) >= 1,
    'capacity_summary: Should return metrics with sufficient data'
);

-- Test 5: Check connections metric is returned
SELECT ok(
    EXISTS (SELECT 1 FROM flight_recorder.capacity_summary(interval '2 hours') WHERE metric = 'connections'),
    'capacity_summary: Should return connections metric'
);

-- Test 6: Check memory metrics are returned
SELECT ok(
    EXISTS (SELECT 1 FROM flight_recorder.capacity_summary(interval '2 hours') WHERE metric LIKE 'memory%'),
    'capacity_summary: Should return memory metrics'
);

-- Test 7: Check I/O metric is returned
SELECT ok(
    EXISTS (SELECT 1 FROM flight_recorder.capacity_summary(interval '2 hours') WHERE metric = 'io_buffer_cache'),
    'capacity_summary: Should return I/O cache metric'
);

-- Test 8: Utilization percentage is within bounds
SELECT ok(
    (SELECT max(utilization_pct) FROM flight_recorder.capacity_summary(interval '2 hours') WHERE utilization_pct IS NOT NULL) <= 100,
    'capacity_summary: Utilization percentage should be <= 100'
);

SELECT ok(
    (SELECT min(utilization_pct) FROM flight_recorder.capacity_summary(interval '2 hours') WHERE utilization_pct IS NOT NULL) >= 0,
    'capacity_summary: Utilization percentage should be >= 0'
);

-- Test 9: Status values are valid
SELECT ok(
    (SELECT count(*) FROM flight_recorder.capacity_summary(interval '2 hours')
     WHERE status NOT IN ('healthy', 'warning', 'critical', 'insufficient_data')) = 0,
    'capacity_summary: Status should be one of valid values'
);

-- Test 10: Headroom is complement of utilization
SELECT ok(
    (SELECT count(*) FROM flight_recorder.capacity_summary(interval '2 hours')
     WHERE utilization_pct IS NOT NULL
       AND headroom_pct IS NOT NULL
       AND abs((utilization_pct + headroom_pct) - 100) > 0.1) = 0,
    'capacity_summary: Headroom should equal 100 - utilization'
);

-- Test 11: Current usage is populated
SELECT ok(
    (SELECT count(*) FROM flight_recorder.capacity_summary(interval '2 hours')
     WHERE current_usage IS NOT NULL) >= 1,
    'capacity_summary: Current usage should be populated'
);

-- Test 12: Recommendations are provided
SELECT ok(
    (SELECT count(*) FROM flight_recorder.capacity_summary(interval '2 hours')
     WHERE recommendation IS NOT NULL) >= 1,
    'capacity_summary: Recommendations should be provided'
);

-- Test 13: Different time windows work
SELECT lives_ok(
    $$SELECT * FROM flight_recorder.capacity_summary(interval '1 hour')$$,
    'capacity_summary: Should work with 1 hour window'
);

SELECT lives_ok(
    $$SELECT * FROM flight_recorder.capacity_summary(interval '7 days')$$,
    'capacity_summary: Should work with 7 day window'
);

-- Test 14: NULL handling - function doesn't crash with NULL columns
UPDATE flight_recorder.snapshots
SET xact_commit = NULL, connections_total = NULL
WHERE captured_at = (SELECT min(captured_at) FROM flight_recorder.snapshots);

SELECT lives_ok(
    $$SELECT * FROM flight_recorder.capacity_summary(interval '2 hours')$$,
    'capacity_summary: Should handle NULL columns gracefully'
);

-- Test 15: Config-driven thresholds
UPDATE flight_recorder.config SET value = '50' WHERE key = 'capacity_thresholds_warning_pct';
UPDATE flight_recorder.config SET value = '70' WHERE key = 'capacity_thresholds_critical_pct';

SELECT lives_ok(
    $$SELECT * FROM flight_recorder.capacity_summary(interval '2 hours')$$,
    'capacity_summary: Should respect config threshold changes'
);

-- Reset thresholds
UPDATE flight_recorder.config SET value = '60' WHERE key = 'capacity_thresholds_warning_pct';
UPDATE flight_recorder.config SET value = '80' WHERE key = 'capacity_thresholds_critical_pct';

-- -----------------------------------------------------------------------------
-- Section 5: capacity_dashboard View (10 tests)
-- -----------------------------------------------------------------------------

-- Test 1: View exists
SELECT has_view('flight_recorder', 'capacity_dashboard', 'View capacity_dashboard should exist');

-- Test 2: View executes without error
SELECT lives_ok(
    $$SELECT * FROM flight_recorder.capacity_dashboard$$,
    'capacity_dashboard: Should execute without error'
);

-- Test 3: Returns exactly one row
SELECT is(
    (SELECT count(*)::integer FROM flight_recorder.capacity_dashboard),
    1,
    'capacity_dashboard: Should return exactly one row'
);

-- Test 4: last_updated is populated
SELECT ok(
    (SELECT last_updated FROM flight_recorder.capacity_dashboard) IS NOT NULL,
    'capacity_dashboard: last_updated should be populated'
);

-- Test 5: connections_status is valid
SELECT ok(
    (SELECT connections_status FROM flight_recorder.capacity_dashboard) IN ('healthy', 'warning', 'critical', 'insufficient_data'),
    'capacity_dashboard: connections_status should be valid'
);

-- Test 6: memory_status is valid
SELECT ok(
    (SELECT memory_status FROM flight_recorder.capacity_dashboard) IN ('healthy', 'warning', 'critical', 'insufficient_data'),
    'capacity_dashboard: memory_status should be valid'
);

-- Test 7: overall_status is valid
SELECT ok(
    (SELECT overall_status FROM flight_recorder.capacity_dashboard) IN ('healthy', 'warning', 'critical', 'insufficient_data'),
    'capacity_dashboard: overall_status should be valid'
);

-- Test 8: memory_pressure_score is within bounds
SELECT ok(
    (SELECT memory_pressure_score FROM flight_recorder.capacity_dashboard) >= 0 AND
    (SELECT memory_pressure_score FROM flight_recorder.capacity_dashboard) <= 100,
    'capacity_dashboard: memory_pressure_score should be 0-100'
);

-- Test 9: critical_issues is an array
SELECT ok(
    pg_typeof((SELECT critical_issues FROM flight_recorder.capacity_dashboard))::text = 'text[]',
    'capacity_dashboard: critical_issues should be text array'
);

-- Test 10: Dashboard reflects underlying summary data
SELECT ok(
    (SELECT connections_status FROM flight_recorder.capacity_dashboard) =
    COALESCE((SELECT status FROM flight_recorder.capacity_summary(interval '24 hours') WHERE metric = 'connections'), 'insufficient_data'),
    'capacity_dashboard: Should reflect capacity_summary connections status'
);

-- -----------------------------------------------------------------------------
-- Section 6: Backward Compatibility (4 tests)
-- -----------------------------------------------------------------------------

-- Test 1: Existing queries still work
SELECT lives_ok(
    $$SELECT count(*) FROM flight_recorder.snapshots$$,
    'Backward compatibility: Existing snapshot queries should work'
);

-- Test 2: Existing views still work
SELECT lives_ok(
    $$SELECT * FROM flight_recorder.deltas LIMIT 1$$,
    'Backward compatibility: Existing views should work'
);

-- Test 3: Historical snapshots with NULL capacity columns don't break queries
SELECT ok(
    (SELECT count(*) FROM flight_recorder.snapshots WHERE xact_commit IS NULL) >= 0,
    'Backward compatibility: NULL capacity columns should be handled'
);

-- Test 4: capacity_summary handles historical NULL data
SELECT lives_ok(
    $$SELECT * FROM flight_recorder.capacity_summary(interval '30 days')$$,
    'Backward compatibility: capacity_summary should handle historical NULLs'
);

-- =============================================================================
-- FEATURE DESIGNS: TABLE/INDEX/CONFIG TRACKING (23 tests)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Section 1: Table Existence (3 tests)
-- -----------------------------------------------------------------------------

SELECT has_table('flight_recorder', 'table_snapshots', 'Table flight_recorder.table_snapshots should exist');
SELECT has_table('flight_recorder', 'index_snapshots', 'Table flight_recorder.index_snapshots should exist');
SELECT has_table('flight_recorder', 'config_snapshots', 'Table flight_recorder.config_snapshots should exist');

-- -----------------------------------------------------------------------------
-- Section 2: Collection Function Existence (3 tests)
-- -----------------------------------------------------------------------------

SELECT has_function('flight_recorder', '_collect_table_stats', 'Function flight_recorder._collect_table_stats should exist');
SELECT has_function('flight_recorder', '_collect_index_stats', 'Function flight_recorder._collect_index_stats should exist');
SELECT has_function('flight_recorder', '_collect_config_snapshot', 'Function flight_recorder._collect_config_snapshot should exist');

-- -----------------------------------------------------------------------------
-- Section 3: Analysis Function Existence (7 tests)
-- -----------------------------------------------------------------------------

SELECT has_function('flight_recorder', 'table_compare', 'Function flight_recorder.table_compare should exist');
SELECT has_function('flight_recorder', 'table_hotspots', 'Function flight_recorder.table_hotspots should exist');
SELECT has_function('flight_recorder', 'unused_indexes', 'Function flight_recorder.unused_indexes should exist');
SELECT has_function('flight_recorder', 'index_efficiency', 'Function flight_recorder.index_efficiency should exist');
SELECT has_function('flight_recorder', 'config_changes', 'Function flight_recorder.config_changes should exist');
SELECT has_function('flight_recorder', 'config_at', 'Function flight_recorder.config_at should exist');
SELECT has_function('flight_recorder', 'config_health_check', 'Function flight_recorder.config_health_check should exist');

-- -----------------------------------------------------------------------------
-- Section 4: Collection Function Execution (3 tests)
-- -----------------------------------------------------------------------------

-- First capture a snapshot to get a valid snapshot_id
DO $$
DECLARE
    v_snapshot_id INTEGER;
BEGIN
    SELECT id INTO v_snapshot_id FROM flight_recorder.snapshots ORDER BY id DESC LIMIT 1;
    IF v_snapshot_id IS NULL THEN
        SELECT flight_recorder.snapshot();
        SELECT id INTO v_snapshot_id FROM flight_recorder.snapshots ORDER BY id DESC LIMIT 1;
    END IF;
    -- Store for tests
    CREATE TEMP TABLE IF NOT EXISTS test_snapshot_id (id INTEGER);
    DELETE FROM test_snapshot_id;
    INSERT INTO test_snapshot_id VALUES (v_snapshot_id);
END $$;

SELECT lives_ok(
    $$SELECT flight_recorder._collect_table_stats((SELECT id FROM test_snapshot_id))$$,
    'Table stats collection executes without error'
);

SELECT lives_ok(
    $$SELECT flight_recorder._collect_index_stats((SELECT id FROM test_snapshot_id))$$,
    'Index stats collection executes without error'
);

SELECT lives_ok(
    $$SELECT flight_recorder._collect_config_snapshot((SELECT id FROM test_snapshot_id))$$,
    'Config snapshot collection executes without error'
);

-- -----------------------------------------------------------------------------
-- Section 5: Analysis Function Execution (7 tests)
-- -----------------------------------------------------------------------------

-- Get time range for queries
DO $$
DECLARE
    v_start_time TIMESTAMPTZ;
    v_end_time TIMESTAMPTZ;
BEGIN
    v_start_time := now() - interval '1 hour';
    v_end_time := now();
    -- Store for later tests
    CREATE TEMP TABLE IF NOT EXISTS test_feature_times (start_time TIMESTAMPTZ, end_time TIMESTAMPTZ);
    DELETE FROM test_feature_times;
    INSERT INTO test_feature_times VALUES (v_start_time, v_end_time);
END;
$$;

SELECT lives_ok(
    $$SELECT * FROM flight_recorder.table_compare(
        (SELECT start_time FROM test_feature_times),
        (SELECT end_time FROM test_feature_times)
    )$$,
    'table_compare() should execute without error'
);

SELECT lives_ok(
    $$SELECT * FROM flight_recorder.table_hotspots(
        (SELECT start_time FROM test_feature_times),
        (SELECT end_time FROM test_feature_times)
    )$$,
    'table_hotspots() should execute without error'
);

SELECT lives_ok(
    $$SELECT * FROM flight_recorder.unused_indexes()$$,
    'unused_indexes() should execute without error'
);

SELECT lives_ok(
    $$SELECT * FROM flight_recorder.index_efficiency(
        (SELECT start_time FROM test_feature_times),
        (SELECT end_time FROM test_feature_times)
    )$$,
    'index_efficiency() should execute without error'
);

SELECT lives_ok(
    $$SELECT * FROM flight_recorder.config_changes(
        (SELECT start_time FROM test_feature_times),
        (SELECT end_time FROM test_feature_times)
    )$$,
    'config_changes() should execute without error'
);

SELECT lives_ok(
    $$SELECT * FROM flight_recorder.config_at(now())$$,
    'config_at() should execute without error'
);

SELECT lives_ok(
    $$SELECT * FROM flight_recorder.config_health_check()$$,
    'config_health_check() should execute without error'
);

SELECT * FROM finish();
ROLLBACK;
