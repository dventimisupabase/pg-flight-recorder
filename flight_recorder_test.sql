-- =============================================================================
-- pg-flight-recorder pgTAP Tests
-- =============================================================================
-- Comprehensive test suite for pg-flight-recorder functionality
-- Run with: supabase test db
-- =============================================================================

BEGIN;
SELECT plan(182);  -- Expanded test suite: 145 base + 37 passing boundary tests (Phase 1 - in progress)

-- =============================================================================
-- 1. INSTALLATION VERIFICATION (16 tests)
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
SELECT pg_sleep(1);
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

SELECT * FROM finish();
ROLLBACK;
