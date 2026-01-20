-- =============================================================================
-- pg-flight-recorder pgTAP Tests - Safety Features
-- =============================================================================
-- Tests: Kill switch, P0-P4 safety features, configuration profiles
-- Sections: 8, 9, 10 (P1/P2), 11, 12, Configuration Profiles
-- Test count: 75
-- =============================================================================

BEGIN;
SELECT plan(75);

-- Disable checkpoint detection during tests to prevent snapshot skipping
UPDATE flight_recorder.config SET value = 'false' WHERE key = 'check_checkpoint_backup';

-- Disable collection jitter to speed up tests (default is 0-10 second random delay)
UPDATE flight_recorder.config SET value = 'false' WHERE key = 'collection_jitter_enabled';

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

-- Test P4: Export includes table_hotspots
SELECT ok(
    (SELECT flight_recorder.export_json(now() - interval '1 hour', now()) ? 'table_hotspots'),
    'P4: export_json() should include table_hotspots in result'
);

-- Test P4: Export includes index_efficiency
SELECT ok(
    (SELECT flight_recorder.export_json(now() - interval '1 hour', now()) ? 'index_efficiency'),
    'P4: export_json() should include index_efficiency in result'
);

-- Test P4: Export includes config_changes
SELECT ok(
    (SELECT flight_recorder.export_json(now() - interval '1 hour', now()) ? 'config_changes'),
    'P4: export_json() should include config_changes in result'
);

-- Test P4: Export includes db_role_config_changes
SELECT ok(
    (SELECT flight_recorder.export_json(now() - interval '1 hour', now()) ? 'db_role_config_changes'),
    'P4: export_json() should include db_role_config_changes in result'
);

-- Test P4: Export version matches schema_version from config
SELECT ok(
    (SELECT flight_recorder.export_json(now() - interval '1 hour', now())->'meta'->>'version' =
            (SELECT value FROM flight_recorder.config WHERE key = 'schema_version')),
    'P4: export_json() version should match schema_version from config'
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
-- 10. CONFIGURATION PROFILES (20 tests)
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

SELECT * FROM finish();
ROLLBACK;
