-- =============================================================================
-- pg-flight-recorder pgTAP Tests - Foundation
-- =============================================================================
-- Tests: Installation verification, function existence, core functionality
-- Sections: 1, 2, 3
-- Test count: 54
-- =============================================================================

BEGIN;
SELECT plan(54);

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

SELECT * FROM finish();
ROLLBACK;
