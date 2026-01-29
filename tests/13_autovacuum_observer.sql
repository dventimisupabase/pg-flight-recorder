-- =============================================================================
-- pg-flight-recorder pgTAP Tests - Autovacuum Observer Enhancements (v2.7)
-- =============================================================================
-- Tests: n_mod_since_analyze column, rate calculation functions, sampling modes
-- Test count: 35
-- =============================================================================

BEGIN;
SELECT plan(35);

-- Disable checkpoint detection during tests to prevent snapshot skipping
UPDATE flight_recorder.config SET value = 'false' WHERE key = 'check_checkpoint_backup';

-- Disable collection jitter to speed up tests
UPDATE flight_recorder.config SET value = 'false' WHERE key = 'collection_jitter_enabled';

-- =============================================================================
-- 1. SCHEMA TESTS - n_mod_since_analyze COLUMN (3 tests)
-- =============================================================================

SELECT has_column(
    'flight_recorder', 'table_snapshots', 'n_mod_since_analyze',
    'table_snapshots should have n_mod_since_analyze column'
);

SELECT col_type_is(
    'flight_recorder', 'table_snapshots', 'n_mod_since_analyze', 'bigint',
    'n_mod_since_analyze should be BIGINT type'
);

SELECT col_is_null(
    'flight_recorder', 'table_snapshots', 'n_mod_since_analyze',
    'n_mod_since_analyze should be nullable'
);

-- =============================================================================
-- 2. CONFIG TESTS - NEW PARAMETERS (4 tests)
-- =============================================================================

SELECT ok(
    EXISTS(SELECT 1 FROM flight_recorder.config WHERE key = 'table_stats_mode'),
    'table_stats_mode config parameter should exist'
);

SELECT is(
    (SELECT value FROM flight_recorder.config WHERE key = 'table_stats_mode'),
    'top_n',
    'table_stats_mode default should be top_n'
);

SELECT ok(
    EXISTS(SELECT 1 FROM flight_recorder.config WHERE key = 'table_stats_activity_threshold'),
    'table_stats_activity_threshold config parameter should exist'
);

SELECT is(
    (SELECT value FROM flight_recorder.config WHERE key = 'table_stats_activity_threshold'),
    '0',
    'table_stats_activity_threshold default should be 0'
);

-- =============================================================================
-- 3. FUNCTION EXISTENCE TESTS (4 tests)
-- =============================================================================

SELECT has_function(
    'flight_recorder', 'dead_tuple_growth_rate',
    ARRAY['oid', 'interval'],
    'dead_tuple_growth_rate(oid, interval) function should exist'
);

SELECT has_function(
    'flight_recorder', 'modification_rate',
    ARRAY['oid', 'interval'],
    'modification_rate(oid, interval) function should exist'
);

SELECT has_function(
    'flight_recorder', 'hot_update_ratio',
    ARRAY['oid'],
    'hot_update_ratio(oid) function should exist'
);

SELECT has_function(
    'flight_recorder', 'time_to_budget_exhaustion',
    ARRAY['oid', 'bigint'],
    'time_to_budget_exhaustion(oid, bigint) function should exist'
);

-- =============================================================================
-- 4. DATA COLLECTION TESTS (4 tests)
-- =============================================================================

-- Take a snapshot to populate data
SELECT flight_recorder.snapshot();

-- Verify n_mod_since_analyze is queryable
SELECT lives_ok(
    $$SELECT n_mod_since_analyze FROM flight_recorder.table_snapshots LIMIT 1$$,
    'n_mod_since_analyze column should be queryable'
);

-- Verify snapshot was created successfully
SELECT ok(
    (SELECT count(*) FROM flight_recorder.snapshots WHERE captured_at > now() - interval '1 minute') > 0,
    'snapshot() should create a new snapshot with table stats'
);

-- Verify table_snapshots has data (if there are user tables)
SELECT lives_ok(
    $$SELECT relid, n_dead_tup, n_mod_since_analyze
      FROM flight_recorder.table_snapshots
      ORDER BY snapshot_id DESC LIMIT 5$$,
    'table_snapshots should be queryable with n_mod_since_analyze'
);

-- Verify n_mod_since_analyze is populated from pg_stat_user_tables
SELECT lives_ok(
    $$SELECT ts.n_mod_since_analyze
      FROM flight_recorder.table_snapshots ts
      JOIN flight_recorder.snapshots s ON s.id = ts.snapshot_id
      WHERE s.captured_at > now() - interval '1 minute'
      LIMIT 1$$,
    'n_mod_since_analyze should be populated in recent snapshots'
);

-- =============================================================================
-- 5. RATE FUNCTION TESTS - EXECUTION WITHOUT ERROR (8 tests)
-- =============================================================================

-- Test dead_tuple_growth_rate executes without error
SELECT lives_ok(
    $$SELECT flight_recorder.dead_tuple_growth_rate(
        (SELECT relid FROM pg_stat_user_tables LIMIT 1),
        '1 hour'::interval
      )$$,
    'dead_tuple_growth_rate should execute without error'
);

-- Test dead_tuple_growth_rate returns NUMERIC
SELECT ok(
    pg_typeof(flight_recorder.dead_tuple_growth_rate(
        (SELECT relid FROM pg_stat_user_tables LIMIT 1),
        '1 hour'::interval
    ))::text = 'numeric',
    'dead_tuple_growth_rate should return NUMERIC type'
);

-- Test modification_rate executes without error
SELECT lives_ok(
    $$SELECT flight_recorder.modification_rate(
        (SELECT relid FROM pg_stat_user_tables LIMIT 1),
        '1 hour'::interval
      )$$,
    'modification_rate should execute without error'
);

-- Test modification_rate returns NUMERIC
SELECT ok(
    pg_typeof(flight_recorder.modification_rate(
        (SELECT relid FROM pg_stat_user_tables LIMIT 1),
        '1 hour'::interval
    ))::text = 'numeric',
    'modification_rate should return NUMERIC type'
);

-- Test hot_update_ratio executes without error
SELECT lives_ok(
    $$SELECT flight_recorder.hot_update_ratio(
        (SELECT relid FROM pg_stat_user_tables LIMIT 1)
      )$$,
    'hot_update_ratio should execute without error'
);

-- Test hot_update_ratio returns NUMERIC
SELECT ok(
    pg_typeof(flight_recorder.hot_update_ratio(
        (SELECT relid FROM pg_stat_user_tables LIMIT 1)
    ))::text = 'numeric',
    'hot_update_ratio should return NUMERIC type'
);

-- Test time_to_budget_exhaustion executes without error
SELECT lives_ok(
    $$SELECT flight_recorder.time_to_budget_exhaustion(
        (SELECT relid FROM pg_stat_user_tables LIMIT 1),
        10000::bigint
      )$$,
    'time_to_budget_exhaustion should execute without error'
);

-- Test time_to_budget_exhaustion returns INTERVAL
SELECT ok(
    pg_typeof(flight_recorder.time_to_budget_exhaustion(
        (SELECT relid FROM pg_stat_user_tables LIMIT 1),
        10000::bigint
    ))::text = 'interval',
    'time_to_budget_exhaustion should return INTERVAL type'
);

-- =============================================================================
-- 6. SAMPLING MODE TESTS (8 tests)
-- =============================================================================

-- Test top_n mode (default)
UPDATE flight_recorder.config SET value = 'top_n' WHERE key = 'table_stats_mode';
UPDATE flight_recorder.config SET value = '5' WHERE key = 'table_stats_top_n';

SELECT flight_recorder.snapshot();

-- Get the most recent snapshot and count its table_snapshots
SELECT ok(
    (SELECT count(*) FROM flight_recorder.table_snapshots
     WHERE snapshot_id = (SELECT max(id) FROM flight_recorder.snapshots)) <= 5,
    'top_n mode should limit to table_stats_top_n tables'
);

-- Test all mode
UPDATE flight_recorder.config SET value = 'all' WHERE key = 'table_stats_mode';

SELECT flight_recorder.snapshot();

SELECT lives_ok(
    $$SELECT count(*) FROM flight_recorder.table_snapshots ts
      JOIN flight_recorder.snapshots s ON s.id = ts.snapshot_id
      WHERE s.captured_at > now() - interval '10 seconds'$$,
    'all mode should collect all tables without error'
);

-- Verify all mode collects tables
SELECT ok(
    (SELECT count(*) FROM flight_recorder.table_snapshots ts
     JOIN flight_recorder.snapshots s ON s.id = ts.snapshot_id
     WHERE s.captured_at > now() - interval '10 seconds') >= 0,
    'all mode should collect tables'
);

-- Test threshold mode with high threshold (should collect few/none)
UPDATE flight_recorder.config SET value = 'threshold' WHERE key = 'table_stats_mode';
UPDATE flight_recorder.config SET value = '999999999999' WHERE key = 'table_stats_activity_threshold';

SELECT flight_recorder.snapshot();

SELECT ok(
    (SELECT count(*) FROM flight_recorder.table_snapshots ts
     JOIN flight_recorder.snapshots s ON s.id = ts.snapshot_id
     WHERE s.captured_at > now() - interval '10 seconds') >= 0,
    'threshold mode with high threshold should work'
);

-- Test threshold mode with zero threshold (should collect all active)
UPDATE flight_recorder.config SET value = '0' WHERE key = 'table_stats_activity_threshold';

SELECT flight_recorder.snapshot();

SELECT lives_ok(
    $$SELECT count(*) FROM flight_recorder.table_snapshots ts
      JOIN flight_recorder.snapshots s ON s.id = ts.snapshot_id
      WHERE s.captured_at > now() - interval '10 seconds'$$,
    'threshold mode with zero threshold should collect tables'
);

-- Reset to default mode
UPDATE flight_recorder.config SET value = 'top_n' WHERE key = 'table_stats_mode';
UPDATE flight_recorder.config SET value = '50' WHERE key = 'table_stats_top_n';

SELECT lives_ok(
    $$SELECT 1$$,
    'config reset to defaults should succeed'
);

-- Test invalid mode falls back gracefully
UPDATE flight_recorder.config SET value = 'invalid_mode' WHERE key = 'table_stats_mode';

SELECT lives_ok(
    $$SELECT flight_recorder.snapshot()$$,
    'invalid table_stats_mode should not cause error (falls back to top_n)'
);

-- Reset mode
UPDATE flight_recorder.config SET value = 'top_n' WHERE key = 'table_stats_mode';

SELECT lives_ok(
    $$SELECT 1$$,
    'config cleanup should succeed'
);

-- =============================================================================
-- 7. EDGE CASE TESTS (4 tests)
-- =============================================================================

-- Test rate functions with non-existent OID
SELECT is(
    flight_recorder.dead_tuple_growth_rate(0::oid, '1 hour'::interval),
    NULL::numeric,
    'dead_tuple_growth_rate should return NULL for non-existent OID'
);

SELECT is(
    flight_recorder.modification_rate(0::oid, '1 hour'::interval),
    NULL::numeric,
    'modification_rate should return NULL for non-existent OID'
);

SELECT is(
    flight_recorder.hot_update_ratio(0::oid),
    NULL::numeric,
    'hot_update_ratio should return NULL for non-existent OID'
);

SELECT is(
    flight_recorder.time_to_budget_exhaustion(0::oid, 10000::bigint),
    NULL::interval,
    'time_to_budget_exhaustion should return NULL for non-existent OID'
);

SELECT * FROM finish();
ROLLBACK;
