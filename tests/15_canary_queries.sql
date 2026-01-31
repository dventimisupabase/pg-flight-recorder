-- =============================================================================
-- pg-flight-recorder pgTAP Tests - Canary Queries
-- =============================================================================
-- Tests: Canary query definitions, execution, status functions, and buffer metrics
-- Test count: 34
-- =============================================================================

BEGIN;
SELECT plan(34);

-- Disable checkpoint detection during tests to prevent snapshot skipping
UPDATE flight_recorder.config SET value = 'false' WHERE key = 'check_checkpoint_backup';

-- Disable collection jitter to speed up tests
UPDATE flight_recorder.config SET value = 'false' WHERE key = 'collection_jitter_enabled';

-- =============================================================================
-- 1. TABLE EXISTENCE (2 tests)
-- =============================================================================

SELECT has_table(
    'flight_recorder', 'canaries',
    'canaries table should exist'
);

SELECT has_table(
    'flight_recorder', 'canary_results',
    'canary_results table should exist'
);

-- =============================================================================
-- 2. COLUMN EXISTENCE - canaries table (5 tests)
-- =============================================================================

SELECT has_column(
    'flight_recorder', 'canaries', 'id',
    'canaries table should have id column'
);

SELECT has_column(
    'flight_recorder', 'canaries', 'name',
    'canaries table should have name column'
);

SELECT has_column(
    'flight_recorder', 'canaries', 'query_text',
    'canaries table should have query_text column'
);

SELECT has_column(
    'flight_recorder', 'canaries', 'threshold_warning',
    'canaries table should have threshold_warning column'
);

SELECT has_column(
    'flight_recorder', 'canaries', 'threshold_critical',
    'canaries table should have threshold_critical column'
);

-- =============================================================================
-- 3. COLUMN EXISTENCE - canary_results table (9 tests)
-- =============================================================================

SELECT has_column(
    'flight_recorder', 'canary_results', 'canary_id',
    'canary_results table should have canary_id column'
);

SELECT has_column(
    'flight_recorder', 'canary_results', 'duration_ms',
    'canary_results table should have duration_ms column'
);

SELECT has_column(
    'flight_recorder', 'canary_results', 'plan',
    'canary_results table should have plan column'
);

SELECT has_column(
    'flight_recorder', 'canary_results', 'success',
    'canary_results table should have success column'
);

SELECT has_column(
    'flight_recorder', 'canary_results', 'shared_blks_hit',
    'canary_results table should have shared_blks_hit column'
);

SELECT has_column(
    'flight_recorder', 'canary_results', 'shared_blks_read',
    'canary_results table should have shared_blks_read column'
);

SELECT has_column(
    'flight_recorder', 'canary_results', 'temp_blks_read',
    'canary_results table should have temp_blks_read column'
);

SELECT has_column(
    'flight_recorder', 'canary_results', 'temp_blks_written',
    'canary_results table should have temp_blks_written column'
);

SELECT has_column(
    'flight_recorder', 'canary_results', 'total_buffers',
    'canary_results table should have total_buffers column'
);

-- =============================================================================
-- 4. CONFIG SETTINGS (5 tests)
-- =============================================================================

SELECT ok(
    EXISTS (SELECT 1 FROM flight_recorder.config WHERE key = 'canary_enabled'),
    'canary_enabled config setting should exist'
);

SELECT ok(
    EXISTS (SELECT 1 FROM flight_recorder.config WHERE key = 'canary_interval_minutes'),
    'canary_interval_minutes config setting should exist'
);

SELECT ok(
    EXISTS (SELECT 1 FROM flight_recorder.config WHERE key = 'canary_capture_plans'),
    'canary_capture_plans config setting should exist'
);

SELECT ok(
    EXISTS (SELECT 1 FROM flight_recorder.config WHERE key = 'retention_canary_days'),
    'retention_canary_days config setting should exist'
);

SELECT ok(
    EXISTS (SELECT 1 FROM flight_recorder.config WHERE key = 'canary_comparison_metric'),
    'canary_comparison_metric config setting should exist'
);

-- =============================================================================
-- 5. PRE-DEFINED CANARIES (2 tests)
-- =============================================================================

SELECT ok(
    (SELECT count(*) FROM flight_recorder.canaries) >= 4,
    'At least 4 pre-defined canaries should exist'
);

SELECT ok(
    EXISTS (SELECT 1 FROM flight_recorder.canaries WHERE name = 'index_lookup'),
    'index_lookup canary should be pre-defined'
);

-- =============================================================================
-- 6. FUNCTION EXISTENCE (4 tests)
-- =============================================================================

SELECT has_function(
    'flight_recorder', 'run_canaries', ARRAY[]::TEXT[],
    'run_canaries() function should exist'
);

SELECT has_function(
    'flight_recorder', 'canary_status', ARRAY[]::TEXT[],
    'canary_status() function should exist'
);

SELECT has_function(
    'flight_recorder', 'enable_canaries', ARRAY[]::TEXT[],
    'enable_canaries() function should exist'
);

SELECT has_function(
    'flight_recorder', 'disable_canaries', ARRAY[]::TEXT[],
    'disable_canaries() function should exist'
);

-- =============================================================================
-- 7. CANARY EXECUTION (2 tests)
-- =============================================================================

-- Enable canaries for testing
UPDATE flight_recorder.config SET value = 'true' WHERE key = 'canary_enabled';

-- Run canaries and verify they execute
SELECT lives_ok(
    $$SELECT * FROM flight_recorder.run_canaries()$$,
    'run_canaries() should execute without error'
);

-- Verify results were recorded
SELECT ok(
    (SELECT count(*) FROM flight_recorder.canary_results) >= 4,
    'At least 4 canary results should be recorded after run_canaries()'
);

-- =============================================================================
-- 8. CANARY STATUS (2 tests)
-- =============================================================================

SELECT lives_ok(
    $$SELECT * FROM flight_recorder.canary_status()$$,
    'canary_status() should execute without error'
);

-- Status should return rows for enabled canaries
SELECT ok(
    (SELECT count(*) FROM flight_recorder.canary_status()) >= 4,
    'canary_status() should return rows for all enabled canaries'
);

-- =============================================================================
-- 9. BUFFER METRICS IN RESULTS (3 tests)
-- =============================================================================

-- Verify buffer columns are populated after run_canaries()
SELECT ok(
    (SELECT count(*) FROM flight_recorder.canary_results WHERE total_buffers IS NOT NULL) >= 4,
    'run_canaries() should populate total_buffers column'
);

-- Verify canary_status returns buffer columns
SELECT ok(
    (SELECT baseline_buffers IS NULL OR baseline_buffers >= 0
     FROM flight_recorder.canary_status() LIMIT 1),
    'canary_status() should return baseline_buffers column'
);

SELECT ok(
    (SELECT metric_used IN ('buffers', 'time')
     FROM flight_recorder.canary_status() LIMIT 1),
    'canary_status() should return metric_used column with valid value'
);

-- Disable canaries after testing
UPDATE flight_recorder.config SET value = 'false' WHERE key = 'canary_enabled';

SELECT * FROM finish();
ROLLBACK;
