-- =============================================================================
-- pg-flight-recorder pgTAP Tests - Canary Queries
-- =============================================================================
-- Tests: Canary query definitions, execution, and status functions
-- Test count: 25
-- =============================================================================

BEGIN;
SELECT plan(25);

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
-- 3. COLUMN EXISTENCE - canary_results table (4 tests)
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

-- =============================================================================
-- 4. CONFIG SETTINGS (4 tests)
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

-- Disable canaries after testing
UPDATE flight_recorder.config SET value = 'false' WHERE key = 'canary_enabled';

SELECT * FROM finish();
ROLLBACK;
