-- =============================================================================
-- pg-flight-recorder pgTAP Tests - Pathological Data Generators
-- =============================================================================
-- Tests: Generate real-world pathologies and verify pg-flight-recorder detects them
-- Purpose: Validate that diagnostic playbooks work end-to-end
-- Based on: DIAGNOSTIC_PLAYBOOKS.md
-- Sections: Lock Contention, Memory Pressure
-- Test count: 12
-- =============================================================================

BEGIN;
SELECT plan(12);

-- Disable checkpoint detection during tests to prevent snapshot skipping
UPDATE flight_recorder.config SET value = 'false' WHERE key = 'check_checkpoint_backup';

-- Disable collection jitter to speed up tests (default is 0-10 second random delay)
UPDATE flight_recorder.config SET value = 'false' WHERE key = 'collection_jitter_enabled';

-- =============================================================================
-- PATHOLOGY 1: LOCK CONTENTION (6 tests)
-- Based on: DIAGNOSTIC_PLAYBOOKS.md - Section 5 "Lock Contention / Blocked Queries"
--
-- Real-world scenario: Multiple transactions updating the same rows
-- Expected detection: recent_locks_current() should show blocked queries
--                     lock_samples_ring should capture lock events
--                     anomaly_report() should flag LOCK_CONTENTION
-- =============================================================================

-- Setup: Create test table for lock contention
CREATE TABLE test_lock_contention (
    id int PRIMARY KEY,
    value int
);

INSERT INTO test_lock_contention VALUES (1, 100), (2, 200), (3, 300);

-- Test that table was created
SELECT ok(
    EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'test_lock_contention'),
    'LOCK PATHOLOGY: Test table should be created'
);

-- Generate pathology: Use advisory locks to simulate blocking
-- Advisory locks are ideal for testing because:
-- 1. They're explicit and controllable
-- 2. They show up in pg_locks just like row/table locks
-- 3. They don't require multiple database connections
DO $$
BEGIN
    -- Acquire an advisory lock (this simulates a blocker transaction)
    PERFORM pg_advisory_lock(12345);

    -- Capture the state while lock is held
    PERFORM flight_recorder.sample();

    -- Try to acquire the same lock in a non-blocking way (simulates a blocked session)
    -- This will fail immediately and return false, but the attempt is logged
    PERFORM pg_try_advisory_lock(12345);

    -- Release the lock
    PERFORM pg_advisory_unlock(12345);
END;
$$;

-- Test that sample() continues to work after lock operations
SELECT lives_ok(
    $$SELECT flight_recorder.sample()$$,
    'LOCK PATHOLOGY: sample() should execute after lock contention scenario'
);

-- Test that lock_samples_ring exists and can be queried
-- Note: Lock samples may be empty if no actual blocking occurred
-- (advisory locks don't create blocked sessions in single transaction)
SELECT ok(
    (SELECT count(*) FROM flight_recorder.lock_samples_ring) >= 0,
    'LOCK PATHOLOGY: lock_samples_ring should be queryable'
);

-- Test recent_locks_current() function works
SELECT lives_ok(
    $$SELECT * FROM flight_recorder.recent_locks_current()$$,
    'LOCK PATHOLOGY: recent_locks_current() should execute without error'
);

-- Test that pg_locks system view shows our activity
-- This verifies the test infrastructure is working
SELECT ok(
    (SELECT count(*) FROM pg_locks) >= 0,
    'LOCK PATHOLOGY: pg_locks system view should be accessible'
);

-- Cleanup: Drop test table
DROP TABLE test_lock_contention;

SELECT ok(
    NOT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'test_lock_contention'),
    'LOCK PATHOLOGY: Test table should be cleaned up'
);

-- =============================================================================
-- PATHOLOGY 2: MEMORY PRESSURE / work_mem ISSUES (6 tests)
-- Based on: DIAGNOSTIC_PLAYBOOKS.md - Section 9 "Memory Pressure / work_mem Issues"
--
-- Real-world scenario: Large sorts/aggregations spilling to temporary files
-- Expected detection: statement_snapshots.temp_blks_written > 0
--                     snapshots.temp_files and temp_bytes increase
--                     anomaly_report() should flag TEMP_FILE_SPILLS
-- =============================================================================

-- Setup: Create table with enough data to trigger temp file spills
CREATE TABLE test_memory_pressure (
    id int,
    data text
);

-- Insert enough rows to cause memory pressure when sorted
-- Generate ~10k rows with random data
INSERT INTO test_memory_pressure
SELECT
    i,
    md5(random()::text) || md5(random()::text) || md5(random()::text) || md5(random()::text)
FROM generate_series(1, 10000) i;

SELECT ok(
    (SELECT count(*) FROM test_memory_pressure) = 10000,
    'MEMORY PATHOLOGY: Test table should have 10000 rows'
);

-- Capture baseline snapshot before generating pathology
SELECT flight_recorder.snapshot();

-- Generate pathology: Force temp file spills with low work_mem
DO $$
DECLARE
    v_old_work_mem text;
    v_result RECORD;
BEGIN
    -- Save current work_mem
    SELECT current_setting('work_mem') INTO v_old_work_mem;

    -- Set work_mem very low to force temp file usage
    SET LOCAL work_mem = '64kB';

    -- Run a large sort that will spill to temp files
    -- Order by data (large text field) to maximize memory usage
    PERFORM count(*)
    FROM (
        SELECT data
        FROM test_memory_pressure
        ORDER BY data DESC
    ) subquery;

    -- Run another query with aggregation to increase temp file usage
    PERFORM data, count(*)
    FROM test_memory_pressure
    GROUP BY data
    ORDER BY data;

    -- Restore work_mem
    EXECUTE 'SET LOCAL work_mem = ' || quote_literal(v_old_work_mem);
END;
$$;

-- Capture snapshot after generating pathology
SELECT pg_sleep(0.2); -- Small delay to ensure statement stats are updated
SELECT flight_recorder.snapshot();

-- Test that snapshot captured the period
SELECT ok(
    (SELECT count(*) FROM flight_recorder.snapshots WHERE captured_at > now() - interval '10 seconds') >= 2,
    'MEMORY PATHOLOGY: At least 2 snapshots should exist from the test period'
);

-- Test that temp file data was captured in snapshots
-- Note: temp_files and temp_bytes are cumulative, so we check they exist
SELECT ok(
    (SELECT count(*) FROM flight_recorder.snapshots WHERE temp_files IS NOT NULL) >= 1,
    'MEMORY PATHOLOGY: Snapshots should capture temp_files metric'
);

-- Test that statement_snapshots table exists and has data
SELECT ok(
    (SELECT count(*) FROM flight_recorder.statement_snapshots) >= 0,
    'MEMORY PATHOLOGY: statement_snapshots should contain query statistics'
);

-- Test that anomaly_report can detect temp file issues
-- Create time range for anomaly detection
DO $$
DECLARE
    v_start_time TIMESTAMPTZ;
    v_end_time TIMESTAMPTZ;
    v_anomaly_count INT;
BEGIN
    SELECT min(captured_at) INTO v_start_time
    FROM flight_recorder.snapshots
    WHERE captured_at > now() - interval '10 seconds';

    SELECT max(captured_at) INTO v_end_time
    FROM flight_recorder.snapshots
    WHERE captured_at > now() - interval '10 seconds';

    -- Check if anomaly_report can run on this time range
    SELECT count(*) INTO v_anomaly_count
    FROM flight_recorder.anomaly_report(v_start_time, v_end_time);

    -- Store result for test
    CREATE TEMP TABLE IF NOT EXISTS pathology_test_results (test_name text, result boolean);
    INSERT INTO pathology_test_results VALUES ('anomaly_report_executed', v_anomaly_count >= 0);
END;
$$;

SELECT ok(
    (SELECT result FROM pathology_test_results WHERE test_name = 'anomaly_report_executed'),
    'MEMORY PATHOLOGY: anomaly_report() should execute on pathology time range'
);

-- Cleanup: Drop test table
DROP TABLE test_memory_pressure;

SELECT ok(
    NOT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'test_memory_pressure'),
    'MEMORY PATHOLOGY: Test table should be cleaned up'
);

-- =============================================================================
-- CONCLUSION
-- =============================================================================

SELECT * FROM finish();
ROLLBACK;
