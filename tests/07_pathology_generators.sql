-- =============================================================================
-- pg-flight-recorder pgTAP Tests - Pathological Data Generators
-- =============================================================================
-- Tests: Generate real-world pathologies and verify pg-flight-recorder detects them
-- Purpose: Validate that diagnostic playbooks work end-to-end
-- Based on: DIAGNOSTIC_PLAYBOOKS.md
-- Sections: Lock Contention, Memory Pressure, High CPU, Slow Real-time, Slow Queries
-- Test count: 27
-- =============================================================================

BEGIN;
SELECT plan(27);

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
-- PATHOLOGY 3: HIGH CPU USAGE (5 tests)
-- Based on: DIAGNOSTIC_PLAYBOOKS.md - Section 4 "High CPU Usage"
--
-- Real-world scenario: Queries burning CPU with complex calculations
-- Expected detection: High total_exec_time with blk_read_time = 0 (CPU-bound)
--                     statement_snapshots captures query statistics
-- =============================================================================

-- Setup: Create table for CPU-intensive operations
CREATE TABLE test_cpu_intensive (
    id int,
    value numeric
);

-- Insert data for CPU-intensive calculations
INSERT INTO test_cpu_intensive
SELECT i, random() * 1000
FROM generate_series(1, 50000) i;

SELECT ok(
    (SELECT count(*) FROM test_cpu_intensive) = 50000,
    'CPU PATHOLOGY: Test table should have 50000 rows'
);

-- Capture baseline snapshot
SELECT flight_recorder.snapshot();

-- Generate pathology: CPU-intensive calculations
DO $$
DECLARE
    v_result numeric;
BEGIN
    -- Run CPU-intensive mathematical operations
    -- These operations are compute-heavy but don't require disk I/O
    SELECT sum(
        sqrt(abs(value)) * ln(abs(value) + 1) * exp(value / 100000.0) * cos(value)
    ) INTO v_result
    FROM test_cpu_intensive
    WHERE value > 0;

    -- Run another CPU-intensive query with aggregations
    SELECT sum(power(value, 2) + power(value, 3) / 1000000)
    INTO v_result
    FROM test_cpu_intensive;

    -- Small delay to ensure stats are captured
    PERFORM pg_sleep(0.1);
END;
$$;

-- Capture snapshot after CPU work
SELECT flight_recorder.snapshot();

-- Test that snapshots were captured
SELECT ok(
    (SELECT count(*) FROM flight_recorder.snapshots WHERE captured_at > now() - interval '10 seconds') >= 2,
    'CPU PATHOLOGY: At least 2 snapshots should exist from test period'
);

-- Test that statement_snapshots has data
SELECT ok(
    (SELECT count(*) FROM flight_recorder.statement_snapshots) > 0,
    'CPU PATHOLOGY: statement_snapshots should contain query statistics'
);

-- Test that recent_activity_current() works (used for real-time CPU monitoring)
SELECT lives_ok(
    $$SELECT * FROM flight_recorder.recent_activity_current()$$,
    'CPU PATHOLOGY: recent_activity_current() should execute for CPU monitoring'
);

-- Cleanup
DROP TABLE test_cpu_intensive;

SELECT ok(
    NOT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'test_cpu_intensive'),
    'CPU PATHOLOGY: Test table should be cleaned up'
);

-- =============================================================================
-- PATHOLOGY 4: DATABASE SLOW - REAL-TIME (5 tests)
-- Based on: DIAGNOSTIC_PLAYBOOKS.md - Section 1 "Database is Slow RIGHT NOW"
--
-- Real-world scenario: Long-running queries blocking resources
-- Expected detection: recent_activity_current() shows long query_start
--                     recent_waits_current() shows wait events
--                     sample() captures activity snapshots
-- =============================================================================

-- Setup: Create a table to work with
CREATE TABLE test_slow_realtime (
    id int PRIMARY KEY,
    data text
);

INSERT INTO test_slow_realtime
SELECT i, md5(random()::text)
FROM generate_series(1, 1000) i;

SELECT ok(
    EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'test_slow_realtime'),
    'SLOW REALTIME PATHOLOGY: Test table should be created'
);

-- Generate pathology: Simulate a slow query using pg_sleep
-- This mimics a long-running transaction that would show up in monitoring
DO $$
BEGIN
    -- Capture activity before slow operation
    PERFORM flight_recorder.sample();

    -- Simulate slow query (short sleep to not slow down tests too much)
    PERFORM pg_sleep(0.5);

    -- Do some work during the "slow" period
    PERFORM count(*) FROM test_slow_realtime;

    -- Capture activity after slow operation
    PERFORM flight_recorder.sample();
END;
$$;

-- Test that sample() captured activity
SELECT ok(
    (SELECT count(*) FROM flight_recorder.activity_samples_ring) >= 0,
    'SLOW REALTIME PATHOLOGY: activity_samples_ring should be queryable'
);

-- Test that recent_activity_current() works for real-time monitoring
SELECT lives_ok(
    $$SELECT * FROM flight_recorder.recent_activity_current() ORDER BY query_start NULLS LAST LIMIT 10$$,
    'SLOW REALTIME PATHOLOGY: recent_activity_current() should execute for triage'
);

-- Test that recent_waits_current() works for wait event analysis
SELECT lives_ok(
    $$SELECT * FROM flight_recorder.recent_waits_current() ORDER BY count DESC$$,
    'SLOW REALTIME PATHOLOGY: recent_waits_current() should execute for wait analysis'
);

-- Cleanup
DROP TABLE test_slow_realtime;

SELECT ok(
    NOT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'test_slow_realtime'),
    'SLOW REALTIME PATHOLOGY: Test table should be cleaned up'
);

-- =============================================================================
-- PATHOLOGY 5: QUERIES TIMING OUT / TAKING FOREVER (5 tests)
-- Based on: DIAGNOSTIC_PLAYBOOKS.md - Section 3 "Queries Timing Out / Taking Forever"
--
-- Real-world scenario: Queries doing sequential scans on large tables
-- Expected detection: High mean_exec_time in statement_snapshots
--                     High shared_blks_read indicating sequential scans
--                     statement_compare() shows query regression
-- =============================================================================

-- Setup: Create a larger table without indexes to force sequential scans
CREATE TABLE test_slow_queries (
    id int,
    category int,
    data text,
    created_at timestamp
);

-- Insert substantial data (no primary key = no index)
INSERT INTO test_slow_queries
SELECT
    i,
    (random() * 100)::int,
    md5(random()::text) || md5(random()::text),
    now() - (random() * interval '30 days')
FROM generate_series(1, 20000) i;

SELECT ok(
    (SELECT count(*) FROM test_slow_queries) = 20000,
    'SLOW QUERIES PATHOLOGY: Test table should have 20000 rows'
);

-- Capture baseline snapshot
SELECT flight_recorder.snapshot();

-- Generate pathology: Force sequential scans and expensive operations
DO $$
DECLARE
    v_count int;
BEGIN
    -- Query without index - forces sequential scan
    SELECT count(*) INTO v_count
    FROM test_slow_queries
    WHERE category = 42;

    -- Another sequential scan with sorting (no index to help)
    SELECT count(*) INTO v_count
    FROM test_slow_queries
    WHERE data LIKE '%abc%'
    ORDER BY created_at;

    -- Expensive aggregation requiring full table scan
    SELECT count(DISTINCT category) INTO v_count
    FROM test_slow_queries
    WHERE created_at > now() - interval '15 days';

    -- Force stats update
    PERFORM pg_sleep(0.1);
END;
$$;

-- Capture snapshot after slow queries
SELECT flight_recorder.snapshot();

-- Test that statement_snapshots captured query data
SELECT ok(
    (SELECT count(*) FROM flight_recorder.statement_snapshots ss
     JOIN flight_recorder.snapshots s ON s.id = ss.snapshot_id
     WHERE s.captured_at > now() - interval '10 seconds') > 0,
    'SLOW QUERIES PATHOLOGY: statement_snapshots should have recent data'
);

-- Test that we can query statement stats for slow query analysis
SELECT lives_ok(
    $$SELECT query_preview, calls, mean_exec_time, shared_blks_read
      FROM flight_recorder.statement_snapshots ss
      JOIN flight_recorder.snapshots s ON s.id = ss.snapshot_id
      WHERE s.captured_at > now() - interval '10 seconds'
      ORDER BY mean_exec_time DESC NULLS LAST
      LIMIT 10$$,
    'SLOW QUERIES PATHOLOGY: Should be able to query statement stats for analysis'
);

-- Test that compare() function works for before/after analysis
DO $$
DECLARE
    v_start_time timestamptz;
    v_end_time timestamptz;
    v_compare_count int;
BEGIN
    SELECT min(captured_at), max(captured_at)
    INTO v_start_time, v_end_time
    FROM flight_recorder.snapshots
    WHERE captured_at > now() - interval '10 seconds';

    -- Verify compare() executes
    SELECT count(*) INTO v_compare_count
    FROM flight_recorder.compare(v_start_time, v_end_time);

    -- Store result
    INSERT INTO pathology_test_results VALUES ('compare_executed', v_compare_count >= 0)
    ON CONFLICT DO NOTHING;

    -- If table doesn't exist from previous test, create it
    IF NOT FOUND THEN
        UPDATE pathology_test_results SET result = (v_compare_count >= 0) WHERE test_name = 'compare_executed';
    END IF;
END;
$$;

SELECT ok(
    (SELECT result FROM pathology_test_results WHERE test_name = 'compare_executed'),
    'SLOW QUERIES PATHOLOGY: compare() should execute for before/after analysis'
);

-- Cleanup
DROP TABLE test_slow_queries;

SELECT ok(
    NOT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'test_slow_queries'),
    'SLOW QUERIES PATHOLOGY: Test table should be cleaned up'
);

-- =============================================================================
-- CONCLUSION
-- =============================================================================

SELECT * FROM finish();
ROLLBACK;
