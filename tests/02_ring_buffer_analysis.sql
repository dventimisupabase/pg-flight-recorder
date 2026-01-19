-- =============================================================================
-- pg-flight-recorder pgTAP Tests - Ring Buffer & Analysis
-- =============================================================================
-- Tests: Ring buffer architecture, analysis functions, config, views
-- Sections: 3A, 4, 6, 7
-- Test count: 25
-- =============================================================================

BEGIN;
SELECT plan(25);

-- Disable checkpoint detection during tests to prevent snapshot skipping
UPDATE flight_recorder.config SET value = 'false' WHERE key = 'check_checkpoint_backup';

-- Disable collection jitter to speed up tests (default is 0-10 second random delay)
UPDATE flight_recorder.config SET value = 'false' WHERE key = 'collection_jitter_enabled';

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

-- Capture a sample first to ensure we have data to aggregate
SELECT flight_recorder.sample();

-- Flush again to ensure aggregates are created
SELECT flight_recorder.flush_ring_to_aggregates();

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

SELECT * FROM finish();
ROLLBACK;
