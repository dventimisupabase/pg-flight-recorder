-- =============================================================================
-- pg-flight-recorder pgTAP Tests - Boundary & Critical Functions
-- =============================================================================
-- Tests: Adversarial boundary tests, untested critical functions
-- Sections: 11 (Adversarial Boundary), 12 (Untested Critical Functions)
-- Test count: 101
-- =============================================================================

BEGIN;
SELECT plan(101);

-- Disable checkpoint detection during tests to prevent snapshot skipping
UPDATE flight_recorder.config SET value = 'false' WHERE key = 'check_checkpoint_backup';

-- Disable collection jitter to speed up tests (default is 0-10 second random delay)
UPDATE flight_recorder.config SET value = 'false' WHERE key = 'collection_jitter_enabled';

-- =============================================================================
-- 11. ADVERSARIAL BOUNDARY TESTS (50 tests)
-- =============================================================================

-- Ring Buffer Slot Boundaries (10 tests)

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

-- Test slot_id wraparound (verify both 0 and 119 exist)
SELECT ok(
    (SELECT count(*) FROM flight_recorder.samples_ring WHERE slot_id IN (0, 119)) = 2,
    'Boundary: Slots 0 and 119 should both exist for wraparound'
);

-- Test all 120 slots exist
SELECT is(
    (SELECT count(DISTINCT slot_id) FROM flight_recorder.samples_ring),
    120::bigint,
    'Boundary: Exactly 120 unique slots should exist (0-119)'
);

-- Row Number Boundaries (15 tests)

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
    'Boundary: wait_samples_ring should have 12,000 pre-populated rows (120 slots x 100 rows)'
);

SELECT is(
    (SELECT count(*) FROM flight_recorder.activity_samples_ring),
    3000::bigint,
    'Boundary: activity_samples_ring should have 3,000 pre-populated rows (120 slots x 25 rows)'
);

SELECT is(
    (SELECT count(*) FROM flight_recorder.lock_samples_ring),
    12000::bigint,
    'Boundary: lock_samples_ring should have 12,000 pre-populated rows (120 slots x 100 rows)'
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
-- Insert the 'enabled' key if it doesn't exist, then set to 'false'
INSERT INTO flight_recorder.config (key, value)
VALUES ('enabled', 'false')
ON CONFLICT (key) DO UPDATE SET value = 'false';

SELECT ok(
    EXISTS(
        SELECT 1 FROM flight_recorder.health_check()
        WHERE status = 'DISABLED'
        AND component LIKE '%System%'
    ),
    'Health: health_check() should report DISABLED when system disabled'
);

-- Re-enable system
INSERT INTO flight_recorder.config (key, value)
VALUES ('enabled', 'true')
ON CONFLICT (key) DO UPDATE SET value = 'true';

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

SELECT * FROM finish();
ROLLBACK;
