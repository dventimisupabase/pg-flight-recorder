-- =============================================================================
-- pg-flight-recorder pgTAP Tests - OID Exhaustion Metrics
-- =============================================================================
-- Tests: OID exhaustion columns exist and are populated with reasonable values
-- Test count: 14
-- =============================================================================

BEGIN;
SELECT plan(14);

-- Disable checkpoint detection during tests to prevent snapshot skipping
UPDATE flight_recorder.config SET value = 'false' WHERE key = 'check_checkpoint_backup';

-- Disable collection jitter to speed up tests
UPDATE flight_recorder.config SET value = 'false' WHERE key = 'collection_jitter_enabled';

-- =============================================================================
-- 1. COLUMN EXISTENCE (2 tests)
-- =============================================================================

SELECT has_column(
    'flight_recorder', 'snapshots', 'max_catalog_oid',
    'snapshots table should have max_catalog_oid column'
);

SELECT has_column(
    'flight_recorder', 'snapshots', 'large_object_count',
    'snapshots table should have large_object_count column'
);

-- =============================================================================
-- 2. DATA POPULATION (4 tests)
-- =============================================================================

-- Take a snapshot to populate data
SELECT flight_recorder.snapshot();

-- Verify max_catalog_oid is populated
SELECT ok(
    (SELECT max_catalog_oid FROM flight_recorder.snapshots ORDER BY id DESC LIMIT 1) IS NOT NULL,
    'max_catalog_oid should be populated after snapshot()'
);

-- Verify large_object_count is populated
SELECT ok(
    (SELECT large_object_count FROM flight_recorder.snapshots ORDER BY id DESC LIMIT 1) IS NOT NULL,
    'large_object_count should be populated after snapshot()'
);

-- Verify max_catalog_oid is a reasonable value (> 0, < 4.3 billion)
SELECT ok(
    (SELECT max_catalog_oid FROM flight_recorder.snapshots ORDER BY id DESC LIMIT 1) > 0,
    'max_catalog_oid should be greater than 0'
);

SELECT ok(
    (SELECT max_catalog_oid FROM flight_recorder.snapshots ORDER BY id DESC LIMIT 1) < 4294967295,
    'max_catalog_oid should be less than max OID (4.3 billion)'
);

-- =============================================================================
-- 3. VALUE REASONABLENESS (2 tests)
-- =============================================================================

-- Verify large_object_count is non-negative
SELECT ok(
    (SELECT large_object_count FROM flight_recorder.snapshots ORDER BY id DESC LIMIT 1) >= 0,
    'large_object_count should be non-negative'
);

-- Verify max_catalog_oid represents actual pg_class OIDs
SELECT ok(
    (SELECT max_catalog_oid FROM flight_recorder.snapshots ORDER BY id DESC LIMIT 1)
        >= (SELECT max(oid)::bigint FROM pg_class) - 1000,
    'max_catalog_oid should be close to actual max pg_class OID'
);

-- =============================================================================
-- 4. ANOMALY REPORT INTEGRATION (2 tests)
-- =============================================================================

-- Verify anomaly_report() runs without error when checking OID exhaustion
SELECT lives_ok(
    $$SELECT * FROM flight_recorder.anomaly_report(now() - interval '1 hour', now())$$,
    'anomaly_report() should run without error with OID exhaustion checks'
);

-- In a fresh test database, OID usage should be low, so no OID exhaustion anomalies expected
SELECT ok(
    NOT EXISTS (
        SELECT 1 FROM flight_recorder.anomaly_report(now() - interval '1 hour', now())
        WHERE anomaly_type = 'OID_EXHAUSTION_RISK'
    ),
    'Fresh database should not trigger OID exhaustion anomalies'
);

-- =============================================================================
-- 5. RATE FUNCTION TESTS (4 tests)
-- =============================================================================

-- Verify oid_consumption_rate function exists and runs
SELECT lives_ok(
    $$SELECT flight_recorder.oid_consumption_rate('1 hour'::interval)$$,
    'oid_consumption_rate() should run without error'
);

-- Verify time_to_oid_exhaustion function exists and runs
SELECT lives_ok(
    $$SELECT flight_recorder.time_to_oid_exhaustion()$$,
    'time_to_oid_exhaustion() should run without error'
);

-- Rate returns NULL when insufficient data or non-negative when data exists
SELECT ok(
    (SELECT flight_recorder.oid_consumption_rate('1 hour'::interval)) IS NULL
    OR (SELECT flight_recorder.oid_consumption_rate('1 hour'::interval)) >= 0,
    'oid_consumption_rate() should return NULL or non-negative value'
);

-- Take another snapshot and verify rate calculation works
SELECT flight_recorder.snapshot();

-- After multiple snapshots, rate should still be NULL or non-negative
SELECT ok(
    (SELECT flight_recorder.oid_consumption_rate('1 hour'::interval)) IS NULL
    OR (SELECT flight_recorder.oid_consumption_rate('1 hour'::interval)) >= 0,
    'oid_consumption_rate() should return NULL or non-negative value after multiple snapshots'
);

SELECT * FROM finish();
ROLLBACK;
