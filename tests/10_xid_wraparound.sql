-- =============================================================================
-- pg-flight-recorder pgTAP Tests - XID Wraparound Metrics
-- =============================================================================
-- Tests: XID age columns exist and are populated with reasonable values
-- Test count: 8
-- =============================================================================

BEGIN;
SELECT plan(8);

-- Disable checkpoint detection during tests to prevent snapshot skipping
UPDATE flight_recorder.config SET value = 'false' WHERE key = 'check_checkpoint_backup';

-- Disable collection jitter to speed up tests
UPDATE flight_recorder.config SET value = 'false' WHERE key = 'collection_jitter_enabled';

-- =============================================================================
-- 1. COLUMN EXISTENCE (2 tests)
-- =============================================================================

SELECT has_column(
    'flight_recorder', 'snapshots', 'datfrozenxid_age',
    'snapshots table should have datfrozenxid_age column'
);

SELECT has_column(
    'flight_recorder', 'table_snapshots', 'relfrozenxid_age',
    'table_snapshots table should have relfrozenxid_age column'
);

-- =============================================================================
-- 2. DATA POPULATION (4 tests)
-- =============================================================================

-- Take a snapshot to populate data
SELECT flight_recorder.snapshot();

-- Verify datfrozenxid_age is populated
SELECT ok(
    (SELECT datfrozenxid_age FROM flight_recorder.snapshots ORDER BY id DESC LIMIT 1) IS NOT NULL,
    'datfrozenxid_age should be populated after snapshot()'
);

-- Verify datfrozenxid_age is a reasonable value (> 0, < 2 billion)
SELECT ok(
    (SELECT datfrozenxid_age FROM flight_recorder.snapshots ORDER BY id DESC LIMIT 1) > 0,
    'datfrozenxid_age should be greater than 0'
);

SELECT ok(
    (SELECT datfrozenxid_age FROM flight_recorder.snapshots ORDER BY id DESC LIMIT 1) < 2000000000,
    'datfrozenxid_age should be less than 2 billion'
);

-- Verify relfrozenxid_age is populated for at least some tables
SELECT ok(
    EXISTS (
        SELECT 1 FROM flight_recorder.table_snapshots ts
        WHERE ts.snapshot_id = (SELECT max(id) FROM flight_recorder.snapshots)
          AND ts.relfrozenxid_age IS NOT NULL
    ),
    'relfrozenxid_age should be populated for tables after snapshot()'
);

-- =============================================================================
-- 3. VALUE REASONABLENESS (2 tests)
-- =============================================================================

-- Verify relfrozenxid_age values are reasonable where populated (>= 0, as newly created tables can have age 0)
SELECT ok(
    NOT EXISTS (
        SELECT 1 FROM flight_recorder.table_snapshots ts
        WHERE ts.snapshot_id = (SELECT max(id) FROM flight_recorder.snapshots)
          AND ts.relfrozenxid_age IS NOT NULL
          AND ts.relfrozenxid_age < 0
    ),
    'relfrozenxid_age values should be non-negative'
);

SELECT ok(
    NOT EXISTS (
        SELECT 1 FROM flight_recorder.table_snapshots ts
        WHERE ts.snapshot_id = (SELECT max(id) FROM flight_recorder.snapshots)
          AND ts.relfrozenxid_age IS NOT NULL
          AND ts.relfrozenxid_age >= 2000000000
    ),
    'relfrozenxid_age values should be less than 2 billion'
);

SELECT * FROM finish();
ROLLBACK;
