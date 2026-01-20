-- Test script to verify change-only config snapshot capture
-- This tests that we only capture parameters when they change, not every snapshot

\set ON_ERROR_STOP on
\set QUIET on

-- Create test database if needed
\connect postgres

-- Install fresh
\i install.sql

-- Disable pg_cron jobs to control execution
SELECT flight_recorder.disable();

-- Enable config snapshots
UPDATE flight_recorder.config SET value = 'true' WHERE key = 'config_snapshots_enabled';
UPDATE flight_recorder.config SET value = 'true' WHERE key = 'db_role_config_snapshots_enabled';

\echo '=== Test 1: First snapshot should capture all parameters ==='

-- Capture first snapshot
SELECT flight_recorder.snapshot();

SELECT
    count(*) as param_count,
    'Should be ~50 parameters on first snapshot' as description
FROM flight_recorder.config_snapshots cs
JOIN flight_recorder.snapshots s ON s.id = cs.snapshot_id;

\echo ''
\echo '=== Test 2: Second snapshot (no changes) should capture 0 parameters ==='

-- Wait a moment to ensure different timestamp
SELECT pg_sleep(0.1);

-- Capture second snapshot (nothing changed)
SELECT flight_recorder.snapshot();

-- Check that second snapshot has NO config_snapshot entries (no changes)
SELECT
    count(*) as param_count,
    'Should be 0 parameters on second snapshot (no changes)' as description
FROM flight_recorder.config_snapshots cs
JOIN flight_recorder.snapshots s ON s.id = cs.snapshot_id
WHERE s.id = (SELECT max(id) FROM flight_recorder.snapshots);

\echo ''
\echo '=== Test 3: config_at() should still work with sparse data ==='

-- Query config at current time should still return all ~50 parameters
SELECT
    count(*) as param_count,
    'config_at(now()) should return all ~50 parameters' as description
FROM flight_recorder.config_at(now());

-- Verify specific parameters exist
SELECT
    CASE
        WHEN EXISTS (SELECT 1 FROM flight_recorder.config_at(now()) WHERE parameter_name = 'shared_buffers')
        THEN 'PASS: shared_buffers found'
        ELSE 'FAIL: shared_buffers not found'
    END as test_result;

SELECT
    CASE
        WHEN EXISTS (SELECT 1 FROM flight_recorder.config_at(now()) WHERE parameter_name = 'work_mem')
        THEN 'PASS: work_mem found'
        ELSE 'FAIL: work_mem not found'
    END as test_result;

\echo ''
\echo '=== Test 4: Manual config change simulation ==='

-- Simulate a config change by directly modifying the config_snapshots table
-- (In production, this would happen when pg_settings actually changes)
WITH latest_snapshot AS (
    SELECT max(id) as id FROM flight_recorder.snapshots
)
INSERT INTO flight_recorder.config_snapshots (snapshot_id, name, setting, unit, source, sourcefile)
VALUES (
    (SELECT id FROM latest_snapshot),
    'work_mem',
    '8MB',  -- Changed value
    NULL,
    'configuration file',
    NULL
);

SELECT
    count(*) as param_count,
    'After simulated change, latest snapshot should have 1 entry' as description
FROM flight_recorder.config_snapshots cs
JOIN flight_recorder.snapshots s ON s.id = cs.snapshot_id
WHERE s.id = (SELECT max(id) FROM flight_recorder.snapshots);

\echo ''
\echo '=== Test 5: config_changes() should detect the change ==='

SELECT
    count(*) as change_count,
    'config_changes() should detect 1 change' as description
FROM flight_recorder.config_changes(
    (SELECT captured_at FROM flight_recorder.snapshots ORDER BY id LIMIT 1),
    (SELECT captured_at FROM flight_recorder.snapshots ORDER BY id DESC LIMIT 1)
)
WHERE parameter_name = 'work_mem';

\echo ''
\echo '=== Test 6: DB/Role config snapshots - first run captures all ==='

-- Create a test database config override
ALTER DATABASE postgres SET work_mem = '16MB';

-- Capture snapshot
SELECT flight_recorder.snapshot();

SELECT
    count(*) as config_count,
    'Should have captured the database override' as description
FROM flight_recorder.db_role_config_snapshots
WHERE database_name = 'postgres' AND parameter_name = 'work_mem';

\echo ''
\echo '=== Test 7: DB/Role config - no change means no capture ==='

-- Capture another snapshot without changes
SELECT pg_sleep(0.1);
SELECT flight_recorder.snapshot();

-- Latest snapshot should have no db/role config entries (no changes)
SELECT
    count(*) as config_count,
    'Should be 0 entries on snapshot with no changes' as description
FROM flight_recorder.db_role_config_snapshots drc
JOIN flight_recorder.snapshots s ON s.id = drc.snapshot_id
WHERE s.id = (SELECT max(id) FROM flight_recorder.snapshots);

\echo ''
\echo '=== Test 8: DB/Role config - change is captured ==='

-- Modify the database config
ALTER DATABASE postgres SET work_mem = '32MB';

SELECT pg_sleep(0.1);
SELECT flight_recorder.snapshot();

-- Latest snapshot should have the changed entry
SELECT
    count(*) as config_count,
    'Should have 1 entry for the changed value' as description
FROM flight_recorder.db_role_config_snapshots drc
JOIN flight_recorder.snapshots s ON s.id = drc.snapshot_id
WHERE s.id = (SELECT max(id) FROM flight_recorder.snapshots)
AND parameter_name = 'work_mem';

\echo ''
\echo '=== Test 9: DB/Role config - removal is tracked ==='

-- Remove the database config
ALTER DATABASE postgres RESET work_mem;

SELECT pg_sleep(0.1);
SELECT flight_recorder.snapshot();

-- Latest snapshot should have an entry with NULL value (indicating removal)
SELECT
    CASE
        WHEN EXISTS (
            SELECT 1 FROM flight_recorder.db_role_config_snapshots drc
            JOIN flight_recorder.snapshots s ON s.id = drc.snapshot_id
            WHERE s.id = (SELECT max(id) FROM flight_recorder.snapshots)
            AND parameter_name = 'work_mem'
            AND parameter_value IS NULL
        )
        THEN 'PASS: Removal tracked with NULL value'
        ELSE 'FAIL: Removal not tracked'
    END as test_result;

\echo ''
\echo '=== Test 10: db_role_config_at() still works correctly ==='

SELECT
    count(*) as total_configs,
    'db_role_config_at() should work with sparse data' as description
FROM flight_recorder.db_role_config_at(now());

\echo ''
\echo '=== Summary: Storage savings ==='

WITH stats AS (
    SELECT
        count(DISTINCT s.id) as snapshot_count,
        count(*) as total_config_rows,
        (SELECT count(*) FROM (SELECT DISTINCT name FROM flight_recorder.config_snapshots) x) as unique_params
    FROM flight_recorder.config_snapshots cs
    JOIN flight_recorder.snapshots s ON s.id = cs.snapshot_id
)
SELECT
    snapshot_count,
    unique_params,
    total_config_rows,
    (snapshot_count * unique_params) as old_approach_would_be,
    total_config_rows::float / NULLIF(snapshot_count * unique_params, 0) * 100 as storage_efficiency_pct,
    'Lower is better - shows % of rows vs old approach' as note
FROM stats;

\echo ''
\echo '=== All tests complete ==='
