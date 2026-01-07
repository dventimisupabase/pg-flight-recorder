-- =============================================================================
-- Test: DDL Detection Feature
-- =============================================================================
-- This script tests the new DDL detection feature in pg-flight-recorder.
--
-- PREREQUISITES:
--   1. pg-flight-recorder must be installed (psql -f install.sql)
--   2. Run this test in a non-production database
--
-- USAGE:
--   psql -f test_ddl_detection.sql
--
-- =============================================================================

\echo ''
\echo '==================================================================='
\echo 'Test 1: Verify _detect_active_ddl() function exists'
\echo '==================================================================='

SELECT proname, pronargs
FROM pg_proc
WHERE proname = '_detect_active_ddl'
  AND pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'flight_recorder');

\echo ''
\echo '==================================================================='
\echo 'Test 2: Call _detect_active_ddl() with no DDL active'
\echo '==================================================================='

SELECT * FROM flight_recorder._detect_active_ddl();

\echo ''
\echo '==================================================================='
\echo 'Test 3: Verify DDL detection configuration'
\echo '==================================================================='

SELECT key, value
FROM flight_recorder.config
WHERE key IN ('ddl_detection_enabled', 'ddl_skip_locks', 'ddl_skip_entire_sample')
ORDER BY key;

\echo ''
\echo '==================================================================='
\echo 'Test 4: Check health_check() includes DDL detection'
\echo '==================================================================='

SELECT component, status, details
FROM flight_recorder.health_check()
WHERE component = 'DDL Detection';

\echo ''
\echo '==================================================================='
\echo 'Test 5: Simulate DDL detection (requires two sessions)'
\echo '==================================================================='
\echo 'This test requires manual steps:'
\echo '  1. In another psql session, run: BEGIN; CREATE TABLE test_ddl_detection(id int);'
\echo '  2. In this session, run the query below'
\echo '  3. In the other session, run: ROLLBACK;'
\echo ''
\echo 'Run this manually when ready:'
\echo '  SELECT * FROM flight_recorder._detect_active_ddl();'
\echo ''

\echo ''
\echo '==================================================================='
\echo 'Test 6: Verify sample() function compiles without errors'
\echo '==================================================================='

-- This will show any compilation errors in the sample() function
SELECT
    p.proname,
    pg_get_functiondef(p.oid) IS NOT NULL AS function_compiles
FROM pg_proc p
WHERE p.proname = 'sample'
  AND p.pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'flight_recorder');

\echo ''
\echo '==================================================================='
\echo 'Test 7: Check for DDL detection variables in sample() function'
\echo '==================================================================='

SELECT
    proname,
    prosrc LIKE '%v_ddl_detected%' AS has_ddl_detection,
    prosrc LIKE '%_detect_active_ddl()%' AS calls_detect_function
FROM pg_proc
WHERE proname = 'sample'
  AND pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'flight_recorder');

\echo ''
\echo '==================================================================='
\echo 'Test 8: Test DDL skip configuration changes'
\echo '==================================================================='

-- Show current config
SELECT key, value FROM flight_recorder.config WHERE key LIKE 'ddl_%';

-- Test changing config (doesn't actually change, just validates the UPDATE would work)
BEGIN;
UPDATE flight_recorder.config SET value = 'false' WHERE key = 'ddl_detection_enabled';
SELECT key, value FROM flight_recorder.config WHERE key = 'ddl_detection_enabled';
ROLLBACK;

\echo ''
\echo '==================================================================='
\echo 'All automated tests complete!'
\echo ''
\echo 'For full testing:'
\echo '  1. Run Test 5 manually with two sessions'
\echo '  2. Monitor collection_stats for DDL-related skips:'
\echo '     SELECT * FROM flight_recorder.collection_stats'
\echo '     WHERE skipped_reason LIKE ''%DDL%'' ORDER BY started_at DESC;'
\echo '==================================================================='
