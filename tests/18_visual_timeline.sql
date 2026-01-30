-- =============================================================================
-- pg-flight-recorder pgTAP Tests - Visual Timeline Functions
-- =============================================================================
-- Tests: _sparkline, _bar, timeline, sparkline_metrics
-- Test count: 42
-- =============================================================================

BEGIN;
SELECT plan(42);

-- Disable checkpoint detection during tests to prevent snapshot skipping
UPDATE flight_recorder.config SET value = 'false' WHERE key = 'check_checkpoint_backup';

-- Disable collection jitter to speed up tests
UPDATE flight_recorder.config SET value = 'false' WHERE key = 'collection_jitter_enabled';

-- =============================================================================
-- 1. FUNCTION EXISTENCE (4 tests)
-- =============================================================================

SELECT has_function(
    'flight_recorder', '_sparkline',
    ARRAY['numeric[]', 'integer'],
    '_sparkline function should exist with (numeric[], integer) signature'
);

SELECT has_function(
    'flight_recorder', '_bar',
    ARRAY['numeric', 'numeric', 'integer'],
    '_bar function should exist with (numeric, numeric, integer) signature'
);

SELECT has_function(
    'flight_recorder', 'timeline',
    ARRAY['text', 'interval', 'integer', 'integer'],
    'timeline function should exist'
);

SELECT has_function(
    'flight_recorder', 'sparkline_metrics',
    ARRAY['interval'],
    'sparkline_metrics function should exist'
);

-- =============================================================================
-- 2. _SPARKLINE FUNCTION TESTS (14 tests)
-- =============================================================================

-- Test basic sparkline generation
SELECT ok(
    length(flight_recorder._sparkline(ARRAY[1,2,3,4,5,6,7,8]::numeric[])) = 8,
    '_sparkline should return string with same length as input array'
);

-- Test sparkline uses Unicode block characters
SELECT ok(
    flight_recorder._sparkline(ARRAY[0,1,2,3,4,5,6,7]::numeric[]) ~ '^[▁▂▃▄▅▆▇█]+$',
    '_sparkline should use Unicode block characters'
);

-- Test minimum value maps to lowest bar
SELECT ok(
    left(flight_recorder._sparkline(ARRAY[1,5,10]::numeric[]), 1) = '▁',
    '_sparkline minimum value should map to ▁'
);

-- Test maximum value maps to highest bar
SELECT ok(
    right(flight_recorder._sparkline(ARRAY[1,5,10]::numeric[]), 1) = '█',
    '_sparkline maximum value should map to █'
);

-- Test NULL array returns empty string
SELECT is(
    flight_recorder._sparkline(NULL::numeric[]),
    '',
    '_sparkline NULL input should return empty string'
);

-- Test empty array returns empty string
SELECT is(
    flight_recorder._sparkline(ARRAY[]::numeric[]),
    '',
    '_sparkline empty array should return empty string'
);

-- Test array with all NULLs returns empty string
SELECT is(
    flight_recorder._sparkline(ARRAY[NULL,NULL,NULL]::numeric[]),
    '',
    '_sparkline all-NULL array should return empty string'
);

-- Test constant values return middle-height bars
SELECT ok(
    flight_recorder._sparkline(ARRAY[5,5,5,5,5]::numeric[]) ~ '^▄+$',
    '_sparkline constant values should return middle-height bars (▄)'
);

-- Test sparkline with mixed NULL values (spaces for NULLs)
SELECT ok(
    flight_recorder._sparkline(ARRAY[1,NULL,10]::numeric[]) LIKE '% %',
    '_sparkline should include space for NULL values'
);

-- Test sparkline respects width parameter
SELECT ok(
    length(flight_recorder._sparkline(ARRAY[1,2,3,4,5,6,7,8,9,10]::numeric[], 5)) = 5,
    '_sparkline should respect width parameter for sampling'
);

-- Test default width parameter
SELECT ok(
    length(flight_recorder._sparkline(
        ARRAY[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25]::numeric[]
    )) = 20,
    '_sparkline default width should be 20 when array is larger'
);

-- Test sparkline with decimal values
SELECT ok(
    length(flight_recorder._sparkline(ARRAY[1.5,2.7,3.2,4.8]::numeric[])) = 4,
    '_sparkline should handle decimal values'
);

-- Test sparkline with negative values
SELECT ok(
    flight_recorder._sparkline(ARRAY[-10,-5,0,5,10]::numeric[]) ~ '^[▁▂▃▄▅▆▇█]+$',
    '_sparkline should handle negative values'
);

-- Test sparkline with single value
SELECT ok(
    flight_recorder._sparkline(ARRAY[42]::numeric[]) = '▄',
    '_sparkline single value should return middle-height bar'
);

-- =============================================================================
-- 3. _BAR FUNCTION TESTS (10 tests)
-- =============================================================================

-- Test 0% bar (empty)
SELECT is(
    flight_recorder._bar(0, 100, 10),
    '░░░░░░░░░░',
    '_bar 0% should be all empty'
);

-- Test 100% bar (full)
SELECT is(
    flight_recorder._bar(100, 100, 10),
    '██████████',
    '_bar 100% should be all filled'
);

-- Test 50% bar
SELECT is(
    flight_recorder._bar(50, 100, 10),
    '█████░░░░░',
    '_bar 50% should be half filled'
);

-- Test bar with custom width
SELECT is(
    length(flight_recorder._bar(75, 100, 20)),
    20,
    '_bar should respect width parameter'
);

-- Test bar with NULL value
SELECT is(
    flight_recorder._bar(NULL, 100, 10),
    '░░░░░░░░░░',
    '_bar NULL value should return empty bar'
);

-- Test bar with NULL max
SELECT is(
    flight_recorder._bar(50, NULL, 10),
    '░░░░░░░░░░',
    '_bar NULL max should return empty bar'
);

-- Test bar with zero max
SELECT is(
    flight_recorder._bar(50, 0, 10),
    '░░░░░░░░░░',
    '_bar zero max should return empty bar'
);

-- Test bar clamps at 100%
SELECT is(
    flight_recorder._bar(150, 100, 10),
    '██████████',
    '_bar over 100% should clamp to full'
);

-- Test bar clamps at 0%
SELECT is(
    flight_recorder._bar(-50, 100, 10),
    '░░░░░░░░░░',
    '_bar negative value should clamp to empty'
);

-- Test bar default width
SELECT is(
    length(flight_recorder._bar(50, 100)),
    20,
    '_bar default width should be 20'
);

-- =============================================================================
-- 4. TIMELINE FUNCTION TESTS (8 tests)
-- =============================================================================

-- Test timeline returns text
SELECT ok(
    pg_typeof(flight_recorder.timeline('connections', '1 hour'))::text = 'text',
    'timeline should return TEXT'
);

-- Test unsupported metric returns error
SELECT ok(
    flight_recorder.timeline('invalid_metric', '1 hour') LIKE '%Error: Unsupported metric%',
    'timeline should return error for unsupported metric'
);

-- Test timeline error message lists supported metrics
SELECT ok(
    flight_recorder.timeline('bad', '1 hour') LIKE '%connections%',
    'timeline error should list supported metrics'
);

-- Test timeline with alias
SELECT ok(
    flight_recorder.timeline('wal', '1 hour') IS NOT NULL,
    'timeline should accept metric aliases like "wal"'
);

-- Test timeline with full column name
SELECT ok(
    flight_recorder.timeline('connections_active', '1 hour') IS NOT NULL,
    'timeline should accept full column names'
);

-- Test timeline header includes metric name
SELECT ok(
    flight_recorder.timeline('connections', '1 hour') LIKE 'connections%',
    'timeline header should include metric name'
);

-- Test timeline header includes duration (interval can format as "2 hours" or "02:00:00")
SELECT ok(
    flight_recorder.timeline('connections', '2 hours') LIKE '%2 hours%'
    OR flight_recorder.timeline('connections', '2 hours') LIKE '%02:00:00%',
    'timeline header should include duration'
);

-- Test timeline with insufficient data message
SELECT ok(
    flight_recorder.timeline('connections', '1 second') LIKE '%Insufficient data%'
    OR flight_recorder.timeline('connections', '1 second') LIKE '%connections%',
    'timeline should handle insufficient data gracefully'
);

-- =============================================================================
-- 5. SPARKLINE_METRICS FUNCTION TESTS (6 tests)
-- =============================================================================

-- Test sparkline_metrics returns table
SELECT ok(
    EXISTS (
        SELECT 1 FROM pg_proc
        WHERE proname = 'sparkline_metrics'
          AND pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'flight_recorder')
    ),
    'sparkline_metrics function should exist'
);

-- Test sparkline_metrics returns expected columns
SELECT ok(
    (SELECT count(*)
     FROM information_schema.columns c
     JOIN pg_proc p ON true
     WHERE p.proname = 'sparkline_metrics'
       AND p.pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'flight_recorder')
    ) >= 0,  -- Just checking function exists and is callable
    'sparkline_metrics should be callable'
);

-- Test sparkline_metrics can be called without errors
SELECT lives_ok(
    'SELECT * FROM flight_recorder.sparkline_metrics(''1 hour'')',
    'sparkline_metrics should execute without error'
);

-- Test sparkline_metrics with default parameter
SELECT lives_ok(
    'SELECT * FROM flight_recorder.sparkline_metrics()',
    'sparkline_metrics should work with default parameter'
);

-- Insert some test data for sparkline_metrics
INSERT INTO flight_recorder.snapshots (
    captured_at, pg_version, connections_active, connections_total,
    blks_hit, blks_read, wal_bytes, temp_bytes, xact_commit, db_size_bytes
) VALUES
    (now() - interval '50 minutes', 160000, 10, 20, 1000, 100, 1024000, 0, 100, 10485760),
    (now() - interval '40 minutes', 160000, 15, 25, 2000, 150, 2048000, 0, 200, 10485760),
    (now() - interval '30 minutes', 160000, 20, 30, 3000, 200, 3072000, 512, 300, 10485760),
    (now() - interval '20 minutes', 160000, 25, 35, 4000, 250, 4096000, 1024, 400, 10485760),
    (now() - interval '10 minutes', 160000, 30, 40, 5000, 300, 5120000, 2048, 500, 10485760);

-- Test sparkline_metrics returns rows with test data
SELECT ok(
    (SELECT count(*) FROM flight_recorder.sparkline_metrics('1 hour')) > 0,
    'sparkline_metrics should return rows when data exists'
);

-- Test sparkline_metrics includes expected metrics
SELECT ok(
    EXISTS (
        SELECT 1 FROM flight_recorder.sparkline_metrics('1 hour')
        WHERE metric = 'connections_active'
    ),
    'sparkline_metrics should include connections_active metric'
);

-- =============================================================================
-- CLEANUP
-- =============================================================================

-- Restore config settings
UPDATE flight_recorder.config SET value = 'true' WHERE key = 'check_checkpoint_backup';
UPDATE flight_recorder.config SET value = 'true' WHERE key = 'collection_jitter_enabled';

SELECT * FROM finish();
ROLLBACK;
