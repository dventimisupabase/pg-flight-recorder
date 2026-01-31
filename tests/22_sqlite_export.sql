-- =============================================================================
-- pg-flight-recorder pgTAP Tests - SQLite Export
-- =============================================================================
-- Tests: export_sql() function for SQLite export
-- Test count: 15
-- =============================================================================

BEGIN;
SELECT plan(15);

-- =============================================================================
-- 1. FUNCTION EXISTS (2 tests)
-- =============================================================================

SELECT has_function(
    'flight_recorder',
    'export_sql',
    'Function export_sql() should exist'
);

SELECT has_function(
    'flight_recorder',
    'export_sql',
    ARRAY['interval'],
    'Function export_sql(interval) should exist'
);

-- =============================================================================
-- 2. BASIC OUTPUT STRUCTURE (6 tests)
-- =============================================================================

-- Test that output contains SQLite pragmas
SELECT ok(
    (SELECT flight_recorder.export_sql() LIKE '%PRAGMA journal_mode=WAL%'),
    'Export should contain PRAGMA journal_mode=WAL'
);

SELECT ok(
    (SELECT flight_recorder.export_sql() LIKE '%BEGIN TRANSACTION%'),
    'Export should contain BEGIN TRANSACTION for performance'
);

SELECT ok(
    (SELECT flight_recorder.export_sql() LIKE '%COMMIT;%'),
    'Export should contain COMMIT'
);

-- Test that core tables are included
SELECT ok(
    (SELECT flight_recorder.export_sql() LIKE '%CREATE TABLE IF NOT EXISTS "snapshots"%'),
    'Export should contain CREATE TABLE for snapshots'
);

SELECT ok(
    (SELECT flight_recorder.export_sql() LIKE '%CREATE TABLE IF NOT EXISTS "query_storms"%'),
    'Export should contain CREATE TABLE for query_storms'
);

SELECT ok(
    (SELECT flight_recorder.export_sql() LIKE '%CREATE TABLE IF NOT EXISTS "config"%'),
    'Export should contain CREATE TABLE for config'
);

-- =============================================================================
-- 3. AI METHODOLOGY GUIDE (4 tests)
-- =============================================================================

SELECT ok(
    (SELECT flight_recorder.export_sql() LIKE '%CREATE TABLE IF NOT EXISTS _guide%'),
    'Export should contain _guide table for AI methodology'
);

SELECT ok(
    (SELECT flight_recorder.export_sql() LIKE '%START HERE%'),
    'Export _guide should contain START HERE step'
);

SELECT ok(
    (SELECT flight_recorder.export_sql() LIKE '%LOW-ORDER APPROXIMATION%'),
    'Export _guide should contain LOW-ORDER APPROXIMATION step'
);

SELECT ok(
    (SELECT flight_recorder.export_sql() LIKE '%CREATE TABLE IF NOT EXISTS _tables%'),
    'Export should contain _tables for AI context'
);

-- =============================================================================
-- 4. METADATA AND INDEXES (3 tests)
-- =============================================================================

SELECT ok(
    (SELECT flight_recorder.export_sql() LIKE '%CREATE TABLE IF NOT EXISTS _export_metadata%'),
    'Export should contain _export_metadata table'
);

SELECT ok(
    (SELECT flight_recorder.export_sql() LIKE '%CREATE INDEX IF NOT EXISTS idx_snapshots_captured_at%'),
    'Export should contain index on snapshots.captured_at'
);

-- Test that since filter works (should still have structure, possibly less data)
SELECT ok(
    (SELECT flight_recorder.export_sql('1 second'::interval) LIKE '%CREATE TABLE IF NOT EXISTS "snapshots"%'),
    'Export with since filter should still create tables'
);

SELECT * FROM finish();
ROLLBACK;
