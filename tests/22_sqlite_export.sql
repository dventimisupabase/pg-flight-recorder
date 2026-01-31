-- =============================================================================
-- pg-flight-recorder pgTAP Tests - SQLite Export
-- =============================================================================
-- Tests: export_sql() function for SQLite export
-- Test count: 21
-- =============================================================================

BEGIN;
SELECT plan(21);

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

-- =============================================================================
-- 5. IN-DATABASE DOCUMENTATION (6 tests)
-- =============================================================================

-- Example queries table
SELECT ok(
    (SELECT flight_recorder.export_sql() LIKE '%CREATE TABLE IF NOT EXISTS _examples%'),
    'Export should contain _examples table with query examples'
);

SELECT ok(
    (SELECT flight_recorder.export_sql() LIKE '%1_quick_status%'),
    'Export _examples should contain quick status queries'
);

SELECT ok(
    (SELECT flight_recorder.export_sql() LIKE '%3_drill_down%'),
    'Export _examples should contain drill-down queries'
);

-- Glossary table
SELECT ok(
    (SELECT flight_recorder.export_sql() LIKE '%CREATE TABLE IF NOT EXISTS _glossary%'),
    'Export should contain _glossary table with term definitions'
);

SELECT ok(
    (SELECT flight_recorder.export_sql() LIKE '%wait_event%definition%'),
    'Export _glossary should define wait_event'
);

-- Columns reference table
SELECT ok(
    (SELECT flight_recorder.export_sql() LIKE '%CREATE TABLE IF NOT EXISTS _columns%'),
    'Export should contain _columns table with column explanations'
);

SELECT * FROM finish();
ROLLBACK;
