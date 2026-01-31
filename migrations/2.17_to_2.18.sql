-- =============================================================================
-- Migration: 2.17 to 2.18
-- =============================================================================
-- Description: Enhanced SQLite Export with Schema Reference
--
-- Improves export_sql() with better AI guidance to prevent query errors:
--   - Adds _schema table with all column names/types for every exported table
--   - Adds step 0 to _guide: "BEFORE CUSTOM QUERIES" - check schema first
--   - Adds 0_schema tier examples for schema lookups
--   - Adds data_inventory, system_health examples to Tier 1
--   - Adds wait_events_from_samples fallback when aggregates empty
--   - Adds cache_hit_ratio, wal_activity, transaction_rate examples
--   - Expands _columns with wait_event_aggregates and wait_samples_archive
--   - Documents metadata tables (_schema, _examples, _columns, etc.) in _tables
--   - Uses INSERT OR REPLACE for idempotent exports
--
-- Data preservation: No schema changes, function-only update
-- =============================================================================

\set ON_ERROR_STOP on

BEGIN;

-- =============================================================================
-- Step 1: Version Guard
-- =============================================================================
DO $$
DECLARE
    v_current TEXT;
    v_expected TEXT := '2.17';
    v_target TEXT := '2.18';
BEGIN
    SELECT value INTO v_current
    FROM flight_recorder.config WHERE key = 'schema_version';

    IF v_current IS NULL THEN
        RAISE EXCEPTION 'schema_version not found. Is Flight Recorder installed?';
    END IF;

    IF v_current != v_expected THEN
        RAISE EXCEPTION 'Migration 2.17->2.18 requires version %, found %', v_expected, v_current;
    END IF;

    RAISE NOTICE 'Migrating from % to %...', v_expected, v_target;
END $$;

-- =============================================================================
-- Step 2: Function Update
-- =============================================================================
-- Recreate export_sql with enhanced AI guidance and schema reference

\i ../install.sql

-- Note: The full install.sql is run which replaces the function.
-- This is safe because export_sql is CREATE OR REPLACE.

-- =============================================================================
-- Step 3: Update Version
-- =============================================================================
UPDATE flight_recorder.config
SET value = '2.18', updated_at = now()
WHERE key = 'schema_version';

COMMIT;

-- =============================================================================
-- Step 4: Post-migration verification
-- =============================================================================
DO $$
DECLARE
    v_version TEXT;
BEGIN
    SELECT value INTO v_version
    FROM flight_recorder.config WHERE key = 'schema_version';

    RAISE NOTICE '';
    RAISE NOTICE '=== Migration Complete ===';
    RAISE NOTICE 'Now at version: %', v_version;
    RAISE NOTICE '';
    RAISE NOTICE 'Changes to export_sql():';
    RAISE NOTICE '  - New _schema table with all column names/types';
    RAISE NOTICE '  - Step 0 in _guide: check schema before custom queries';
    RAISE NOTICE '  - New 0_schema tier with schema lookup examples';
    RAISE NOTICE '  - New data_inventory and system_health examples';
    RAISE NOTICE '  - Fallback wait_events_from_samples for empty aggregates';
    RAISE NOTICE '  - cache_hit_ratio, wal_activity, transaction_rate examples';
    RAISE NOTICE '  - Extended _columns documentation';
    RAISE NOTICE '  - INSERT OR REPLACE for idempotent exports';
    RAISE NOTICE '';
END $$;
