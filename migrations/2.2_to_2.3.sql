-- =============================================================================
-- Migration: 2.2 to 2.3
-- =============================================================================
-- Description: Add XID wraparound forecasting metrics
--
-- Changes:
--   - Add datfrozenxid_age column to snapshots table for database-level XID age
--   - Add relfrozenxid_age column to table_snapshots table for per-table XID age
--
-- Data preservation: Existing data unchanged, new columns will be NULL for
--                    historical snapshots and populated going forward
-- =============================================================================

\set ON_ERROR_STOP on

BEGIN;

-- =============================================================================
-- Step 1: Version Guard
-- =============================================================================
DO $$
DECLARE
    v_current TEXT;
    v_expected TEXT := '2.2';
    v_target TEXT := '2.3';
BEGIN
    SELECT value INTO v_current
    FROM flight_recorder.config WHERE key = 'schema_version';

    IF v_current IS NULL THEN
        RAISE EXCEPTION 'schema_version not found. Is Flight Recorder installed?';
    END IF;

    IF v_current != v_expected THEN
        RAISE EXCEPTION 'Migration 2.2->2.3 requires version %, found %', v_expected, v_current;
    END IF;

    RAISE NOTICE 'Migrating from % to %...', v_expected, v_target;
END $$;

-- =============================================================================
-- Step 2: Schema Changes (Data Preserving)
-- =============================================================================

-- Add datfrozenxid_age column to snapshots table
ALTER TABLE flight_recorder.snapshots
    ADD COLUMN IF NOT EXISTS datfrozenxid_age INTEGER;

-- Add relfrozenxid_age column to table_snapshots table
ALTER TABLE flight_recorder.table_snapshots
    ADD COLUMN IF NOT EXISTS relfrozenxid_age INTEGER;

-- =============================================================================
-- Step 3: Update Version
-- =============================================================================
UPDATE flight_recorder.config
SET value = '2.3', updated_at = now()
WHERE key = 'schema_version';

COMMIT;

-- =============================================================================
-- Step 4: Post-migration verification
-- =============================================================================
DO $$
DECLARE
    v_version TEXT;
    v_snapshots_col_exists BOOLEAN;
    v_table_snapshots_col_exists BOOLEAN;
BEGIN
    SELECT value INTO v_version
    FROM flight_recorder.config WHERE key = 'schema_version';

    -- Verify datfrozenxid_age column exists in snapshots
    SELECT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'flight_recorder'
          AND table_name = 'snapshots'
          AND column_name = 'datfrozenxid_age'
    ) INTO v_snapshots_col_exists;

    -- Verify relfrozenxid_age column exists in table_snapshots
    SELECT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'flight_recorder'
          AND table_name = 'table_snapshots'
          AND column_name = 'relfrozenxid_age'
    ) INTO v_table_snapshots_col_exists;

    IF NOT v_snapshots_col_exists THEN
        RAISE WARNING 'datfrozenxid_age column not found in snapshots table';
    END IF;

    IF NOT v_table_snapshots_col_exists THEN
        RAISE WARNING 'relfrozenxid_age column not found in table_snapshots table';
    END IF;

    RAISE NOTICE '';
    RAISE NOTICE '=== Migration Complete ===';
    RAISE NOTICE 'Now at version: %', v_version;
    RAISE NOTICE 'Added XID wraparound forecasting columns:';
    RAISE NOTICE '  - snapshots.datfrozenxid_age (database-level)';
    RAISE NOTICE '  - table_snapshots.relfrozenxid_age (per-table)';
    RAISE NOTICE '';
    RAISE NOTICE 'XID age thresholds (from ring_buffer_health):';
    RAISE NOTICE '  - Warning: > 100,000,000';
    RAISE NOTICE '  - Critical: > 200,000,000';
    RAISE NOTICE '';
END $$;
