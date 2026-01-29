-- =============================================================================
-- Migration: X.Y to X.Z
-- =============================================================================
-- Description: [Brief description of what this migration does]
--
-- Changes:
--   - [List of schema changes]
--   - [New tables/columns/functions]
--   - [Renamed or removed items]
--
-- Data preservation: [Describe how existing data is preserved]
-- =============================================================================

\set ON_ERROR_STOP on

BEGIN;

-- =============================================================================
-- Step 1: Version Guard
-- =============================================================================
-- Ensure we're migrating from the expected version
DO $$
DECLARE
    v_current TEXT;
    v_expected TEXT := 'X.Y';  -- Source version
    v_target TEXT := 'X.Z';    -- Target version
BEGIN
    SELECT value INTO v_current
    FROM flight_recorder.config WHERE key = 'schema_version';

    IF v_current IS NULL THEN
        RAISE EXCEPTION 'schema_version not found. Is Flight Recorder installed?';
    END IF;

    IF v_current != v_expected THEN
        RAISE EXCEPTION 'Migration X.Y→X.Z requires version %, found %', v_expected, v_current;
    END IF;

    RAISE NOTICE 'Migrating from % to %...', v_expected, v_target;
END $$;

-- =============================================================================
-- Step 2: Schema Changes (Data Preserving)
-- =============================================================================

-- Example: Add a new column (safe, no data loss)
-- ALTER TABLE flight_recorder.snapshots
--     ADD COLUMN IF NOT EXISTS new_metric BIGINT;

-- Example: Add a new table
-- CREATE TABLE IF NOT EXISTS flight_recorder.new_table (
--     id SERIAL PRIMARY KEY,
--     ...
-- );

-- Example: Rename a table (create view for backwards compatibility)
-- ALTER TABLE flight_recorder.old_name RENAME TO new_name;
-- CREATE OR REPLACE VIEW flight_recorder.old_name AS
--     SELECT * FROM flight_recorder.new_name;

-- Example: Add index for performance
-- CREATE INDEX IF NOT EXISTS idx_new_index
--     ON flight_recorder.some_table (some_column);

-- =============================================================================
-- Step 3: Function Updates
-- =============================================================================
-- Use CREATE OR REPLACE for all function changes
-- (Functions are idempotent, so they're safe to recreate)

-- CREATE OR REPLACE FUNCTION flight_recorder.updated_function()
-- ...

-- =============================================================================
-- Step 4: Data Migrations (if needed)
-- =============================================================================
-- Migrate data from old format to new format
-- Be careful with large tables - consider batching

-- Example: Backfill new column with default value
-- UPDATE flight_recorder.snapshots
-- SET new_metric = 0
-- WHERE new_metric IS NULL;

-- =============================================================================
-- Step 5: Update Version
-- =============================================================================
UPDATE flight_recorder.config
SET value = 'X.Z', updated_at = now()
WHERE key = 'schema_version';

COMMIT;

-- =============================================================================
-- Step 6: Post-migration verification
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
END $$;
