-- =============================================================================
-- Migration: 2.0 to 2.1
-- =============================================================================
-- Description: Add I/O read timing columns from pg_stat_io (PG16+)
--
-- Changes:
--   - Add io_*_reads columns for read counts by backend type
--   - Add io_*_read_time columns for read timing by backend type
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
    v_expected TEXT := '2.0';
    v_target TEXT := '2.1';
BEGIN
    SELECT value INTO v_current
    FROM flight_recorder.config WHERE key = 'schema_version';

    IF v_current IS NULL THEN
        RAISE EXCEPTION 'schema_version not found. Is Flight Recorder installed?';
    END IF;

    IF v_current != v_expected THEN
        RAISE EXCEPTION 'Migration 2.0->2.1 requires version %, found %', v_expected, v_current;
    END IF;

    RAISE NOTICE 'Migrating from % to %...', v_expected, v_target;
END $$;

-- =============================================================================
-- Step 2: Schema Changes (Data Preserving)
-- =============================================================================

-- Add read columns to snapshots table (before corresponding write columns)
ALTER TABLE flight_recorder.snapshots
    ADD COLUMN IF NOT EXISTS io_checkpointer_reads BIGINT,
    ADD COLUMN IF NOT EXISTS io_checkpointer_read_time DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS io_autovacuum_reads BIGINT,
    ADD COLUMN IF NOT EXISTS io_autovacuum_read_time DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS io_client_reads BIGINT,
    ADD COLUMN IF NOT EXISTS io_client_read_time DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS io_bgwriter_reads BIGINT,
    ADD COLUMN IF NOT EXISTS io_bgwriter_read_time DOUBLE PRECISION;

-- =============================================================================
-- Step 3: Update Version
-- =============================================================================
UPDATE flight_recorder.config
SET value = '2.1', updated_at = now()
WHERE key = 'schema_version';

COMMIT;

-- =============================================================================
-- Step 4: Post-migration verification
-- =============================================================================
DO $$
DECLARE
    v_version TEXT;
    v_col_count INTEGER;
BEGIN
    SELECT value INTO v_version
    FROM flight_recorder.config WHERE key = 'schema_version';

    -- Verify new columns exist
    SELECT count(*) INTO v_col_count
    FROM information_schema.columns
    WHERE table_schema = 'flight_recorder'
      AND table_name = 'snapshots'
      AND column_name IN (
          'io_checkpointer_reads', 'io_checkpointer_read_time',
          'io_autovacuum_reads', 'io_autovacuum_read_time',
          'io_client_reads', 'io_client_read_time',
          'io_bgwriter_reads', 'io_bgwriter_read_time'
      );

    IF v_col_count != 8 THEN
        RAISE WARNING 'Expected 8 new columns, found %', v_col_count;
    END IF;

    RAISE NOTICE '';
    RAISE NOTICE '=== Migration Complete ===';
    RAISE NOTICE 'Now at version: %', v_version;
    RAISE NOTICE 'Added % I/O read timing columns', v_col_count;
    RAISE NOTICE '';
    RAISE NOTICE 'Note: Read timing requires track_io_timing = on';
    RAISE NOTICE '';
END $$;
