-- =============================================================================
-- Migration: 2.3 to 2.4
-- =============================================================================
-- Description: Add client_addr to activity sampling
--
-- Changes:
--   - Add client_addr column to activity_samples_ring
--   - Add client_addr column to activity_samples_archive
--
-- Data preservation: Existing data unchanged, new column will be NULL for
--                    historical samples and populated going forward
-- =============================================================================

\set ON_ERROR_STOP on

BEGIN;

-- =============================================================================
-- Step 1: Version Guard
-- =============================================================================
DO $$
DECLARE
    v_current TEXT;
    v_expected TEXT := '2.3';
    v_target TEXT := '2.4';
BEGIN
    SELECT value INTO v_current
    FROM flight_recorder.config WHERE key = 'schema_version';

    IF v_current IS NULL THEN
        RAISE EXCEPTION 'schema_version not found. Is Flight Recorder installed?';
    END IF;

    IF v_current != v_expected THEN
        RAISE EXCEPTION 'Migration 2.3->2.4 requires version %, found %', v_expected, v_current;
    END IF;

    RAISE NOTICE 'Migrating from % to %...', v_expected, v_target;
END $$;

-- =============================================================================
-- Step 2: Schema Changes (Data Preserving)
-- =============================================================================

-- Add client_addr column to activity_samples_ring (UNLOGGED)
ALTER TABLE flight_recorder.activity_samples_ring
    ADD COLUMN IF NOT EXISTS client_addr INET;

-- Add client_addr column to activity_samples_archive
ALTER TABLE flight_recorder.activity_samples_archive
    ADD COLUMN IF NOT EXISTS client_addr INET;

-- =============================================================================
-- Step 3: Update Version
-- =============================================================================
UPDATE flight_recorder.config
SET value = '2.4', updated_at = now()
WHERE key = 'schema_version';

COMMIT;

-- =============================================================================
-- Step 4: Post-migration verification
-- =============================================================================
DO $$
DECLARE
    v_version TEXT;
    v_ring_col_exists BOOLEAN;
    v_archive_col_exists BOOLEAN;
BEGIN
    SELECT value INTO v_version
    FROM flight_recorder.config WHERE key = 'schema_version';

    -- Verify client_addr column exists in activity_samples_ring
    SELECT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'flight_recorder'
          AND table_name = 'activity_samples_ring'
          AND column_name = 'client_addr'
    ) INTO v_ring_col_exists;

    -- Verify client_addr column exists in activity_samples_archive
    SELECT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'flight_recorder'
          AND table_name = 'activity_samples_archive'
          AND column_name = 'client_addr'
    ) INTO v_archive_col_exists;

    IF NOT v_ring_col_exists THEN
        RAISE WARNING 'client_addr column not found in activity_samples_ring';
    END IF;

    IF NOT v_archive_col_exists THEN
        RAISE WARNING 'client_addr column not found in activity_samples_archive';
    END IF;

    RAISE NOTICE '';
    RAISE NOTICE '=== Migration Complete ===';
    RAISE NOTICE 'Now at version: %', v_version;
    RAISE NOTICE 'Added client_addr to activity sampling:';
    RAISE NOTICE '  - activity_samples_ring.client_addr';
    RAISE NOTICE '  - activity_samples_archive.client_addr';
    RAISE NOTICE '';
    RAISE NOTICE 'Query client IP addresses:';
    RAISE NOTICE '  SELECT usename, application_name, client_addr FROM flight_recorder.recent_activity;';
    RAISE NOTICE '';
END $$;
