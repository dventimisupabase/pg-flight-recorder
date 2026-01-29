-- =============================================================================
-- Migration: 2.1 to 2.2
-- =============================================================================
-- Description: Add configurable ring buffer slots
--
-- Changes:
--   - Add ring_buffer_slots config parameter (default 120)
--   - Expand samples_ring CHECK constraint to allow up to 2880 slots
--
-- Data preservation: Ring buffer data preserved (UNLOGGED, transient)
-- =============================================================================

\set ON_ERROR_STOP on

BEGIN;

-- =============================================================================
-- Step 1: Version Guard
-- =============================================================================
DO $$
DECLARE
    v_current TEXT;
    v_expected TEXT := '2.1';
    v_target TEXT := '2.2';
BEGIN
    SELECT value INTO v_current
    FROM flight_recorder.config WHERE key = 'schema_version';

    IF v_current IS NULL THEN
        RAISE EXCEPTION 'schema_version not found. Is Flight Recorder installed?';
    END IF;

    IF v_current != v_expected THEN
        RAISE EXCEPTION 'Migration 2.1->2.2 requires version %, found %', v_expected, v_current;
    END IF;

    RAISE NOTICE 'Migrating from % to %...', v_expected, v_target;
END $$;

-- =============================================================================
-- Step 2: Add ring_buffer_slots config parameter
-- =============================================================================
INSERT INTO flight_recorder.config (key, value)
VALUES ('ring_buffer_slots', '120')
ON CONFLICT (key) DO NOTHING;

-- =============================================================================
-- Step 3: Expand samples_ring CHECK constraint
-- =============================================================================
-- The samples_ring table has a CHECK constraint limiting slot_id.
-- We need to expand it from < 120 to < 2880 for configurable ring buffer sizes.

-- Drop the old constraint and add the new one
ALTER TABLE flight_recorder.samples_ring
    DROP CONSTRAINT IF EXISTS samples_ring_slot_id_check;

ALTER TABLE flight_recorder.samples_ring
    ADD CONSTRAINT samples_ring_slot_id_check CHECK (slot_id >= 0 AND slot_id < 2880);

-- =============================================================================
-- Step 4: Update Version
-- =============================================================================
UPDATE flight_recorder.config
SET value = '2.2', updated_at = now()
WHERE key = 'schema_version';

COMMIT;

-- =============================================================================
-- Step 5: Post-migration verification
-- =============================================================================
DO $$
DECLARE
    v_version TEXT;
    v_slots_exists BOOLEAN;
BEGIN
    SELECT value INTO v_version
    FROM flight_recorder.config WHERE key = 'schema_version';

    -- Verify ring_buffer_slots config exists
    SELECT EXISTS (
        SELECT 1 FROM flight_recorder.config
        WHERE key = 'ring_buffer_slots'
    ) INTO v_slots_exists;

    IF NOT v_slots_exists THEN
        RAISE WARNING 'ring_buffer_slots config parameter not found';
    END IF;

    RAISE NOTICE '';
    RAISE NOTICE '=== Migration Complete ===';
    RAISE NOTICE 'Now at version: %', v_version;
    RAISE NOTICE 'Added configurable ring buffer slots (72-2880 range)';
    RAISE NOTICE '';
    RAISE NOTICE 'To change ring buffer size:';
    RAISE NOTICE '  1. UPDATE flight_recorder.config SET value = ''360'' WHERE key = ''ring_buffer_slots'';';
    RAISE NOTICE '  2. SELECT flight_recorder.rebuild_ring_buffers();';
    RAISE NOTICE '';
END $$;
