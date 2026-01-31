-- =============================================================================
-- Migration: 2.7 to 2.8
-- =============================================================================
-- Description: OID Exhaustion Detection
--
-- Changes:
--   - Add max_catalog_oid column to snapshots (highest OID in pg_class)
--   - Add large_object_count column to snapshots (count from pg_largeobject_metadata)
--   - Add rate calculation functions for OID consumption analysis
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
    v_expected TEXT := '2.7';
    v_target TEXT := '2.8';
BEGIN
    SELECT value INTO v_current
    FROM flight_recorder.config WHERE key = 'schema_version';

    IF v_current IS NULL THEN
        RAISE EXCEPTION 'schema_version not found. Is Flight Recorder installed?';
    END IF;

    IF v_current != v_expected THEN
        RAISE EXCEPTION 'Migration 2.7->2.8 requires version %, found %', v_expected, v_current;
    END IF;

    RAISE NOTICE 'Migrating from % to %...', v_expected, v_target;
END $$;

-- =============================================================================
-- Step 2: Schema Changes (Data Preserving)
-- =============================================================================

-- Add OID exhaustion tracking columns to snapshots
ALTER TABLE flight_recorder.snapshots
    ADD COLUMN IF NOT EXISTS max_catalog_oid BIGINT;

ALTER TABLE flight_recorder.snapshots
    ADD COLUMN IF NOT EXISTS large_object_count BIGINT;

-- =============================================================================
-- Step 3: Rate Calculation Functions
-- =============================================================================

-- Calculates the rate of OID consumption over a time window
CREATE OR REPLACE FUNCTION flight_recorder.oid_consumption_rate(
    p_window INTERVAL
)
RETURNS NUMERIC
LANGUAGE plpgsql STABLE AS $$
DECLARE
    v_first_snapshot RECORD;
    v_last_snapshot RECORD;
    v_delta_oids BIGINT;
    v_delta_seconds NUMERIC;
BEGIN
    SELECT max_catalog_oid, captured_at
    INTO v_first_snapshot
    FROM flight_recorder.snapshots
    WHERE captured_at >= now() - p_window
      AND max_catalog_oid IS NOT NULL
    ORDER BY captured_at ASC
    LIMIT 1;

    SELECT max_catalog_oid, captured_at
    INTO v_last_snapshot
    FROM flight_recorder.snapshots
    WHERE captured_at >= now() - p_window
      AND max_catalog_oid IS NOT NULL
    ORDER BY captured_at DESC
    LIMIT 1;

    IF v_first_snapshot.captured_at IS NULL OR v_last_snapshot.captured_at IS NULL
       OR v_first_snapshot.captured_at = v_last_snapshot.captured_at THEN
        RETURN NULL;
    END IF;

    v_delta_oids := v_last_snapshot.max_catalog_oid - v_first_snapshot.max_catalog_oid;
    v_delta_seconds := EXTRACT(EPOCH FROM (v_last_snapshot.captured_at - v_first_snapshot.captured_at));

    IF v_delta_seconds <= 0 THEN
        RETURN NULL;
    END IF;

    RETURN ROUND(v_delta_oids::numeric / v_delta_seconds, 6);
END;
$$;
COMMENT ON FUNCTION flight_recorder.oid_consumption_rate(INTERVAL) IS 'Returns OID consumption rate (OIDs/second) over a time window';

-- Estimates time until OID exhaustion based on current consumption rate
CREATE OR REPLACE FUNCTION flight_recorder.time_to_oid_exhaustion()
RETURNS INTERVAL
LANGUAGE plpgsql STABLE AS $$
DECLARE
    v_current_max_oid BIGINT;
    v_consumption_rate NUMERIC;
    v_oid_max BIGINT := 4294967295;  -- 2^32 - 1
    v_remaining_oids BIGINT;
    v_seconds_to_exhaustion NUMERIC;
BEGIN
    SELECT max_catalog_oid
    INTO v_current_max_oid
    FROM flight_recorder.snapshots
    WHERE max_catalog_oid IS NOT NULL
    ORDER BY captured_at DESC
    LIMIT 1;

    IF v_current_max_oid IS NULL THEN
        RETURN NULL;
    END IF;

    -- Use 1-hour window for rate calculation
    v_consumption_rate := flight_recorder.oid_consumption_rate('1 hour'::interval);

    IF v_consumption_rate IS NULL OR v_consumption_rate <= 0 THEN
        RETURN NULL;  -- No consumption or negative rate
    END IF;

    v_remaining_oids := v_oid_max - v_current_max_oid;

    IF v_remaining_oids <= 0 THEN
        RETURN '0 seconds'::interval;
    END IF;

    v_seconds_to_exhaustion := v_remaining_oids::numeric / v_consumption_rate;

    RETURN make_interval(secs => v_seconds_to_exhaustion);
END;
$$;
COMMENT ON FUNCTION flight_recorder.time_to_oid_exhaustion() IS 'Estimates time until OID exhaustion based on consumption rate over the last hour';

-- =============================================================================
-- Step 4: Update Version
-- =============================================================================
UPDATE flight_recorder.config
SET value = '2.8', updated_at = now()
WHERE key = 'schema_version';

COMMIT;

-- =============================================================================
-- Step 5: Post-migration verification
-- =============================================================================
DO $$
DECLARE
    v_version TEXT;
    v_max_catalog_oid_exists BOOLEAN;
    v_large_object_count_exists BOOLEAN;
    v_oid_rate_exists BOOLEAN;
    v_time_to_exhaustion_exists BOOLEAN;
BEGIN
    SELECT value INTO v_version
    FROM flight_recorder.config WHERE key = 'schema_version';

    -- Verify columns exist
    SELECT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'flight_recorder'
          AND table_name = 'snapshots'
          AND column_name = 'max_catalog_oid'
    ) INTO v_max_catalog_oid_exists;

    SELECT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'flight_recorder'
          AND table_name = 'snapshots'
          AND column_name = 'large_object_count'
    ) INTO v_large_object_count_exists;

    -- Verify functions exist
    SELECT EXISTS (
        SELECT 1 FROM pg_proc p
        JOIN pg_namespace n ON p.pronamespace = n.oid
        WHERE n.nspname = 'flight_recorder' AND p.proname = 'oid_consumption_rate'
    ) INTO v_oid_rate_exists;

    SELECT EXISTS (
        SELECT 1 FROM pg_proc p
        JOIN pg_namespace n ON p.pronamespace = n.oid
        WHERE n.nspname = 'flight_recorder' AND p.proname = 'time_to_oid_exhaustion'
    ) INTO v_time_to_exhaustion_exists;

    IF NOT v_max_catalog_oid_exists THEN
        RAISE WARNING 'max_catalog_oid column not found in snapshots';
    END IF;

    IF NOT v_large_object_count_exists THEN
        RAISE WARNING 'large_object_count column not found in snapshots';
    END IF;

    IF NOT v_oid_rate_exists THEN
        RAISE WARNING 'oid_consumption_rate function not found';
    END IF;

    IF NOT v_time_to_exhaustion_exists THEN
        RAISE WARNING 'time_to_oid_exhaustion function not found';
    END IF;

    RAISE NOTICE '';
    RAISE NOTICE '=== Migration Complete ===';
    RAISE NOTICE 'Now at version: %', v_version;
    RAISE NOTICE 'Added OID exhaustion detection:';
    RAISE NOTICE '  - snapshots.max_catalog_oid (highest OID in pg_class)';
    RAISE NOTICE '  - snapshots.large_object_count (count from pg_largeobject_metadata)';
    RAISE NOTICE '';
    RAISE NOTICE 'New rate calculation functions:';
    RAISE NOTICE '  - oid_consumption_rate(window): OIDs/second over time window';
    RAISE NOTICE '  - time_to_oid_exhaustion(): Estimated time until OID exhaustion';
    RAISE NOTICE '';
END $$;
