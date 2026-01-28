-- =============================================================================
-- Migration: 2.5 to 2.6
-- =============================================================================
-- Description: Low-hanging fruit anomaly detection enhancements
--
-- Changes:
--   - Add database conflict columns to snapshots table (pg_stat_database_conflicts)
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
    v_expected TEXT := '2.5';
    v_target TEXT := '2.6';
BEGIN
    SELECT value INTO v_current
    FROM flight_recorder.config WHERE key = 'schema_version';

    IF v_current IS NULL THEN
        RAISE EXCEPTION 'schema_version not found. Is Flight Recorder installed?';
    END IF;

    IF v_current != v_expected THEN
        RAISE EXCEPTION 'Migration 2.5->2.6 requires version %, found %', v_expected, v_current;
    END IF;

    RAISE NOTICE 'Migrating from % to %...', v_expected, v_target;
END $$;

-- =============================================================================
-- Step 2: Schema Changes (Data Preserving)
-- =============================================================================

-- Add database conflict columns to snapshots table (from pg_stat_database_conflicts)
-- These track queries cancelled on standby replicas due to recovery conflicts
ALTER TABLE flight_recorder.snapshots
    ADD COLUMN IF NOT EXISTS confl_tablespace BIGINT;
ALTER TABLE flight_recorder.snapshots
    ADD COLUMN IF NOT EXISTS confl_lock BIGINT;
ALTER TABLE flight_recorder.snapshots
    ADD COLUMN IF NOT EXISTS confl_snapshot BIGINT;
ALTER TABLE flight_recorder.snapshots
    ADD COLUMN IF NOT EXISTS confl_bufferpin BIGINT;
ALTER TABLE flight_recorder.snapshots
    ADD COLUMN IF NOT EXISTS confl_deadlock BIGINT;
-- PG16+: logical replication slot conflicts (will be NULL on older versions)
ALTER TABLE flight_recorder.snapshots
    ADD COLUMN IF NOT EXISTS confl_active_logicalslot BIGINT;

-- =============================================================================
-- Step 3: Update Version
-- =============================================================================
UPDATE flight_recorder.config
SET value = '2.6', updated_at = now()
WHERE key = 'schema_version';

COMMIT;

-- =============================================================================
-- Step 4: Post-migration verification
-- =============================================================================
DO $$
DECLARE
    v_version TEXT;
    v_confl_tablespace_exists BOOLEAN;
    v_confl_lock_exists BOOLEAN;
    v_confl_snapshot_exists BOOLEAN;
    v_confl_bufferpin_exists BOOLEAN;
    v_confl_deadlock_exists BOOLEAN;
    v_confl_logicalslot_exists BOOLEAN;
BEGIN
    SELECT value INTO v_version
    FROM flight_recorder.config WHERE key = 'schema_version';

    -- Verify conflict columns exist in snapshots table
    SELECT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'flight_recorder'
          AND table_name = 'snapshots'
          AND column_name = 'confl_tablespace'
    ) INTO v_confl_tablespace_exists;

    SELECT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'flight_recorder'
          AND table_name = 'snapshots'
          AND column_name = 'confl_lock'
    ) INTO v_confl_lock_exists;

    SELECT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'flight_recorder'
          AND table_name = 'snapshots'
          AND column_name = 'confl_snapshot'
    ) INTO v_confl_snapshot_exists;

    SELECT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'flight_recorder'
          AND table_name = 'snapshots'
          AND column_name = 'confl_bufferpin'
    ) INTO v_confl_bufferpin_exists;

    SELECT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'flight_recorder'
          AND table_name = 'snapshots'
          AND column_name = 'confl_deadlock'
    ) INTO v_confl_deadlock_exists;

    SELECT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'flight_recorder'
          AND table_name = 'snapshots'
          AND column_name = 'confl_active_logicalslot'
    ) INTO v_confl_logicalslot_exists;

    IF NOT v_confl_tablespace_exists THEN
        RAISE WARNING 'confl_tablespace column not found in snapshots';
    END IF;

    IF NOT v_confl_lock_exists THEN
        RAISE WARNING 'confl_lock column not found in snapshots';
    END IF;

    IF NOT v_confl_snapshot_exists THEN
        RAISE WARNING 'confl_snapshot column not found in snapshots';
    END IF;

    IF NOT v_confl_bufferpin_exists THEN
        RAISE WARNING 'confl_bufferpin column not found in snapshots';
    END IF;

    IF NOT v_confl_deadlock_exists THEN
        RAISE WARNING 'confl_deadlock column not found in snapshots';
    END IF;

    IF NOT v_confl_logicalslot_exists THEN
        RAISE WARNING 'confl_active_logicalslot column not found in snapshots';
    END IF;

    RAISE NOTICE '';
    RAISE NOTICE '=== Migration Complete ===';
    RAISE NOTICE 'Now at version: %', v_version;
    RAISE NOTICE 'Added anomaly detection enhancements:';
    RAISE NOTICE '  - snapshots.confl_tablespace (replica conflict: tablespace)';
    RAISE NOTICE '  - snapshots.confl_lock (replica conflict: lock timeout)';
    RAISE NOTICE '  - snapshots.confl_snapshot (replica conflict: old snapshots)';
    RAISE NOTICE '  - snapshots.confl_bufferpin (replica conflict: buffer pins)';
    RAISE NOTICE '  - snapshots.confl_deadlock (replica conflict: deadlocks)';
    RAISE NOTICE '  - snapshots.confl_active_logicalslot (replica conflict: logical slots, PG16+)';
    RAISE NOTICE '';
    RAISE NOTICE 'New anomaly types in anomaly_report():';
    RAISE NOTICE '  - IDLE_IN_TRANSACTION: Sessions blocking vacuum/replication';
    RAISE NOTICE '  - DEAD_TUPLE_ACCUMULATION: Tables with high dead tuple ratio';
    RAISE NOTICE '  - VACUUM_STARVATION: Tables with growing dead tuples, no vacuum';
    RAISE NOTICE '  - CONNECTION_LEAK: Sessions open > 7 days';
    RAISE NOTICE '  - REPLICATION_LAG_GROWING: Replica lag trending upward';
    RAISE NOTICE '';
    RAISE NOTICE 'New view:';
    RAISE NOTICE '  SELECT * FROM flight_recorder.recent_idle_in_transaction;';
    RAISE NOTICE '';
END $$;
