-- =============================================================================
-- Migration: 2.4 to 2.5
-- =============================================================================
-- Description: Targeted statistics enhancements
--
-- Changes:
--   - Add backend_start, xact_start columns to activity_samples_ring
--   - Add backend_start, xact_start columns to activity_samples_archive
--   - Add archiver columns to snapshots table
--   - Create vacuum_progress_snapshots table
--
-- Data preservation: Existing data unchanged, new columns will be NULL for
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
    v_expected TEXT := '2.4';
    v_target TEXT := '2.5';
BEGIN
    SELECT value INTO v_current
    FROM flight_recorder.config WHERE key = 'schema_version';

    IF v_current IS NULL THEN
        RAISE EXCEPTION 'schema_version not found. Is Flight Recorder installed?';
    END IF;

    IF v_current != v_expected THEN
        RAISE EXCEPTION 'Migration 2.4->2.5 requires version %, found %', v_expected, v_current;
    END IF;

    RAISE NOTICE 'Migrating from % to %...', v_expected, v_target;
END $$;

-- =============================================================================
-- Step 2: Schema Changes (Data Preserving)
-- =============================================================================

-- Add backend_start and xact_start columns to activity_samples_ring (UNLOGGED)
ALTER TABLE flight_recorder.activity_samples_ring
    ADD COLUMN IF NOT EXISTS backend_start TIMESTAMPTZ;
ALTER TABLE flight_recorder.activity_samples_ring
    ADD COLUMN IF NOT EXISTS xact_start TIMESTAMPTZ;

-- Add backend_start and xact_start columns to activity_samples_archive
ALTER TABLE flight_recorder.activity_samples_archive
    ADD COLUMN IF NOT EXISTS backend_start TIMESTAMPTZ;
ALTER TABLE flight_recorder.activity_samples_archive
    ADD COLUMN IF NOT EXISTS xact_start TIMESTAMPTZ;

-- Add archiver columns to snapshots table
ALTER TABLE flight_recorder.snapshots
    ADD COLUMN IF NOT EXISTS archived_count BIGINT;
ALTER TABLE flight_recorder.snapshots
    ADD COLUMN IF NOT EXISTS last_archived_wal TEXT;
ALTER TABLE flight_recorder.snapshots
    ADD COLUMN IF NOT EXISTS last_archived_time TIMESTAMPTZ;
ALTER TABLE flight_recorder.snapshots
    ADD COLUMN IF NOT EXISTS failed_count BIGINT;
ALTER TABLE flight_recorder.snapshots
    ADD COLUMN IF NOT EXISTS last_failed_wal TEXT;
ALTER TABLE flight_recorder.snapshots
    ADD COLUMN IF NOT EXISTS last_failed_time TIMESTAMPTZ;
ALTER TABLE flight_recorder.snapshots
    ADD COLUMN IF NOT EXISTS archiver_stats_reset TIMESTAMPTZ;

-- Create vacuum_progress_snapshots table
CREATE TABLE IF NOT EXISTS flight_recorder.vacuum_progress_snapshots (
    snapshot_id         INTEGER REFERENCES flight_recorder.snapshots(id) ON DELETE CASCADE,
    pid                 INTEGER NOT NULL,
    datid               OID,
    datname             TEXT,
    relid               OID,
    relname             TEXT,
    phase               TEXT,
    heap_blks_total     BIGINT,
    heap_blks_scanned   BIGINT,
    heap_blks_vacuumed  BIGINT,
    index_vacuum_count  BIGINT,
    max_dead_tuples     BIGINT,
    num_dead_tuples     BIGINT,
    PRIMARY KEY (snapshot_id, pid)
);
COMMENT ON TABLE flight_recorder.vacuum_progress_snapshots IS 'Vacuum progress snapshots from pg_stat_progress_vacuum for monitoring long-running vacuums';

-- =============================================================================
-- Step 3: Update Version
-- =============================================================================
UPDATE flight_recorder.config
SET value = '2.5', updated_at = now()
WHERE key = 'schema_version';

COMMIT;

-- =============================================================================
-- Step 4: Post-migration verification
-- =============================================================================
DO $$
DECLARE
    v_version TEXT;
    v_ring_backend_start_exists BOOLEAN;
    v_ring_xact_start_exists BOOLEAN;
    v_archive_backend_start_exists BOOLEAN;
    v_archive_xact_start_exists BOOLEAN;
    v_archived_count_exists BOOLEAN;
    v_vacuum_progress_exists BOOLEAN;
BEGIN
    SELECT value INTO v_version
    FROM flight_recorder.config WHERE key = 'schema_version';

    -- Verify backend_start column exists in activity_samples_ring
    SELECT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'flight_recorder'
          AND table_name = 'activity_samples_ring'
          AND column_name = 'backend_start'
    ) INTO v_ring_backend_start_exists;

    -- Verify xact_start column exists in activity_samples_ring
    SELECT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'flight_recorder'
          AND table_name = 'activity_samples_ring'
          AND column_name = 'xact_start'
    ) INTO v_ring_xact_start_exists;

    -- Verify backend_start column exists in activity_samples_archive
    SELECT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'flight_recorder'
          AND table_name = 'activity_samples_archive'
          AND column_name = 'backend_start'
    ) INTO v_archive_backend_start_exists;

    -- Verify xact_start column exists in activity_samples_archive
    SELECT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'flight_recorder'
          AND table_name = 'activity_samples_archive'
          AND column_name = 'xact_start'
    ) INTO v_archive_xact_start_exists;

    -- Verify archived_count column exists in snapshots
    SELECT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'flight_recorder'
          AND table_name = 'snapshots'
          AND column_name = 'archived_count'
    ) INTO v_archived_count_exists;

    -- Verify vacuum_progress_snapshots table exists
    SELECT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'flight_recorder'
          AND table_name = 'vacuum_progress_snapshots'
    ) INTO v_vacuum_progress_exists;

    IF NOT v_ring_backend_start_exists THEN
        RAISE WARNING 'backend_start column not found in activity_samples_ring';
    END IF;

    IF NOT v_ring_xact_start_exists THEN
        RAISE WARNING 'xact_start column not found in activity_samples_ring';
    END IF;

    IF NOT v_archive_backend_start_exists THEN
        RAISE WARNING 'backend_start column not found in activity_samples_archive';
    END IF;

    IF NOT v_archive_xact_start_exists THEN
        RAISE WARNING 'xact_start column not found in activity_samples_archive';
    END IF;

    IF NOT v_archived_count_exists THEN
        RAISE WARNING 'archived_count column not found in snapshots';
    END IF;

    IF NOT v_vacuum_progress_exists THEN
        RAISE WARNING 'vacuum_progress_snapshots table not found';
    END IF;

    RAISE NOTICE '';
    RAISE NOTICE '=== Migration Complete ===';
    RAISE NOTICE 'Now at version: %', v_version;
    RAISE NOTICE 'Added targeted statistics enhancements:';
    RAISE NOTICE '  - activity_samples_ring.backend_start, xact_start';
    RAISE NOTICE '  - activity_samples_archive.backend_start, xact_start';
    RAISE NOTICE '  - snapshots archiver columns (archived_count, etc.)';
    RAISE NOTICE '  - vacuum_progress_snapshots table';
    RAISE NOTICE '';
    RAISE NOTICE 'Query session/transaction ages:';
    RAISE NOTICE '  SELECT usename, session_age, xact_age FROM flight_recorder.recent_activity;';
    RAISE NOTICE '';
    RAISE NOTICE 'Query vacuum progress:';
    RAISE NOTICE '  SELECT * FROM flight_recorder.recent_vacuum_progress;';
    RAISE NOTICE '';
    RAISE NOTICE 'Query archiver status:';
    RAISE NOTICE '  SELECT * FROM flight_recorder.archiver_status;';
    RAISE NOTICE '';
END $$;
