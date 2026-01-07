-- =============================================================================
-- OPTIMIZATION 3: Cache pg_control_checkpoint() Result
-- =============================================================================
-- Impact: Eliminate redundant calls to expensive function
-- Location: Multiple locations in snapshot() function (lines 1958-1959, 1994, etc.)
--
-- Current problem:
--   pg_control_checkpoint() is called twice in the same query:
--     (pg_control_checkpoint()).redo_lsn,
--     (pg_control_checkpoint()).checkpoint_time,
--
--   This function reads the pg_control file from disk, which is expensive.
--
-- Solution:
--   Call once, store in variable, reference fields from variable.
-- =============================================================================

-- At the top of flight_recorder.snapshot() function, add variable:
DECLARE
    ...existing variables...
    v_checkpoint_info RECORD;  -- NEW: Cache checkpoint info
BEGIN
    ...existing code...

    -- Section 1: Collect system stats (add this before the INSERT)
    BEGIN
        PERFORM flight_recorder._set_section_timeout();

        -- OPTIMIZATION: Call pg_control_checkpoint() once and cache result
        v_checkpoint_info := pg_control_checkpoint();

        -- Count active autovacuum workers
        SELECT count(*)::integer INTO v_autovacuum_workers
        FROM pg_stat_activity
        WHERE backend_type = 'autovacuum worker';

        ...rest of Section 1...
    END;

    -- Then in all INSERT statements, replace:
    -- BEFORE:
    --   (pg_control_checkpoint()).redo_lsn,
    --   (pg_control_checkpoint()).checkpoint_time,
    --
    -- AFTER:
    --   v_checkpoint_info.redo_lsn,
    --   v_checkpoint_info.checkpoint_time,

-- =============================================================================
-- FULL DIFF FOR PG17 INSERT (lines 1955-1972):
-- =============================================================================

    IF v_pg_version = 17 THEN
        -- PG17: checkpointer stats in pg_stat_checkpointer
        INSERT INTO flight_recorder.snapshots (
            captured_at, pg_version,
            wal_records, wal_fpi, wal_bytes, wal_write_time, wal_sync_time,
            checkpoint_lsn, checkpoint_time,
            ckpt_timed, ckpt_requested, ckpt_write_time, ckpt_sync_time, ckpt_buffers,
            bgw_buffers_clean, bgw_maxwritten_clean, bgw_buffers_alloc,
            bgw_buffers_backend, bgw_buffers_backend_fsync,
            autovacuum_workers, slots_count, slots_max_retained_wal,
            io_checkpointer_writes, io_checkpointer_write_time, io_checkpointer_fsyncs, io_checkpointer_fsync_time,
            io_autovacuum_writes, io_autovacuum_write_time,
            io_client_writes, io_client_write_time,
            io_bgwriter_writes, io_bgwriter_write_time,
            temp_files, temp_bytes
        )
        SELECT
            v_captured_at, v_pg_version,
            w.wal_records, w.wal_fpi, w.wal_bytes, w.wal_write_time, w.wal_sync_time,
            v_checkpoint_info.redo_lsn,        -- CHANGED: Use cached value
            v_checkpoint_info.checkpoint_time, -- CHANGED: Use cached value
            c.num_timed, c.num_requested, c.write_time, c.sync_time, c.buffers_written,
            b.buffers_clean, b.maxwritten_clean, b.buffers_alloc,
            NULL, NULL,  -- buffers_backend not in PG17
            v_autovacuum_workers, v_slots_count, v_slots_max_retained,
            v_io_ckpt_writes, v_io_ckpt_write_time, v_io_ckpt_fsyncs, v_io_ckpt_fsync_time,
            v_io_av_writes, v_io_av_write_time,
            v_io_client_writes, v_io_client_write_time,
            v_io_bgw_writes, v_io_bgw_write_time,
            v_temp_files, v_temp_bytes
        FROM pg_stat_wal w
        CROSS JOIN pg_stat_checkpointer c
        CROSS JOIN pg_stat_bgwriter b
        RETURNING id INTO v_snapshot_id;

    -- Repeat similar changes for PG16 and PG15 INSERT statements

-- =============================================================================
-- PERFORMANCE COMPARISON
-- =============================================================================
-- pg_control_checkpoint() overhead: ~50-100 microseconds (reads pg_control file)
--
-- BEFORE (current code):
--   - pg_control_checkpoint() calls per snapshot: 2
--   - Total overhead: ~100-200 microseconds per snapshot
--
-- AFTER (optimized):
--   - pg_control_checkpoint() calls per snapshot: 1
--   - Total overhead: ~50-100 microseconds per snapshot
--
-- NET IMPROVEMENT:
--   - Function calls: 50% reduction (2 → 1)
--   - Time saved: ~50-100 microseconds per snapshot (every 5 minutes)
--   - Annualized: ~1-2 seconds per year (negligible but correct)
--
-- VERDICT: LOW IMPACT but easy fix for correctness/cleanliness
-- =============================================================================

-- =============================================================================
-- WHY THIS MATTERS
-- =============================================================================
-- While the absolute time savings is small, this represents a CODE SMELL:
--   - Redundant I/O operations (reading pg_control file twice)
--   - Potential inconsistency (checkpoint could theoretically change between calls)
--   - Poor pattern (same function called multiple times with no caching)
--
-- Modern PostgreSQL JIT might not optimize this away, so explicit caching is better.
-- =============================================================================
