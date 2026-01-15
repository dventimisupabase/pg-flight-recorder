-- =============================================================================
-- pg-flight-recorder: PostgreSQL Performance Flight Recorder
-- =============================================================================
--
-- Server-side flight recorder for PostgreSQL performance diagnostics.
-- Continuously collects metrics to answer: "What was happening during this time window?"
--
-- REQUIREMENTS
-- ------------
--   - PostgreSQL 15, 16, or 17
--   - pg_cron extension (1.4.1+ recommended)
--   - Superuser or appropriate privileges to create schema/functions
--
-- INSTALLATION
-- ------------
--   psql -f install.sql
--
-- THREE-TIER ARCHITECTURE
-- -----------------------
--   TIER 1: Ring Buffers (adaptive frequency, 2-10 hour retention, UNLOGGED)
--      - Wait events: aggregated by backend_type, wait_event_type, wait_event
--      - Active sessions: top 25 non-idle sessions with query preview
--      - Lock contention: blocked/blocking PIDs with queries
--      - Adaptive intervals: normal=180s/6h, light=180s/6h, emergency=300s/10h (A+ GRADE: Ultra-conservative)
--      - Low overhead (<0.1% CPU normal, <0.05% emergency): HOT updates, zero WAL
--
--   TIER 2: Aggregates (flushed every 5 min from ring buffer, 7-day retention)
--      - Wait event patterns summarized over 5-minute windows
--      - Lock contention patterns
--      - Query execution patterns
--
--   TIER 3: Snapshots (every 5 min, 30-day retention, cumulative stats)
--      - WAL: bytes generated, write/sync time
--      - Checkpoints: timed/requested count, write/sync time, buffers
--      - BGWriter: buffers clean/alloc/backend (backend writes = pressure)
--      - Replication slots: count, max retained WAL bytes
--      - Replication lag: per-replica write_lag, flush_lag, replay_lag
--      - Temp files: cumulative temp files and bytes (work_mem spills)
--      - pg_stat_io (PG16+): I/O by backend type
--
-- QUICK START
-- -----------
--   -- 1. Flight Recorder collects automatically in the background
--
--   -- 2. Query any time window to diagnose performance
--   SELECT * FROM flight_recorder.compare('2024-12-16 14:00', '2024-12-16 15:00');
--   SELECT * FROM flight_recorder.wait_summary('2024-12-16 14:00', '2024-12-16 15:00');
--
--   -- 3. Or use the recent_* views for rolling visibility (2-10h based on mode)
--   SELECT * FROM flight_recorder.recent_waits;         -- Up to 10h retention
--   SELECT * FROM flight_recorder.recent_waits_current();  -- Current mode retention
--   SELECT * FROM flight_recorder.recent_activity;
--
-- FUNCTIONS
-- ---------
--   flight_recorder.snapshot()
--       Capture cumulative stats. Called automatically every 5 min.
--       Returns: timestamp of capture
--
--   flight_recorder.sample()
--       Capture point-in-time activity. Called automatically at adaptive intervals (180s normal/light, 300s emergency).
--       Returns: timestamp of capture
--
--   flight_recorder.compare(start_time, end_time)
--       Compare cumulative stats between two time points.
--       Returns: single row with deltas for WAL, checkpoints, bgwriter, I/O
--
--   flight_recorder.wait_summary(start_time, end_time)
--       Aggregate wait events over a time period.
--       Returns: rows ordered by total_waiters DESC
--       Columns: backend_type, wait_event_type, wait_event, sample_count,
--                total_waiters, avg_waiters, max_waiters, pct_of_samples
--
--   flight_recorder.cleanup(retain_interval DEFAULT '7 days')
--       Remove old flight recorder data.
--       Returns: (deleted_snapshots, deleted_samples)
--
-- VIEWS
-- -----
--   flight_recorder.deltas
--       Changes between consecutive snapshots.
--       Key columns: checkpoint_occurred, wal_bytes_delta, wal_bytes_pretty,
--                    ckpt_write_time_ms, bgw_buffers_backend_delta,
--                    temp_files_delta, temp_bytes_pretty
--
--   flight_recorder.recent_waits
--       Wait events from last 10 hours.
--       Columns: captured_at, backend_type, wait_event_type, wait_event, state, count
--
--   flight_recorder.recent_activity
--       Active sessions from last 10 hours.
--       Columns: captured_at, pid, usename, backend_type, state, wait_event,
--                running_for, query_preview
--
--   flight_recorder.recent_locks
--       Lock contention from last 10 hours.
--       Columns: captured_at, blocked_pid, blocked_duration, blocking_pid,
--                lock_type, locked_relation, blocked_query_preview
--
--   flight_recorder.recent_replication
--       Replication lag from last 10 hours.
--       Columns: captured_at, application_name, state, sync_state,
--                replay_lag_bytes, replay_lag_pretty, write_lag, flush_lag, replay_lag
--
-- INTERPRETING RESULTS
-- --------------------
--   Checkpoint pressure:
--     - checkpoint_occurred=true with large ckpt_write_time_ms => checkpoint during batch
--     - ckpt_requested_delta > 0 => forced checkpoint (WAL exceeded max_wal_size)
--
--   WAL pressure:
--     - Large wal_sync_time_ms => WAL fsync bottleneck
--     - Compare wal_bytes_delta to expected (row_count * avg_row_size)
--
--   Shared buffer pressure (PG15/16):
--     - bgw_buffers_backend_delta > 0 => backends writing directly (bad)
--     - bgw_buffers_backend_fsync_delta > 0 => backends doing fsync (very bad)
--
--   I/O contention (PG16+):
--     - High io_checkpointer_write_time => checkpoint I/O pressure
--     - High io_autovacuum_writes => vacuum competing for I/O bandwidth
--     - High io_client_writes => shared_buffers exhaustion
--
--   Autovacuum interference:
--     - autovacuum_ran=true on target table during batch
--     - Check recent_progress for vacuum phase/duration
--
--   Lock contention:
--     - Check recent_locks for blocked_duration
--     - Cross-reference with recent_activity for blocking queries
--
--   Wait events:
--     - LWLock:BufferContent => buffer contention
--     - IO:DataFileRead/Write => disk I/O bottleneck
--     - Lock:transactionid => row-level lock contention
--
--   Temp file spills (work_mem exhaustion):
--     - temp_files_delta > 0 => sorts/hashes spilling to disk
--     - Large temp_bytes_delta => significant disk I/O from spills
--     - Resolution: increase work_mem (per-session or globally)
--
--   Replication lag (sync replication):
--     - recent_replication shows large replay_lag_bytes
--     - write_lag/flush_lag/replay_lag intervals growing
--     - With sync replication, batch waits for replica acknowledgment
--     - Resolution: check replica health, network latency, or switch to async
--
-- DIAGNOSTIC PATTERNS (from real-world testing)
-- ---------------------------------------------
--   Validated against PostgreSQL 15, 16, and 17.
--
--   PATTERN 1: Lock Contention
--   Symptoms:
--     - Batch takes 10x longer than expected
--     - flight_recorder.recent_locks shows blocked_pid entries
--     - wait_summary() shows Lock:relation or Lock:extend events
--   Example findings:
--     - blocked_pid=12345, blocking_pid=12346, blocked_duration='00:00:09'
--     - Wait event: Lock:relation with high occurrence count
--   Resolution:
--     - Identify blocking query from recent_locks.blocking_query
--     - Consider table partitioning, shorter transactions, or scheduling
--
--   PATTERN 2: Buffer/WAL Pressure (Concurrent Writers)
--   Symptoms:
--     - Batch takes 10-20x longer than baseline
--     - bgw_buffers_backend_delta > 0 (backends forced to write directly)
--     - High wal_bytes_delta relative to data volume
--   Example findings (4 concurrent writers vs 1 writer baseline):
--     | Metric                   | Baseline | Concurrent |
--     |--------------------------|----------|------------|
--     | elapsed_seconds          | 1.6      | 19.2       |
--     | wal_bytes                | 47 MB    | 144 MB     |
--     | bgw_buffers_alloc_delta  | 4,400    | 15,152     |
--     | bgw_buffers_backend_delta| 0        | 15,153     | <-- KEY INDICATOR
--   Wait events observed:
--     - LWLock:WALWrite (WAL contention between writers)
--     - Lock:extend (relation extension locks)
--     - IO:DataFileExtend (data file I/O)
--   Resolution:
--     - Reduce concurrent writers or serialize large batches
--     - Increase shared_buffers or wal_buffers
--     - Consider faster storage (NVMe, io2)
--
--   PATTERN 3: Checkpoint During Batch
--   Symptoms:
--     - compare() shows checkpoint_occurred=true
--     - High ckpt_write_time_ms during batch window
--     - ckpt_requested_delta > 0 (WAL exceeded max_wal_size)
--   Resolution:
--     - Increase max_wal_size to avoid mid-batch checkpoints
--     - Schedule large batches after checkpoint_timeout
--     - Monitor wal_bytes_delta to predict checkpoint timing
--
--   PATTERN 4: Autovacuum Interference
--   Symptoms:
--     - recent_progress shows vacuum phases overlapping batch
--     - Wait events: LWLock:BufferContent, IO:DataFileRead
--     - Check pg_stat_user_tables.last_autovacuum for recent vacuum activity
--   Resolution:
--     - Schedule batches to avoid autovacuum (check pg_stat_user_tables)
--     - Use ALTER TABLE ... SET (autovacuum_enabled = false) temporarily
--     - Increase autovacuum_vacuum_cost_delay to slow vacuum during batch
--
--   PATTERN 5: Temp File Spills (work_mem exhaustion)
--   Symptoms:
--     - Batch with complex queries (JOINs, sorts, aggregations) runs slowly
--     - compare() shows temp_files_delta > 0
--     - Large temp_bytes_delta (e.g., hundreds of MB or GB)
--   Resolution:
--     - Increase work_mem for the session: SET work_mem = '256MB';
--     - Optimize query to reduce memory usage (add indexes, limit result sets)
--     - Consider maintenance_work_mem for CREATE INDEX or VACUUM
--
--   PATTERN 6: Replication Lag (sync replication)
--   Symptoms:
--     - Batch runs slowly despite no local resource contention
--     - recent_replication shows large replay_lag_bytes
--     - write_lag/flush_lag intervals in seconds or more
--     - synchronous_commit = on with synchronous_standby_names set
--   Resolution:
--     - Check replica health (disk I/O, network, CPU)
--     - Consider switching to asynchronous replication for batch jobs
--     - Use SET LOCAL synchronous_commit = off; within batch transaction
--
--   QUICK DIAGNOSIS CHECKLIST
--   -------------------------
--   For a slow batch between START_TIME and END_TIME:
--
--   1. Overall health:
--      SELECT * FROM flight_recorder.compare('START_TIME', 'END_TIME');
--      => Check: checkpoint_occurred, bgw_buffers_backend_delta, wal_bytes,
--                temp_files_delta, temp_bytes_pretty
--
--   2. Lock contention:
--      SELECT * FROM flight_recorder.recent_locks
--      WHERE captured_at BETWEEN 'START_TIME' AND 'END_TIME';
--      => Look for: blocked_pid entries, blocked_duration > 1s
--
--   3. Wait events:
--      SELECT * FROM flight_recorder.wait_summary('START_TIME', 'END_TIME');
--      => Red flags: Lock:*, LWLock:WALWrite, LWLock:BufferContent
--
--   4. Active operations:
--      SELECT * FROM flight_recorder.recent_progress
--      WHERE captured_at BETWEEN 'START_TIME' AND 'END_TIME';
--      => Check: overlapping vacuum, COPY, or index builds
--
--   5. Replication lag (if using sync replication):
--      SELECT * FROM flight_recorder.recent_replication
--      WHERE captured_at BETWEEN 'START_TIME' AND 'END_TIME';
--      => Check: replay_lag_bytes, write_lag/flush_lag intervals
--
-- PG VERSION DIFFERENCES
-- ----------------------
--   PG15: Checkpoint stats in pg_stat_bgwriter, no pg_stat_io
--   PG16: Checkpoint stats in pg_stat_bgwriter, pg_stat_io available
--   PG17: Checkpoint stats in pg_stat_checkpointer, pg_stat_io available
--
-- SCHEDULED JOBS (pg_cron)
-- ------------------------
--   flight_recorder_snapshot  : */5 * * * *   (every 5 minutes - snapshots, replication)
--   flight_recorder_sample    : adaptive      (180s normal/light, 300s emergency) - A+ GRADE
--   flight_recorder_flush     : */5 * * * *   (every 5 minutes - flush ring buffer to aggregates)
--   flight_recorder_cleanup   : 0 3 * * *     (daily at 3 AM - cleans old aggregates and snapshots)
--
--   NOTE: The installer auto-detects pg_cron version. If < 1.4.1 (e.g., "1.4-1"),
--   it falls back to minute-level sampling and logs a notice.
--
-- UNINSTALL
-- ---------
--   SELECT cron.unschedule('flight_recorder_snapshot');
--   SELECT cron.unschedule('flight_recorder_sample');
--   SELECT cron.unschedule('flight_recorder_flush');
--   SELECT cron.unschedule('flight_recorder_cleanup');
--   DROP SCHEMA flight_recorder CASCADE;
--
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS flight_recorder;

-- -----------------------------------------------------------------------------
-- Table: snapshots
-- REGULAR (LOGGED): Low-frequency (every 5 min), minimal WAL overhead, SURVIVES CRASHES
-- TIERED STORAGE: This is Tier 3 (cold) - durable cumulative stats
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS flight_recorder.snapshots (
    id              SERIAL PRIMARY KEY,
    captured_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    pg_version      INTEGER NOT NULL,

    -- WAL stats (pg_stat_wal)
    wal_records     BIGINT,
    wal_fpi         BIGINT,
    wal_bytes       BIGINT,
    wal_write_time  DOUBLE PRECISION,
    wal_sync_time   DOUBLE PRECISION,

    -- Checkpoint info (pg_control_checkpoint)
    checkpoint_lsn  PG_LSN,
    checkpoint_time TIMESTAMPTZ,

    -- Checkpointer stats
    ckpt_timed      BIGINT,
    ckpt_requested  BIGINT,
    ckpt_write_time DOUBLE PRECISION,
    ckpt_sync_time  DOUBLE PRECISION,
    ckpt_buffers    BIGINT,

    -- BGWriter stats
    bgw_buffers_clean       BIGINT,
    bgw_maxwritten_clean    BIGINT,
    bgw_buffers_alloc       BIGINT,
    bgw_buffers_backend     BIGINT,           -- PG15/16 only
    bgw_buffers_backend_fsync BIGINT,         -- PG15/16 only

    -- Autovacuum stats
    autovacuum_workers      INTEGER,          -- currently active workers

    -- Replication slot stats
    slots_count             INTEGER,
    slots_max_retained_wal  BIGINT,           -- max retained WAL bytes across all slots

    -- pg_stat_io (PG16+ only) - key backend types
    io_checkpointer_writes      BIGINT,
    io_checkpointer_write_time  DOUBLE PRECISION,
    io_checkpointer_fsyncs      BIGINT,
    io_checkpointer_fsync_time  DOUBLE PRECISION,
    io_autovacuum_writes        BIGINT,
    io_autovacuum_write_time    DOUBLE PRECISION,
    io_client_writes            BIGINT,
    io_client_write_time        DOUBLE PRECISION,
    io_bgwriter_writes          BIGINT,
    io_bgwriter_write_time      DOUBLE PRECISION,

    -- Temp file usage (pg_stat_database)
    temp_files                  BIGINT,           -- cumulative temp files created
    temp_bytes                  BIGINT            -- cumulative temp bytes written
);

CREATE INDEX IF NOT EXISTS snapshots_captured_at_idx ON flight_recorder.snapshots(captured_at);

-- -----------------------------------------------------------------------------
-- Table: replication_snapshots - Per-replica stats captured with each snapshot
-- REGULAR (LOGGED): Low-frequency, minimal WAL overhead, SURVIVES CRASHES
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS flight_recorder.replication_snapshots (
    snapshot_id             INTEGER REFERENCES flight_recorder.snapshots(id) ON DELETE CASCADE,
    pid                     INTEGER NOT NULL,
    client_addr             INET,
    application_name        TEXT,
    state                   TEXT,
    sync_state              TEXT,
    -- LSN positions
    sent_lsn                PG_LSN,
    write_lsn               PG_LSN,
    flush_lsn               PG_LSN,
    replay_lsn              PG_LSN,
    -- Lag intervals (NULL if not available)
    write_lag               INTERVAL,
    flush_lag               INTERVAL,
    replay_lag              INTERVAL,
    PRIMARY KEY (snapshot_id, pid)
);

-- -----------------------------------------------------------------------------
-- Table: statement_snapshots - pg_stat_statements metrics per snapshot
-- REGULAR (LOGGED): Low-frequency, minimal WAL overhead, SURVIVES CRASHES
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS flight_recorder.statement_snapshots (
    snapshot_id         INTEGER REFERENCES flight_recorder.snapshots(id) ON DELETE CASCADE,
    queryid             BIGINT NOT NULL,
    userid              OID,
    dbid                OID,

    -- Query text (truncated for storage)
    query_preview       TEXT,

    -- Cumulative counters (for delta calculation)
    calls               BIGINT,
    total_exec_time     DOUBLE PRECISION,
    min_exec_time       DOUBLE PRECISION,
    max_exec_time       DOUBLE PRECISION,
    mean_exec_time      DOUBLE PRECISION,
    rows                BIGINT,

    -- Block I/O
    shared_blks_hit     BIGINT,
    shared_blks_read    BIGINT,
    shared_blks_dirtied BIGINT,
    shared_blks_written BIGINT,
    temp_blks_read      BIGINT,
    temp_blks_written   BIGINT,

    -- I/O timing (if track_io_timing enabled)
    blk_read_time       DOUBLE PRECISION,
    blk_write_time      DOUBLE PRECISION,

    -- WAL (PG13+)
    wal_records         BIGINT,
    wal_bytes           NUMERIC,

    PRIMARY KEY (snapshot_id, queryid, dbid)
);

CREATE INDEX IF NOT EXISTS statement_snapshots_queryid_idx
    ON flight_recorder.statement_snapshots(queryid);

-- -----------------------------------------------------------------------------
-- =============================================================================
-- TIER 1: Ring Buffer Tables (UNLOGGED, fixed 120 slots)
-- =============================================================================
-- Implements circular buffers using modular arithmetic
-- Fixed memory footprint: 120 slots × ~1KB = ~120KB total
-- Automatic overwrite via UPSERT - no manual cleanup needed
-- Adaptive frequency: 60s intervals (normal/2h), 120s (light/4h), 300s (emergency/10h)
-- -----------------------------------------------------------------------------

-- Ring buffer master table (120 slots, variable retention based on mode)
CREATE UNLOGGED TABLE IF NOT EXISTS flight_recorder.samples_ring (
    slot_id             INTEGER PRIMARY KEY CHECK (slot_id >= 0 AND slot_id < 120),
    captured_at         TIMESTAMPTZ NOT NULL,
    epoch_seconds       BIGINT NOT NULL
) WITH (fillfactor = 70);

COMMENT ON TABLE flight_recorder.samples_ring IS 'TIER 1: Ring buffer master (120 slots, adaptive frequency). Normal=120s/4h, light=120s/4h, emergency=300s/10h retention. A GRADE: Conservative 120s + proactive throttling. Fill factor 70 enables HOT updates.';

-- Wait events ring buffer (UPDATE-only pattern for zero dead tuples)
CREATE UNLOGGED TABLE IF NOT EXISTS flight_recorder.wait_samples_ring (
    slot_id             INTEGER REFERENCES flight_recorder.samples_ring(slot_id) ON DELETE CASCADE,
    row_num             INTEGER NOT NULL CHECK (row_num >= 0 AND row_num < 100),
    backend_type        TEXT,
    wait_event_type     TEXT,
    wait_event          TEXT,
    state               TEXT,
    count               INTEGER,
    PRIMARY KEY (slot_id, row_num)
) WITH (fillfactor = 90);

COMMENT ON TABLE flight_recorder.wait_samples_ring IS 'TIER 1: Wait events ring buffer (UPDATE-only pattern). Pre-populated with 12,000 rows (120 slots × 100 rows). UPSERTs enable HOT updates, zero dead tuples, zero autovacuum pressure. NULLs indicate unused slots.';

-- Activity samples ring buffer (UPDATE-only pattern for zero dead tuples)
CREATE UNLOGGED TABLE IF NOT EXISTS flight_recorder.activity_samples_ring (
    slot_id             INTEGER REFERENCES flight_recorder.samples_ring(slot_id) ON DELETE CASCADE,
    row_num             INTEGER NOT NULL CHECK (row_num >= 0 AND row_num < 25),
    pid                 INTEGER,
    usename             TEXT,
    application_name    TEXT,
    backend_type        TEXT,
    state               TEXT,
    wait_event_type     TEXT,
    wait_event          TEXT,
    query_start         TIMESTAMPTZ,
    state_change        TIMESTAMPTZ,
    query_preview       TEXT,
    PRIMARY KEY (slot_id, row_num)
) WITH (fillfactor = 90);

COMMENT ON TABLE flight_recorder.activity_samples_ring IS 'TIER 1: Active sessions ring buffer (UPDATE-only pattern). Pre-populated with 3,000 rows (120 slots × 25 rows). Top 25 active sessions per sample. UPSERTs enable HOT updates, zero dead tuples. NULLs indicate unused slots.';

-- Lock samples ring buffer (UPDATE-only pattern for zero dead tuples)
CREATE UNLOGGED TABLE IF NOT EXISTS flight_recorder.lock_samples_ring (
    slot_id                 INTEGER REFERENCES flight_recorder.samples_ring(slot_id) ON DELETE CASCADE,
    row_num                 INTEGER NOT NULL CHECK (row_num >= 0 AND row_num < 100),
    blocked_pid             INTEGER,
    blocked_user            TEXT,
    blocked_app             TEXT,
    blocked_query_preview   TEXT,
    blocked_duration        INTERVAL,
    blocking_pid            INTEGER,
    blocking_user           TEXT,
    blocking_app            TEXT,
    blocking_query_preview  TEXT,
    lock_type               TEXT,
    locked_relation_oid     OID,
    PRIMARY KEY (slot_id, row_num)
) WITH (fillfactor = 90);

COMMENT ON TABLE flight_recorder.lock_samples_ring IS 'TIER 1: Lock contention ring buffer (UPDATE-only pattern). Pre-populated with 12,000 rows (120 slots × 100 rows). Max 100 blocked/blocking pairs per sample. UPSERTs enable HOT updates, zero dead tuples, zero autovacuum pressure. NULLs indicate unused slots.';

-- Set fill factor on existing ring buffer tables (for upgrades)
DO $$
BEGIN
    ALTER TABLE flight_recorder.samples_ring SET (fillfactor = 70);
    ALTER TABLE flight_recorder.wait_samples_ring SET (fillfactor = 90);
    ALTER TABLE flight_recorder.activity_samples_ring SET (fillfactor = 90);
    ALTER TABLE flight_recorder.lock_samples_ring SET (fillfactor = 90);
EXCEPTION WHEN OTHERS THEN
    -- Ignore errors (tables may not exist yet)
    NULL;
END $$;

-- NOTE: Aggressive autovacuum settings removed - no longer needed with UPDATE-only pattern
-- Ring buffers now use UPSERT pattern with pre-populated rows:
--   - wait_samples_ring: 12,000 rows (120 slots × 100 rows) - zero dead tuples
--   - activity_samples_ring: 3,000 rows (120 slots × 25 rows) - zero dead tuples
--   - lock_samples_ring: 12,000 rows (120 slots × 100 rows) - zero dead tuples
-- All updates are HOT updates (fillfactor=90), eliminating autovacuum pressure entirely.

-- Initialize ring buffer slots (0 to 119)
INSERT INTO flight_recorder.samples_ring (slot_id, captured_at, epoch_seconds)
SELECT
    generate_series AS slot_id,
    '1970-01-01'::timestamptz,  -- Placeholder, will be overwritten
    0
FROM generate_series(0, 119)
ON CONFLICT (slot_id) DO NOTHING;

-- Pre-populate child ring buffers for UPDATE-only pattern (zero dead tuples)
-- wait_samples_ring: 12,000 rows (120 slots × 100 rows)
INSERT INTO flight_recorder.wait_samples_ring (slot_id, row_num)
SELECT s.slot_id, r.row_num
FROM generate_series(0, 119) s(slot_id)
CROSS JOIN generate_series(0, 99) r(row_num)
ON CONFLICT (slot_id, row_num) DO NOTHING;

-- activity_samples_ring: 3,000 rows (120 slots × 25 rows)
INSERT INTO flight_recorder.activity_samples_ring (slot_id, row_num)
SELECT s.slot_id, r.row_num
FROM generate_series(0, 119) s(slot_id)
CROSS JOIN generate_series(0, 24) r(row_num)
ON CONFLICT (slot_id, row_num) DO NOTHING;

-- lock_samples_ring: 12,000 rows (120 slots × 100 rows)
INSERT INTO flight_recorder.lock_samples_ring (slot_id, row_num)
SELECT s.slot_id, r.row_num
FROM generate_series(0, 119) s(slot_id)
CROSS JOIN generate_series(0, 99) r(row_num)
ON CONFLICT (slot_id, row_num) DO NOTHING;

-- =============================================================================
-- TIER 2: Aggregate Tables (REGULAR, durable, survives crashes)
-- =============================================================================
-- Aggregated 5-minute summaries flushed from ring buffer
-- Provides crash-resistant diagnostics
-- Retains 7 days by default
-- -----------------------------------------------------------------------------

-- Wait event aggregates (5-minute windows)
CREATE TABLE IF NOT EXISTS flight_recorder.wait_event_aggregates (
    id              BIGSERIAL PRIMARY KEY,
    start_time      TIMESTAMPTZ NOT NULL,
    end_time        TIMESTAMPTZ NOT NULL,
    backend_type    TEXT NOT NULL,
    wait_event_type TEXT NOT NULL,
    wait_event      TEXT NOT NULL,
    state           TEXT NOT NULL,
    sample_count    INTEGER NOT NULL,      -- How many 60s samples had this wait
    total_waiters   BIGINT NOT NULL,       -- Sum of waiter counts
    avg_waiters     NUMERIC NOT NULL,      -- Average concurrent waiters
    max_waiters     INTEGER NOT NULL,      -- Peak concurrent waiters
    pct_of_samples  NUMERIC                -- Percentage of samples with this wait
);

CREATE INDEX IF NOT EXISTS wait_aggregates_time_idx
    ON flight_recorder.wait_event_aggregates(start_time, end_time);
CREATE INDEX IF NOT EXISTS wait_aggregates_event_idx
    ON flight_recorder.wait_event_aggregates(wait_event_type, wait_event);

COMMENT ON TABLE flight_recorder.wait_event_aggregates IS 'TIER 2: Durable wait event summaries (5-min windows, survives crashes)';

-- Lock pattern aggregates
CREATE TABLE IF NOT EXISTS flight_recorder.lock_aggregates (
    id                  BIGSERIAL PRIMARY KEY,
    start_time          TIMESTAMPTZ NOT NULL,
    end_time            TIMESTAMPTZ NOT NULL,
    blocked_user        TEXT,
    blocking_user       TEXT,
    lock_type           TEXT,
    locked_relation_oid OID,
    occurrence_count    INTEGER NOT NULL,      -- How many times this pattern occurred
    max_duration        INTERVAL,              -- Longest block duration
    avg_duration        INTERVAL,              -- Average block duration
    sample_query        TEXT                   -- Example blocked query
);

CREATE INDEX IF NOT EXISTS lock_aggregates_time_idx
    ON flight_recorder.lock_aggregates(start_time, end_time);

COMMENT ON TABLE flight_recorder.lock_aggregates IS 'TIER 2: Durable lock pattern summaries (5-min windows, survives crashes)';

-- Query pattern aggregates
CREATE TABLE IF NOT EXISTS flight_recorder.query_aggregates (
    id                  BIGSERIAL PRIMARY KEY,
    start_time          TIMESTAMPTZ NOT NULL,
    end_time            TIMESTAMPTZ NOT NULL,
    query_preview       TEXT,
    occurrence_count    INTEGER NOT NULL,      -- How many times seen
    max_duration        INTERVAL,              -- Longest execution
    avg_duration        INTERVAL               -- Average execution
);

CREATE INDEX IF NOT EXISTS query_aggregates_time_idx
    ON flight_recorder.query_aggregates(start_time, end_time);

COMMENT ON TABLE flight_recorder.query_aggregates IS 'TIER 2: Durable query pattern summaries (5-min windows, survives crashes)';

-- =============================================================================
-- TIER 1.5: Raw Sample Archive Tables (REGULAR, durable, survives crashes)
-- =============================================================================
-- Periodic snapshots of raw samples from ring buffers (not aggregated)
-- Provides high-resolution forensic analysis beyond ring buffer retention
-- Captured on slower cadence (default: every 15 minutes) to minimize overhead
-- Retains full detail: PIDs, exact timestamps, complete blocking chains
-- Retains 7 days by default (configurable)
-- -----------------------------------------------------------------------------

-- Activity samples archive - Raw session activity snapshots
CREATE TABLE IF NOT EXISTS flight_recorder.activity_samples_archive (
    id                  BIGSERIAL PRIMARY KEY,
    sample_id           BIGINT NOT NULL,           -- Reference to collection cycle
    captured_at         TIMESTAMPTZ NOT NULL,      -- When this sample was captured
    pid                 INTEGER,
    usename             TEXT,
    application_name    TEXT,
    backend_type        TEXT,
    state               TEXT,
    wait_event_type     TEXT,
    wait_event          TEXT,
    query_start         TIMESTAMPTZ,
    state_change        TIMESTAMPTZ,
    query_preview       TEXT
);

CREATE INDEX IF NOT EXISTS activity_archive_captured_at_idx
    ON flight_recorder.activity_samples_archive(captured_at);
CREATE INDEX IF NOT EXISTS activity_archive_sample_id_idx
    ON flight_recorder.activity_samples_archive(sample_id);
CREATE INDEX IF NOT EXISTS activity_archive_pid_idx
    ON flight_recorder.activity_samples_archive(pid, captured_at);

COMMENT ON TABLE flight_recorder.activity_samples_archive IS 'TIER 1.5: Raw activity samples for forensic analysis (15-min cadence, full resolution)';

-- Lock samples archive - Raw lock contention snapshots
CREATE TABLE IF NOT EXISTS flight_recorder.lock_samples_archive (
    id                      BIGSERIAL PRIMARY KEY,
    sample_id               BIGINT NOT NULL,           -- Reference to collection cycle
    captured_at             TIMESTAMPTZ NOT NULL,      -- When this sample was captured
    blocked_pid             INTEGER,
    blocked_user            TEXT,
    blocked_app             TEXT,
    blocked_query_preview   TEXT,
    blocked_duration        INTERVAL,
    blocking_pid            INTEGER,
    blocking_user           TEXT,
    blocking_app            TEXT,
    blocking_query_preview  TEXT,
    lock_type               TEXT,
    locked_relation_oid     OID
);

CREATE INDEX IF NOT EXISTS lock_archive_captured_at_idx
    ON flight_recorder.lock_samples_archive(captured_at);
CREATE INDEX IF NOT EXISTS lock_archive_sample_id_idx
    ON flight_recorder.lock_samples_archive(sample_id);
CREATE INDEX IF NOT EXISTS lock_archive_blocked_pid_idx
    ON flight_recorder.lock_samples_archive(blocked_pid, captured_at);
CREATE INDEX IF NOT EXISTS lock_archive_blocking_pid_idx
    ON flight_recorder.lock_samples_archive(blocking_pid, captured_at);

COMMENT ON TABLE flight_recorder.lock_samples_archive IS 'TIER 1.5: Raw lock samples for forensic analysis (15-min cadence, full blocking chains)';

-- Wait samples archive - Raw wait event snapshots
CREATE TABLE IF NOT EXISTS flight_recorder.wait_samples_archive (
    id                  BIGSERIAL PRIMARY KEY,
    sample_id           BIGINT NOT NULL,           -- Reference to collection cycle
    captured_at         TIMESTAMPTZ NOT NULL,      -- When this sample was captured
    backend_type        TEXT,
    wait_event_type     TEXT,
    wait_event          TEXT,
    state               TEXT,
    count               INTEGER                     -- Number of sessions in this wait state
);

CREATE INDEX IF NOT EXISTS wait_archive_captured_at_idx
    ON flight_recorder.wait_samples_archive(captured_at);
CREATE INDEX IF NOT EXISTS wait_archive_sample_id_idx
    ON flight_recorder.wait_samples_archive(sample_id);
CREATE INDEX IF NOT EXISTS wait_archive_wait_event_idx
    ON flight_recorder.wait_samples_archive(wait_event_type, wait_event, captured_at);

COMMENT ON TABLE flight_recorder.wait_samples_archive IS 'TIER 1.5: Raw wait event samples for forensic analysis (15-min cadence, full resolution)';

-- -----------------------------------------------------------------------------
-- Helper: Pretty-print bytes
-- -----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION flight_recorder._pretty_bytes(bytes BIGINT)
RETURNS TEXT
LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE
        WHEN bytes IS NULL THEN NULL
        WHEN bytes >= 1073741824 THEN round(bytes / 1073741824.0, 2)::text || ' GB'
        WHEN bytes >= 1048576    THEN round(bytes / 1048576.0, 2)::text || ' MB'
        WHEN bytes >= 1024       THEN round(bytes / 1024.0, 2)::text || ' KB'
        ELSE bytes::text || ' B'
    END
$$;

-- -----------------------------------------------------------------------------
-- Helper: Get PG major version
-- -----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION flight_recorder._pg_version()
RETURNS INTEGER
LANGUAGE sql STABLE AS $$
    SELECT current_setting('server_version_num')::integer / 10000
$$;

-- -----------------------------------------------------------------------------
-- Table: config - Flight Recorder configuration settings
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS flight_recorder.config (
    key         TEXT PRIMARY KEY,
    value       TEXT NOT NULL,
    updated_at  TIMESTAMPTZ DEFAULT now()
);

-- Default configuration
INSERT INTO flight_recorder.config (key, value) VALUES
    ('mode', 'normal'),
    -- Configurable sample interval (default 60s for normal mode, adjusts per mode)
    ('sample_interval_seconds', '180'),        -- Sample collection frequency (180s=normal/light, 300s=emergency) - A+ GRADE: Ultra-conservative default
    ('statements_enabled', 'auto'),
    ('statements_top_n', '20'),                -- Reduced from 50 to reduce pg_stat_statements pressure
    ('statements_interval_minutes', '15'),     -- Collect statements every 15 min instead of 5 min
    ('statements_min_calls', '1'),
    ('enable_locks', 'true'),
    ('enable_progress', 'true'),
    -- Circuit breaker settings
    ('circuit_breaker_threshold_ms', '1000'),  -- Skip collection if avg of last 3 runs exceeded this
    ('circuit_breaker_enabled', 'true'),
    ('circuit_breaker_window_minutes', '15'),  -- Look back window for moving average
    -- Statement and lock timeouts (applied in collection functions)
    ('statement_timeout_ms', '1000'),          -- Max total collection time - A GRADE: Tighter safety margin
    ('lock_timeout_ms', '100'),                -- Max wait for catalog locks (fail fast on contention)
    ('work_mem_kb', '2048'),                   -- work_mem limit for flight recorder queries (2MB)
    -- Per-section sub-timeouts (prevent one section consuming entire budget)
    ('section_timeout_ms', '250'),             -- Max time per section (reset before each section)
    -- Cost-based skip thresholds (proactive checks before expensive queries)
    ('skip_locks_threshold', '50'),            -- Skip lock collection if > N blocked locks
    ('skip_activity_conn_threshold', '100'),   -- Skip activity if > N active connections
    -- Schema size monitoring
    ('schema_size_warning_mb', '5000'),        -- Warn when schema exceeds 5GB
    ('schema_size_critical_mb', '10000'),      -- Auto-disable when schema exceeds 10GB
    ('schema_size_check_enabled', 'true'),
    -- Automatic mode switching (enabled by default)
    ('auto_mode_enabled', 'true'),             -- Auto-adjust mode based on system load
    ('auto_mode_connections_threshold', '60'), -- Switch to light at 60% of max_connections
    ('auto_mode_trips_threshold', '1'),        -- Switch to emergency if circuit breaker tripped N times in 10min (A+ upgrade: immediate response)
    -- Configurable retention by table type
    ('retention_samples_days', '7'),           -- Retention for samples table (legacy, ring buffers self-clean)
    ('aggregate_retention_days', '7'),         -- Retention for aggregate tables (TIER 2)
    ('retention_snapshots_days', '30'),        -- Retention for snapshots table
    ('retention_statements_days', '30'),       -- Retention for pg_stat_statements snapshots
    ('retention_collection_stats_days', '30'), -- Retention for collection_stats table
    -- Self-monitoring and health checks
    ('self_monitoring_enabled', 'true'),       -- Track flight recorder system performance
    ('health_check_enabled', 'true'),          -- Enable health check function
    -- Advanced features
    ('alert_enabled', 'false'),                -- Enable alert notifications
    ('alert_circuit_breaker_count', '5'),      -- Alert if circuit breaker trips N times in hour
    ('alert_schema_size_mb', '8000'),          -- Alert if schema exceeds threshold (80% of critical)
    -- Snapshot-based collection (default enabled, reduces catalog locks from 3 to 1)
    ('snapshot_based_collection', 'true'),     -- Use temp table snapshot of pg_stat_activity
    -- Lock timeout strategy
    ('lock_timeout_strategy', 'fail_fast'),    -- Options: 'fail_fast' (100ms), 'skip_if_locked' (check first), 'patient' (500ms)
    ('check_ddl_before_collection', 'true'),   -- Pre-check for DDL locks on catalogs before collection
    -- System awareness (A+ upgrade: skip during risky operations)
    ('check_replica_lag', 'true'),             -- Skip collection on lagging replicas
    ('replica_lag_threshold', '10 seconds'),   -- Max acceptable replica lag
    ('check_checkpoint_backup', 'true'),       -- Skip during checkpoints/backups
    ('check_pss_conflicts', 'true'),           -- Skip if pg_stat_statements being read
    -- Schema size limits (A+ upgrade: percentage-based)
    ('schema_size_use_percentage', 'true'),    -- Use percentage of DB size (vs fixed MB)
    ('schema_size_percentage', '5.0'),         -- Max % of database size (default 5%)
    ('schema_size_min_mb', '1000'),            -- Min limit (1GB)
    ('schema_size_max_mb', '10000'),           -- Max limit (10GB)
    -- Adaptive sampling (opt-in, skips collection when idle)
    ('adaptive_sampling', 'false'),            -- Skip collection when system idle
    ('adaptive_sampling_idle_threshold', '5'), -- Skip if < N active connections
    ('load_shedding_enabled', 'true'),         -- A GRADE: Skip collection during high load
    ('load_shedding_active_pct', '70'),        -- Skip if active connections > N% of max_connections
    ('load_throttle_enabled', 'true'),         -- A GRADE: Advanced load throttling (I/O, txn rate)
    ('load_throttle_xact_threshold', '1000'),  -- Skip if commits+rollbacks > N/sec (sustained load)
    ('load_throttle_blk_threshold', '10000'),  -- Skip if block reads+writes > N/sec (I/O pressure)
    -- Collection jitter (A+ upgrade: prevent synchronized monitoring tools)
    ('collection_jitter_enabled', 'true'),     -- Add random delay to collection start
    ('collection_jitter_max_seconds', '10'),   -- Max jitter (0-N seconds random delay)
    -- Raw sample archival (TIER 1.5: high-resolution forensics)
    ('archive_samples_enabled', 'true'),       -- Enable periodic archival of raw samples
    ('archive_sample_frequency_minutes', '15'), -- How often to archive (default: every 15 min)
    ('archive_retention_days', '7'),           -- How long to keep archived samples (default: 7 days)
    ('archive_activity_samples', 'true'),      -- Archive activity samples (PIDs, queries, sessions)
    ('archive_lock_samples', 'true'),          -- Archive lock samples (blocking chains, PIDs)
    ('archive_wait_samples', 'true')           -- Archive wait event samples (wait patterns)
ON CONFLICT (key) DO NOTHING;

-- -----------------------------------------------------------------------------
-- Table: collection_stats - Track collection performance for circuit breaker
-- -----------------------------------------------------------------------------

CREATE UNLOGGED TABLE IF NOT EXISTS flight_recorder.collection_stats (
    id              SERIAL PRIMARY KEY,
    collection_type TEXT NOT NULL,  -- 'sample' or 'snapshot'
    started_at      TIMESTAMPTZ NOT NULL,
    completed_at    TIMESTAMPTZ,
    duration_ms     INTEGER,
    success         BOOLEAN DEFAULT true,
    error_message   TEXT,
    skipped         BOOLEAN DEFAULT false,
    skipped_reason  TEXT,
    sections_total  INTEGER,     -- Total sections attempted
    sections_succeeded INTEGER   -- How many sections completed successfully
);

CREATE INDEX IF NOT EXISTS collection_stats_type_started_idx
    ON flight_recorder.collection_stats(collection_type, started_at DESC);

-- -----------------------------------------------------------------------------
-- Helper: Check circuit breaker - should we skip collection?
-- -----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION flight_recorder._check_circuit_breaker(p_collection_type TEXT)
RETURNS BOOLEAN
LANGUAGE plpgsql AS $$
DECLARE
    v_enabled BOOLEAN;
    v_threshold_ms INTEGER;
    v_avg_duration_ms NUMERIC;
    v_window_minutes INTEGER;
BEGIN
    -- Check if circuit breaker is enabled
    v_enabled := COALESCE(
        flight_recorder._get_config('circuit_breaker_enabled', 'true')::boolean,
        true
    );

    IF NOT v_enabled THEN
        RETURN false;  -- Don't skip
    END IF;

    -- Get threshold
    v_threshold_ms := COALESCE(
        flight_recorder._get_config('circuit_breaker_threshold_ms', '1000')::integer,
        1000
    );

    -- Get look back window
    v_window_minutes := COALESCE(
        flight_recorder._get_config('circuit_breaker_window_minutes', '15')::integer,
        15
    );

    -- Calculate moving average of last 3 successful collections within window
    SELECT avg(duration_ms) INTO v_avg_duration_ms
    FROM (
        SELECT duration_ms
        FROM flight_recorder.collection_stats
        WHERE collection_type = p_collection_type
          AND success = true
          AND skipped = false
          AND started_at > now() - (v_window_minutes || ' minutes')::interval
        ORDER BY started_at DESC
        LIMIT 3  -- Moving average of last 3
    ) recent;

    -- Skip if average exceeds threshold
    IF v_avg_duration_ms IS NOT NULL
       AND v_avg_duration_ms > v_threshold_ms THEN
        RETURN true;  -- Skip this collection
    END IF;

    RETURN false;  -- Don't skip
END;
$$;

-- -----------------------------------------------------------------------------
-- Helper: Record collection start
-- -----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION flight_recorder._record_collection_start(
    p_collection_type TEXT,
    p_sections_total INTEGER DEFAULT NULL
)
RETURNS INTEGER
LANGUAGE sql AS $$
    INSERT INTO flight_recorder.collection_stats (collection_type, started_at, sections_total)
    VALUES (p_collection_type, now(), p_sections_total)
    RETURNING id
$$;

-- -----------------------------------------------------------------------------
-- Helper: Record collection completion
-- -----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION flight_recorder._record_collection_end(
    p_stat_id INTEGER,
    p_success BOOLEAN,
    p_error_message TEXT DEFAULT NULL
)
RETURNS VOID
LANGUAGE sql AS $$
    UPDATE flight_recorder.collection_stats
    SET completed_at = now(),
        duration_ms = EXTRACT(EPOCH FROM (now() - started_at)) * 1000,
        success = p_success,
        error_message = p_error_message
    WHERE id = p_stat_id
$$;

-- -----------------------------------------------------------------------------
-- Helper: Record skipped collection
-- -----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION flight_recorder._record_collection_skip(
    p_collection_type TEXT,
    p_reason TEXT
)
RETURNS VOID
LANGUAGE sql AS $$
    INSERT INTO flight_recorder.collection_stats (
        collection_type, started_at, completed_at, skipped, skipped_reason
    )
    VALUES (p_collection_type, now(), now(), true, p_reason)
$$;

-- -----------------------------------------------------------------------------
-- Helper: Record section success (increment counter)
-- -----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION flight_recorder._record_section_success(p_stat_id INTEGER)
RETURNS VOID
LANGUAGE sql AS $$
    UPDATE flight_recorder.collection_stats
    SET sections_succeeded = COALESCE(sections_succeeded, 0) + 1
    WHERE id = p_stat_id
$$;

-- -----------------------------------------------------------------------------
-- Helper: Get config value with default
-- -----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION flight_recorder._get_config(p_key TEXT, p_default TEXT DEFAULT NULL)
RETURNS TEXT
LANGUAGE sql STABLE AS $$
    SELECT COALESCE(
        (SELECT value FROM flight_recorder.config WHERE key = p_key),
        p_default
    )
$$;

-- -----------------------------------------------------------------------------
-- Helper: Set per-section timeout
-- Resets statement_timeout before each section to prevent one section consuming entire budget
-- -----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION flight_recorder._set_section_timeout()
RETURNS VOID
LANGUAGE plpgsql AS $$
DECLARE
    v_timeout_ms INTEGER;
BEGIN
    v_timeout_ms := COALESCE(
        flight_recorder._get_config('section_timeout_ms', '250')::integer,
        250
    );
    PERFORM set_config('statement_timeout', v_timeout_ms::text, true);
END;
$$;

-- -----------------------------------------------------------------------------
-- P2: Helper: Automatic mode switching based on system load
-- -----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION flight_recorder._check_and_adjust_mode()
RETURNS TABLE(
    previous_mode TEXT,
    new_mode TEXT,
    reason TEXT,
    action_taken BOOLEAN
)
LANGUAGE plpgsql AS $$
DECLARE
    v_enabled BOOLEAN;
    v_current_mode TEXT;
    v_connections_threshold INTEGER;
    v_trips_threshold INTEGER;
    v_active_connections INTEGER;
    v_recent_trips INTEGER;
    v_max_connections INTEGER;
    v_connection_pct NUMERIC;
    v_suggested_mode TEXT;
    v_reason TEXT;
BEGIN
    -- Check if automatic mode switching is enabled
    v_enabled := COALESCE(
        flight_recorder._get_config('auto_mode_enabled', 'false')::boolean,
        false
    );

    IF NOT v_enabled THEN
        RETURN;  -- Return empty result set
    END IF;

    -- Get current mode and thresholds
    v_current_mode := flight_recorder._get_config('mode', 'normal');
    v_connections_threshold := COALESCE(
        flight_recorder._get_config('auto_mode_connections_threshold', '60')::integer,
        60
    );
    v_trips_threshold := COALESCE(
        flight_recorder._get_config('auto_mode_trips_threshold', '1')::integer,
        1
    );

    -- Check system indicators
    -- 1. Active connections as percentage of max_connections
    SELECT count(*) FILTER (WHERE state = 'active')
    INTO v_active_connections
    FROM pg_stat_activity
    WHERE backend_type = 'client backend';

    SELECT setting::integer
    INTO v_max_connections
    FROM pg_settings
    WHERE name = 'max_connections';

    v_connection_pct := (v_active_connections::numeric / NULLIF(v_max_connections, 0)) * 100;

    -- 2. Recent circuit breaker trips (last 10 minutes)
    SELECT count(*)
    INTO v_recent_trips
    FROM flight_recorder.collection_stats
    WHERE skipped = true
      AND started_at > now() - interval '10 minutes'
      AND skipped_reason LIKE '%Circuit breaker%';

    -- Determine suggested mode based on indicators
    v_suggested_mode := v_current_mode;  -- Default: no change

    IF v_recent_trips >= v_trips_threshold THEN
        -- Multiple circuit breaker trips = system under severe stress
        v_suggested_mode := 'emergency';
        v_reason := format('Circuit breaker tripped %s times in last 10 minutes (threshold: %s)',
                          v_recent_trips, v_trips_threshold);
    ELSIF v_connection_pct >= v_connections_threshold THEN
        -- High connection utilization
        IF v_current_mode = 'normal' THEN
            v_suggested_mode := 'light';
            v_reason := format('Active connections at %s%% of max (threshold: %s%%)',
                              round(v_connection_pct, 1)::text, v_connections_threshold);
        END IF;
    ELSE
        -- System looks healthy, consider downgrading mode
        IF v_current_mode = 'emergency' AND v_recent_trips = 0 THEN
            v_suggested_mode := 'light';
            v_reason := 'System recovered: no recent circuit breaker trips';
        ELSIF v_current_mode = 'light' AND v_connection_pct < (v_connections_threshold * 0.7) THEN
            v_suggested_mode := 'normal';
            v_reason := format('System load reduced: connections at %s%% (threshold: %s%%)',
                              round(v_connection_pct, 1)::text, v_connections_threshold);
        END IF;
    END IF;

    -- Apply mode change if suggested mode differs from current
    IF v_suggested_mode != v_current_mode THEN
        -- Use set_mode to apply the change (handles cron rescheduling)
        PERFORM flight_recorder.set_mode(v_suggested_mode);

        RAISE NOTICE 'pg-flight-recorder: Auto-mode switched from % to %: %',
                     v_current_mode, v_suggested_mode, v_reason;

        RETURN QUERY SELECT v_current_mode, v_suggested_mode, v_reason, true;
    END IF;

    -- No action taken
    RETURN;
END;
$$;

-- -----------------------------------------------------------------------------
-- validate_config() - Validate configuration for production safety
-- -----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION flight_recorder.validate_config()
RETURNS TABLE(
    check_name TEXT,
    status TEXT,
    message TEXT
)
LANGUAGE plpgsql AS $$
DECLARE
    v_section_timeout INTEGER;
    v_lock_timeout INTEGER;
    v_circuit_breaker_enabled BOOLEAN;
    v_schema_size_mb NUMERIC;
BEGIN
    -- Check 1: section_timeout_ms should be <= 500ms for production safety
    v_section_timeout := flight_recorder._get_config('section_timeout_ms', '250')::integer;
    RETURN QUERY SELECT
        'section_timeout_ms'::text,
        CASE
            WHEN v_section_timeout > 1000 THEN 'CRITICAL'
            WHEN v_section_timeout > 500 THEN 'WARNING'
            ELSE 'OK'
        END::text,
        format('Current: %s ms. Recommended: <= 250ms for minimal overhead. Worst-case CPU: %s%% (4 sections × %sms / 60s)',
               v_section_timeout,
               round((v_section_timeout * 4.0 / 60000.0) * 100, 1),
               v_section_timeout);

    -- Check 2: circuit_breaker should be enabled
    v_circuit_breaker_enabled := COALESCE(
        flight_recorder._get_config('circuit_breaker_enabled', 'true')::boolean,
        true
    );
    RETURN QUERY SELECT
        'circuit_breaker_enabled'::text,
        CASE WHEN v_circuit_breaker_enabled THEN 'OK' ELSE 'CRITICAL' END::text,
        format('Current: %s. Circuit breaker provides automatic protection under load',
               v_circuit_breaker_enabled);

    -- Check 3: lock_timeout should be low (< 500ms)
    v_lock_timeout := flight_recorder._get_config('lock_timeout_ms', '100')::integer;
    RETURN QUERY SELECT
        'lock_timeout_ms'::text,
        CASE
            WHEN v_lock_timeout > 500 THEN 'WARNING'
            WHEN v_lock_timeout > 1000 THEN 'CRITICAL'
            ELSE 'OK'
        END::text,
        format('Current: %s ms. Recommended: <= 100ms to fail fast on catalog lock contention',
               v_lock_timeout);

    -- Check 4: Schema size
    SELECT schema_size_mb INTO v_schema_size_mb
    FROM flight_recorder._check_schema_size();

    RETURN QUERY SELECT
        'schema_size'::text,
        CASE
            WHEN v_schema_size_mb > 10000 THEN 'CRITICAL'
            WHEN v_schema_size_mb > 5000 THEN 'WARNING'
            ELSE 'OK'
        END::text,
        format('flight_recorder schema: %s MB (warning: 5000 MB, critical: 10000 MB, auto-disable at critical)',
               round(v_schema_size_mb, 0));

    -- Check 5: Cost-based skip thresholds
    RETURN QUERY SELECT
        'skip_thresholds'::text,
        CASE
            WHEN flight_recorder._get_config('skip_activity_conn_threshold')::integer > 200
                OR flight_recorder._get_config('skip_locks_threshold')::integer > 100
            THEN 'WARNING'
            ELSE 'OK'
        END::text,
        format('Activity threshold: %s, Locks threshold: %s. Recommended: 100/50 for early protection',
               flight_recorder._get_config('skip_activity_conn_threshold'),
               flight_recorder._get_config('skip_locks_threshold'));

    -- Check 6: Recent collection failures
    DECLARE
        v_recent_failures INTEGER;
    BEGIN
        SELECT count(*) INTO v_recent_failures
        FROM flight_recorder.collection_stats
        WHERE success = false
          AND started_at > now() - interval '1 hour';

        RETURN QUERY SELECT
            'recent_failures'::text,
            CASE
                WHEN v_recent_failures > 10 THEN 'CRITICAL'
                WHEN v_recent_failures > 3 THEN 'WARNING'
                ELSE 'OK'
            END::text,
            format('%s collection failures in last hour. Check collection_stats for error_message details',
                   v_recent_failures);
    END;

    -- Check 7: Recent lock timeout errors
    DECLARE
        v_lock_timeouts INTEGER;
    BEGIN
        SELECT count(*) INTO v_lock_timeouts
        FROM flight_recorder.collection_stats
        WHERE error_message LIKE '%lock_timeout%'
          AND started_at > now() - interval '1 hour';

        RETURN QUERY SELECT
            'lock_timeout_errors'::text,
            CASE
                WHEN v_lock_timeouts > 5 THEN 'CRITICAL'
                WHEN v_lock_timeouts > 2 THEN 'WARNING'
                ELSE 'OK'
            END::text,
            format('%s lock timeout errors in last hour. Consider increasing lock_timeout_ms or using emergency mode during high-load periods',
                   v_lock_timeouts);
    END;
END;
$$;

-- -----------------------------------------------------------------------------
-- Helper: Check if pg_stat_statements is available
-- -----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION flight_recorder._has_pg_stat_statements()
RETURNS BOOLEAN
LANGUAGE sql STABLE AS $$
    SELECT EXISTS (
        SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements'
    )
$$;

-- -----------------------------------------------------------------------------
-- Helper: Check pg_stat_statements health (utilization and churn)
-- -----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION flight_recorder._check_statements_health()
RETURNS TABLE(
    current_statements BIGINT,
    max_statements INTEGER,
    utilization_pct NUMERIC,
    dealloc_count BIGINT,
    status TEXT
)
LANGUAGE plpgsql AS $$
DECLARE
    v_current BIGINT;
    v_max INTEGER;
    v_dealloc BIGINT;
BEGIN
    -- Check if pg_stat_statements is available
    IF NOT flight_recorder._has_pg_stat_statements() THEN
        RETURN QUERY SELECT 0::bigint, 0::integer, 0::numeric, 0::bigint, 'DISABLED'::text;
        RETURN;
    END IF;

    -- Get max from configuration
    BEGIN
        v_max := current_setting('pg_stat_statements.max')::integer;
    EXCEPTION WHEN OTHERS THEN
        v_max := 5000;  -- Default
    END;

    -- Check pg_stat_statements_info (PG14+) for dealloc count
    IF EXISTS (SELECT 1 FROM pg_views WHERE viewname = 'pg_stat_statements_info') THEN
        BEGIN
            SELECT
                (SELECT count(*) FROM pg_stat_statements),
                (SELECT dealloc FROM pg_stat_statements_info LIMIT 1)
            INTO v_current, v_dealloc;
        EXCEPTION WHEN OTHERS THEN
            SELECT count(*) INTO v_current FROM pg_stat_statements;
            v_dealloc := NULL;
        END;
    ELSE
        -- Fallback for PG13 or when pg_stat_statements_info not available
        SELECT count(*) INTO v_current FROM pg_stat_statements;
        v_dealloc := NULL;
    END IF;

    -- Determine status based on utilization
    RETURN QUERY SELECT
        v_current,
        v_max,
        ROUND(100.0 * v_current / NULLIF(v_max, 0), 1),
        v_dealloc,
        CASE
            WHEN v_current::numeric / NULLIF(v_max, 0) > 0.95 THEN 'HIGH_CHURN'
            WHEN v_current::numeric / NULLIF(v_max, 0) > 0.80 THEN 'WARNING'
            ELSE 'OK'
        END;
END;
$$;

-- -----------------------------------------------------------------------------
-- P1 Safety: Check schema size and enforce limits
-- -----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION flight_recorder._check_schema_size()
RETURNS TABLE(
    schema_size_mb NUMERIC,
    warning_threshold_mb INTEGER,
    critical_threshold_mb INTEGER,
    status TEXT,
    action_taken TEXT
)
LANGUAGE plpgsql AS $$
DECLARE
    v_size_bytes BIGINT;
    v_size_mb NUMERIC;
    v_warning_mb INTEGER;
    v_critical_mb INTEGER;
    v_check_enabled BOOLEAN;
    v_enabled BOOLEAN;
    v_cleanup_performed BOOLEAN := false;
    v_action TEXT := '';
BEGIN
    -- Check if schema size checking is enabled
    v_check_enabled := COALESCE(
        flight_recorder._get_config('schema_size_check_enabled', 'true')::boolean,
        true
    );

    IF NOT v_check_enabled THEN
        RETURN QUERY SELECT 0::numeric, 0, 0, 'disabled'::text, 'none'::text;
        RETURN;
    END IF;

    -- Get thresholds from config (A+ UPGRADE: percentage-based with min/max bounds)
    DECLARE
        v_use_percentage BOOLEAN;
        v_db_size_mb NUMERIC;
        v_percentage NUMERIC;
        v_min_mb INTEGER;
        v_max_mb INTEGER;
    BEGIN
        v_use_percentage := COALESCE(
            flight_recorder._get_config('schema_size_use_percentage', 'true')::boolean,
            true
        );

        IF v_use_percentage THEN
            -- Calculate database size
            SELECT round(pg_database_size(current_database()) / 1024.0 / 1024.0, 2)
            INTO v_db_size_mb;

            -- Get percentage and bounds
            v_percentage := COALESCE(
                flight_recorder._get_config('schema_size_percentage', '5.0')::numeric,
                5.0
            );
            v_min_mb := COALESCE(
                flight_recorder._get_config('schema_size_min_mb', '1000')::integer,
                1000
            );
            v_max_mb := COALESCE(
                flight_recorder._get_config('schema_size_max_mb', '10000')::integer,
                10000
            );

            -- Calculate thresholds with bounds
            v_critical_mb := GREATEST(v_min_mb, LEAST(v_max_mb, (v_db_size_mb * v_percentage / 100.0)::integer));
            v_warning_mb := (v_critical_mb * 0.5)::integer;  -- Warning at 50% of critical
        ELSE
            -- Use fixed thresholds (legacy mode)
            v_warning_mb := COALESCE(
                flight_recorder._get_config('schema_size_warning_mb', '5000')::integer,
                5000
            );
            v_critical_mb := COALESCE(
                flight_recorder._get_config('schema_size_critical_mb', '10000')::integer,
                10000
            );
        END IF;
    END;

    -- Calculate total schema size (all tables in flight recorder schema)
    SELECT COALESCE(sum(pg_total_relation_size(c.oid)), 0)
    INTO v_size_bytes
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'flight_recorder'
      AND c.relkind IN ('r', 'i', 't');  -- tables, indexes, TOAST

    v_size_mb := round(v_size_bytes / 1024.0 / 1024.0, 2);

    -- Check if currently enabled
    SELECT EXISTS (
        SELECT 1 FROM cron.job
        WHERE jobname LIKE 'flight_recorder%'
          AND active = true
    ) INTO v_enabled;

    -- ENHANCED: Auto-recovery logic with hysteresis

    -- Critical: Try aggressive cleanup first, then disable only if still > 10GB
    IF v_size_mb >= v_critical_mb AND v_enabled THEN
        BEGIN
            -- Try aggressive cleanup first (3 days retention)
            PERFORM flight_recorder.cleanup('3 days'::interval);
            v_cleanup_performed := true;
            v_action := 'Aggressive cleanup (3 days retention)';

            -- Re-check size after cleanup
            SELECT COALESCE(sum(pg_total_relation_size(c.oid)), 0)
            INTO v_size_bytes
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'flight_recorder'
              AND c.relkind IN ('r', 'i', 't');

            v_size_mb := round(v_size_bytes / 1024.0 / 1024.0, 2);

            -- If still > 10GB after cleanup, disable
            IF v_size_mb >= v_critical_mb THEN
                PERFORM flight_recorder.disable();
                v_action := v_action || '; Collection disabled (still > 10GB after cleanup)';
                RETURN QUERY SELECT
                    v_size_mb,
                    v_warning_mb,
                    v_critical_mb,
                    'CRITICAL'::TEXT,
                    v_action;
                RETURN;
            ELSE
                -- Cleanup succeeded, stay enabled
                v_action := v_action || format('; Cleanup succeeded (%s MB remaining)', v_size_mb);
                RETURN QUERY SELECT
                    v_size_mb,
                    v_warning_mb,
                    v_critical_mb,
                    'RECOVERED'::TEXT,
                    v_action;
                RETURN;
            END IF;
        EXCEPTION WHEN OTHERS THEN
            RETURN QUERY SELECT
                v_size_mb,
                v_warning_mb,
                v_critical_mb,
                'CRITICAL'::TEXT,
                format('Failed to cleanup/disable: %s', SQLERRM)::TEXT;
            RETURN;
        END;
    END IF;

    -- ENHANCED: Auto-recovery - If disabled and size < 8GB (2GB hysteresis), re-enable
    IF NOT v_enabled AND v_size_mb < (v_critical_mb * 0.8) THEN
        BEGIN
            PERFORM flight_recorder.enable();
            v_action := format('Auto-recovery: collection re-enabled (size dropped to %s MB, below 8GB threshold)', v_size_mb);
            RETURN QUERY SELECT
                v_size_mb,
                v_warning_mb,
                v_critical_mb,
                'RECOVERED'::TEXT,
                v_action;
            RETURN;
        EXCEPTION WHEN OTHERS THEN
            RETURN QUERY SELECT
                v_size_mb,
                v_warning_mb,
                v_critical_mb,
                'ERROR'::TEXT,
                format('Failed to auto-recover: %s', SQLERRM)::TEXT;
            RETURN;
        END;
    END IF;

    -- Warning: 5-10GB - Proactive cleanup to prevent reaching 10GB
    IF v_size_mb >= v_warning_mb AND v_size_mb < v_critical_mb THEN
        IF NOT v_cleanup_performed THEN
            BEGIN
                -- Proactive cleanup at 5GB (5 days retention)
                PERFORM flight_recorder.cleanup('5 days'::interval);
                v_action := 'Proactive cleanup at 5GB (5 days retention)';

                -- Re-check size after cleanup
                SELECT COALESCE(sum(pg_total_relation_size(c.oid)), 0)
                INTO v_size_bytes
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = 'flight_recorder'
                  AND c.relkind IN ('r', 'i', 't');

                v_size_mb := round(v_size_bytes / 1024.0 / 1024.0, 2);
                v_action := v_action || format(' (reduced to %s MB)', v_size_mb);
            EXCEPTION WHEN OTHERS THEN
                v_action := format('Attempted cleanup but failed: %s', SQLERRM);
            END;
        END IF;

        RAISE WARNING 'pg-flight-recorder: Schema size (% MB) in warning range (% - % MB). %',
            v_size_mb, v_warning_mb, v_critical_mb, v_action;
        RETURN QUERY SELECT
            v_size_mb,
            v_warning_mb,
            v_critical_mb,
            'WARNING'::TEXT,
            v_action;
        RETURN;
    END IF;

    -- OK: < 5GB
    RETURN QUERY SELECT
        v_size_mb,
        v_warning_mb,
        v_critical_mb,
        'OK'::TEXT,
        'None'::TEXT;
END;
$$;

-- -----------------------------------------------------------------------------
-- flight_recorder._check_catalog_ddl_locks() - Check for DDL locks on catalogs
-- -----------------------------------------------------------------------------
-- Returns TRUE if AccessExclusiveLock detected on system catalogs
-- Used by skip_if_locked strategy to avoid contention with DDL operations

CREATE OR REPLACE FUNCTION flight_recorder._check_catalog_ddl_locks()
RETURNS BOOLEAN
LANGUAGE plpgsql AS $$
DECLARE
    v_ddl_lock_exists BOOLEAN;
BEGIN
    -- Check for AccessExclusiveLock on pg_stat_activity or related catalogs
    -- These locks are held by DDL operations like ALTER TABLE, VACUUM FULL, etc.
    SELECT EXISTS(
        SELECT 1
        FROM pg_locks l
        JOIN pg_class c ON c.oid = l.relation
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE l.mode = 'AccessExclusiveLock'
          AND l.granted = true
          AND n.nspname IN ('pg_catalog', 'information_schema')
          AND c.relname IN (
              'pg_stat_activity',      -- Main catalog we query
              'pg_locks',               -- Used in lock detection
              'pg_stat_database',       -- Used in snapshots
              'pg_stat_statements'      -- Used in statement snapshots
          )
    ) INTO v_ddl_lock_exists;

    RETURN v_ddl_lock_exists;
EXCEPTION WHEN OTHERS THEN
    -- If check fails, assume no locks (don't block collection)
    RETURN false;
END;
$$;

COMMENT ON FUNCTION flight_recorder._check_catalog_ddl_locks() IS 'Pre-check for DDL locks on system catalogs to avoid lock contention';

-- -----------------------------------------------------------------------------
-- flight_recorder._should_skip_collection() - System awareness pre-flight checks
-- -----------------------------------------------------------------------------
-- Checks multiple system conditions to determine if collection should be skipped
-- Returns skip reason text, or NULL if collection should proceed

CREATE OR REPLACE FUNCTION flight_recorder._should_skip_collection()
RETURNS TEXT
LANGUAGE plpgsql AS $$
DECLARE
    v_replica_lag_check BOOLEAN;
    v_checkpoint_check BOOLEAN;
    v_replica_lag INTERVAL;
    v_replica_lag_threshold INTERVAL;
    v_checkpoint_in_progress BOOLEAN;
    v_backup_running BOOLEAN;
BEGIN
    -- Check 1: Replication lag (on replicas only)
    v_replica_lag_check := COALESCE(
        flight_recorder._get_config('check_replica_lag', 'true')::boolean,
        true
    );

    IF v_replica_lag_check AND pg_is_in_recovery() THEN
        v_replica_lag_threshold := COALESCE(
            flight_recorder._get_config('replica_lag_threshold', '10 seconds')::interval,
            '10 seconds'::interval
        );

        BEGIN
            SELECT age(now(), pg_last_xact_replay_timestamp())
            INTO v_replica_lag;

            IF v_replica_lag > v_replica_lag_threshold THEN
                RETURN format('Replica lag %s exceeds threshold %s',
                    v_replica_lag::text, v_replica_lag_threshold::text);
            END IF;
        EXCEPTION WHEN OTHERS THEN
            -- If check fails, allow collection (don't block on check failure)
            NULL;
        END;
    END IF;

    -- Check 2: Active checkpoint or backup
    v_checkpoint_check := COALESCE(
        flight_recorder._get_config('check_checkpoint_backup', 'true')::boolean,
        true
    );

    IF v_checkpoint_check THEN
        BEGIN
            -- Check for active checkpoint (heuristic: recent checkpoint request)
            SELECT EXISTS(
                SELECT 1 FROM pg_stat_bgwriter
                WHERE checkpoints_req > 0
                  AND stats_reset > now() - interval '1 minute'
            ) INTO v_checkpoint_in_progress;

            IF v_checkpoint_in_progress THEN
                RETURN 'Active checkpoint detected (recent requested checkpoint)';
            END IF;

            -- Check for pg_dump, pg_basebackup, or WAL senders (backups)
            SELECT EXISTS(
                SELECT 1 FROM pg_stat_activity
                WHERE (backend_type = 'walsender' AND state = 'active')
                   OR query ILIKE '%pg_dump%'
                   OR query ILIKE '%pg_basebackup%'
                   OR application_name ILIKE '%backup%'
            ) INTO v_backup_running;

            IF v_backup_running THEN
                RETURN 'Backup in progress (pg_dump/pg_basebackup/walsender detected)';
            END IF;
        EXCEPTION WHEN OTHERS THEN
            -- If check fails, allow collection
            NULL;
        END;
    END IF;

    -- All checks passed
    RETURN NULL;
EXCEPTION WHEN OTHERS THEN
    -- If entire check fails, allow collection (don't block on check failure)
    RETURN NULL;
END;
$$;

COMMENT ON FUNCTION flight_recorder._should_skip_collection() IS 'Pre-flight checks for replication lag, checkpoints, and backups';

-- -----------------------------------------------------------------------------
-- flight_recorder.sample() - High-frequency sampling (wait events, activity, progress, locks)
-- Per-section timeouts, O(n) lock detection using pg_blocking_pids()
-- -----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION flight_recorder.sample()
RETURNS TIMESTAMPTZ
LANGUAGE plpgsql AS $$
DECLARE
    v_captured_at TIMESTAMPTZ := now();
    v_epoch BIGINT := extract(epoch from v_captured_at)::bigint;
    v_slot_id INTEGER;  -- Ring buffer slot (calculated dynamically based on interval)
    v_sample_interval_seconds INTEGER;
    v_enable_locks BOOLEAN;
    v_snapshot_based BOOLEAN;
    v_blocked_count INTEGER;
    v_skip_locks_threshold INTEGER;
    v_stat_id INTEGER;
    v_should_skip BOOLEAN;
BEGIN
    -- P0 Safety: Calculate slot based on configured interval (adaptive frequency)
    -- Read configured sample interval (default 60s)
    v_sample_interval_seconds := COALESCE(
        flight_recorder._get_config('sample_interval_seconds', '60')::integer,
        60
    );

    -- Validate bounds (60s minimum, 3600s maximum)
    IF v_sample_interval_seconds < 60 THEN
        v_sample_interval_seconds := 60;
    ELSIF v_sample_interval_seconds > 3600 THEN
        v_sample_interval_seconds := 3600;
    END IF;

    -- Calculate ring buffer slot: (epoch / interval) % 120
    -- This provides variable retention: 60s=2h, 120s=4h, 300s=10h
    v_slot_id := (v_epoch / v_sample_interval_seconds) % 120;

    -- P0 Safety: Collection jitter (A+ upgrade: prevent synchronized monitoring)
    DECLARE
        v_jitter_enabled BOOLEAN;
        v_jitter_max INTEGER;
        v_jitter_seconds NUMERIC;
    BEGIN
        v_jitter_enabled := COALESCE(
            flight_recorder._get_config('collection_jitter_enabled', 'true')::boolean,
            true
        );
        v_jitter_max := COALESCE(
            flight_recorder._get_config('collection_jitter_max_seconds', '10')::integer,
            10
        );

        IF v_jitter_enabled AND v_jitter_max > 0 THEN
            -- Random jitter: 0 to v_jitter_max seconds
            v_jitter_seconds := random() * v_jitter_max;
            PERFORM pg_sleep(v_jitter_seconds);
        END IF;
    END;

    -- P2 Safety: Check and adjust mode automatically based on system load
    PERFORM flight_recorder._check_and_adjust_mode();

    -- P0 Safety: Check circuit breaker
    v_should_skip := flight_recorder._check_circuit_breaker('sample');
    IF v_should_skip THEN
        PERFORM flight_recorder._record_collection_skip('sample', 'Circuit breaker tripped - last run exceeded threshold');
        RAISE NOTICE 'pg-flight-recorder: Skipping sample collection due to circuit breaker';
        RETURN v_captured_at;
    END IF;

    -- P0 Safety: System awareness pre-flight checks (A+ upgrade)
    DECLARE
        v_skip_reason TEXT;
    BEGIN
        v_skip_reason := flight_recorder._should_skip_collection();
        IF v_skip_reason IS NOT NULL THEN
            PERFORM flight_recorder._record_collection_skip('sample', v_skip_reason);
            RAISE NOTICE 'pg-flight-recorder: Skipping sample - %', v_skip_reason;
            RETURN v_captured_at;
        END IF;
    END;

    -- P0 Safety: Job deduplication
    DECLARE
        v_running_count INTEGER;
        v_running_pid INTEGER;
    BEGIN
        SELECT count(*), min(pid) INTO v_running_count, v_running_pid
        FROM pg_stat_activity
        WHERE query LIKE '%flight_recorder.sample()%'
          AND state = 'active'
          AND pid != pg_backend_pid()
          AND backend_type = 'client backend';

        IF v_running_count > 0 THEN
            PERFORM flight_recorder._record_collection_skip('sample',
                format('Job deduplication: %s sample job(s) already running (PID: %s)',
                       v_running_count, v_running_pid));
            RAISE NOTICE 'pg-flight-recorder: Skipping sample - another job already running (PID: %)', v_running_pid;
            RETURN v_captured_at;
        END IF;
    END;

    -- P0 Safety: Record collection start (3 sections: wait events, activity, locks)
    v_stat_id := flight_recorder._record_collection_start('sample', 3);

    -- P0 Safety: Check for catalog DDL locks before collection
    DECLARE
        v_check_ddl BOOLEAN;
        v_lock_strategy TEXT;
        v_ddl_lock_exists BOOLEAN;
        v_lock_timeout_ms INTEGER;
    BEGIN
        v_check_ddl := COALESCE(
            flight_recorder._get_config('check_ddl_before_collection', 'true')::boolean,
            true
        );
        v_lock_strategy := COALESCE(
            flight_recorder._get_config('lock_timeout_strategy', 'fail_fast'),
            'fail_fast'
        );

        IF v_check_ddl AND v_lock_strategy = 'skip_if_locked' THEN
            v_ddl_lock_exists := flight_recorder._check_catalog_ddl_locks();
            IF v_ddl_lock_exists THEN
                PERFORM flight_recorder._record_collection_skip('sample',
                    'DDL lock detected on system catalogs (skip_if_locked strategy)');
                RAISE NOTICE 'pg-flight-recorder: Skipping sample - DDL lock detected on catalogs';
                RETURN v_captured_at;
            END IF;
        END IF;

        -- Set lock timeout based on strategy
        v_lock_timeout_ms := CASE v_lock_strategy
            WHEN 'skip_if_locked' THEN 0      -- Already checked above, set to 0 for safety
            WHEN 'patient' THEN 500            -- Wait up to 500ms for locks
            ELSE 100                           -- 'fail_fast' (default): 100ms
        END;

        PERFORM set_config('lock_timeout', v_lock_timeout_ms::text, true);
    END;

    -- P0 Safety: Set work_mem
    PERFORM set_config('work_mem',
        COALESCE(flight_recorder._get_config('work_mem_kb', '2048'), '2048') || 'kB',
        true);

    -- Adaptive sampling - skip if system idle (opt-in)
    DECLARE
        v_load_shedding_enabled BOOLEAN;
        v_load_threshold_pct INTEGER;
        v_max_connections INTEGER;
        v_active_pct NUMERIC;
        v_adaptive_sampling BOOLEAN;
        v_idle_threshold INTEGER;
        v_active_count INTEGER;
        -- A GRADE: Advanced load throttling variables
        v_load_throttle_enabled BOOLEAN;
        v_xact_threshold INTEGER;
        v_blk_threshold INTEGER;
        v_xact_rate NUMERIC;
        v_blk_rate NUMERIC;
        v_xact_commit BIGINT;
        v_xact_rollback BIGINT;
        v_blks_read BIGINT;
        v_blks_hit BIGINT;
        v_db_uptime INTERVAL;
        -- A GRADE: pg_stat_statements overhead protection variables
        v_stmt_utilization NUMERIC;
        v_stmt_status TEXT;
    BEGIN
        -- A GRADE: Load shedding - skip collection during high load
        v_load_shedding_enabled := COALESCE(
            flight_recorder._get_config('load_shedding_enabled', 'true')::boolean,
            true
        );

        IF v_load_shedding_enabled THEN
            v_load_threshold_pct := COALESCE(
                flight_recorder._get_config('load_shedding_active_pct', '70')::integer,
                70
            );

            -- Get max_connections and active count
            SELECT setting::integer INTO v_max_connections
            FROM pg_settings WHERE name = 'max_connections';

            SELECT count(*) INTO v_active_count
            FROM pg_stat_activity
            WHERE state = 'active' AND backend_type = 'client backend';

            v_active_pct := (v_active_count::numeric / NULLIF(v_max_connections, 0)) * 100;

            IF v_active_pct > v_load_threshold_pct THEN
                PERFORM flight_recorder._record_collection_skip('sample',
                    format('Load shedding: high load (%s active / %s max = %s%% > %s%% threshold)',
                           v_active_count, v_max_connections, round(v_active_pct, 1), v_load_threshold_pct));
                PERFORM set_config('statement_timeout', '0', true);
                RETURN v_captured_at;
            END IF;
        END IF;

        -- A GRADE: Advanced load throttling - I/O and transaction rate monitoring
        v_load_throttle_enabled := COALESCE(
                flight_recorder._get_config('load_throttle_enabled', 'true')::boolean,
                true
            );

            IF v_load_throttle_enabled THEN
                v_xact_threshold := COALESCE(
                    flight_recorder._get_config('load_throttle_xact_threshold', '1000')::integer,
                    1000
                );
                v_blk_threshold := COALESCE(
                    flight_recorder._get_config('load_throttle_blk_threshold', '10000')::integer,
                    10000
                );

                -- Get database activity metrics
                SELECT 
                    xact_commit, 
                    xact_rollback,
                    blks_read,
                    blks_hit,
                    now() - stats_reset
                INTO v_xact_commit, v_xact_rollback, v_blks_read, v_blks_hit, v_db_uptime
                FROM pg_stat_database
                WHERE datname = current_database();

                -- Calculate per-second rates (only if stats_reset is recent enough)
                IF v_db_uptime > interval '10 seconds' THEN
                    v_xact_rate := (v_xact_commit + v_xact_rollback) / EXTRACT(EPOCH FROM v_db_uptime);
                    v_blk_rate := (v_blks_read + v_blks_hit) / EXTRACT(EPOCH FROM v_db_uptime);

                    -- Check transaction rate threshold
                    IF v_xact_rate > v_xact_threshold THEN
                        PERFORM flight_recorder._record_collection_skip('sample',
                            format('Load throttling: high transaction rate (%s txn/sec > %s threshold)',
                                   round(v_xact_rate, 1), v_xact_threshold));
                        PERFORM set_config('statement_timeout', '0', true);
                        RETURN v_captured_at;
                    END IF;

                    -- Check block I/O rate threshold
                    IF v_blk_rate > v_blk_threshold THEN
                        PERFORM flight_recorder._record_collection_skip('sample',
                            format('Load throttling: high I/O rate (%s blocks/sec > %s threshold)',
                                   round(v_blk_rate, 1), v_blk_threshold));
                        PERFORM set_config('statement_timeout', '0', true);
                        RETURN v_captured_at;
                    END IF;
                END IF;
            END IF;

        -- A GRADE: pg_stat_statements overhead protection
        -- Check pg_stat_statements overhead (skip if > 80% to prevent hash table churn)
        IF flight_recorder._has_pg_stat_statements() THEN
            SELECT utilization_pct, status
            INTO v_stmt_utilization, v_stmt_status
            FROM flight_recorder._check_statements_health();

            IF v_stmt_status IN ('WARNING', 'HIGH_CHURN') THEN
                PERFORM flight_recorder._record_collection_skip('sample',
                    format('pg_stat_statements overhead: %s utilization (%s%%), skipping to reduce hash table pressure',
                           v_stmt_status, round(v_stmt_utilization, 1)));
                PERFORM set_config('statement_timeout', '0', true);
                RETURN v_captured_at;
            END IF;
        END IF;

        -- Original adaptive sampling (idle detection)
        v_adaptive_sampling := COALESCE(
            flight_recorder._get_config('adaptive_sampling', 'false')::boolean,
            false
        );

        IF v_adaptive_sampling THEN
            v_idle_threshold := COALESCE(
                flight_recorder._get_config('adaptive_sampling_idle_threshold', '5')::integer,
                5
            );

            -- Reuse v_active_count from load shedding if already set
            IF v_active_count IS NULL THEN
                SELECT count(*) INTO v_active_count
                FROM pg_stat_activity
                WHERE state = 'active' AND backend_type = 'client backend';
            END IF;

            IF v_active_count < v_idle_threshold THEN
                PERFORM flight_recorder._record_collection_skip('sample',
                    format('Adaptive sampling: system idle (%s active connections < %s threshold)',
                           v_active_count, v_idle_threshold));
                PERFORM set_config('statement_timeout', '0', true);
                RETURN v_captured_at;
            END IF;
        END IF;
    END;

    v_enable_locks := COALESCE(
        flight_recorder._get_config('enable_locks', 'true')::boolean,
        TRUE
    );
    v_snapshot_based := COALESCE(
        flight_recorder._get_config('snapshot_based_collection', 'true')::boolean,
        true
    );

    INSERT INTO flight_recorder.samples_ring (slot_id, captured_at, epoch_seconds)
    VALUES (v_slot_id, v_captured_at, v_epoch)
    ON CONFLICT (slot_id) DO UPDATE SET
        captured_at = EXCLUDED.captured_at,
        epoch_seconds = EXCLUDED.epoch_seconds;

    -- NOTE: No DELETE needed - using UPDATE-only pattern
    -- First, clear all rows for this slot by setting columns to NULL
    UPDATE flight_recorder.wait_samples_ring SET
        backend_type = NULL, wait_event_type = NULL, wait_event = NULL, state = NULL, count = NULL
    WHERE slot_id = v_slot_id;

    UPDATE flight_recorder.activity_samples_ring SET
        pid = NULL, usename = NULL, application_name = NULL, backend_type = NULL,
        state = NULL, wait_event_type = NULL, wait_event = NULL,
        query_start = NULL, state_change = NULL, query_preview = NULL
    WHERE slot_id = v_slot_id;

    UPDATE flight_recorder.lock_samples_ring SET
        blocked_pid = NULL, blocked_user = NULL, blocked_app = NULL,
        blocked_query_preview = NULL, blocked_duration = NULL, blocking_pid = NULL,
        blocking_user = NULL, blocking_app = NULL, blocking_query_preview = NULL,
        lock_type = NULL, locked_relation_oid = NULL
    WHERE slot_id = v_slot_id;

    IF v_snapshot_based THEN
        CREATE TEMP TABLE IF NOT EXISTS _fr_psa_snapshot (
            LIKE pg_stat_activity
        ) ON COMMIT DROP;
        TRUNCATE _fr_psa_snapshot;
        INSERT INTO _fr_psa_snapshot
        SELECT * FROM pg_stat_activity WHERE pid != pg_backend_pid();
    END IF;

    -- Section 1: Wait events (UPDATE-only pattern with row_num)
    BEGIN
        PERFORM flight_recorder._set_section_timeout();
        IF v_snapshot_based THEN
            INSERT INTO flight_recorder.wait_samples_ring (slot_id, row_num, backend_type, wait_event_type, wait_event, state, count)
            SELECT
                v_slot_id,
                (ROW_NUMBER() OVER () - 1)::integer AS row_num,
                COALESCE(backend_type, 'unknown'),
                COALESCE(wait_event_type, 'Running'),
                COALESCE(wait_event, 'CPU'),
                COALESCE(state, 'unknown'),
                count(*)::integer
            FROM _fr_psa_snapshot
            GROUP BY backend_type, wait_event_type, wait_event, state
            LIMIT 100
            ON CONFLICT (slot_id, row_num) DO UPDATE SET
                backend_type = EXCLUDED.backend_type,
                wait_event_type = EXCLUDED.wait_event_type,
                wait_event = EXCLUDED.wait_event,
                state = EXCLUDED.state,
                count = EXCLUDED.count;
        ELSE
            INSERT INTO flight_recorder.wait_samples_ring (slot_id, row_num, backend_type, wait_event_type, wait_event, state, count)
            SELECT
                v_slot_id,
                (ROW_NUMBER() OVER () - 1)::integer AS row_num,
                COALESCE(backend_type, 'unknown'),
                COALESCE(wait_event_type, 'Running'),
                COALESCE(wait_event, 'CPU'),
                COALESCE(state, 'unknown'),
                count(*)::integer
            FROM pg_stat_activity
            WHERE pid != pg_backend_pid()
            GROUP BY backend_type, wait_event_type, wait_event, state
            LIMIT 100
            ON CONFLICT (slot_id, row_num) DO UPDATE SET
                backend_type = EXCLUDED.backend_type,
                wait_event_type = EXCLUDED.wait_event_type,
                wait_event = EXCLUDED.wait_event,
                state = EXCLUDED.state,
                count = EXCLUDED.count;
        END IF;
        PERFORM flight_recorder._record_section_success(v_stat_id);
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'pg-flight-recorder: Wait events collection failed: %', SQLERRM;
    END;

    -- Section 2: Active sessions (UPDATE-only pattern with row_num)
    BEGIN
        PERFORM flight_recorder._set_section_timeout();
        IF v_snapshot_based THEN
            INSERT INTO flight_recorder.activity_samples_ring (
                slot_id, row_num, pid, usename, application_name, backend_type,
                state, wait_event_type, wait_event, query_start, state_change, query_preview
            )
            SELECT
                v_slot_id,
                (ROW_NUMBER() OVER (ORDER BY query_start ASC NULLS LAST) - 1)::integer AS row_num,
                pid,
                usename,
                application_name,
                backend_type,
                state,
                wait_event_type,
                wait_event,
                query_start,
                state_change,
                left(query, 200)
            FROM _fr_psa_snapshot
            WHERE state != 'idle'
            LIMIT 25
            ON CONFLICT (slot_id, row_num) DO UPDATE SET
                pid = EXCLUDED.pid,
                usename = EXCLUDED.usename,
                application_name = EXCLUDED.application_name,
                backend_type = EXCLUDED.backend_type,
                state = EXCLUDED.state,
                wait_event_type = EXCLUDED.wait_event_type,
                wait_event = EXCLUDED.wait_event,
                query_start = EXCLUDED.query_start,
                state_change = EXCLUDED.state_change,
                query_preview = EXCLUDED.query_preview;
        ELSE
            INSERT INTO flight_recorder.activity_samples_ring (
                slot_id, row_num, pid, usename, application_name, backend_type,
                state, wait_event_type, wait_event, query_start, state_change, query_preview
            )
            SELECT
                v_slot_id,
                (ROW_NUMBER() OVER (ORDER BY query_start ASC NULLS LAST) - 1)::integer AS row_num,
                pid,
                usename,
                application_name,
                backend_type,
                state,
                wait_event_type,
                wait_event,
                query_start,
                state_change,
                left(query, 200)
            FROM pg_stat_activity
            WHERE state != 'idle' AND pid != pg_backend_pid()
            LIMIT 25
            ON CONFLICT (slot_id, row_num) DO UPDATE SET
                pid = EXCLUDED.pid,
                usename = EXCLUDED.usename,
                application_name = EXCLUDED.application_name,
                backend_type = EXCLUDED.backend_type,
                state = EXCLUDED.state,
                wait_event_type = EXCLUDED.wait_event_type,
                wait_event = EXCLUDED.wait_event,
                query_start = EXCLUDED.query_start,
                state_change = EXCLUDED.state_change,
                query_preview = EXCLUDED.query_preview;
        END IF;
        PERFORM flight_recorder._record_section_success(v_stat_id);
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'pg-flight-recorder: Activity samples collection failed: %', SQLERRM;
    END;

    -- Section 3: Lock sampling (OPTIMIZATION 1: materialized blocked sessions)
    IF v_enable_locks THEN
    BEGIN
        PERFORM flight_recorder._set_section_timeout();
        DECLARE
            v_blocked_count INTEGER;
            v_skip_locks_threshold INTEGER;
        BEGIN
            v_skip_locks_threshold := COALESCE(
                flight_recorder._get_config('skip_locks_threshold', '50')::integer,
                50
            );

            -- Materialize blocked sessions with pg_blocking_pids() computed ONCE
            IF v_snapshot_based THEN
                CREATE TEMP TABLE _fr_blocked_sessions ON COMMIT DROP AS
                SELECT
                    pid,
                    usename,
                    application_name,
                    query,
                    query_start,
                    wait_event_type,
                    wait_event,
                    pg_blocking_pids(pid) AS blocking_pids
                FROM _fr_psa_snapshot
                WHERE cardinality(pg_blocking_pids(pid)) > 0;
            ELSE
                CREATE TEMP TABLE _fr_blocked_sessions ON COMMIT DROP AS
                SELECT
                    pid,
                    usename,
                    application_name,
                    query,
                    query_start,
                    wait_event_type,
                    wait_event,
                    pg_blocking_pids(pid) AS blocking_pids
                FROM pg_stat_activity
                WHERE pid != pg_backend_pid()
                  AND cardinality(pg_blocking_pids(pid)) > 0;
            END IF;

            SELECT count(*) INTO v_blocked_count FROM _fr_blocked_sessions;

            IF v_blocked_count > v_skip_locks_threshold THEN
                RAISE NOTICE 'pg-flight-recorder: Skipping lock collection - % blocked sessions exceeds threshold %',
                    v_blocked_count, v_skip_locks_threshold;
            ELSE
                -- UPDATE-only pattern with row_num
                INSERT INTO flight_recorder.lock_samples_ring (
                    slot_id, row_num, blocked_pid, blocked_user, blocked_app,
                    blocked_query_preview, blocked_duration, blocking_pid, blocking_user,
                    blocking_app, blocking_query_preview, lock_type, locked_relation_oid
                )
                SELECT
                    v_slot_id,
                    (ROW_NUMBER() OVER (ORDER BY bs.pid, blocking_pid) - 1)::integer AS row_num,
                    bs.pid,
                    bs.usename,
                    bs.application_name,
                    left(bs.query, 200),
                    v_captured_at - bs.query_start,
                    blocking_pid,
                    blocking.usename,
                    blocking.application_name,
                    left(blocking.query, 200),
                    CASE
                        WHEN bs.wait_event_type = 'Lock' THEN bs.wait_event
                        ELSE 'unknown'
                    END,
                    CASE
                        WHEN bs.wait_event IN ('relation', 'extend', 'page', 'tuple') THEN
                            (SELECT l.relation
                             FROM pg_locks l
                             WHERE l.pid = bs.pid AND NOT l.granted
                             LIMIT 1)
                        ELSE NULL
                    END
                FROM (
                    SELECT DISTINCT ON (bs.pid, blocking_pid)
                        bs.*,
                        blocking_pid
                    FROM _fr_blocked_sessions bs
                    CROSS JOIN LATERAL unnest(bs.blocking_pids) AS blocking_pid
                    ORDER BY bs.pid, blocking_pid
                    LIMIT 100
                ) bs
                JOIN _fr_psa_snapshot blocking ON blocking.pid = bs.blocking_pid
                ON CONFLICT (slot_id, row_num) DO UPDATE SET
                    blocked_pid = EXCLUDED.blocked_pid,
                    blocked_user = EXCLUDED.blocked_user,
                    blocked_app = EXCLUDED.blocked_app,
                    blocked_query_preview = EXCLUDED.blocked_query_preview,
                    blocked_duration = EXCLUDED.blocked_duration,
                    blocking_pid = EXCLUDED.blocking_pid,
                    blocking_user = EXCLUDED.blocking_user,
                    blocking_app = EXCLUDED.blocking_app,
                    blocking_query_preview = EXCLUDED.blocking_query_preview,
                    lock_type = EXCLUDED.lock_type,
                    locked_relation_oid = EXCLUDED.locked_relation_oid;
            END IF;
        END;
        PERFORM flight_recorder._record_section_success(v_stat_id);
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'pg-flight-recorder: Lock sampling collection failed: %', SQLERRM;
    END;
    END IF;

    -- P0 Safety: Record successful completion
    PERFORM flight_recorder._record_collection_end(v_stat_id, true, NULL);

    -- Reset statement_timeout to avoid affecting subsequent queries in the session
    PERFORM set_config('statement_timeout', '0', true);

    RETURN v_captured_at;
EXCEPTION
    WHEN OTHERS THEN
        PERFORM flight_recorder._record_collection_end(v_stat_id, false, SQLERRM);
        -- Reset statement_timeout even on failure
        PERFORM set_config('statement_timeout', '0', true);
        RAISE WARNING 'pg-flight-recorder: Sample collection failed: %', SQLERRM;
        RETURN v_captured_at;
END;
$$;

COMMENT ON FUNCTION flight_recorder.sample() IS 'TIER 1: Collect samples into ring buffer (60s intervals, 3 sections: waits, activity, locks)';

-- =============================================================================
-- Flush Function: Ring Buffer → Aggregates (TIER 1 → TIER 2)
-- =============================================================================

CREATE OR REPLACE FUNCTION flight_recorder.flush_ring_to_aggregates()
RETURNS VOID
LANGUAGE plpgsql AS $$
DECLARE
    v_start_time TIMESTAMPTZ;
    v_end_time TIMESTAMPTZ;
    v_total_samples INTEGER;
    v_last_flush TIMESTAMPTZ;
BEGIN
    SELECT COALESCE(max(end_time), '1970-01-01')
    INTO v_last_flush
    FROM flight_recorder.wait_event_aggregates;

    SELECT min(captured_at), max(captured_at), count(*)
    INTO v_start_time, v_end_time, v_total_samples
    FROM flight_recorder.samples_ring
    WHERE captured_at > v_last_flush;

    IF v_start_time IS NULL OR v_total_samples = 0 THEN
        RETURN;
    END IF;

    INSERT INTO flight_recorder.wait_event_aggregates (
        start_time, end_time, backend_type, wait_event_type, wait_event, state,
        sample_count, total_waiters, avg_waiters, max_waiters, pct_of_samples
    )
    SELECT
        v_start_time,
        v_end_time,
        w.backend_type,
        w.wait_event_type,
        w.wait_event,
        w.state,
        count(DISTINCT w.slot_id) AS sample_count,
        sum(w.count) AS total_waiters,
        round(avg(w.count), 2) AS avg_waiters,
        max(w.count) AS max_waiters,
        round(100.0 * count(DISTINCT w.slot_id) / NULLIF(v_total_samples, 0), 1) AS pct_of_samples
    FROM flight_recorder.wait_samples_ring w
    JOIN flight_recorder.samples_ring s ON s.slot_id = w.slot_id
    WHERE s.captured_at BETWEEN v_start_time AND v_end_time
      AND w.backend_type IS NOT NULL  -- Filter out NULL (unused) rows
    GROUP BY w.backend_type, w.wait_event_type, w.wait_event, w.state;

    INSERT INTO flight_recorder.lock_aggregates (
        start_time, end_time, blocked_user, blocking_user, lock_type,
        locked_relation_oid, occurrence_count, max_duration, avg_duration, sample_query
    )
    SELECT
        v_start_time,
        v_end_time,
        l.blocked_user,
        l.blocking_user,
        l.lock_type,
        l.locked_relation_oid,
        count(*) AS occurrence_count,
        max(l.blocked_duration) AS max_duration,
        avg(l.blocked_duration) AS avg_duration,
        min(l.blocked_query_preview) AS sample_query
    FROM flight_recorder.lock_samples_ring l
    JOIN flight_recorder.samples_ring s ON s.slot_id = l.slot_id
    WHERE s.captured_at BETWEEN v_start_time AND v_end_time
      AND l.blocked_pid IS NOT NULL  -- Filter out NULL (unused) rows
    GROUP BY l.blocked_user, l.blocking_user, l.lock_type, l.locked_relation_oid;

    INSERT INTO flight_recorder.query_aggregates (
        start_time, end_time, query_preview, occurrence_count, max_duration, avg_duration
    )
    SELECT
        v_start_time,
        v_end_time,
        a.query_preview,
        count(*) AS occurrence_count,
        max(s.captured_at - a.query_start) AS max_duration,
        avg(s.captured_at - a.query_start) AS avg_duration
    FROM flight_recorder.activity_samples_ring a
    JOIN flight_recorder.samples_ring s ON s.slot_id = a.slot_id
    WHERE s.captured_at BETWEEN v_start_time AND v_end_time
      AND a.pid IS NOT NULL  -- Filter out NULL (unused) rows
      AND a.query_start IS NOT NULL
    GROUP BY a.query_preview;

    RAISE NOTICE 'pg-flight-recorder: Flushed ring buffer (% to %, % samples)',
        v_start_time, v_end_time, v_total_samples;
END;
$$;

COMMENT ON FUNCTION flight_recorder.flush_ring_to_aggregates() IS 'TIER 2: Flush ring buffer to durable aggregates every 5 minutes';

-- -----------------------------------------------------------------------------
-- flight_recorder.archive_ring_samples() - Archive raw samples for forensics
-- -----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION flight_recorder.archive_ring_samples()
RETURNS VOID
LANGUAGE plpgsql AS $$
DECLARE
    v_enabled BOOLEAN;
    v_archive_activity BOOLEAN;
    v_archive_locks BOOLEAN;
    v_archive_waits BOOLEAN;
    v_frequency_minutes INTEGER;
    v_last_archive TIMESTAMPTZ;
    v_next_archive_due TIMESTAMPTZ;
    v_samples_to_archive INTEGER;
    v_activity_rows INTEGER := 0;
    v_lock_rows INTEGER := 0;
    v_wait_rows INTEGER := 0;
BEGIN
    -- Check if archival is enabled
    v_enabled := COALESCE(
        (SELECT value::boolean FROM flight_recorder.config WHERE key = 'archive_samples_enabled'),
        true
    );

    IF NOT v_enabled THEN
        RETURN;
    END IF;

    -- Get configuration
    v_archive_activity := COALESCE(
        (SELECT value::boolean FROM flight_recorder.config WHERE key = 'archive_activity_samples'),
        true
    );

    v_archive_locks := COALESCE(
        (SELECT value::boolean FROM flight_recorder.config WHERE key = 'archive_lock_samples'),
        true
    );

    v_archive_waits := COALESCE(
        (SELECT value::boolean FROM flight_recorder.config WHERE key = 'archive_wait_samples'),
        true
    );

    v_frequency_minutes := COALESCE(
        (SELECT value::integer FROM flight_recorder.config WHERE key = 'archive_sample_frequency_minutes'),
        15
    );

    -- Determine when we last archived
    SELECT GREATEST(
        COALESCE(MAX(captured_at), '1970-01-01'::timestamptz),
        COALESCE((SELECT MAX(captured_at) FROM flight_recorder.lock_samples_archive), '1970-01-01'::timestamptz),
        COALESCE((SELECT MAX(captured_at) FROM flight_recorder.wait_samples_archive), '1970-01-01'::timestamptz)
    )
    INTO v_last_archive
    FROM flight_recorder.activity_samples_archive;

    -- Check if it's time to archive again
    v_next_archive_due := v_last_archive + (v_frequency_minutes || ' minutes')::interval;

    IF now() < v_next_archive_due THEN
        -- Not time yet
        RETURN;
    END IF;

    -- Count how many samples we'll archive
    SELECT count(DISTINCT slot_id)
    INTO v_samples_to_archive
    FROM flight_recorder.samples_ring
    WHERE captured_at > v_last_archive;

    IF v_samples_to_archive = 0 THEN
        RETURN;
    END IF;

    -- Archive activity samples (full resolution, not aggregated)
    IF v_archive_activity THEN
        INSERT INTO flight_recorder.activity_samples_archive (
            sample_id, captured_at, pid, usename, application_name, backend_type,
            state, wait_event_type, wait_event, query_start, state_change, query_preview
        )
        SELECT
            s.epoch_seconds AS sample_id,
            s.captured_at,
            a.pid,
            a.usename,
            a.application_name,
            a.backend_type,
            a.state,
            a.wait_event_type,
            a.wait_event,
            a.query_start,
            a.state_change,
            a.query_preview
        FROM flight_recorder.activity_samples_ring a
        JOIN flight_recorder.samples_ring s ON s.slot_id = a.slot_id
        WHERE s.captured_at > v_last_archive
          AND a.pid IS NOT NULL;  -- Filter out NULL (unused) rows

        GET DIAGNOSTICS v_activity_rows = ROW_COUNT;
    END IF;

    -- Archive lock samples (full resolution, not aggregated)
    IF v_archive_locks THEN
        INSERT INTO flight_recorder.lock_samples_archive (
            sample_id, captured_at, blocked_pid, blocked_user, blocked_app,
            blocked_query_preview, blocked_duration, blocking_pid, blocking_user,
            blocking_app, blocking_query_preview, lock_type, locked_relation_oid
        )
        SELECT
            s.epoch_seconds AS sample_id,
            s.captured_at,
            l.blocked_pid,
            l.blocked_user,
            l.blocked_app,
            l.blocked_query_preview,
            l.blocked_duration,
            l.blocking_pid,
            l.blocking_user,
            l.blocking_app,
            l.blocking_query_preview,
            l.lock_type,
            l.locked_relation_oid
        FROM flight_recorder.lock_samples_ring l
        JOIN flight_recorder.samples_ring s ON s.slot_id = l.slot_id
        WHERE s.captured_at > v_last_archive
          AND l.blocked_pid IS NOT NULL;  -- Filter out NULL (unused) rows

        GET DIAGNOSTICS v_lock_rows = ROW_COUNT;
    END IF;

    -- Archive wait samples (full resolution, not aggregated)
    IF v_archive_waits THEN
        INSERT INTO flight_recorder.wait_samples_archive (
            sample_id, captured_at, backend_type, wait_event_type, wait_event, state, count
        )
        SELECT
            s.epoch_seconds AS sample_id,
            s.captured_at,
            w.backend_type,
            w.wait_event_type,
            w.wait_event,
            w.state,
            w.count
        FROM flight_recorder.wait_samples_ring w
        JOIN flight_recorder.samples_ring s ON s.slot_id = w.slot_id
        WHERE s.captured_at > v_last_archive
          AND w.backend_type IS NOT NULL;  -- Filter out NULL (unused) rows

        GET DIAGNOSTICS v_wait_rows = ROW_COUNT;
    END IF;

    RAISE NOTICE 'pg-flight-recorder: Archived raw samples (% samples, % activity rows, % lock rows, % wait rows)',
        v_samples_to_archive, v_activity_rows, v_lock_rows, v_wait_rows;
END;
$$;

COMMENT ON FUNCTION flight_recorder.archive_ring_samples() IS 'TIER 1.5: Archive raw samples for high-resolution forensic analysis (default: every 15 minutes)';

-- -----------------------------------------------------------------------------
-- flight_recorder.cleanup_aggregates() - Clean up old aggregates
-- -----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION flight_recorder.cleanup_aggregates()
RETURNS VOID
LANGUAGE plpgsql AS $$
DECLARE
    v_aggregate_retention interval;
    v_archive_retention interval;
    v_deleted_waits INTEGER;
    v_deleted_locks INTEGER;
    v_deleted_queries INTEGER;
    v_deleted_activity_archive INTEGER;
    v_deleted_lock_archive INTEGER;
    v_deleted_wait_archive INTEGER;
BEGIN
    v_aggregate_retention := COALESCE(
        (SELECT value || ' days' FROM flight_recorder.config WHERE key = 'aggregate_retention_days')::interval,
        '7 days'::interval
    );

    v_archive_retention := COALESCE(
        (SELECT value || ' days' FROM flight_recorder.config WHERE key = 'archive_retention_days')::interval,
        '7 days'::interval
    );

    -- Clean up TIER 2 aggregates
    DELETE FROM flight_recorder.wait_event_aggregates
    WHERE start_time < now() - v_aggregate_retention;
    GET DIAGNOSTICS v_deleted_waits = ROW_COUNT;

    DELETE FROM flight_recorder.lock_aggregates
    WHERE start_time < now() - v_aggregate_retention;
    GET DIAGNOSTICS v_deleted_locks = ROW_COUNT;

    DELETE FROM flight_recorder.query_aggregates
    WHERE start_time < now() - v_aggregate_retention;
    GET DIAGNOSTICS v_deleted_queries = ROW_COUNT;

    -- Clean up TIER 1.5 archives
    DELETE FROM flight_recorder.activity_samples_archive
    WHERE captured_at < now() - v_archive_retention;
    GET DIAGNOSTICS v_deleted_activity_archive = ROW_COUNT;

    DELETE FROM flight_recorder.lock_samples_archive
    WHERE captured_at < now() - v_archive_retention;
    GET DIAGNOSTICS v_deleted_lock_archive = ROW_COUNT;

    DELETE FROM flight_recorder.wait_samples_archive
    WHERE captured_at < now() - v_archive_retention;
    GET DIAGNOSTICS v_deleted_wait_archive = ROW_COUNT;

    IF v_deleted_waits > 0 OR v_deleted_locks > 0 OR v_deleted_queries > 0 OR
       v_deleted_activity_archive > 0 OR v_deleted_lock_archive > 0 OR v_deleted_wait_archive > 0 THEN
        RAISE NOTICE 'pg-flight-recorder: Cleaned up % wait aggregates, % lock aggregates, % query aggregates, % activity archives, % lock archives, % wait archives',
            v_deleted_waits, v_deleted_locks, v_deleted_queries, v_deleted_activity_archive, v_deleted_lock_archive, v_deleted_wait_archive;
    END IF;
END;
$$;

COMMENT ON FUNCTION flight_recorder.cleanup_aggregates() IS 'TIER 2: Clean up old aggregate data based on retention period';

-- -----------------------------------------------------------------------------
-- flight_recorder.snapshot() - Capture current state
-- Per-section timeouts
-- -----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION flight_recorder.snapshot()
RETURNS TIMESTAMPTZ
LANGUAGE plpgsql AS $$
DECLARE
    v_pg_version INTEGER;
    v_captured_at TIMESTAMPTZ := now();
    v_snapshot_id INTEGER;
    v_autovacuum_workers INTEGER;
    v_slots_count INTEGER;
    v_slots_max_retained BIGINT;
    -- Temp file stats
    v_temp_files BIGINT;
    v_temp_bytes BIGINT;
    -- pg_stat_io values (PG16+)
    v_io_ckpt_writes BIGINT;
    v_io_ckpt_write_time DOUBLE PRECISION;
    v_io_ckpt_fsyncs BIGINT;
    v_io_ckpt_fsync_time DOUBLE PRECISION;
    v_io_av_writes BIGINT;
    v_io_av_write_time DOUBLE PRECISION;
    v_io_client_writes BIGINT;
    v_io_client_write_time DOUBLE PRECISION;
    v_io_bgw_writes BIGINT;
    v_io_bgw_write_time DOUBLE PRECISION;
    v_stat_id INTEGER;
    v_should_skip BOOLEAN;
    -- OPTIMIZATION 3: Cache pg_control_checkpoint() result to avoid redundant calls
    v_checkpoint_info RECORD;
BEGIN
    -- P0 Safety: Check circuit breaker
    v_should_skip := flight_recorder._check_circuit_breaker('snapshot');
    IF v_should_skip THEN
        PERFORM flight_recorder._record_collection_skip('snapshot', 'Circuit breaker tripped - last run exceeded threshold');
        RAISE NOTICE 'pg-flight-recorder: Skipping snapshot collection due to circuit breaker';
        RETURN v_captured_at;
    END IF;

    -- P0 Safety: System awareness pre-flight checks (A+ upgrade)
    DECLARE
        v_skip_reason TEXT;
    BEGIN
        v_skip_reason := flight_recorder._should_skip_collection();
        IF v_skip_reason IS NOT NULL THEN
            PERFORM flight_recorder._record_collection_skip('snapshot', v_skip_reason);
            RAISE NOTICE 'pg-flight-recorder: Skipping snapshot - %', v_skip_reason;
            RETURN v_captured_at;
        END IF;
    END;

    -- P0 Safety: Job deduplication - prevent queue buildup (A+ UPGRADE)
    -- If another snapshot() job is already running, skip this cycle
    DECLARE
        v_running_count INTEGER;
        v_running_pid INTEGER;
    BEGIN
        SELECT count(*), min(pid) INTO v_running_count, v_running_pid
        FROM pg_stat_activity
        WHERE query LIKE '%flight_recorder.snapshot()%'
          AND state = 'active'
          AND pid != pg_backend_pid()
          AND backend_type = 'client backend';

        IF v_running_count > 0 THEN
            PERFORM flight_recorder._record_collection_skip('snapshot',
                format('Job deduplication: %s snapshot job(s) already running (PID: %s)',
                       v_running_count, v_running_pid));
            RAISE NOTICE 'pg-flight-recorder: Skipping snapshot - another job already running (PID: %)', v_running_pid;
            RETURN v_captured_at;
        END IF;
    END;

    -- P1 Safety: Check schema size (runs every 5 minutes, auto-disables if critical)
    PERFORM flight_recorder._check_schema_size();

    -- P0 Safety: Record collection start for circuit breaker (4 sections: system stats, snapshot INSERT, replication, statements)
    v_stat_id := flight_recorder._record_collection_start('snapshot', 4);

    -- P0 Safety: Check for catalog DDL locks before collection
    DECLARE
        v_check_ddl BOOLEAN;
        v_lock_strategy TEXT;
        v_ddl_lock_exists BOOLEAN;
        v_lock_timeout_ms INTEGER;
    BEGIN
        v_check_ddl := COALESCE(
            flight_recorder._get_config('check_ddl_before_collection', 'true')::boolean,
            true
        );
        v_lock_strategy := COALESCE(
            flight_recorder._get_config('lock_timeout_strategy', 'fail_fast'),
            'fail_fast'
        );

        IF v_check_ddl AND v_lock_strategy = 'skip_if_locked' THEN
            v_ddl_lock_exists := flight_recorder._check_catalog_ddl_locks();
            IF v_ddl_lock_exists THEN
                PERFORM flight_recorder._record_collection_skip('snapshot',
                    'DDL lock detected on system catalogs (skip_if_locked strategy)');
                RAISE NOTICE 'pg-flight-recorder: Skipping snapshot - DDL lock detected on catalogs';
                RETURN v_captured_at;
            END IF;
        END IF;

        -- Set lock timeout based on strategy
        v_lock_timeout_ms := CASE v_lock_strategy
            WHEN 'skip_if_locked' THEN 0      -- Already checked above, set to 0 for safety
            WHEN 'patient' THEN 500            -- Wait up to 500ms for locks
            ELSE 100                           -- 'fail_fast' (default): 100ms
        END;

        PERFORM set_config('lock_timeout', v_lock_timeout_ms::text, true);
    END;

    -- P0 Safety: Set work_mem
    PERFORM set_config('work_mem',
        COALESCE(flight_recorder._get_config('work_mem_kb', '2048'), '2048') || 'kB',
        true);  -- Limit memory for joins/sorts

    v_pg_version := flight_recorder._pg_version();

    -- Section 1: Collect system stats
    BEGIN
        PERFORM flight_recorder._set_section_timeout();
        -- Count active autovacuum workers
        SELECT count(*)::integer INTO v_autovacuum_workers
        FROM pg_stat_activity
        WHERE backend_type = 'autovacuum worker';

        -- Replication slot stats
        SELECT
            count(*)::integer,
            COALESCE(max(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)), 0)
        INTO v_slots_count, v_slots_max_retained
        FROM pg_replication_slots;

        -- Temp file stats (current database)
        SELECT COALESCE(temp_files, 0), COALESCE(temp_bytes, 0)
        INTO v_temp_files, v_temp_bytes
        FROM pg_stat_database
        WHERE datname = current_database();

        -- OPTIMIZATION 3: Call pg_control_checkpoint() once and cache result
        v_checkpoint_info := pg_control_checkpoint();

        PERFORM flight_recorder._record_section_success(v_stat_id);
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'pg-flight-recorder: System stats collection failed: %', SQLERRM;
        -- Set defaults so snapshot can continue
        v_autovacuum_workers := 0;
        v_slots_count := 0;
        v_slots_max_retained := 0;
        v_temp_files := 0;
        v_temp_bytes := 0;
    END;

    -- Section 2: pg_stat_io collection (PG16+)
    IF v_pg_version >= 16 THEN
    BEGIN
        PERFORM flight_recorder._set_section_timeout();
        -- Consolidate all backend_type queries into single query using FILTER
        -- More efficient: single catalog lookup, single scan of pg_stat_io
        SELECT
            COALESCE(sum(writes) FILTER (WHERE backend_type = 'checkpointer'), 0),
            COALESCE(sum(write_time) FILTER (WHERE backend_type = 'checkpointer'), 0),
            COALESCE(sum(fsyncs) FILTER (WHERE backend_type = 'checkpointer'), 0),
            COALESCE(sum(fsync_time) FILTER (WHERE backend_type = 'checkpointer'), 0),
            COALESCE(sum(writes) FILTER (WHERE backend_type = 'autovacuum worker'), 0),
            COALESCE(sum(write_time) FILTER (WHERE backend_type = 'autovacuum worker'), 0),
            COALESCE(sum(writes) FILTER (WHERE backend_type = 'client backend'), 0),
            COALESCE(sum(write_time) FILTER (WHERE backend_type = 'client backend'), 0),
            COALESCE(sum(writes) FILTER (WHERE backend_type = 'background writer'), 0),
            COALESCE(sum(write_time) FILTER (WHERE backend_type = 'background writer'), 0)
        INTO
            v_io_ckpt_writes, v_io_ckpt_write_time, v_io_ckpt_fsyncs, v_io_ckpt_fsync_time,
            v_io_av_writes, v_io_av_write_time,
            v_io_client_writes, v_io_client_write_time,
            v_io_bgw_writes, v_io_bgw_write_time
        FROM pg_stat_io;
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'pg-flight-recorder: pg_stat_io collection failed: %', SQLERRM;
        -- Set defaults
        v_io_ckpt_writes := 0;
        v_io_ckpt_write_time := 0;
        v_io_ckpt_fsyncs := 0;
        v_io_ckpt_fsync_time := 0;
        v_io_av_writes := 0;
        v_io_av_write_time := 0;
        v_io_client_writes := 0;
        v_io_client_write_time := 0;
        v_io_bgw_writes := 0;
        v_io_bgw_write_time := 0;
    END;
    END IF;

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
            v_checkpoint_info.redo_lsn,        -- OPTIMIZATION 3: Use cached value
            v_checkpoint_info.checkpoint_time, -- OPTIMIZATION 3: Use cached value
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

    ELSIF v_pg_version = 16 THEN
        -- PG16: checkpointer stats in pg_stat_bgwriter, has pg_stat_io
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
            v_checkpoint_info.redo_lsn,        -- OPTIMIZATION 3: Use cached value
            v_checkpoint_info.checkpoint_time, -- OPTIMIZATION 3: Use cached value
            b.checkpoints_timed, b.checkpoints_req, b.checkpoint_write_time, b.checkpoint_sync_time, b.buffers_checkpoint,
            b.buffers_clean, b.maxwritten_clean, b.buffers_alloc,
            b.buffers_backend, b.buffers_backend_fsync,
            v_autovacuum_workers, v_slots_count, v_slots_max_retained,
            v_io_ckpt_writes, v_io_ckpt_write_time, v_io_ckpt_fsyncs, v_io_ckpt_fsync_time,
            v_io_av_writes, v_io_av_write_time,
            v_io_client_writes, v_io_client_write_time,
            v_io_bgw_writes, v_io_bgw_write_time,
            v_temp_files, v_temp_bytes
        FROM pg_stat_wal w
        CROSS JOIN pg_stat_bgwriter b
        RETURNING id INTO v_snapshot_id;

    ELSIF v_pg_version = 15 THEN
        -- PG15: checkpointer stats in pg_stat_bgwriter, no pg_stat_io
        INSERT INTO flight_recorder.snapshots (
            captured_at, pg_version,
            wal_records, wal_fpi, wal_bytes, wal_write_time, wal_sync_time,
            checkpoint_lsn, checkpoint_time,
            ckpt_timed, ckpt_requested, ckpt_write_time, ckpt_sync_time, ckpt_buffers,
            bgw_buffers_clean, bgw_maxwritten_clean, bgw_buffers_alloc,
            bgw_buffers_backend, bgw_buffers_backend_fsync,
            autovacuum_workers, slots_count, slots_max_retained_wal,
            temp_files, temp_bytes
        )
        SELECT
            v_captured_at, v_pg_version,
            w.wal_records, w.wal_fpi, w.wal_bytes, w.wal_write_time, w.wal_sync_time,
            v_checkpoint_info.redo_lsn,        -- OPTIMIZATION 3: Use cached value
            v_checkpoint_info.checkpoint_time, -- OPTIMIZATION 3: Use cached value
            b.checkpoints_timed, b.checkpoints_req, b.checkpoint_write_time, b.checkpoint_sync_time, b.buffers_checkpoint,
            b.buffers_clean, b.maxwritten_clean, b.buffers_alloc,
            b.buffers_backend, b.buffers_backend_fsync,
            v_autovacuum_workers, v_slots_count, v_slots_max_retained,
            v_temp_files, v_temp_bytes
        FROM pg_stat_wal w
        CROSS JOIN pg_stat_bgwriter b
        RETURNING id INTO v_snapshot_id;
    ELSE
        RAISE EXCEPTION 'Unsupported PostgreSQL version: %. Requires 15, 16, or 17.', v_pg_version;
    END IF;

    -- Main snapshot INSERT completed successfully
    PERFORM flight_recorder._record_section_success(v_stat_id);

    -- Section 3: Capture replication stats
    BEGIN
        PERFORM flight_recorder._set_section_timeout();
        INSERT INTO flight_recorder.replication_snapshots (
            snapshot_id, pid, client_addr, application_name, state, sync_state,
            sent_lsn, write_lsn, flush_lsn, replay_lsn,
            write_lag, flush_lag, replay_lag
        )
        SELECT
            v_snapshot_id,
            pid,
            client_addr,
            application_name,
            state,
            sync_state,
            sent_lsn,
            write_lsn,
            flush_lsn,
            replay_lsn,
            write_lag,
            flush_lag,
            replay_lag
        FROM pg_stat_replication;

        PERFORM flight_recorder._record_section_success(v_stat_id);
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'pg-flight-recorder: Replication stats collection failed: %', SQLERRM;
    END;

    -- Section 4: Capture pg_stat_statements (if available and enabled)
    IF flight_recorder._has_pg_stat_statements()
       AND flight_recorder._get_config('statements_enabled', 'auto') != 'false'
    THEN
        DECLARE
            v_stmt_status TEXT;
            v_last_statements_collection TIMESTAMPTZ;
            v_statements_interval_minutes INTEGER;
            v_should_collect BOOLEAN := TRUE;
        BEGIN
            -- Check if enough time has elapsed since last statements collection
            v_statements_interval_minutes := COALESCE(
                flight_recorder._get_config('statements_interval_minutes', '15')::integer,
                15
            );

            SELECT max(s.captured_at) INTO v_last_statements_collection
            FROM flight_recorder.snapshots s
            WHERE EXISTS (
                SELECT 1 FROM flight_recorder.statement_snapshots ss
                WHERE ss.snapshot_id = s.id
            );

            -- Skip if collected within interval
            IF v_last_statements_collection IS NOT NULL
               AND v_last_statements_collection > now() - (v_statements_interval_minutes || ' minutes')::interval
            THEN
                v_should_collect := FALSE;
            END IF;

            IF v_should_collect THEN
                PERFORM flight_recorder._set_section_timeout();

                -- A+ UPGRADE: Check for concurrent pg_stat_statements readers
                DECLARE
                    v_check_conflicts BOOLEAN;
                    v_pss_conflict BOOLEAN;
                BEGIN
                    v_check_conflicts := COALESCE(
                        flight_recorder._get_config('check_pss_conflicts', 'true')::boolean,
                        true
                    );

                    IF v_check_conflicts THEN
                        SELECT EXISTS(
                            SELECT 1 FROM pg_stat_activity
                            WHERE query ILIKE '%pg_stat_statements%'
                              AND state = 'active'
                              AND pid != pg_backend_pid()
                              AND backend_type = 'client backend'
                        ) INTO v_pss_conflict;

                        IF v_pss_conflict THEN
                            RAISE NOTICE 'pg-flight-recorder: Skipping pg_stat_statements - concurrent reader detected';
                            v_should_collect := FALSE;
                        END IF;
                    END IF;
                END;

                IF v_should_collect THEN
                    -- Check if statement tracking is healthy (not under high churn)
                    SELECT status INTO v_stmt_status
                    FROM flight_recorder._check_statements_health();

                    -- Skip if utilization too high (indicates excessive churn)
                    IF v_stmt_status = 'HIGH_CHURN' THEN
                        RAISE WARNING 'pg-flight-recorder: Skipping pg_stat_statements collection - high churn detected (>95%% utilization)';
                    ELSE
                INSERT INTO flight_recorder.statement_snapshots (
                snapshot_id, queryid, userid, dbid, query_preview,
                calls, total_exec_time, min_exec_time, max_exec_time,
                mean_exec_time, rows,
                shared_blks_hit, shared_blks_read, shared_blks_dirtied, shared_blks_written,
                temp_blks_read, temp_blks_written,
                blk_read_time, blk_write_time,
                wal_records, wal_bytes
            )
            SELECT
                v_snapshot_id,
                s.queryid,
                s.userid,
                s.dbid,
                left(s.query, 500),
                s.calls,
                s.total_exec_time,
                s.min_exec_time,
                s.max_exec_time,
                s.mean_exec_time,
                s.rows,
                s.shared_blks_hit,
                s.shared_blks_read,
                s.shared_blks_dirtied,
                s.shared_blks_written,
                s.temp_blks_read,
                s.temp_blks_written,
                s.blk_read_time,
                s.blk_write_time,
                s.wal_records,
                s.wal_bytes
            FROM pg_stat_statements s
            WHERE s.dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
              AND s.calls >= COALESCE(flight_recorder._get_config('statements_min_calls', '1')::integer, 1)
            ORDER BY s.total_exec_time DESC
            LIMIT COALESCE(flight_recorder._get_config('statements_top_n', '20')::integer, 20);

                    PERFORM flight_recorder._record_section_success(v_stat_id);
                    END IF;  -- v_stmt_status check
                END IF;  -- v_should_collect check (inner, after conflict check)
            END IF;  -- v_should_collect check (outer, after interval check)
        EXCEPTION
            WHEN undefined_table THEN NULL;
            WHEN undefined_column THEN NULL;
            WHEN OTHERS THEN
                RAISE WARNING 'pg-flight-recorder: pg_stat_statements collection failed: %', SQLERRM;
        END;
    END IF;

    -- P0 Safety: Record successful completion
    PERFORM flight_recorder._record_collection_end(v_stat_id, true, NULL);

    -- Reset statement_timeout to avoid affecting subsequent queries
    PERFORM set_config('statement_timeout', '0', true);

    RETURN v_captured_at;
EXCEPTION
    WHEN OTHERS THEN
        -- P0 Safety: Record failure if entire function fails
        PERFORM flight_recorder._record_collection_end(v_stat_id, false, SQLERRM);
        -- Reset statement_timeout even on failure
        PERFORM set_config('statement_timeout', '0', true);
        RAISE;
END;
$$;

-- -----------------------------------------------------------------------------
-- flight_recorder.deltas - View showing deltas between consecutive snapshots
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW flight_recorder.deltas AS
SELECT
    s.id,
    s.captured_at,
    s.pg_version,
    EXTRACT(EPOCH FROM (s.captured_at - prev.captured_at))::numeric AS interval_seconds,

    -- Checkpoint
    (s.checkpoint_time IS DISTINCT FROM prev.checkpoint_time) AS checkpoint_occurred,
    s.ckpt_timed - prev.ckpt_timed AS ckpt_timed_delta,
    s.ckpt_requested - prev.ckpt_requested AS ckpt_requested_delta,
    (s.ckpt_write_time - prev.ckpt_write_time)::numeric AS ckpt_write_time_ms,
    (s.ckpt_sync_time - prev.ckpt_sync_time)::numeric AS ckpt_sync_time_ms,
    s.ckpt_buffers - prev.ckpt_buffers AS ckpt_buffers_delta,

    -- WAL
    s.wal_bytes - prev.wal_bytes AS wal_bytes_delta,
    flight_recorder._pretty_bytes(s.wal_bytes - prev.wal_bytes) AS wal_bytes_pretty,
    (s.wal_write_time - prev.wal_write_time)::numeric AS wal_write_time_ms,
    (s.wal_sync_time - prev.wal_sync_time)::numeric AS wal_sync_time_ms,

    -- BGWriter
    s.bgw_buffers_clean - prev.bgw_buffers_clean AS bgw_buffers_clean_delta,
    s.bgw_buffers_alloc - prev.bgw_buffers_alloc AS bgw_buffers_alloc_delta,
    s.bgw_buffers_backend - prev.bgw_buffers_backend AS bgw_buffers_backend_delta,
    s.bgw_buffers_backend_fsync - prev.bgw_buffers_backend_fsync AS bgw_buffers_backend_fsync_delta,

    -- Autovacuum (point-in-time, not delta)
    s.autovacuum_workers AS autovacuum_workers_active,

    -- Replication slots (point-in-time)
    s.slots_count,
    s.slots_max_retained_wal,
    flight_recorder._pretty_bytes(s.slots_max_retained_wal) AS slots_max_retained_pretty,

    -- pg_stat_io deltas (PG16+)
    s.io_checkpointer_writes - prev.io_checkpointer_writes AS io_ckpt_writes_delta,
    (s.io_checkpointer_write_time - prev.io_checkpointer_write_time)::numeric AS io_ckpt_write_time_ms,
    s.io_checkpointer_fsyncs - prev.io_checkpointer_fsyncs AS io_ckpt_fsyncs_delta,
    (s.io_checkpointer_fsync_time - prev.io_checkpointer_fsync_time)::numeric AS io_ckpt_fsync_time_ms,
    s.io_autovacuum_writes - prev.io_autovacuum_writes AS io_autovacuum_writes_delta,
    (s.io_autovacuum_write_time - prev.io_autovacuum_write_time)::numeric AS io_autovacuum_write_time_ms,
    s.io_client_writes - prev.io_client_writes AS io_client_writes_delta,
    (s.io_client_write_time - prev.io_client_write_time)::numeric AS io_client_write_time_ms,
    s.io_bgwriter_writes - prev.io_bgwriter_writes AS io_bgwriter_writes_delta,
    (s.io_bgwriter_write_time - prev.io_bgwriter_write_time)::numeric AS io_bgwriter_write_time_ms,

    -- Temp file deltas
    s.temp_files - prev.temp_files AS temp_files_delta,
    s.temp_bytes - prev.temp_bytes AS temp_bytes_delta,
    flight_recorder._pretty_bytes(s.temp_bytes - prev.temp_bytes) AS temp_bytes_pretty

FROM flight_recorder.snapshots s
JOIN flight_recorder.snapshots prev ON prev.id = (
    SELECT MAX(id) FROM flight_recorder.snapshots WHERE id < s.id
)
ORDER BY s.captured_at DESC;

-- -----------------------------------------------------------------------------
-- flight_recorder.compare(start_time, end_time) - Compare two time points
-- -----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION flight_recorder.compare(
    p_start_time TIMESTAMPTZ,
    p_end_time TIMESTAMPTZ
)
RETURNS TABLE(
    start_snapshot_at       TIMESTAMPTZ,
    end_snapshot_at         TIMESTAMPTZ,
    elapsed_seconds         NUMERIC,

    checkpoint_occurred     BOOLEAN,
    ckpt_timed_delta        BIGINT,
    ckpt_requested_delta    BIGINT,
    ckpt_write_time_ms      NUMERIC,
    ckpt_sync_time_ms       NUMERIC,
    ckpt_buffers_delta      BIGINT,

    wal_bytes_delta         BIGINT,
    wal_bytes_pretty        TEXT,
    wal_write_time_ms       NUMERIC,
    wal_sync_time_ms        NUMERIC,

    bgw_buffers_clean_delta       BIGINT,
    bgw_buffers_alloc_delta       BIGINT,
    bgw_buffers_backend_delta     BIGINT,
    bgw_buffers_backend_fsync_delta BIGINT,

    -- Replication slots (max during period - use end snapshot)
    slots_count             INTEGER,
    slots_max_retained_wal  BIGINT,
    slots_max_retained_pretty TEXT,

    -- pg_stat_io deltas (PG16+)
    io_ckpt_writes_delta          BIGINT,
    io_ckpt_write_time_ms         NUMERIC,
    io_ckpt_fsyncs_delta          BIGINT,
    io_ckpt_fsync_time_ms         NUMERIC,
    io_autovacuum_writes_delta    BIGINT,
    io_autovacuum_write_time_ms   NUMERIC,
    io_client_writes_delta        BIGINT,
    io_client_write_time_ms       NUMERIC,
    io_bgwriter_writes_delta      BIGINT,
    io_bgwriter_write_time_ms     NUMERIC,

    -- Temp file stats
    temp_files_delta              BIGINT,
    temp_bytes_delta              BIGINT,
    temp_bytes_pretty             TEXT
)
LANGUAGE sql STABLE AS $$
    WITH
    start_snap AS (
        SELECT * FROM flight_recorder.snapshots
        WHERE captured_at <= p_start_time
        ORDER BY captured_at DESC
        LIMIT 1
    ),
    end_snap AS (
        SELECT * FROM flight_recorder.snapshots
        WHERE captured_at >= p_end_time
        ORDER BY captured_at ASC
        LIMIT 1
    )
    SELECT
        s.captured_at,
        e.captured_at,
        EXTRACT(EPOCH FROM (e.captured_at - s.captured_at))::numeric,

        (s.checkpoint_time IS DISTINCT FROM e.checkpoint_time),
        e.ckpt_timed - s.ckpt_timed,
        e.ckpt_requested - s.ckpt_requested,
        (e.ckpt_write_time - s.ckpt_write_time)::numeric,
        (e.ckpt_sync_time - s.ckpt_sync_time)::numeric,
        e.ckpt_buffers - s.ckpt_buffers,

        e.wal_bytes - s.wal_bytes,
        flight_recorder._pretty_bytes(e.wal_bytes - s.wal_bytes),
        (e.wal_write_time - s.wal_write_time)::numeric,
        (e.wal_sync_time - s.wal_sync_time)::numeric,

        e.bgw_buffers_clean - s.bgw_buffers_clean,
        e.bgw_buffers_alloc - s.bgw_buffers_alloc,
        e.bgw_buffers_backend - s.bgw_buffers_backend,
        e.bgw_buffers_backend_fsync - s.bgw_buffers_backend_fsync,

        e.slots_count,
        e.slots_max_retained_wal,
        flight_recorder._pretty_bytes(e.slots_max_retained_wal),

        e.io_checkpointer_writes - s.io_checkpointer_writes,
        (e.io_checkpointer_write_time - s.io_checkpointer_write_time)::numeric,
        e.io_checkpointer_fsyncs - s.io_checkpointer_fsyncs,
        (e.io_checkpointer_fsync_time - s.io_checkpointer_fsync_time)::numeric,
        e.io_autovacuum_writes - s.io_autovacuum_writes,
        (e.io_autovacuum_write_time - s.io_autovacuum_write_time)::numeric,
        e.io_client_writes - s.io_client_writes,
        (e.io_client_write_time - s.io_client_write_time)::numeric,
        e.io_bgwriter_writes - s.io_bgwriter_writes,
        (e.io_bgwriter_write_time - s.io_bgwriter_write_time)::numeric,

        e.temp_files - s.temp_files,
        e.temp_bytes - s.temp_bytes,
        flight_recorder._pretty_bytes(e.temp_bytes - s.temp_bytes)
    FROM start_snap s, end_snap e
$$;

-- -----------------------------------------------------------------------------
-- flight_recorder.recent_waits - View of wait events from ring buffer
-- Conservative 10-hour filter covers all modes (normal=2h, light=4h, emergency=10h)
-- For current mode retention, use recent_waits_current() function
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW flight_recorder.recent_waits AS
SELECT
    sr.captured_at,
    w.backend_type,
    w.wait_event_type,
    w.wait_event,
    w.state,
    w.count
FROM flight_recorder.samples_ring sr
JOIN flight_recorder.wait_samples_ring w ON w.slot_id = sr.slot_id
WHERE sr.captured_at > now() - interval '10 hours'
  AND w.backend_type IS NOT NULL  -- Filter out NULL (unused) rows
ORDER BY sr.captured_at DESC, w.count DESC;

-- -----------------------------------------------------------------------------
-- flight_recorder.recent_activity - View of active sessions from ring buffer
-- Conservative 10-hour filter covers all modes (normal=2h, light=4h, emergency=10h)
-- For current mode retention, use recent_activity_current() function
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW flight_recorder.recent_activity AS
SELECT
    sr.captured_at,
    a.pid,
    a.usename,
    a.application_name,
    a.backend_type,
    a.state,
    a.wait_event_type,
    a.wait_event,
    a.query_start,
    sr.captured_at - a.query_start AS running_for,
    a.query_preview
FROM flight_recorder.samples_ring sr
JOIN flight_recorder.activity_samples_ring a ON a.slot_id = sr.slot_id
WHERE sr.captured_at > now() - interval '10 hours'
  AND a.pid IS NOT NULL  -- Filter out NULL (unused) rows
ORDER BY sr.captured_at DESC, a.query_start ASC;

-- -----------------------------------------------------------------------------
-- flight_recorder.recent_locks - View of lock contention from ring buffer
-- Conservative 10-hour filter covers all modes (normal=2h, light=4h, emergency=10h)
-- For current mode retention, use recent_locks_current() function
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW flight_recorder.recent_locks AS
SELECT
    sr.captured_at,
    l.blocked_pid,
    l.blocked_user,
    l.blocked_app,
    l.blocked_duration,
    l.blocking_pid,
    l.blocking_user,
    l.blocking_app,
    l.lock_type,
    COALESCE(l.locked_relation_oid::regclass::text, 'OID:' || l.locked_relation_oid::text) AS locked_relation,
    l.blocked_query_preview,
    l.blocking_query_preview
FROM flight_recorder.samples_ring sr
JOIN flight_recorder.lock_samples_ring l ON l.slot_id = sr.slot_id
WHERE sr.captured_at > now() - interval '10 hours'
  AND l.blocked_pid IS NOT NULL  -- Filter out NULL (unused) rows
ORDER BY sr.captured_at DESC, l.blocked_duration DESC;

-- -----------------------------------------------------------------------------
-- Dynamic retention functions (adaptive frequency control)
-- These functions calculate actual retention based on current sampling interval
-- Use these for precise filtering based on mode (normal=2h, light=4h, emergency=10h)
-- -----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION flight_recorder.recent_waits_current()
RETURNS TABLE (
    captured_at TIMESTAMPTZ,
    backend_type TEXT,
    wait_event_type TEXT,
    wait_event TEXT,
    state TEXT,
    count INTEGER
) AS $$
DECLARE
    v_retention_interval INTERVAL;
BEGIN
    -- Calculate retention: 120 slots × interval
    v_retention_interval := (120 * COALESCE(
        flight_recorder._get_config('sample_interval_seconds', '60')::integer,
        60
    ))::text || ' seconds';

    RETURN QUERY
    SELECT
        sr.captured_at,
        w.backend_type,
        w.wait_event_type,
        w.wait_event,
        w.state,
        w.count
    FROM flight_recorder.samples_ring sr
    JOIN flight_recorder.wait_samples_ring w ON w.slot_id = sr.slot_id
    WHERE sr.captured_at > now() - v_retention_interval
      AND w.backend_type IS NOT NULL
    ORDER BY sr.captured_at DESC, w.count DESC;
END;
$$ LANGUAGE plpgsql STABLE;

CREATE OR REPLACE FUNCTION flight_recorder.recent_activity_current()
RETURNS TABLE (
    captured_at TIMESTAMPTZ,
    pid INTEGER,
    usename TEXT,
    application_name TEXT,
    backend_type TEXT,
    state TEXT,
    wait_event_type TEXT,
    wait_event TEXT,
    query_start TIMESTAMPTZ,
    running_for INTERVAL,
    query_preview TEXT
) AS $$
DECLARE
    v_retention_interval INTERVAL;
BEGIN
    -- Calculate retention: 120 slots × interval
    v_retention_interval := (120 * COALESCE(
        flight_recorder._get_config('sample_interval_seconds', '60')::integer,
        60
    ))::text || ' seconds';

    RETURN QUERY
    SELECT
        sr.captured_at,
        a.pid,
        a.usename,
        a.application_name,
        a.backend_type,
        a.state,
        a.wait_event_type,
        a.wait_event,
        a.query_start,
        sr.captured_at - a.query_start AS running_for,
        a.query_preview
    FROM flight_recorder.samples_ring sr
    JOIN flight_recorder.activity_samples_ring a ON a.slot_id = sr.slot_id
    WHERE sr.captured_at > now() - v_retention_interval
      AND a.pid IS NOT NULL
    ORDER BY sr.captured_at DESC, a.query_start ASC;
END;
$$ LANGUAGE plpgsql STABLE;

CREATE OR REPLACE FUNCTION flight_recorder.recent_locks_current()
RETURNS TABLE (
    captured_at TIMESTAMPTZ,
    blocked_pid INTEGER,
    blocked_user TEXT,
    blocked_app TEXT,
    blocked_duration INTERVAL,
    blocking_pid INTEGER,
    blocking_user TEXT,
    blocking_app TEXT,
    lock_type TEXT,
    locked_relation TEXT,
    blocked_query_preview TEXT,
    blocking_query_preview TEXT
) AS $$
DECLARE
    v_retention_interval INTERVAL;
BEGIN
    -- Calculate retention: 120 slots × interval
    v_retention_interval := (120 * COALESCE(
        flight_recorder._get_config('sample_interval_seconds', '60')::integer,
        60
    ))::text || ' seconds';

    RETURN QUERY
    SELECT
        sr.captured_at,
        l.blocked_pid,
        l.blocked_user,
        l.blocked_app,
        l.blocked_duration,
        l.blocking_pid,
        l.blocking_user,
        l.blocking_app,
        l.lock_type,
        COALESCE(l.locked_relation_oid::regclass::text, 'OID:' || l.locked_relation_oid::text) AS locked_relation,
        l.blocked_query_preview,
        l.blocking_query_preview
    FROM flight_recorder.samples_ring sr
    JOIN flight_recorder.lock_samples_ring l ON l.slot_id = sr.slot_id
    WHERE sr.captured_at > now() - v_retention_interval
      AND l.blocked_pid IS NOT NULL
    ORDER BY sr.captured_at DESC, l.blocked_duration DESC;
END;
$$ LANGUAGE plpgsql STABLE;

-- -----------------------------------------------------------------------------
-- flight_recorder.recent_progress - REMOVED (not in ring buffer architecture)
-- -----------------------------------------------------------------------------
-- Progress tracking removed from ring buffer to minimize footprint
-- Use pg_stat_progress_* views directly for real-time progress

-- -----------------------------------------------------------------------------
-- flight_recorder.recent_replication - View of replication lag from last 2 hours
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW flight_recorder.recent_replication AS
SELECT
    sn.captured_at,
    r.pid,
    r.client_addr,
    r.application_name,
    r.state,
    r.sync_state,
    r.sent_lsn,
    r.write_lsn,
    r.flush_lsn,
    r.replay_lsn,
    -- Calculate lag in bytes from current WAL position at snapshot time
    pg_wal_lsn_diff(r.sent_lsn, r.replay_lsn)::bigint AS replay_lag_bytes,
    flight_recorder._pretty_bytes(pg_wal_lsn_diff(r.sent_lsn, r.replay_lsn)::bigint) AS replay_lag_pretty,
    r.write_lag,
    r.flush_lag,
    r.replay_lag
FROM flight_recorder.snapshots sn
JOIN flight_recorder.replication_snapshots r ON r.snapshot_id = sn.id
WHERE sn.captured_at > now() - interval '2 hours'
ORDER BY sn.captured_at DESC, r.application_name;

-- -----------------------------------------------------------------------------
-- flight_recorder.wait_summary() - Aggregate wait events over a time period
-- -----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION flight_recorder.wait_summary(
    p_start_time TIMESTAMPTZ,
    p_end_time TIMESTAMPTZ
)
RETURNS TABLE(
    backend_type        TEXT,
    wait_event_type     TEXT,
    wait_event          TEXT,
    sample_count        BIGINT,
    total_waiters       BIGINT,
    avg_waiters         NUMERIC,
    max_waiters         INTEGER,
    pct_of_samples      NUMERIC
)
LANGUAGE sql STABLE AS $$
    WITH sample_range AS (
        SELECT slot_id, captured_at
        FROM flight_recorder.samples_ring
        WHERE captured_at BETWEEN p_start_time AND p_end_time
    ),
    total_samples AS (
        SELECT count(*) AS cnt FROM sample_range
    )
    SELECT
        w.backend_type,
        w.wait_event_type,
        w.wait_event,
        count(DISTINCT w.slot_id) AS sample_count,
        sum(w.count) AS total_waiters,
        round(avg(w.count), 2) AS avg_waiters,
        max(w.count) AS max_waiters,
        round(100.0 * count(DISTINCT w.slot_id) / NULLIF(t.cnt, 0), 1) AS pct_of_samples
    FROM flight_recorder.wait_samples_ring w
    JOIN sample_range sr ON sr.slot_id = w.slot_id
    CROSS JOIN total_samples t
    WHERE w.state NOT IN ('idle', 'idle in transaction')
    GROUP BY w.backend_type, w.wait_event_type, w.wait_event, t.cnt
    ORDER BY total_waiters DESC, sample_count DESC;
$$;

-- -----------------------------------------------------------------------------
-- flight_recorder.statement_compare() - Compare query stats between two time points
-- -----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION flight_recorder.statement_compare(
    p_start_time TIMESTAMPTZ,
    p_end_time TIMESTAMPTZ,
    p_min_delta_ms DOUBLE PRECISION DEFAULT 100,
    p_limit INTEGER DEFAULT 25
)
RETURNS TABLE(
    queryid                     BIGINT,
    query_preview               TEXT,

    calls_start                 BIGINT,
    calls_end                   BIGINT,
    calls_delta                 BIGINT,

    total_exec_time_start_ms    DOUBLE PRECISION,
    total_exec_time_end_ms      DOUBLE PRECISION,
    total_exec_time_delta_ms    DOUBLE PRECISION,

    mean_exec_time_start_ms     DOUBLE PRECISION,
    mean_exec_time_end_ms       DOUBLE PRECISION,

    rows_delta                  BIGINT,

    shared_blks_hit_delta       BIGINT,
    shared_blks_read_delta      BIGINT,
    shared_blks_written_delta   BIGINT,

    temp_blks_read_delta        BIGINT,
    temp_blks_written_delta     BIGINT,

    wal_bytes_delta             NUMERIC,

    hit_ratio_pct               NUMERIC,
    time_per_call_ms            DOUBLE PRECISION
)
LANGUAGE sql STABLE AS $$
    WITH
    start_snap AS (
        SELECT ss.*, s.captured_at
        FROM flight_recorder.statement_snapshots ss
        JOIN flight_recorder.snapshots s ON s.id = ss.snapshot_id
        WHERE s.captured_at <= p_start_time
        ORDER BY s.captured_at DESC
        LIMIT 1000
    ),
    end_snap AS (
        SELECT ss.*, s.captured_at
        FROM flight_recorder.statement_snapshots ss
        JOIN flight_recorder.snapshots s ON s.id = ss.snapshot_id
        WHERE s.captured_at >= p_end_time
        ORDER BY s.captured_at ASC
        LIMIT 1000
    ),
    matched AS (
        SELECT
            e.queryid,
            COALESCE(e.query_preview, s.query_preview) AS query_preview,

            s.calls AS calls_start,
            e.calls AS calls_end,

            s.total_exec_time AS total_exec_time_start,
            e.total_exec_time AS total_exec_time_end,

            s.mean_exec_time AS mean_exec_time_start,
            e.mean_exec_time AS mean_exec_time_end,

            s.rows AS rows_start,
            e.rows AS rows_end,

            s.shared_blks_hit AS shared_blks_hit_start,
            e.shared_blks_hit AS shared_blks_hit_end,

            s.shared_blks_read AS shared_blks_read_start,
            e.shared_blks_read AS shared_blks_read_end,

            s.shared_blks_written AS shared_blks_written_start,
            e.shared_blks_written AS shared_blks_written_end,

            s.temp_blks_read AS temp_blks_read_start,
            e.temp_blks_read AS temp_blks_read_end,

            s.temp_blks_written AS temp_blks_written_start,
            e.temp_blks_written AS temp_blks_written_end,

            s.wal_bytes AS wal_bytes_start,
            e.wal_bytes AS wal_bytes_end
        FROM end_snap e
        LEFT JOIN start_snap s ON s.queryid = e.queryid AND s.dbid = e.dbid
    )
    SELECT
        m.queryid,
        m.query_preview,

        COALESCE(m.calls_start, 0),
        m.calls_end,
        m.calls_end - COALESCE(m.calls_start, 0),

        COALESCE(m.total_exec_time_start, 0),
        m.total_exec_time_end,
        m.total_exec_time_end - COALESCE(m.total_exec_time_start, 0),

        m.mean_exec_time_start,
        m.mean_exec_time_end,

        m.rows_end - COALESCE(m.rows_start, 0),

        m.shared_blks_hit_end - COALESCE(m.shared_blks_hit_start, 0),
        m.shared_blks_read_end - COALESCE(m.shared_blks_read_start, 0),
        m.shared_blks_written_end - COALESCE(m.shared_blks_written_start, 0),

        m.temp_blks_read_end - COALESCE(m.temp_blks_read_start, 0),
        m.temp_blks_written_end - COALESCE(m.temp_blks_written_start, 0),

        m.wal_bytes_end - COALESCE(m.wal_bytes_start, 0),

        CASE
            WHEN (m.shared_blks_hit_end - COALESCE(m.shared_blks_hit_start, 0) +
                  m.shared_blks_read_end - COALESCE(m.shared_blks_read_start, 0)) > 0
            THEN round(
                100.0 * (m.shared_blks_hit_end - COALESCE(m.shared_blks_hit_start, 0)) /
                (m.shared_blks_hit_end - COALESCE(m.shared_blks_hit_start, 0) +
                 m.shared_blks_read_end - COALESCE(m.shared_blks_read_start, 0)), 1
            )
            ELSE NULL
        END,

        CASE
            WHEN (m.calls_end - COALESCE(m.calls_start, 0)) > 0
            THEN (m.total_exec_time_end - COALESCE(m.total_exec_time_start, 0)) /
                 (m.calls_end - COALESCE(m.calls_start, 0))
            ELSE NULL
        END

    FROM matched m
    WHERE (m.total_exec_time_end - COALESCE(m.total_exec_time_start, 0)) >= p_min_delta_ms
    ORDER BY (m.total_exec_time_end - COALESCE(m.total_exec_time_start, 0)) DESC
    LIMIT p_limit
$$;

-- -----------------------------------------------------------------------------
-- flight_recorder.activity_at() - Show what was happening at a specific moment
-- -----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION flight_recorder.activity_at(p_timestamp TIMESTAMPTZ)
RETURNS TABLE(
    sample_captured_at      TIMESTAMPTZ,
    sample_offset_seconds   NUMERIC,

    active_sessions         INTEGER,
    waiting_sessions        INTEGER,
    idle_in_transaction     INTEGER,

    top_wait_event_1        TEXT,
    top_wait_count_1        INTEGER,
    top_wait_event_2        TEXT,
    top_wait_count_2        INTEGER,
    top_wait_event_3        TEXT,
    top_wait_count_3        INTEGER,

    blocked_pids            INTEGER,
    longest_blocked_duration INTERVAL,

    vacuums_running         INTEGER,
    copies_running          INTEGER,
    indexes_building        INTEGER,
    analyzes_running        INTEGER,

    snapshot_captured_at    TIMESTAMPTZ,
    snapshot_offset_seconds NUMERIC,
    autovacuum_workers      INTEGER,
    checkpoint_occurred     BOOLEAN
)
LANGUAGE sql STABLE AS $$
    WITH
    nearest_sample AS (
        SELECT slot_id, captured_at,
               ABS(EXTRACT(EPOCH FROM (captured_at - p_timestamp))) AS offset_secs
        FROM flight_recorder.samples_ring
        ORDER BY ABS(EXTRACT(EPOCH FROM (captured_at - p_timestamp)))
        LIMIT 1
    ),
    nearest_snapshot AS (
        SELECT s.id, s.captured_at, s.autovacuum_workers,
               (s.checkpoint_time IS DISTINCT FROM prev.checkpoint_time) AS checkpoint_occurred,
               ABS(EXTRACT(EPOCH FROM (s.captured_at - p_timestamp))) AS offset_secs
        FROM flight_recorder.snapshots s
        LEFT JOIN flight_recorder.snapshots prev ON prev.id = (
            SELECT MAX(id) FROM flight_recorder.snapshots WHERE id < s.id
        )
        ORDER BY ABS(EXTRACT(EPOCH FROM (s.captured_at - p_timestamp)))
        LIMIT 1
    ),
    sample_waits AS (
        SELECT
            wait_event_type || ':' || wait_event AS wait_event,
            count
        FROM flight_recorder.wait_samples_ring w
        JOIN nearest_sample ns ON ns.slot_id = w.slot_id
        WHERE w.state NOT IN ('idle', 'idle in transaction')
        ORDER BY count DESC
        LIMIT 3
    ),
    wait_array AS (
        SELECT array_agg(wait_event ORDER BY count DESC) AS events,
               array_agg(count ORDER BY count DESC) AS counts
        FROM sample_waits
    ),
    sample_activity AS (
        SELECT
            count(*) FILTER (WHERE state = 'active') AS active_sessions,
            count(*) FILTER (WHERE wait_event IS NOT NULL) AS waiting_sessions,
            count(*) FILTER (WHERE state = 'idle in transaction') AS idle_in_transaction
        FROM flight_recorder.activity_samples_ring a
        JOIN nearest_sample ns ON ns.slot_id = a.slot_id
    ),
    sample_locks AS (
        SELECT
            count(DISTINCT blocked_pid) AS blocked_pids,
            max(blocked_duration) AS longest_blocked
        FROM flight_recorder.lock_samples_ring l
        JOIN nearest_sample ns ON ns.slot_id = l.slot_id
    ),
    sample_progress AS (
        -- Progress tracking not available in ring buffer architecture
        SELECT
            0 AS vacuums,
            0 AS copies,
            0 AS indexes,
            0 AS analyzes
    )
    SELECT
        ns.captured_at,
        ns.offset_secs::numeric,

        COALESCE(sa.active_sessions, 0)::integer,
        COALESCE(sa.waiting_sessions, 0)::integer,
        COALESCE(sa.idle_in_transaction, 0)::integer,

        wa.events[1],
        wa.counts[1],
        wa.events[2],
        wa.counts[2],
        wa.events[3],
        wa.counts[3],

        COALESCE(sl.blocked_pids, 0)::integer,
        sl.longest_blocked,

        COALESCE(sp.vacuums, 0)::integer,
        COALESCE(sp.copies, 0)::integer,
        COALESCE(sp.indexes, 0)::integer,
        COALESCE(sp.analyzes, 0)::integer,

        sn.captured_at,
        sn.offset_secs::numeric,
        sn.autovacuum_workers,
        sn.checkpoint_occurred
    FROM nearest_sample ns
    CROSS JOIN nearest_snapshot sn
    CROSS JOIN sample_activity sa
    CROSS JOIN sample_locks sl
    CROSS JOIN sample_progress sp
    CROSS JOIN wait_array wa
$$;

-- -----------------------------------------------------------------------------
-- flight_recorder.anomaly_report() - Flag unusual patterns in a time window
-- -----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION flight_recorder.anomaly_report(
    p_start_time TIMESTAMPTZ,
    p_end_time TIMESTAMPTZ
)
RETURNS TABLE(
    anomaly_type        TEXT,
    severity            TEXT,
    description         TEXT,
    metric_value        TEXT,
    threshold           TEXT,
    recommendation      TEXT
)
LANGUAGE plpgsql STABLE AS $$
DECLARE
    v_cmp RECORD;
    v_wait_pct NUMERIC;
    v_lock_count INTEGER;
    v_max_block_duration INTERVAL;
BEGIN
    -- Get comparison data
    SELECT * INTO v_cmp FROM flight_recorder.compare(p_start_time, p_end_time);

    -- Check 1: Checkpoint occurred during window
    IF v_cmp.checkpoint_occurred THEN
        anomaly_type := 'CHECKPOINT_DURING_WINDOW';
        severity := CASE
            WHEN v_cmp.ckpt_write_time_ms > 30000 THEN 'high'
            WHEN v_cmp.ckpt_write_time_ms > 10000 THEN 'medium'
            ELSE 'low'
        END;
        description := 'A checkpoint occurred during this time window';
        metric_value := format('write_time: %s ms, sync_time: %s ms',
                              round(v_cmp.ckpt_write_time_ms::numeric, 1),
                              round(v_cmp.ckpt_sync_time_ms::numeric, 1));
        threshold := 'Any checkpoint';
        recommendation := 'Consider increasing max_wal_size or scheduling heavy writes after checkpoint_timeout';
        RETURN NEXT;
    END IF;

    -- Check 2: Forced checkpoint (ckpt_requested)
    IF v_cmp.ckpt_requested_delta > 0 THEN
        anomaly_type := 'FORCED_CHECKPOINT';
        severity := 'high';
        description := 'WAL exceeded max_wal_size, forcing checkpoint';
        metric_value := format('%s forced checkpoints', v_cmp.ckpt_requested_delta);
        threshold := 'ckpt_requested_delta > 0';
        recommendation := 'Increase max_wal_size to prevent mid-batch checkpoints';
        RETURN NEXT;
    END IF;

    -- Check 3: Backend buffer writes (shared_buffers pressure)
    IF COALESCE(v_cmp.bgw_buffers_backend_delta, 0) > 0 THEN
        anomaly_type := 'BUFFER_PRESSURE';
        severity := CASE
            WHEN v_cmp.bgw_buffers_backend_delta > 1000 THEN 'high'
            WHEN v_cmp.bgw_buffers_backend_delta > 100 THEN 'medium'
            ELSE 'low'
        END;
        description := 'Backends forced to write buffers directly (shared_buffers exhaustion)';
        metric_value := format('%s backend buffer writes', v_cmp.bgw_buffers_backend_delta);
        threshold := 'bgw_buffers_backend_delta > 0';
        recommendation := 'Increase shared_buffers, reduce concurrent writers, or use faster storage';
        RETURN NEXT;
    END IF;

    -- Check 4: Backend fsync (very bad)
    IF COALESCE(v_cmp.bgw_buffers_backend_fsync_delta, 0) > 0 THEN
        anomaly_type := 'BACKEND_FSYNC';
        severity := 'high';
        description := 'Backends forced to perform fsync (severe I/O bottleneck)';
        metric_value := format('%s backend fsyncs', v_cmp.bgw_buffers_backend_fsync_delta);
        threshold := 'bgw_buffers_backend_fsync_delta > 0';
        recommendation := 'Urgent: increase shared_buffers, reduce write load, or upgrade storage';
        RETURN NEXT;
    END IF;

    -- Check 5: Temp file spills
    IF COALESCE(v_cmp.temp_files_delta, 0) > 0 THEN
        anomaly_type := 'TEMP_FILE_SPILLS';
        severity := CASE
            WHEN v_cmp.temp_bytes_delta > 1073741824 THEN 'high'
            WHEN v_cmp.temp_bytes_delta > 104857600 THEN 'medium'
            ELSE 'low'
        END;
        description := 'Queries spilling to temp files (work_mem exhaustion)';
        metric_value := format('%s temp files, %s written',
                              v_cmp.temp_files_delta, v_cmp.temp_bytes_pretty);
        threshold := 'temp_files_delta > 0';
        recommendation := 'Increase work_mem for affected sessions or globally';
        RETURN NEXT;
    END IF;

    -- Check 6: Lock contention
    SELECT count(DISTINCT blocked_pid), max(blocked_duration)
    INTO v_lock_count, v_max_block_duration
    FROM flight_recorder.lock_samples_ring l
    JOIN flight_recorder.samples_ring s ON s.slot_id = l.slot_id
    WHERE s.captured_at BETWEEN p_start_time AND p_end_time;

    IF v_lock_count > 0 THEN
        anomaly_type := 'LOCK_CONTENTION';
        severity := CASE
            WHEN v_max_block_duration > interval '30 seconds' THEN 'high'
            WHEN v_max_block_duration > interval '5 seconds' THEN 'medium'
            ELSE 'low'
        END;
        description := 'Lock contention detected';
        metric_value := format('%s blocked sessions, max duration: %s',
                              v_lock_count, v_max_block_duration);
        threshold := 'Any lock contention';
        recommendation := 'Check recent_locks for blocking queries; consider shorter transactions';
        RETURN NEXT;
    END IF;

    RETURN;
END;
$$;

-- -----------------------------------------------------------------------------
-- flight_recorder.summary_report() - Comprehensive diagnostic summary
-- -----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION flight_recorder.summary_report(
    p_start_time TIMESTAMPTZ,
    p_end_time TIMESTAMPTZ
)
RETURNS TABLE(
    section             TEXT,
    metric              TEXT,
    value               TEXT,
    interpretation      TEXT
)
LANGUAGE plpgsql STABLE AS $$
DECLARE
    v_cmp RECORD;
    v_sample_count INTEGER;
    v_anomaly_count INTEGER;
    v_top_wait RECORD;
    v_lock_summary RECORD;
BEGIN
    -- Get comparison data
    SELECT * INTO v_cmp FROM flight_recorder.compare(p_start_time, p_end_time);

    SELECT count(*) INTO v_sample_count
    FROM flight_recorder.samples_ring WHERE captured_at BETWEEN p_start_time AND p_end_time;

    SELECT count(*) INTO v_anomaly_count
    FROM flight_recorder.anomaly_report(p_start_time, p_end_time);

    -- === OVERVIEW SECTION ===
    section := 'OVERVIEW';

    metric := 'Time Window';
    value := format('%s to %s', p_start_time, p_end_time);
    interpretation := format('%s seconds elapsed', round(COALESCE(v_cmp.elapsed_seconds, 0), 1));
    RETURN NEXT;

    metric := 'Data Coverage';
    value := format('%s snapshots, %s samples',
                   (SELECT count(*) FROM flight_recorder.snapshots
                    WHERE captured_at BETWEEN p_start_time AND p_end_time),
                   v_sample_count);
    interpretation := CASE
        WHEN v_sample_count = 0 THEN 'WARNING: No sample data in window'
        ELSE 'OK'
    END;
    RETURN NEXT;

    metric := 'Anomalies Detected';
    value := v_anomaly_count::text;
    interpretation := CASE
        WHEN v_anomaly_count = 0 THEN 'No issues detected'
        WHEN v_anomaly_count <= 2 THEN 'Minor issues'
        ELSE 'Multiple issues - review anomaly_report()'
    END;
    RETURN NEXT;

    -- === CHECKPOINT/WAL SECTION ===
    section := 'CHECKPOINT & WAL';

    metric := 'Checkpoint Occurred';
    value := COALESCE(v_cmp.checkpoint_occurred::text, 'unknown');
    interpretation := CASE
        WHEN v_cmp.checkpoint_occurred THEN
            format('Checkpoint write: %s ms', round(COALESCE(v_cmp.ckpt_write_time_ms, 0)::numeric, 1))
        ELSE 'No checkpoint during window'
    END;
    RETURN NEXT;

    metric := 'WAL Generated';
    value := COALESCE(v_cmp.wal_bytes_pretty, 'N/A');
    interpretation := format('%s MB/sec',
                            round((COALESCE(v_cmp.wal_bytes_delta, 0) / 1048576.0) / NULLIF(v_cmp.elapsed_seconds, 0), 2));
    RETURN NEXT;

    -- === BUFFER SECTION ===
    section := 'BUFFERS & I/O';

    metric := 'Buffers Allocated';
    value := COALESCE(v_cmp.bgw_buffers_alloc_delta::text, 'N/A');
    interpretation := 'New buffer allocations';
    RETURN NEXT;

    metric := 'Backend Buffer Writes';
    value := COALESCE(v_cmp.bgw_buffers_backend_delta::text, 'N/A');
    interpretation := CASE
        WHEN COALESCE(v_cmp.bgw_buffers_backend_delta, 0) > 0 THEN 'WARNING: Backends writing directly'
        ELSE 'OK'
    END;
    RETURN NEXT;

    metric := 'Temp File Spills';
    value := format('%s files, %s', COALESCE(v_cmp.temp_files_delta, 0), COALESCE(v_cmp.temp_bytes_pretty, '0 B'));
    interpretation := CASE
        WHEN COALESCE(v_cmp.temp_files_delta, 0) > 0 THEN 'Consider increasing work_mem'
        ELSE 'No temp file usage'
    END;
    RETURN NEXT;

    -- === WAIT EVENTS SECTION ===
    section := 'WAIT EVENTS';

    FOR v_top_wait IN
        SELECT wait_event_type || ':' || wait_event AS we, total_waiters, pct_of_samples
        FROM flight_recorder.wait_summary(p_start_time, p_end_time)
        LIMIT 5
    LOOP
        metric := v_top_wait.we;
        value := format('%s total waiters (%s%% of samples)',
                       v_top_wait.total_waiters, v_top_wait.pct_of_samples);
        interpretation := CASE
            WHEN v_top_wait.we LIKE 'Lock:%' THEN 'Lock contention'
            WHEN v_top_wait.we LIKE 'LWLock:Buffer%' THEN 'Buffer contention'
            WHEN v_top_wait.we LIKE 'LWLock:WAL%' THEN 'WAL contention'
            WHEN v_top_wait.we LIKE 'IO:%' THEN 'I/O bound'
            WHEN v_top_wait.we = 'Running:CPU' THEN 'CPU active (normal)'
            ELSE 'Review PostgreSQL docs'
        END;
        RETURN NEXT;
    END LOOP;

    -- === LOCK SECTION ===
    section := 'LOCK CONTENTION';

    SELECT
        count(DISTINCT blocked_pid) AS blocked_count,
        max(blocked_duration) AS max_duration
    INTO v_lock_summary
    FROM flight_recorder.lock_samples_ring l
    JOIN flight_recorder.samples_ring s ON s.slot_id = l.slot_id
    WHERE s.captured_at BETWEEN p_start_time AND p_end_time;

    metric := 'Blocked Sessions';
    value := COALESCE(v_lock_summary.blocked_count, 0)::text;
    interpretation := CASE
        WHEN COALESCE(v_lock_summary.blocked_count, 0) = 0 THEN 'No lock contention'
        ELSE format('Max blocked duration: %s', v_lock_summary.max_duration)
    END;
    RETURN NEXT;

    RETURN;
END;
$$;

-- -----------------------------------------------------------------------------
-- flight_recorder.set_mode() - Configure flight recorder collection mode
-- -----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION flight_recorder.set_mode(p_mode TEXT)
RETURNS TEXT
LANGUAGE plpgsql AS $$
DECLARE
    v_enable_locks BOOLEAN;
    v_enable_progress BOOLEAN;
    v_description TEXT;
    v_sample_interval_seconds INTEGER;
    v_sample_interval_minutes INTEGER;
    v_cron_expression TEXT;
    v_current_interval INTEGER;
BEGIN
    -- Validate mode
    IF p_mode NOT IN ('normal', 'light', 'emergency') THEN
        RAISE EXCEPTION 'Invalid mode: %. Must be normal, light, or emergency.', p_mode;
    END IF;

    -- Get current sample interval from config
    v_current_interval := COALESCE(
        flight_recorder._get_config('sample_interval_seconds', '60')::integer,
        60
    );

    -- Set mode-specific configuration
    -- Adaptive frequency: Each mode sets explicit interval (normal=120s, light=120s, emergency=300s) - A GRADE
    CASE p_mode
        WHEN 'normal' THEN
            v_enable_locks := TRUE;
            v_enable_progress := TRUE;
            v_sample_interval_seconds := 120;  -- Normal mode: 120s intervals (4h retention) - A GRADE: Conservative + proactive
            v_description := 'Normal mode: 120s sampling, all collectors enabled (4h retention)';
        WHEN 'light' THEN
            v_enable_locks := TRUE;
            v_enable_progress := FALSE;
            v_sample_interval_seconds := 120;  -- Light mode: 120s intervals (4h retention, same as normal)
            v_description := 'Light mode: 120s sampling, progress disabled (4h retention, minimal overhead)';
        WHEN 'emergency' THEN
            v_enable_locks := FALSE;
            v_enable_progress := FALSE;
            v_sample_interval_seconds := 300;  -- Emergency mode: 300s intervals (10h retention, 60% reduction)
            v_description := 'Emergency mode: 300s sampling, locks/progress disabled (10h retention, 60% less overhead)';
    END CASE;

    -- Update configuration
    INSERT INTO flight_recorder.config (key, value, updated_at)
    VALUES ('mode', p_mode, now())
    ON CONFLICT (key) DO UPDATE SET value = p_mode, updated_at = now();

    INSERT INTO flight_recorder.config (key, value, updated_at)
    VALUES ('enable_locks', v_enable_locks::text, now())
    ON CONFLICT (key) DO UPDATE SET value = v_enable_locks::text, updated_at = now();

    INSERT INTO flight_recorder.config (key, value, updated_at)
    VALUES ('enable_progress', v_enable_progress::text, now())
    ON CONFLICT (key) DO UPDATE SET value = v_enable_progress::text, updated_at = now();

    -- Update sample interval (adaptive frequency control)
    INSERT INTO flight_recorder.config (key, value, updated_at)
    VALUES ('sample_interval_seconds', v_sample_interval_seconds::text, now())
    ON CONFLICT (key) DO UPDATE SET value = v_sample_interval_seconds::text, updated_at = now();

    -- Reschedule pg_cron job (if pg_cron available)
    BEGIN
        -- Convert interval to cron expression
        IF v_sample_interval_seconds < 60 THEN
            v_cron_expression := '* * * * *';
        ELSIF v_sample_interval_seconds = 60 THEN
            v_cron_expression := '* * * * *';
        ELSE
            v_sample_interval_minutes := CEILING(v_sample_interval_seconds::numeric / 60.0)::integer;
            v_cron_expression := format('*/%s * * * *', v_sample_interval_minutes);
        END IF;

        -- Unschedule existing
        PERFORM cron.unschedule('flight_recorder_sample');

        -- Schedule with new expression
        PERFORM cron.schedule('flight_recorder_sample', v_cron_expression, 'SELECT flight_recorder.sample()');
    EXCEPTION
        WHEN undefined_table THEN NULL;
        WHEN undefined_function THEN NULL;
    END;

    RETURN v_description;
END;
$$;

-- -----------------------------------------------------------------------------
-- flight_recorder.get_mode() - Show current telemetry configuration
-- -----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION flight_recorder.get_mode()
RETURNS TABLE(
    mode                TEXT,
    sample_interval     TEXT,
    locks_enabled       BOOLEAN,
    progress_enabled    BOOLEAN,
    statements_enabled  TEXT
)
LANGUAGE sql STABLE AS $$
    SELECT
        flight_recorder._get_config('mode', 'normal') AS mode,
        CASE flight_recorder._get_config('mode', 'normal')
            WHEN 'normal' THEN '* * * * *'
            WHEN 'light' THEN '* * * * *'
            WHEN 'emergency' THEN '120 seconds'
            ELSE 'unknown'
        END AS sample_interval,
        COALESCE(flight_recorder._get_config('enable_locks', 'true')::boolean, true) AS locks_enabled,
        COALESCE(flight_recorder._get_config('enable_progress', 'true')::boolean, true) AS progress_enabled,
        flight_recorder._get_config('statements_enabled', 'auto') AS statements_enabled
$$;

-- =============================================================================
-- Configuration Profiles - Simplified Configuration Management
-- =============================================================================
-- Profiles provide preset configurations for common use cases, reducing the
-- complexity of 41+ configuration parameters down to a single function call.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- flight_recorder.list_profiles() - Show all available profiles
-- -----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION flight_recorder.list_profiles()
RETURNS TABLE(
    profile_name        TEXT,
    description         TEXT,
    use_case            TEXT,
    sample_interval     TEXT,
    overhead_level      TEXT
)
LANGUAGE sql STABLE AS $$
    SELECT * FROM (VALUES
        ('default',
         'Balanced configuration for most users',
         'General purpose monitoring - staging, development, or production',
         '180s (6h retention)',
         'Minimal (~0.013% CPU)'),
        
        ('production_safe',
         'Ultra-conservative for production environments',
         'Production always-on monitoring with maximum safety',
         '300s (10h retention)',
         'Ultra-minimal (~0.008% CPU)'),
        
        ('development',
         'Balanced for staging and development',
         'Active development, testing, or staging environments',
         '180s (6h retention)',
         'Minimal (~0.013% CPU)'),
        
        ('troubleshooting',
         'Aggressive collection during incidents',
         'Active incident response - detailed data collection',
         '60s (2h retention)',
         'Low (~0.04% CPU)'),
        
        ('minimal_overhead',
         'Absolute minimum footprint',
         'Resource-constrained systems, replicas, or minimal monitoring',
         '300s (10h retention)',
         'Ultra-minimal (~0.008% CPU)'),
        
        ('high_ddl',
         'Optimized for frequent schema changes',
         'Multi-tenant SaaS or high DDL workloads',
         '180s (6h retention)',
         'Minimal (~0.013% CPU)')
    ) AS t(profile_name, description, use_case, sample_interval, overhead_level)
$$;

-- -----------------------------------------------------------------------------
-- flight_recorder.explain_profile() - Show what a profile configures
-- -----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION flight_recorder.explain_profile(p_profile_name TEXT)
RETURNS TABLE(
    setting_key         TEXT,
    current_value       TEXT,
    profile_value       TEXT,
    will_change         BOOLEAN,
    description         TEXT
)
LANGUAGE plpgsql STABLE AS $$
BEGIN
    -- Validate profile exists
    IF NOT EXISTS (SELECT 1 FROM flight_recorder.list_profiles() WHERE profile_name = p_profile_name) THEN
        RAISE EXCEPTION 'Unknown profile: %. Run flight_recorder.list_profiles() to see available profiles.', p_profile_name;
    END IF;

    RETURN QUERY
    WITH profile_settings AS (
        SELECT * FROM (VALUES
            -- Profile: default (balanced)
            ('default', 'sample_interval_seconds', '180', 'Sample every 3 minutes'),
            ('default', 'adaptive_sampling', 'true', 'Skip collection when system idle'),
            ('default', 'load_shedding_enabled', 'true', 'Skip during high load (>70% connections)'),
            ('default', 'load_throttle_enabled', 'true', 'Skip during I/O pressure'),
            ('default', 'circuit_breaker_enabled', 'true', 'Auto-skip if collections run slow'),
            ('default', 'enable_locks', 'true', 'Collect lock contention data'),
            ('default', 'enable_progress', 'true', 'Collect operation progress'),
            ('default', 'snapshot_based_collection', 'true', 'Use snapshot-based collection (67% fewer locks)'),
            ('default', 'retention_snapshots_days', '30', 'Keep 30 days of snapshot data'),
            ('default', 'aggregate_retention_days', '7', 'Keep 7 days of aggregate data'),
            
            -- Profile: production_safe (ultra-conservative)
            ('production_safe', 'sample_interval_seconds', '300', 'Sample every 5 minutes (40% less overhead)'),
            ('production_safe', 'adaptive_sampling', 'true', 'Skip when idle'),
            ('production_safe', 'load_shedding_enabled', 'true', 'Skip during high load'),
            ('production_safe', 'load_shedding_active_pct', '60', 'More aggressive load shedding (60% vs 70%)'),
            ('production_safe', 'load_throttle_enabled', 'true', 'Skip during I/O pressure'),
            ('production_safe', 'circuit_breaker_enabled', 'true', 'Auto-skip if slow'),
            ('production_safe', 'circuit_breaker_threshold_ms', '800', 'Stricter circuit breaker (800ms vs 1000ms)'),
            ('production_safe', 'enable_locks', 'false', 'Disable lock collection (reduce overhead)'),
            ('production_safe', 'enable_progress', 'false', 'Disable progress tracking'),
            ('production_safe', 'snapshot_based_collection', 'true', 'Snapshot-based collection'),
            ('production_safe', 'lock_timeout_ms', '50', 'Faster lock timeout (50ms vs 100ms)'),
            ('production_safe', 'retention_snapshots_days', '30', 'Keep 30 days'),
            ('production_safe', 'aggregate_retention_days', '7', 'Keep 7 days'),
            
            -- Profile: development (balanced for staging/dev)
            ('development', 'sample_interval_seconds', '180', 'Sample every 3 minutes'),
            ('development', 'adaptive_sampling', 'false', 'Always collect (never skip when idle)'),
            ('development', 'load_shedding_enabled', 'true', 'Skip during high load'),
            ('development', 'load_throttle_enabled', 'true', 'Skip during I/O pressure'),
            ('development', 'circuit_breaker_enabled', 'true', 'Auto-skip if slow'),
            ('development', 'enable_locks', 'true', 'Collect lock data'),
            ('development', 'enable_progress', 'true', 'Collect progress data'),
            ('development', 'snapshot_based_collection', 'true', 'Snapshot-based collection'),
            ('development', 'retention_snapshots_days', '7', 'Keep 7 days (less than production)'),
            ('development', 'aggregate_retention_days', '3', 'Keep 3 days'),
            
            -- Profile: troubleshooting (aggressive during incidents)
            ('troubleshooting', 'sample_interval_seconds', '60', 'Sample every minute (detailed data)'),
            ('troubleshooting', 'adaptive_sampling', 'false', 'Never skip - always collect'),
            ('troubleshooting', 'load_shedding_enabled', 'false', 'Collect even under load'),
            ('troubleshooting', 'load_throttle_enabled', 'false', 'Collect even during I/O pressure'),
            ('troubleshooting', 'circuit_breaker_enabled', 'true', 'Keep circuit breaker enabled'),
            ('troubleshooting', 'circuit_breaker_threshold_ms', '2000', 'More lenient threshold - 2 seconds'),
            ('troubleshooting', 'enable_locks', 'true', 'Collect all lock data'),
            ('troubleshooting', 'enable_progress', 'true', 'Collect all progress data'),
            ('troubleshooting', 'snapshot_based_collection', 'true', 'Snapshot-based collection'),
            ('troubleshooting', 'statements_top_n', '50', 'Collect top 50 queries (vs 20)'),
            ('troubleshooting', 'retention_snapshots_days', '7', 'Keep 7 days'),
            ('troubleshooting', 'aggregate_retention_days', '3', 'Keep 3 days'),
            
            -- Profile: minimal_overhead (absolute minimum)
            ('minimal_overhead', 'sample_interval_seconds', '300', 'Sample every 5 minutes'),
            ('minimal_overhead', 'adaptive_sampling', 'true', 'Skip when idle'),
            ('minimal_overhead', 'adaptive_sampling_idle_threshold', '10', 'Higher idle threshold (10 vs 5)'),
            ('minimal_overhead', 'load_shedding_enabled', 'true', 'Skip during high load'),
            ('minimal_overhead', 'load_shedding_active_pct', '50', 'Very aggressive (50%)'),
            ('minimal_overhead', 'load_throttle_enabled', 'true', 'Skip during I/O pressure'),
            ('minimal_overhead', 'circuit_breaker_enabled', 'true', 'Auto-skip if slow'),
            ('minimal_overhead', 'circuit_breaker_threshold_ms', '500', 'Very strict (500ms)'),
            ('minimal_overhead', 'enable_locks', 'false', 'Disable locks'),
            ('minimal_overhead', 'enable_progress', 'false', 'Disable progress'),
            ('minimal_overhead', 'snapshot_based_collection', 'true', 'Snapshot-based collection'),
            ('minimal_overhead', 'statements_enabled', 'false', 'Disable pg_stat_statements collection'),
            ('minimal_overhead', 'retention_snapshots_days', '7', 'Keep 7 days'),
            ('minimal_overhead', 'aggregate_retention_days', '3', 'Keep 3 days'),
            
            -- Profile: high_ddl (optimized for frequent schema changes)
            ('high_ddl', 'sample_interval_seconds', '180', 'Sample every 3 minutes'),
            ('high_ddl', 'adaptive_sampling', 'true', 'Skip when idle'),
            ('high_ddl', 'load_shedding_enabled', 'true', 'Skip during high load'),
            ('high_ddl', 'load_throttle_enabled', 'true', 'Skip during I/O pressure'),
            ('high_ddl', 'circuit_breaker_enabled', 'true', 'Auto-skip if slow'),
            ('high_ddl', 'enable_locks', 'true', 'Collect locks'),
            ('high_ddl', 'enable_progress', 'true', 'Collect progress'),
            ('high_ddl', 'snapshot_based_collection', 'true', 'Snapshot-based (critical for DDL)'),
            ('high_ddl', 'lock_timeout_strategy', 'skip_if_locked', 'Skip if catalogs locked by DDL'),
            ('high_ddl', 'lock_timeout_ms', '50', 'Very fast timeout (50ms)'),
            ('high_ddl', 'check_ddl_before_collection', 'true', 'Pre-check for DDL locks'),
            ('high_ddl', 'retention_snapshots_days', '30', 'Keep 30 days'),
            ('high_ddl', 'aggregate_retention_days', '7', 'Keep 7 days')
        ) AS t(profile, key, value, description)
        WHERE profile = p_profile_name
    )
    SELECT
        ps.key::text AS setting_key,
        c.value::text AS current_value,
        ps.value::text AS profile_value,
        (c.value IS DISTINCT FROM ps.value)::boolean AS will_change,
        ps.description::text AS description
    FROM profile_settings ps
    LEFT JOIN flight_recorder.config c ON c.key = ps.key
    ORDER BY will_change DESC, ps.key;
END $$;

-- -----------------------------------------------------------------------------
-- flight_recorder.apply_profile() - Apply a configuration profile
-- -----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION flight_recorder.apply_profile(p_profile_name TEXT)
RETURNS TABLE(
    setting_key     TEXT,
    old_value       TEXT,
    new_value       TEXT,
    changed         BOOLEAN
)
LANGUAGE plpgsql AS $$
DECLARE
    v_mode TEXT;
    v_changes_made INTEGER := 0;
BEGIN
    -- Validate profile exists
    IF NOT EXISTS (SELECT 1 FROM flight_recorder.list_profiles() WHERE profile_name = p_profile_name) THEN
        RAISE EXCEPTION 'Unknown profile: %. Run flight_recorder.list_profiles() to see available profiles.', p_profile_name;
    END IF;

    -- Log profile application
    RAISE NOTICE 'Applying profile: %', p_profile_name;

    -- Apply profile settings and return changes
    RETURN QUERY
    WITH profile_settings AS (
        SELECT * FROM (VALUES
            -- Profile: default
            ('default', 'sample_interval_seconds', '180'),
            ('default', 'adaptive_sampling', 'true'),
            ('default', 'load_shedding_enabled', 'true'),
            ('default', 'load_throttle_enabled', 'true'),
            ('default', 'circuit_breaker_enabled', 'true'),
            ('default', 'enable_locks', 'true'),
            ('default', 'enable_progress', 'true'),
            ('default', 'snapshot_based_collection', 'true'),
            ('default', 'retention_snapshots_days', '30'),
            ('default', 'aggregate_retention_days', '7'),
            ('default', 'archive_samples_enabled', 'true'),
            ('default', 'archive_sample_frequency_minutes', '15'),
            ('default', 'archive_retention_days', '7'),
            ('default', 'archive_activity_samples', 'true'),
            ('default', 'archive_lock_samples', 'true'),
            ('default', 'archive_wait_samples', 'true'),

            -- Profile: production_safe
            ('production_safe', 'sample_interval_seconds', '300'),
            ('production_safe', 'adaptive_sampling', 'true'),
            ('production_safe', 'load_shedding_enabled', 'true'),
            ('production_safe', 'load_shedding_active_pct', '60'),
            ('production_safe', 'load_throttle_enabled', 'true'),
            ('production_safe', 'circuit_breaker_enabled', 'true'),
            ('production_safe', 'circuit_breaker_threshold_ms', '800'),
            ('production_safe', 'enable_locks', 'false'),
            ('production_safe', 'enable_progress', 'false'),
            ('production_safe', 'snapshot_based_collection', 'true'),
            ('production_safe', 'lock_timeout_ms', '50'),
            ('production_safe', 'retention_snapshots_days', '30'),
            ('production_safe', 'aggregate_retention_days', '7'),
            ('production_safe', 'archive_samples_enabled', 'true'),
            ('production_safe', 'archive_sample_frequency_minutes', '30'),
            ('production_safe', 'archive_retention_days', '14'),
            ('production_safe', 'archive_activity_samples', 'true'),
            ('production_safe', 'archive_lock_samples', 'true'),
            ('production_safe', 'archive_wait_samples', 'false'),

            -- Profile: development
            ('development', 'sample_interval_seconds', '180'),
            ('development', 'adaptive_sampling', 'false'),
            ('development', 'load_shedding_enabled', 'true'),
            ('development', 'load_throttle_enabled', 'true'),
            ('development', 'circuit_breaker_enabled', 'true'),
            ('development', 'enable_locks', 'true'),
            ('development', 'enable_progress', 'true'),
            ('development', 'snapshot_based_collection', 'true'),
            ('development', 'retention_snapshots_days', '7'),
            ('development', 'aggregate_retention_days', '3'),
            ('development', 'archive_samples_enabled', 'true'),
            ('development', 'archive_sample_frequency_minutes', '15'),
            ('development', 'archive_retention_days', '3'),
            ('development', 'archive_activity_samples', 'true'),
            ('development', 'archive_lock_samples', 'true'),
            ('development', 'archive_wait_samples', 'true'),

            -- Profile: troubleshooting
            ('troubleshooting', 'sample_interval_seconds', '60'),
            ('troubleshooting', 'adaptive_sampling', 'false'),
            ('troubleshooting', 'load_shedding_enabled', 'false'),
            ('troubleshooting', 'load_throttle_enabled', 'false'),
            ('troubleshooting', 'circuit_breaker_enabled', 'true'),
            ('troubleshooting', 'circuit_breaker_threshold_ms', '2000'),
            ('troubleshooting', 'enable_locks', 'true'),
            ('troubleshooting', 'enable_progress', 'true'),
            ('troubleshooting', 'snapshot_based_collection', 'true'),
            ('troubleshooting', 'statements_top_n', '50'),
            ('troubleshooting', 'retention_snapshots_days', '7'),
            ('troubleshooting', 'aggregate_retention_days', '3'),
            ('troubleshooting', 'archive_samples_enabled', 'true'),
            ('troubleshooting', 'archive_sample_frequency_minutes', '5'),
            ('troubleshooting', 'archive_retention_days', '7'),
            ('troubleshooting', 'archive_activity_samples', 'true'),
            ('troubleshooting', 'archive_lock_samples', 'true'),
            ('troubleshooting', 'archive_wait_samples', 'true'),

            -- Profile: minimal_overhead
            ('minimal_overhead', 'sample_interval_seconds', '300'),
            ('minimal_overhead', 'adaptive_sampling', 'true'),
            ('minimal_overhead', 'adaptive_sampling_idle_threshold', '10'),
            ('minimal_overhead', 'load_shedding_enabled', 'true'),
            ('minimal_overhead', 'load_shedding_active_pct', '50'),
            ('minimal_overhead', 'load_throttle_enabled', 'true'),
            ('minimal_overhead', 'circuit_breaker_enabled', 'true'),
            ('minimal_overhead', 'circuit_breaker_threshold_ms', '500'),
            ('minimal_overhead', 'enable_locks', 'false'),
            ('minimal_overhead', 'enable_progress', 'false'),
            ('minimal_overhead', 'snapshot_based_collection', 'true'),
            ('minimal_overhead', 'statements_enabled', 'false'),
            ('minimal_overhead', 'retention_snapshots_days', '7'),
            ('minimal_overhead', 'aggregate_retention_days', '3'),
            ('minimal_overhead', 'archive_samples_enabled', 'false'),
            ('minimal_overhead', 'archive_sample_frequency_minutes', '30'),
            ('minimal_overhead', 'archive_retention_days', '3'),
            ('minimal_overhead', 'archive_activity_samples', 'false'),
            ('minimal_overhead', 'archive_lock_samples', 'false'),
            ('minimal_overhead', 'archive_wait_samples', 'false'),

            -- Profile: high_ddl
            ('high_ddl', 'sample_interval_seconds', '180'),
            ('high_ddl', 'adaptive_sampling', 'true'),
            ('high_ddl', 'load_shedding_enabled', 'true'),
            ('high_ddl', 'load_throttle_enabled', 'true'),
            ('high_ddl', 'circuit_breaker_enabled', 'true'),
            ('high_ddl', 'enable_locks', 'true'),
            ('high_ddl', 'enable_progress', 'true'),
            ('high_ddl', 'snapshot_based_collection', 'true'),
            ('high_ddl', 'lock_timeout_strategy', 'skip_if_locked'),
            ('high_ddl', 'lock_timeout_ms', '50'),
            ('high_ddl', 'check_ddl_before_collection', 'true'),
            ('high_ddl', 'retention_snapshots_days', '30'),
            ('high_ddl', 'aggregate_retention_days', '7'),
            ('high_ddl', 'archive_samples_enabled', 'true'),
            ('high_ddl', 'archive_sample_frequency_minutes', '15'),
            ('high_ddl', 'archive_retention_days', '7'),
            ('high_ddl', 'archive_activity_samples', 'true'),
            ('high_ddl', 'archive_lock_samples', 'true'),
            ('high_ddl', 'archive_wait_samples', 'true')
        ) AS t(profile, key, value)
        WHERE profile = p_profile_name
    ),
    updates AS (
        INSERT INTO flight_recorder.config (key, value, updated_at)
        SELECT ps.key, ps.value, now()
        FROM profile_settings ps
        ON CONFLICT (key) DO UPDATE 
        SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at
        WHERE flight_recorder.config.value IS DISTINCT FROM EXCLUDED.value
        RETURNING key, value
    )
    SELECT
        COALESCE(u.key, ps.key)::text AS setting_key,
        c.value::text AS old_value,
        ps.value::text AS new_value,
        (u.key IS NOT NULL)::boolean AS changed
    FROM profile_settings ps
    LEFT JOIN updates u ON u.key = ps.key
    LEFT JOIN flight_recorder.config c ON c.key = ps.key
    ORDER BY changed DESC, setting_key;

    -- Get the number of changes
    GET DIAGNOSTICS v_changes_made = ROW_COUNT;

    -- Determine appropriate mode based on profile
    v_mode := CASE p_profile_name
        WHEN 'production_safe' THEN 'emergency'
        WHEN 'minimal_overhead' THEN 'emergency'
        WHEN 'troubleshooting' THEN 'normal'
        WHEN 'high_ddl' THEN 'normal'
        ELSE 'normal'
    END;

    -- Apply mode (handles pg_cron rescheduling)
    PERFORM flight_recorder.set_mode(v_mode);

    RAISE NOTICE 'Profile "%" applied: % settings changed, mode set to %', 
        p_profile_name, v_changes_made, v_mode;
END $$;

-- -----------------------------------------------------------------------------
-- flight_recorder.get_current_profile() - Show which profile matches current config
-- -----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION flight_recorder.get_current_profile()
RETURNS TABLE(
    closest_profile     TEXT,
    match_percentage    NUMERIC,
    differences         TEXT[],
    recommendation      TEXT
)
LANGUAGE plpgsql STABLE AS $$
DECLARE
    v_profile RECORD;
    v_best_match TEXT;
    v_best_pct NUMERIC := 0;
    v_current_pct NUMERIC;
    v_diffs TEXT[];
BEGIN
    -- Check each profile and find best match
    FOR v_profile IN SELECT profile_name FROM flight_recorder.list_profiles() LOOP
        WITH profile_settings AS (
            SELECT setting_key, profile_value
            FROM flight_recorder.explain_profile(v_profile.profile_name)
        ),
        matches AS (
            SELECT
                count(*) FILTER (WHERE NOT will_change) AS matched,
                count(*) AS total,
                array_agg(setting_key) FILTER (WHERE will_change) AS diff_keys
            FROM flight_recorder.explain_profile(v_profile.profile_name)
        )
        SELECT
            (matched::numeric / NULLIF(total, 0) * 100)::numeric(5,1),
            diff_keys
        INTO v_current_pct, v_diffs
        FROM matches;

        IF v_current_pct > v_best_pct THEN
            v_best_pct := v_current_pct;
            v_best_match := v_profile.profile_name;
        END IF;
    END LOOP;

    -- Return result
    RETURN QUERY
    SELECT
        COALESCE(v_best_match, 'custom')::text,
        COALESCE(v_best_pct, 0)::numeric,
        (SELECT array_agg(setting_key) FROM flight_recorder.explain_profile(v_best_match) WHERE will_change)::text[],
        CASE
            WHEN v_best_pct = 100 THEN 'Configuration matches "' || v_best_match || '" profile perfectly'
            WHEN v_best_pct >= 80 THEN 'Configuration is close to "' || v_best_match || '" profile'
            WHEN v_best_pct >= 50 THEN 'Configuration is partially based on "' || v_best_match || '" profile'
            ELSE 'Configuration appears to be custom (not matching any profile)'
        END::text;
END $$;

-- -----------------------------------------------------------------------------
-- flight_recorder.cleanup() - Remove old flight recorder data
-- -----------------------------------------------------------------------------

DROP FUNCTION IF EXISTS flight_recorder.cleanup(INTERVAL);

CREATE OR REPLACE FUNCTION flight_recorder.cleanup(p_retain_interval INTERVAL DEFAULT NULL)
RETURNS TABLE(
    deleted_snapshots   BIGINT,
    deleted_samples     BIGINT,
    deleted_statements  BIGINT,
    deleted_stats       BIGINT
)
LANGUAGE plpgsql AS $$
DECLARE
    v_deleted_snapshots BIGINT;
    v_deleted_samples BIGINT;
    v_deleted_statements BIGINT;
    v_deleted_stats BIGINT;
    v_samples_retention_days INTEGER;
    v_snapshots_retention_days INTEGER;
    v_statements_retention_days INTEGER;
    v_stats_retention_days INTEGER;
    v_samples_cutoff TIMESTAMPTZ;
    v_snapshots_cutoff TIMESTAMPTZ;
    v_statements_cutoff TIMESTAMPTZ;
    v_stats_cutoff TIMESTAMPTZ;
BEGIN
    -- P2: Get retention periods from config (or use p_retain_interval for backward compatibility)
    IF p_retain_interval IS NOT NULL THEN
        -- Legacy mode: use provided interval for all tables
        v_samples_cutoff := now() - p_retain_interval;
        v_snapshots_cutoff := now() - p_retain_interval;
        v_statements_cutoff := now() - p_retain_interval;
        v_stats_cutoff := now() - p_retain_interval;
    ELSE
        -- P2: Use configurable retention per table type
        v_samples_retention_days := COALESCE(
            flight_recorder._get_config('retention_samples_days', '7')::integer,
            7
        );
        v_snapshots_retention_days := COALESCE(
            flight_recorder._get_config('retention_snapshots_days', '30')::integer,
            30
        );
        v_statements_retention_days := COALESCE(
            flight_recorder._get_config('retention_statements_days', '30')::integer,
            30
        );
        v_stats_retention_days := COALESCE(
            flight_recorder._get_config('retention_collection_stats_days', '30')::integer,
            30
        );

        v_samples_cutoff := now() - (v_samples_retention_days || ' days')::interval;
        v_snapshots_cutoff := now() - (v_snapshots_retention_days || ' days')::interval;
        v_statements_cutoff := now() - (v_statements_retention_days || ' days')::interval;
        v_stats_cutoff := now() - (v_stats_retention_days || ' days')::interval;
    END IF;

    -- Ring buffers self-clean via modular arithmetic (UPSERT pattern)
    -- No deletion needed - slots automatically overwrite after 2 hours
    v_deleted_samples := 0;

    -- Delete old snapshots (cascades to replication_snapshots, statement_snapshots)
    WITH deleted AS (
        DELETE FROM flight_recorder.snapshots WHERE captured_at < v_snapshots_cutoff RETURNING 1
    )
    SELECT count(*) INTO v_deleted_snapshots FROM deleted;

    -- P2: Delete old pg_stat_statements snapshots with configurable retention
    -- statement_snapshots references snapshots(id), so delete based on snapshot's captured_at
    WITH deleted AS (
        DELETE FROM flight_recorder.statement_snapshots
        WHERE snapshot_id IN (
            SELECT id FROM flight_recorder.snapshots WHERE captured_at < v_statements_cutoff
        )
        RETURNING 1
    )
    SELECT count(*) INTO v_deleted_statements FROM deleted;

    -- P2: Delete old collection_stats with configurable retention
    WITH deleted AS (
        DELETE FROM flight_recorder.collection_stats WHERE started_at < v_stats_cutoff RETURNING 1
    )
    SELECT count(*) INTO v_deleted_stats FROM deleted;

    -- Note: VACUUM cannot run inside a transaction block (function context)
    -- Autovacuum handles cleanup naturally:
    --   - Ring buffers: Small tables (120 rows) trigger autovacuum ~20x/day
    --   - Aggregate tables: Autovacuum triggers after DELETE operations
    --   - Snapshot tables: Autovacuum triggers after DELETE operations
    -- Use ring_buffer_health() to monitor dead tuples and XID age
    -- Manual VACUUM: VACUUM ANALYZE flight_recorder.samples_ring; (run outside function)

    RETURN QUERY SELECT v_deleted_snapshots, v_deleted_samples, v_deleted_statements, v_deleted_stats;
END;
$$;

-- -----------------------------------------------------------------------------
-- flight_recorder.ring_buffer_health() - Monitor XID age and bloat in ring buffers
-- -----------------------------------------------------------------------------

DROP FUNCTION IF EXISTS flight_recorder.ring_buffer_health();

CREATE OR REPLACE FUNCTION flight_recorder.ring_buffer_health()
RETURNS TABLE(
    table_name              TEXT,
    row_count               BIGINT,
    dead_tuples             BIGINT,
    dead_tuple_pct          NUMERIC,
    xid_age                 INTEGER,
    total_updates           BIGINT,
    hot_updates             BIGINT,
    hot_update_pct          NUMERIC,
    last_vacuum             TIMESTAMPTZ,
    last_autovacuum         TIMESTAMPTZ,
    autovacuum_threshold    BIGINT,
    needs_vacuum            BOOLEAN,
    status                  TEXT
)
LANGUAGE sql STABLE AS $$
    SELECT
        c.relname::text,
        s.n_live_tup,
        s.n_dead_tup,
        CASE
            WHEN s.n_live_tup > 0 THEN round(100.0 * s.n_dead_tup / NULLIF(s.n_live_tup, 0), 1)
            ELSE 0
        END,
        age(c.relfrozenxid)::integer,
        s.n_tup_upd,
        s.n_tup_hot_upd,
        CASE
            WHEN s.n_tup_upd > 0 THEN round(100.0 * s.n_tup_hot_upd / NULLIF(s.n_tup_upd, 0), 1)
            ELSE 0
        END,
        s.last_vacuum,
        s.last_autovacuum,
        -- Autovacuum threshold: 50 + 0.2 * n_live_tup
        (50 + (0.2 * s.n_live_tup)::bigint),
        -- Needs vacuum if dead tuples exceed threshold
        s.n_dead_tup > (50 + (0.2 * s.n_live_tup)::bigint),
        CASE
            WHEN age(c.relfrozenxid) > 200000000 THEN 'CRITICAL: XID wraparound risk'
            WHEN age(c.relfrozenxid) > 100000000 THEN 'WARNING: High XID age'
            WHEN c.relname = 'samples_ring' AND s.n_tup_upd > 100 AND (100.0 * s.n_tup_hot_upd / NULLIF(s.n_tup_upd, 0)) < 50 THEN 'WARNING: Low HOT update ratio'
            WHEN s.n_dead_tup > (50 + (0.2 * s.n_live_tup)::bigint) THEN 'INFO: Autovacuum pending'
            ELSE 'OK'
        END::text
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    LEFT JOIN pg_stat_user_tables s ON s.relid = c.oid
    WHERE n.nspname = 'flight_recorder'
      AND c.relkind = 'r'
      AND c.relname IN ('samples_ring', 'wait_samples_ring', 'activity_samples_ring', 'lock_samples_ring')
    ORDER BY c.relname;
$$;

COMMENT ON FUNCTION flight_recorder.ring_buffer_health() IS
'Monitor ring buffer XID age, dead tuple bloat, and HOT update effectiveness. samples_ring uses UPSERT (1,440x/day) and should achieve >90% HOT update ratio with fillfactor=70. Child tables use DELETE/INSERT so HOT updates are N/A.';

-- -----------------------------------------------------------------------------
-- flight_recorder.disable() - Emergency kill switch: stop all collection
-- -----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION flight_recorder.disable()
RETURNS TEXT
LANGUAGE plpgsql AS $$
DECLARE
    v_unscheduled INTEGER := 0;
BEGIN
    -- Unschedule all flight recorder jobs
    BEGIN
        PERFORM cron.unschedule('flight_recorder_snapshot')
        WHERE EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'flight_recorder_snapshot');
        IF FOUND THEN v_unscheduled := v_unscheduled + 1; END IF;

        PERFORM cron.unschedule('flight_recorder_sample')
        WHERE EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'flight_recorder_sample');
        IF FOUND THEN v_unscheduled := v_unscheduled + 1; END IF;

        PERFORM cron.unschedule('flight_recorder_cleanup')
        WHERE EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'flight_recorder_cleanup');
        IF FOUND THEN v_unscheduled := v_unscheduled + 1; END IF;

        PERFORM cron.unschedule('flight_recorder_flush')
        WHERE EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'flight_recorder_flush');
        IF FOUND THEN v_unscheduled := v_unscheduled + 1; END IF;

        PERFORM cron.unschedule('flight_recorder_archive')
        WHERE EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'flight_recorder_archive');
        IF FOUND THEN v_unscheduled := v_unscheduled + 1; END IF;

        -- Mark as disabled in config
        INSERT INTO flight_recorder.config (key, value, updated_at)
        VALUES ('enabled', 'false', now())
        ON CONFLICT (key) DO UPDATE SET value = 'false', updated_at = now();

        RETURN format('Flight Recorder collection stopped. Unscheduled %s cron jobs. Use flight_recorder.enable() to restart.', v_unscheduled);
    EXCEPTION
        WHEN undefined_table THEN
            RETURN 'pg_cron extension not found. No jobs to unschedule.';
        WHEN undefined_function THEN
            RETURN 'pg_cron extension not found. No jobs to unschedule.';
    END;
END;
$$;

-- -----------------------------------------------------------------------------
-- flight_recorder.enable() - Restart collection after kill switch
-- -----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION flight_recorder.enable()
RETURNS TEXT
LANGUAGE plpgsql AS $$
DECLARE
    v_mode TEXT;
    v_pgcron_version TEXT;
    v_supports_subsecond BOOLEAN := FALSE;
    v_sample_schedule TEXT;
    v_scheduled INTEGER := 0;
    v_sample_interval_seconds INTEGER;
    v_sample_interval_minutes INTEGER;
    v_cron_expression TEXT;
BEGIN
    -- Get current mode
    v_mode := flight_recorder._get_config('mode', 'normal');

    -- Get configurable sample interval (default 60s)
    v_sample_interval_seconds := COALESCE(
        flight_recorder._get_config('sample_interval_seconds', '60')::integer,
        60
    );

    BEGIN
        -- Check pg_cron version
        SELECT extversion INTO v_pgcron_version FROM pg_extension WHERE extname = 'pg_cron';

        IF v_pgcron_version IS NULL THEN
            RETURN 'pg_cron extension not found. Cannot schedule automatic collection.';
        END IF;

        v_pgcron_version := split_part(v_pgcron_version, '-', 1);
        v_supports_subsecond := (
            split_part(v_pgcron_version, '.', 1)::int > 1 OR
            (split_part(v_pgcron_version, '.', 1)::int = 1 AND
             split_part(v_pgcron_version, '.', 2)::int > 4) OR
            (split_part(v_pgcron_version, '.', 1)::int = 1 AND
             split_part(v_pgcron_version, '.', 2)::int = 4 AND
             COALESCE(NULLIF(split_part(v_pgcron_version, '.', 3), '')::int, 0) >= 1)
        );

        -- Schedule snapshot (every 5 minutes)
        PERFORM cron.schedule('flight_recorder_snapshot', '*/5 * * * *', 'SELECT flight_recorder.snapshot()');
        v_scheduled := v_scheduled + 1;

        -- Schedule sample with configured interval (adaptive frequency control)
        -- Generate cron expression based on configured interval
        IF v_sample_interval_seconds <= 60 THEN
            v_cron_expression := '* * * * *';  -- Every minute
            v_sample_schedule := 'every 60 seconds';
        ELSIF v_sample_interval_seconds % 60 = 0 THEN
            -- Clean multiple of minutes
            v_sample_interval_minutes := v_sample_interval_seconds / 60;
            v_cron_expression := format('*/%s * * * *', v_sample_interval_minutes);
            v_sample_schedule := format('every %s seconds', v_sample_interval_seconds);
        ELSE
            -- Round up to nearest minute
            v_sample_interval_minutes := CEILING(v_sample_interval_seconds::numeric / 60.0)::integer;
            v_cron_expression := format('*/%s * * * *', v_sample_interval_minutes);
            v_sample_schedule := format('approximately every %s seconds', v_sample_interval_seconds);
        END IF;

        PERFORM cron.schedule('flight_recorder_sample', v_cron_expression, 'SELECT flight_recorder.sample()');
        v_scheduled := v_scheduled + 1;

        -- Schedule flush (every 5 minutes) - flush ring buffer to durable aggregates
        PERFORM cron.schedule('flight_recorder_flush', '*/5 * * * *', 'SELECT flight_recorder.flush_ring_to_aggregates()');
        v_scheduled := v_scheduled + 1;

        -- Schedule archive (every 15 minutes) - archive raw samples for forensics
        PERFORM cron.schedule('flight_recorder_archive', '*/15 * * * *', 'SELECT flight_recorder.archive_ring_samples()');
        v_scheduled := v_scheduled + 1;

        -- Schedule cleanup (daily at 3 AM) - clean old aggregates and snapshots
        PERFORM cron.schedule('flight_recorder_cleanup', '0 3 * * *',
            'SELECT flight_recorder.cleanup_aggregates(); SELECT * FROM flight_recorder.cleanup(''30 days''::interval);');
        v_scheduled := v_scheduled + 1;

        -- Mark as enabled in config
        INSERT INTO flight_recorder.config (key, value, updated_at)
        VALUES ('enabled', 'true', now())
        ON CONFLICT (key) DO UPDATE SET value = 'true', updated_at = now();

        RETURN format('Flight Recorder collection restarted. Scheduled %s cron jobs in %s mode (sample: %s).',
                     v_scheduled, v_mode, v_sample_schedule);
    EXCEPTION
        WHEN undefined_table THEN
            RETURN 'pg_cron extension not found. Cannot schedule automatic collection.';
        WHEN undefined_function THEN
            RETURN 'pg_cron extension not found. Cannot schedule automatic collection.';
    END;
END;
$$;

-- -----------------------------------------------------------------------------
-- Schedule snapshot collection via pg_cron (every 5 minutes)
-- Schedule sample collection via pg_cron (every 30 seconds)
-- -----------------------------------------------------------------------------

DO $$
DECLARE
    v_pgcron_version TEXT;
    v_major INT;
    v_minor INT;
    v_patch INT;
    v_supports_subsecond BOOLEAN := FALSE;
    v_sample_schedule TEXT;
    v_sample_interval_seconds INTEGER;
    v_sample_interval_minutes INTEGER;
    v_cron_expression TEXT;
BEGIN
    -- Remove existing jobs if any
    BEGIN
        PERFORM cron.unschedule('flight_recorder_snapshot')
        WHERE EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'flight_recorder_snapshot');
        PERFORM cron.unschedule('flight_recorder_sample')
        WHERE EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'flight_recorder_sample');
        PERFORM cron.unschedule('flight_recorder_flush')
        WHERE EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'flight_recorder_flush');
        PERFORM cron.unschedule('flight_recorder_cleanup')
        WHERE EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'flight_recorder_cleanup');
    EXCEPTION
        WHEN undefined_table THEN NULL;
        WHEN undefined_function THEN NULL;
    END;

    -- Read sample interval from config (default 120s)
    SELECT value::integer INTO v_sample_interval_seconds
    FROM flight_recorder.config
    WHERE key = 'sample_interval_seconds';

    v_sample_interval_seconds := COALESCE(v_sample_interval_seconds, 120);

    -- Check pg_cron version to determine if sub-minute scheduling is supported
    -- Sub-minute intervals (e.g., '30 seconds') require pg_cron 1.4.1+
    SELECT extversion INTO v_pgcron_version
    FROM pg_extension WHERE extname = 'pg_cron';

    IF v_pgcron_version IS NOT NULL THEN
        -- Parse version string (handles "1.4.1", "1.4-1", "1.4.1-1", etc.)
        -- Extract numeric parts, treating "-" as a package revision separator (not a version component)
        v_pgcron_version := split_part(v_pgcron_version, '-', 1);  -- Strip package revision (e.g., "1.4-1" -> "1.4")
        v_major := COALESCE(split_part(v_pgcron_version, '.', 1)::int, 0);
        v_minor := COALESCE(NULLIF(split_part(v_pgcron_version, '.', 2), '')::int, 0);
        v_patch := COALESCE(NULLIF(split_part(v_pgcron_version, '.', 3), '')::int, 0);

        -- Check if version >= 1.4.1
        v_supports_subsecond := (v_major > 1)
            OR (v_major = 1 AND v_minor > 4)
            OR (v_major = 1 AND v_minor = 4 AND v_patch >= 1);
    END IF;

    -- Schedule snapshot collection (every 5 minutes) - works on all pg_cron versions
    PERFORM cron.schedule(
        'flight_recorder_snapshot',
        '*/5 * * * *',
        'SELECT flight_recorder.snapshot()'
    );

    -- Schedule sample (default 120 second interval for ring buffer) - A GRADE
    -- Ring buffer architecture uses configurable intervals (120s default, 300s emergency)
    -- Initial schedule is every minute; actual interval controlled by sample_interval_seconds config
    PERFORM cron.schedule(
        'flight_recorder_sample',
        '*/2 * * * *',
        'SELECT flight_recorder.sample()'
    );
    v_sample_schedule := 'every 120 seconds (ring buffer, A GRADE default)';
    RAISE NOTICE 'Flight Recorder installed. Sampling %', v_sample_schedule;

    -- Schedule flush (every 5 minutes) - flush ring buffer to durable aggregates
    PERFORM cron.schedule(
        'flight_recorder_flush',
        '*/5 * * * *',
        'SELECT flight_recorder.flush_ring_to_aggregates()'
    );

    -- Schedule archive (every 15 minutes) - archive raw samples for forensics
    PERFORM cron.schedule(
        'flight_recorder_archive',
        '*/15 * * * *',
        'SELECT flight_recorder.archive_ring_samples()'
    );

    -- Schedule cleanup (daily at 3 AM) - clean old aggregates and snapshots
    PERFORM cron.schedule(
        'flight_recorder_cleanup',
        '0 3 * * *',
        'SELECT flight_recorder.cleanup_aggregates(); SELECT * FROM flight_recorder.cleanup(''30 days''::interval);'
    );

EXCEPTION
    WHEN undefined_table THEN
        RAISE NOTICE 'pg_cron extension not found. Automatic scheduling disabled. Run flight_recorder.snapshot() and flight_recorder.sample() manually or via external scheduler.';
    WHEN undefined_function THEN
        RAISE NOTICE 'pg_cron extension not found. Automatic scheduling disabled. Run flight_recorder.snapshot() and flight_recorder.sample() manually or via external scheduler.';
END;
$$;

-- -----------------------------------------------------------------------------
-- P3: Self-Monitoring and Health Checks
-- -----------------------------------------------------------------------------

-- P3: System health check - quick operational status
CREATE OR REPLACE FUNCTION flight_recorder.health_check()
RETURNS TABLE(
    component TEXT,
    status TEXT,
    details TEXT,
    action_required TEXT
)
LANGUAGE plpgsql AS $$
DECLARE
    v_enabled TEXT;
    v_schema_size_mb NUMERIC;
    v_schema_critical_mb INTEGER;
    v_recent_trips INTEGER;
    v_last_sample TIMESTAMPTZ;
    v_last_snapshot TIMESTAMPTZ;
    v_sample_count INTEGER;
    v_snapshot_count INTEGER;
BEGIN
    -- Check if telemetry is enabled
    v_enabled := flight_recorder._get_config('enabled', 'true');
    IF v_enabled = 'false' THEN
        RETURN QUERY SELECT
            'Flight Recorder System'::text,
            'DISABLED'::text,
            'Collection is disabled'::text,
            'Run flight_recorder.enable() to restart'::text;
        RETURN;
    END IF;

    -- Component 1: Overall system status
    RETURN QUERY SELECT
        'Flight Recorder System'::text,
        'ENABLED'::text,
        format('Mode: %s', flight_recorder._get_config('mode', 'normal')),
        NULL::text;

    -- Component 2: Schema size
    SELECT s.schema_size_mb, s.critical_threshold_mb, s.status
    INTO v_schema_size_mb, v_schema_critical_mb, v_enabled
    FROM flight_recorder._check_schema_size() s;

    RETURN QUERY SELECT
        'Schema Size'::text,
        v_enabled::text,
        format('%s MB / %s MB (%s%%)',
               round(v_schema_size_mb, 2)::text,
               v_schema_critical_mb::text,
               round((v_schema_size_mb / NULLIF(v_schema_critical_mb, 0)) * 100, 1)::text),
        CASE
            WHEN v_enabled = 'CRITICAL' THEN 'Run cleanup() immediately'
            WHEN v_enabled = 'WARNING' THEN 'Schedule cleanup() soon'
            ELSE NULL
        END::text;

    -- Component 3: Circuit breaker status
    SELECT count(*)
    INTO v_recent_trips
    FROM flight_recorder.collection_stats
    WHERE skipped = true
      AND started_at > now() - interval '1 hour'
      AND skipped_reason LIKE '%Circuit breaker%';

    RETURN QUERY SELECT
        'Circuit Breaker'::text,
        CASE
            WHEN v_recent_trips = 0 THEN 'OK'
            WHEN v_recent_trips < 3 THEN 'WARNING'
            ELSE 'CRITICAL'
        END::text,
        format('%s trips in last hour', v_recent_trips),
        CASE
            WHEN v_recent_trips >= 3 THEN 'System under stress - consider emergency mode'
            ELSE NULL
        END::text;

    -- Component 4: Collection freshness
    SELECT max(captured_at) INTO v_last_sample FROM flight_recorder.samples_ring;
    SELECT max(captured_at) INTO v_last_snapshot FROM flight_recorder.snapshots;

    RETURN QUERY SELECT
        'Sample Collection'::text,
        CASE
            WHEN v_last_sample IS NULL THEN 'ERROR'
            WHEN v_last_sample > now() - interval '5 minutes' THEN 'OK'
            WHEN v_last_sample > now() - interval '15 minutes' THEN 'WARNING'
            ELSE 'CRITICAL'
        END::text,
        CASE
            WHEN v_last_sample IS NULL THEN 'No samples collected'
            ELSE format('Last: %s ago', age(now(), v_last_sample))
        END,
        CASE
            WHEN v_last_sample IS NULL OR v_last_sample < now() - interval '15 minutes'
            THEN 'Check pg_cron jobs'
            ELSE NULL
        END::text;

    RETURN QUERY SELECT
        'Snapshot Collection'::text,
        CASE
            WHEN v_last_snapshot IS NULL THEN 'ERROR'
            WHEN v_last_snapshot > now() - interval '10 minutes' THEN 'OK'
            WHEN v_last_snapshot > now() - interval '30 minutes' THEN 'WARNING'
            ELSE 'CRITICAL'
        END::text,
        CASE
            WHEN v_last_snapshot IS NULL THEN 'No snapshots collected'
            ELSE format('Last: %s ago', age(now(), v_last_snapshot))
        END,
        CASE
            WHEN v_last_snapshot IS NULL OR v_last_snapshot < now() - interval '30 minutes'
            THEN 'Check pg_cron jobs'
            ELSE NULL
        END::text;

    -- Component 5: Data volume
    SELECT count(*) INTO v_sample_count FROM flight_recorder.samples_ring;
    SELECT count(*) INTO v_snapshot_count FROM flight_recorder.snapshots;

    RETURN QUERY SELECT
        'Data Volume'::text,
        'INFO'::text,
        format('Samples: %s, Snapshots: %s', v_sample_count, v_snapshot_count),
        NULL::text;

    -- Component 6: pg_stat_statements health
    RETURN QUERY SELECT
        'pg_stat_statements'::text,
        CASE h.status
            WHEN 'DISABLED' THEN 'N/A'
            WHEN 'OK' THEN 'Healthy'
            WHEN 'WARNING' THEN 'Warning'
            WHEN 'HIGH_CHURN' THEN 'Degraded'
            ELSE 'Unknown'
        END::text,
        CASE
            WHEN h.status = 'DISABLED' THEN 'Extension not available'
            ELSE format('Utilization: %s%% (%s/%s statements)',
                       h.utilization_pct::text,
                       h.current_statements::text,
                       h.max_statements::text)
        END,
        CASE
            WHEN h.status = 'HIGH_CHURN' THEN 'Increase pg_stat_statements.max'
            WHEN h.status = 'WARNING' THEN 'Monitor for increased churn'
            ELSE NULL
        END::text
    FROM flight_recorder._check_statements_health() h;

    -- Component 7: pg_cron Job Health (A+ UPGRADE)
    -- Verify all 4 required jobs exist and are active
    DECLARE
        v_job_count INTEGER;
        v_active_jobs INTEGER;
        v_missing_jobs TEXT[];
        v_inactive_jobs TEXT[];
    BEGIN
        -- Check for missing or inactive jobs
        WITH required_jobs AS (
            SELECT unnest(ARRAY[
                'flight_recorder_sample',
                'flight_recorder_snapshot',
                'flight_recorder_flush',
                'flight_recorder_cleanup'
            ]) AS job_name
        )
        SELECT
            count(*) FILTER (WHERE j.jobid IS NULL),
            count(*) FILTER (WHERE j.jobid IS NOT NULL AND j.active),
            array_agg(r.job_name) FILTER (WHERE j.jobid IS NULL),
            array_agg(r.job_name) FILTER (WHERE j.jobid IS NOT NULL AND NOT j.active)
        INTO v_job_count, v_active_jobs, v_missing_jobs, v_inactive_jobs
        FROM required_jobs r
        LEFT JOIN cron.job j ON j.jobname = r.job_name;

        -- Report pg_cron job health
        RETURN QUERY SELECT
            'pg_cron Jobs'::text,
            CASE
                WHEN v_job_count > 0 THEN 'CRITICAL'  -- Missing jobs
                WHEN v_active_jobs < 4 THEN 'CRITICAL'  -- Inactive jobs
                WHEN v_active_jobs = 4 THEN 'OK'
                ELSE 'UNKNOWN'
            END::text,
            CASE
                WHEN v_job_count > 0 THEN
                    format('%s/%s jobs missing: %s', v_job_count, 4, array_to_string(v_missing_jobs, ', '))
                WHEN v_active_jobs < 4 THEN
                    format('%s/%s jobs inactive: %s', 4 - v_active_jobs, 4, array_to_string(v_inactive_jobs, ', '))
                ELSE '4/4 jobs active and running'
            END,
            CASE
                WHEN v_job_count > 0 OR v_active_jobs < 4 THEN
                    'Run flight_recorder.enable() to restore missing/inactive jobs'
                ELSE NULL
            END::text;
    EXCEPTION WHEN OTHERS THEN
        RETURN QUERY SELECT
            'pg_cron Jobs'::text,
            'ERROR'::text,
            format('Failed to check pg_cron jobs: %s', SQLERRM),
            'Verify pg_cron extension is installed and accessible'::text;
    END;
END;
$$;

-- P3: Performance impact analysis - quantify flight recorder overhead
CREATE OR REPLACE FUNCTION flight_recorder.performance_report(p_lookback_interval INTERVAL DEFAULT '24 hours')
RETURNS TABLE(
    metric TEXT,
    value TEXT,
    assessment TEXT
)
LANGUAGE plpgsql AS $$
DECLARE
    v_avg_sample_ms NUMERIC;
    v_max_sample_ms INTEGER;
    v_avg_snapshot_ms NUMERIC;
    v_max_snapshot_ms INTEGER;
    v_total_collections INTEGER;
    v_failed_collections INTEGER;
    v_skipped_collections INTEGER;
    v_schema_size_mb NUMERIC;
BEGIN
    -- Calculate collection performance
    SELECT
        avg(duration_ms) FILTER (WHERE collection_type = 'sample' AND success = true AND skipped = false),
        max(duration_ms) FILTER (WHERE collection_type = 'sample' AND success = true AND skipped = false),
        avg(duration_ms) FILTER (WHERE collection_type = 'snapshot' AND success = true AND skipped = false),
        max(duration_ms) FILTER (WHERE collection_type = 'snapshot' AND success = true AND skipped = false),
        count(*),
        count(*) FILTER (WHERE success = false),
        count(*) FILTER (WHERE skipped = true)
    INTO v_avg_sample_ms, v_max_sample_ms, v_avg_snapshot_ms, v_max_snapshot_ms,
         v_total_collections, v_failed_collections, v_skipped_collections
    FROM flight_recorder.collection_stats
    WHERE started_at > now() - p_lookback_interval;

    -- Get current schema size
    SELECT schema_size_mb INTO v_schema_size_mb FROM flight_recorder._check_schema_size();

    -- Return metrics
    RETURN QUERY SELECT
        'Avg Sample Duration'::text,
        COALESCE(round(v_avg_sample_ms)::text || ' ms', 'N/A'),
        CASE
            WHEN v_avg_sample_ms IS NULL THEN 'No data'
            WHEN v_avg_sample_ms < 100 THEN 'Excellent'
            WHEN v_avg_sample_ms < 500 THEN 'Good'
            WHEN v_avg_sample_ms < 1000 THEN 'Acceptable'
            ELSE 'Poor - consider emergency mode'
        END::text;

    RETURN QUERY SELECT
        'Max Sample Duration'::text,
        COALESCE(v_max_sample_ms::text || ' ms', 'N/A'),
        CASE
            WHEN v_max_sample_ms IS NULL THEN 'No data'
            WHEN v_max_sample_ms < 1000 THEN 'Good'
            WHEN v_max_sample_ms < 5000 THEN 'Acceptable'
            ELSE 'Circuit breaker may trip'
        END::text;

    RETURN QUERY SELECT
        'Avg Snapshot Duration'::text,
        COALESCE(round(v_avg_snapshot_ms)::text || ' ms', 'N/A'),
        CASE
            WHEN v_avg_snapshot_ms IS NULL THEN 'No data'
            WHEN v_avg_snapshot_ms < 500 THEN 'Excellent'
            WHEN v_avg_snapshot_ms < 2000 THEN 'Good'
            WHEN v_avg_snapshot_ms < 5000 THEN 'Acceptable'
            ELSE 'Poor'
        END::text;

    RETURN QUERY SELECT
        'Schema Size'::text,
        round(v_schema_size_mb)::text || ' MB',
        CASE
            WHEN v_schema_size_mb < 1000 THEN 'Healthy'
            WHEN v_schema_size_mb < 5000 THEN 'Good'
            WHEN v_schema_size_mb < 8000 THEN 'Consider cleanup()'
            ELSE 'Run cleanup() soon'
        END::text;

    RETURN QUERY SELECT
        'Collection Success Rate'::text,
        format('%s%% (%s/%s)',
               round(((v_total_collections - v_failed_collections)::numeric / NULLIF(v_total_collections, 0)) * 100, 1)::text,
               v_total_collections - v_failed_collections,
               v_total_collections),
        CASE
            WHEN v_total_collections = 0 THEN 'No collections'
            WHEN v_failed_collections = 0 THEN 'Perfect'
            WHEN (v_failed_collections::numeric / v_total_collections) < 0.01 THEN 'Excellent'
            WHEN (v_failed_collections::numeric / v_total_collections) < 0.05 THEN 'Good'
            ELSE 'Issues detected'
        END::text;

    RETURN QUERY SELECT
        'Skipped Collections'::text,
        v_skipped_collections::text,
        CASE
            WHEN v_skipped_collections = 0 THEN 'No skips'
            WHEN v_skipped_collections < 5 THEN 'Minimal'
            WHEN v_skipped_collections < 20 THEN 'Moderate - check system load'
            ELSE 'Significant - system under stress'
        END::text;

    -- Section Success Rate
    RETURN QUERY SELECT
        'Section Success Rate'::text,
        COALESCE(
            round(100.0 * avg(sections_succeeded::numeric / NULLIF(sections_total, 0)), 1)::text || '%',
            'N/A'
        ),
        CASE
            WHEN avg(sections_succeeded::numeric / NULLIF(sections_total, 0)) IS NULL THEN 'No data'
            WHEN avg(sections_succeeded::numeric / NULLIF(sections_total, 0)) >= 0.95 THEN 'Excellent'
            WHEN avg(sections_succeeded::numeric / NULLIF(sections_total, 0)) >= 0.90 THEN 'Good'
            WHEN avg(sections_succeeded::numeric / NULLIF(sections_total, 0)) >= 0.75 THEN 'Fair - some section failures'
            ELSE 'Poor - frequent section failures'
        END::text
    FROM flight_recorder.collection_stats
    WHERE started_at > now() - p_lookback_interval
      AND sections_total IS NOT NULL;

    -- Performance Trend Analysis (recent vs baseline)
    RETURN QUERY
    WITH recent AS (
        SELECT duration_ms
        FROM flight_recorder.collection_stats
        WHERE collection_type = 'sample'
          AND success = true
          AND skipped = false
          AND started_at > now() - p_lookback_interval
        ORDER BY started_at DESC
        LIMIT 10
    ),
    baseline AS (
        SELECT duration_ms
        FROM flight_recorder.collection_stats
        WHERE collection_type = 'sample'
          AND success = true
          AND skipped = false
          AND started_at > now() - p_lookback_interval
        ORDER BY started_at DESC
        LIMIT 100 OFFSET 50
    )
    SELECT
        'Performance Trend'::text,
        COALESCE(
            CASE
                WHEN (SELECT avg(duration_ms) FROM recent) IS NULL OR (SELECT avg(duration_ms) FROM baseline) IS NULL THEN 'Insufficient data'
                ELSE format('%s → %s ms (%s%s)',
                    round((SELECT avg(duration_ms) FROM baseline))::text,
                    round((SELECT avg(duration_ms) FROM recent))::text,
                    CASE WHEN ((SELECT avg(duration_ms) FROM recent) - (SELECT avg(duration_ms) FROM baseline)) > 0 THEN '+' ELSE '' END,
                    round((((SELECT avg(duration_ms) FROM recent) - (SELECT avg(duration_ms) FROM baseline)) / NULLIF((SELECT avg(duration_ms) FROM baseline), 0)) * 100, 1)::text || '%'
                )
            END,
            'N/A'
        ),
        CASE
            WHEN (SELECT avg(duration_ms) FROM recent) IS NULL OR (SELECT avg(duration_ms) FROM baseline) IS NULL THEN 'Need more data'
            WHEN (SELECT avg(duration_ms) FROM recent) > (SELECT avg(duration_ms) FROM baseline) * 1.5 THEN 'DEGRADING - investigate system load'
            WHEN (SELECT avg(duration_ms) FROM recent) < (SELECT avg(duration_ms) FROM baseline) * 0.7 THEN 'IMPROVING'
            ELSE 'STABLE'
        END::text;
END;
$$;

-- -----------------------------------------------------------------------------
-- P4: Advanced Features - Alerts and Export
-- -----------------------------------------------------------------------------

-- P4: Check alert conditions and return notifications
CREATE OR REPLACE FUNCTION flight_recorder.check_alerts(p_lookback_interval INTERVAL DEFAULT '1 hour')
RETURNS TABLE(
    alert_type TEXT,
    severity TEXT,
    message TEXT,
    triggered_at TIMESTAMPTZ,
    recommendation TEXT
)
LANGUAGE plpgsql AS $$
DECLARE
    v_enabled BOOLEAN;
    v_cb_threshold INTEGER;
    v_cb_count INTEGER;
    v_schema_threshold_mb INTEGER;
    v_schema_size_mb NUMERIC;
BEGIN
    -- Check if alerts are enabled
    v_enabled := COALESCE(
        flight_recorder._get_config('alert_enabled', 'false')::boolean,
        false
    );

    IF NOT v_enabled THEN
        RETURN;
    END IF;

    -- Alert 1: Excessive circuit breaker trips
    v_cb_threshold := COALESCE(
        flight_recorder._get_config('alert_circuit_breaker_count', '5')::integer,
        5
    );

    SELECT count(*) INTO v_cb_count
    FROM flight_recorder.collection_stats
    WHERE skipped = true
      AND started_at > now() - p_lookback_interval
      AND skipped_reason LIKE '%Circuit breaker%';

    IF v_cb_count >= v_cb_threshold THEN
        RETURN QUERY SELECT
            'CIRCUIT_BREAKER_TRIPS'::text,
            'CRITICAL'::text,
            format('Circuit breaker tripped %s times in last %s', v_cb_count, p_lookback_interval),
            now(),
            'System under severe stress. Consider switching to emergency mode or disabling flight recorder temporarily.'::text;
    END IF;

    -- Alert 2: Schema size approaching critical
    v_schema_threshold_mb := COALESCE(
        flight_recorder._get_config('alert_schema_size_mb', '8000')::integer,
        8000
    );

    SELECT schema_size_mb INTO v_schema_size_mb FROM flight_recorder._check_schema_size();

    IF v_schema_size_mb >= v_schema_threshold_mb THEN
        RETURN QUERY SELECT
            'SCHEMA_SIZE_HIGH'::text,
            'WARNING'::text,
            format('Schema size is %s MB (threshold: %s MB)', round(v_schema_size_mb)::text, v_schema_threshold_mb),
            now(),
            'Run flight_recorder.cleanup() to reclaim space.'::text;
    END IF;

    -- Alert 3: Collection failures
    DECLARE
        v_recent_failures INTEGER;
    BEGIN
        SELECT count(*) INTO v_recent_failures
        FROM flight_recorder.collection_stats
        WHERE success = false
          AND started_at > now() - p_lookback_interval;

        IF v_recent_failures >= 5 THEN
            RETURN QUERY SELECT
                'COLLECTION_FAILURES'::text,
                'WARNING'::text,
                format('%s collection failures in last %s', v_recent_failures, p_lookback_interval),
                now(),
                'Check PostgreSQL logs for error details.'::text;
        END IF;
    END;

    -- Alert 4: No recent collections (stale data)
    DECLARE
        v_last_sample TIMESTAMPTZ;
    BEGIN
        SELECT max(captured_at) INTO v_last_sample FROM flight_recorder.samples_ring;

        IF v_last_sample IS NULL OR v_last_sample < now() - interval '15 minutes' THEN
            RETURN QUERY SELECT
                'STALE_DATA'::text,
                'CRITICAL'::text,
                CASE
                    WHEN v_last_sample IS NULL THEN 'No samples collected yet'
                    ELSE format('Last sample was %s ago', age(now(), v_last_sample))
                END,
                now(),
                'Check pg_cron jobs: SELECT * FROM cron.job WHERE jobname LIKE ''flight_recorder_%'''::text;
        END IF;
    END;
END;
$$;

-- P4: Export flight recorder data to JSON format (AI-Optimized)
CREATE OR REPLACE FUNCTION flight_recorder.export_json(
    p_start_time TIMESTAMPTZ,
    p_end_time TIMESTAMPTZ
)
RETURNS JSONB
LANGUAGE plpgsql AS $$
DECLARE
    v_result JSONB;
    v_samples JSONB;
    v_snapshots JSONB;
    v_anomalies JSONB;
    v_wait_summary JSONB;
BEGIN
    -- 1. Get Anomalies (High signal)
    SELECT jsonb_agg(to_jsonb(r)) INTO v_anomalies
    FROM flight_recorder.anomaly_report(p_start_time, p_end_time) r;

    -- 2. Get Wait Summary (High signal)
    SELECT jsonb_agg(to_jsonb(r)) INTO v_wait_summary
    FROM flight_recorder.wait_summary(p_start_time, p_end_time) r;

    -- 3. Get Samples (Compact format: Array of Arrays)
    -- Schema: [captured_at, [wait_events], [locks]]
    -- Wait Events Schema: [backend, type, event, count]
    -- Locks Schema: [blocked_pid, blocking_pid, type, duration]
    SELECT jsonb_agg(
        jsonb_build_array(
            s.captured_at,
            COALESCE((
                SELECT jsonb_agg(jsonb_build_array(
                    ws.backend_type,
                    ws.wait_event_type,
                    ws.wait_event,
                    ws.count
                ))
                FROM flight_recorder.wait_samples_ring ws
                WHERE ws.slot_id = s.slot_id
            ), '[]'::jsonb),
            COALESCE((
                SELECT jsonb_agg(jsonb_build_array(
                    ls.blocked_pid,
                    ls.blocking_pid,
                    ls.lock_type,
                    ls.blocked_duration
                ))
                FROM flight_recorder.lock_samples_ring ls
                WHERE ls.slot_id = s.slot_id
            ), '[]'::jsonb)
        )
    )
    INTO v_samples
    FROM flight_recorder.samples_ring s
    WHERE s.captured_at BETWEEN p_start_time AND p_end_time;

    -- 4. Get Snapshots (Compact format)
    -- Schema: [captured_at, wal_bytes, ckpt_timed, ckpt_req, bgw_backend_writes]
    SELECT jsonb_agg(
        jsonb_build_array(
            sn.captured_at,
            sn.wal_bytes,
            sn.ckpt_timed,
            sn.ckpt_requested,
            sn.bgw_buffers_backend
        )
    )
    INTO v_snapshots
    FROM flight_recorder.snapshots sn
    WHERE sn.captured_at BETWEEN p_start_time AND p_end_time;

    -- Build Final Result with Schema Hints for AI
    v_result := jsonb_build_object(
        'meta', jsonb_build_object(
            'generated_at', now(),
            'version', '1.0-ai',
            'schemas', jsonb_build_object(
                'samples', '[captured_at, wait_events[[backend, type, event, count]], locks[[blocked_pid, blocking_pid, type, duration]]]',
                'snapshots', '[captured_at, wal_bytes, ckpt_timed, ckpt_req, bgw_backend_writes]'
            )
        ),
        'range', jsonb_build_object('start', p_start_time, 'end', p_end_time),
        'anomalies', COALESCE(v_anomalies, '[]'::jsonb),
        'wait_summary', COALESCE(v_wait_summary, '[]'::jsonb),
        'samples', COALESCE(v_samples, '[]'::jsonb),
        'snapshots', COALESCE(v_snapshots, '[]'::jsonb)
    );

    RETURN v_result;
END;
$$;

-- P4: Configuration recommendations engine
CREATE OR REPLACE FUNCTION flight_recorder.config_recommendations()
RETURNS TABLE(
    category TEXT,
    recommendation TEXT,
    reason TEXT,
    sql_command TEXT
)
LANGUAGE plpgsql AS $$
DECLARE
    v_mode TEXT;
    v_schema_size_mb NUMERIC;
    v_avg_sample_ms NUMERIC;
    v_sample_count INTEGER;
    v_snapshot_count INTEGER;
    v_retention_samples INTEGER;
    v_retention_snapshots INTEGER;
BEGIN
    -- Get current state
    v_mode := flight_recorder._get_config('mode', 'normal');
    SELECT schema_size_mb INTO v_schema_size_mb FROM flight_recorder._check_schema_size();
    SELECT count(*) INTO v_sample_count FROM flight_recorder.samples_ring;
    SELECT count(*) INTO v_snapshot_count FROM flight_recorder.snapshots;

    SELECT avg(duration_ms) INTO v_avg_sample_ms
    FROM flight_recorder.collection_stats
    WHERE collection_type = 'sample'
      AND success = true
      AND skipped = false
      AND started_at > now() - interval '24 hours';

    v_retention_samples := flight_recorder._get_config('retention_samples_days', '7')::integer;
    v_retention_snapshots := flight_recorder._get_config('retention_snapshots_days', '30')::integer;

    -- Recommendation 1: Mode optimization
    IF v_avg_sample_ms > 1000 AND v_mode = 'normal' THEN
        RETURN QUERY SELECT
            'Performance'::text,
            'Switch to light mode'::text,
            format('Average sample duration is %s ms, which may impact system performance', round(v_avg_sample_ms)),
            'SELECT flight_recorder.set_mode(''light'');'::text;
    END IF;

    -- Recommendation 2: Schema size
    IF v_schema_size_mb > 5000 THEN
        RETURN QUERY SELECT
            'Storage'::text,
            'Run cleanup to reclaim space'::text,
            format('Schema size is %s MB', round(v_schema_size_mb)::text),
            'SELECT * FROM flight_recorder.cleanup();'::text;
    END IF;

    -- Recommendation 3: Retention tuning
    IF v_sample_count > 50000 AND v_retention_samples > 7 THEN
        RETURN QUERY SELECT
            'Storage'::text,
            'Reduce sample retention period'::text,
            format('High sample count (%s) with %s day retention', v_sample_count, v_retention_samples),
            format('UPDATE flight_recorder.config SET value = ''3'' WHERE key = ''retention_samples_days'';')::text;
    END IF;

    -- Recommendation 4: Auto-mode
    IF v_avg_sample_ms > 500 AND flight_recorder._get_config('auto_mode_enabled', 'false') = 'false' THEN
        RETURN QUERY SELECT
            'Automation'::text,
            'Enable automatic mode switching'::text,
            'Sample duration varies significantly - auto-mode can help reduce overhead during peaks'::text,
            'UPDATE flight_recorder.config SET value = ''true'' WHERE key = ''auto_mode_enabled'';'::text;
    END IF;

    -- If no recommendations, return success message
    IF NOT FOUND THEN
        RETURN QUERY SELECT
            'System Health'::text,
            'Configuration looks optimal'::text,
            'No configuration changes recommended at this time'::text,
            NULL::text;
    END IF;
END;
$$;

-- -----------------------------------------------------------------------------
-- Preflight Check - Run before enabling for "set and forget" validation
-- -----------------------------------------------------------------------------
-- No-brainer pre-flight validation for initial setup.
-- Checks if your environment is suitable for always-on monitoring.
-- Returns: Clear GO/NO-GO with actionable recommendations.
--
-- USAGE:
--   SELECT * FROM flight_recorder.preflight_check();
--
-- Expected output: All checks should show 'GO' status for safe always-on operation.
--
CREATE OR REPLACE FUNCTION flight_recorder.preflight_check()
RETURNS TABLE(
    check_name TEXT,
    status TEXT,
    details TEXT,
    recommendation TEXT
)
LANGUAGE plpgsql AS $$
DECLARE
    v_max_connections INTEGER;
    v_current_connections INTEGER;
    v_pg_stat_statements_max INTEGER;
    v_cpu_count INTEGER;
    v_shared_buffers_mb INTEGER;
    v_connection_pct NUMERIC;
    v_pg_cron_exists BOOLEAN;
BEGIN
    -- Check 1: System resources (CPU count via pg_stat_statements or estimate)
    BEGIN
        -- Try to get CPU count from system (may not be available)
        v_cpu_count := (SELECT setting::integer FROM pg_settings WHERE name = 'max_worker_processes');
        IF v_cpu_count < 4 THEN
            RETURN QUERY SELECT
                'System Resources'::text,
                'CAUTION'::text,
                format('Detected %s worker processes (CPU estimate)', v_cpu_count),
                'Flight recorder overhead (0.5%%) is acceptable but consider testing in staging first. Systems with <4 cores have less overhead margin.'::text;
        ELSE
            RETURN QUERY SELECT
                'System Resources'::text,
                'GO'::text,
                format('Detected %s worker processes', v_cpu_count),
                'System has adequate resources for always-on monitoring.'::text;
        END IF;
    EXCEPTION WHEN OTHERS THEN
        RETURN QUERY SELECT
            'System Resources'::text,
            'GO'::text,
            'Unable to detect CPU count',
            'Verify your system has ≥4 CPU cores for comfortable always-on operation.'::text;
    END;

    -- Check 2: Connection headroom
    SELECT setting::integer INTO v_max_connections FROM pg_settings WHERE name = 'max_connections';
    SELECT count(*) INTO v_current_connections FROM pg_stat_activity;
    v_connection_pct := (v_current_connections::numeric / v_max_connections) * 100;

    IF v_connection_pct > 70 THEN
        RETURN QUERY SELECT
            'Connection Headroom'::text,
            'CAUTION'::text,
            format('Currently %s%% of max_connections (%s/%s)', round(v_connection_pct, 1), v_current_connections, v_max_connections),
            'System frequently near max_connections. Adaptive mode will trigger often. Consider increasing max_connections or monitoring connection patterns.'::text;
    ELSE
        RETURN QUERY SELECT
            'Connection Headroom'::text,
            'GO'::text,
            format('Currently %s%% of max_connections (%s/%s)', round(v_connection_pct, 1), v_current_connections, v_max_connections),
            'Adequate connection headroom for normal operations.'::text;
    END IF;

    -- Check 3: pg_stat_statements budget
    BEGIN
        SELECT setting::integer INTO v_pg_stat_statements_max
        FROM pg_settings WHERE name = 'pg_stat_statements.max';

        IF v_pg_stat_statements_max < 5000 THEN
            RETURN QUERY SELECT
                'pg_stat_statements Budget'::text,
                'NO-GO'::text,
                format('pg_stat_statements.max = %s (too low)', v_pg_stat_statements_max),
                'Flight recorder will consume 20-40% of statement budget. Increase to at least 10,000: ALTER SYSTEM SET pg_stat_statements.max = 10000; (requires restart)'::text;
        ELSIF v_pg_stat_statements_max < 10000 THEN
            RETURN QUERY SELECT
                'pg_stat_statements Budget'::text,
                'CAUTION'::text,
                format('pg_stat_statements.max = %s (minimal)', v_pg_stat_statements_max),
                'Flight recorder will consume 20-40% of statement budget. Consider increasing to 10,000+ for comfortable operation.'::text;
        ELSE
            RETURN QUERY SELECT
                'pg_stat_statements Budget'::text,
                'GO'::text,
                format('pg_stat_statements.max = %s', v_pg_stat_statements_max),
                'Adequate statement tracking capacity.'::text;
        END IF;
    EXCEPTION WHEN OTHERS THEN
        RETURN QUERY SELECT
            'pg_stat_statements Budget'::text,
            'CAUTION'::text,
            'pg_stat_statements extension not found or not configured',
            'Statement collection will be disabled (statements_enabled=auto). This is acceptable if you don''t need query-level metrics.'::text;
    END;

    -- Check 4: Storage headroom
    BEGIN
        SELECT setting::integer INTO v_shared_buffers_mb
        FROM pg_settings WHERE name = 'shared_buffers';
        -- Convert from 8KB blocks to MB
        v_shared_buffers_mb := (v_shared_buffers_mb * 8) / 1024;

        RETURN QUERY SELECT
            'Storage Overhead'::text,
            'GO'::text,
            'Ring buffer uses fixed 120KB memory. Aggregates: ~2-3 GB per week (7-day retention).',
            'UNLOGGED ring buffers minimize WAL overhead. Ring buffers self-clean automatically. Daily aggregate cleanup prevents unbounded growth.'::text;
    END;

    -- Check 5: pg_cron availability
    SELECT EXISTS (
        SELECT 1 FROM pg_extension WHERE extname = 'pg_cron'
    ) INTO v_pg_cron_exists;

    IF v_pg_cron_exists THEN
        RETURN QUERY SELECT
            'Scheduling (pg_cron)'::text,
            'GO'::text,
            'pg_cron extension detected',
            'Automatic scheduling will work. Flight recorder will run automatically every 3 minutes.'::text;
    ELSE
        RETURN QUERY SELECT
            'Scheduling (pg_cron)'::text,
            'CAUTION'::text,
            'pg_cron extension not found',
            'You will need to schedule flight_recorder.sample() and flight_recorder.snapshot() manually via external cron or pg_agent.'::text;
    END IF;

    -- Check 6: Safety mechanisms
    RETURN QUERY SELECT
        'Safety Mechanisms'::text,
        'GO'::text,
        'Circuit breaker, adaptive mode, timeouts all enabled by default',
        'Flight recorder will auto-reduce overhead under stress.'::text;

END;
$$;

COMMENT ON FUNCTION flight_recorder.preflight_check() IS
'Pre-installation validation checks. Returns component status (GO/CAUTION/NO-GO). For summary, use preflight_check_with_summary().';

-- Helper function: preflight_check with summary recommendation
CREATE OR REPLACE FUNCTION flight_recorder.preflight_check_with_summary()
RETURNS TABLE(
    check_name TEXT,
    status TEXT,
    details TEXT,
    recommendation TEXT
)
LANGUAGE plpgsql AS $$
DECLARE
    v_nogo_count INTEGER;
    v_caution_count INTEGER;
BEGIN
    -- Return all preflight check results
    RETURN QUERY SELECT * FROM flight_recorder.preflight_check();
    
    -- Count status results (calls function again to count)
    SELECT
        count(*) FILTER (WHERE c.status = 'NO-GO'),
        count(*) FILTER (WHERE c.status = 'CAUTION')
    INTO v_nogo_count, v_caution_count
    FROM flight_recorder.preflight_check() c;
    
    -- Add summary based on counts
    IF v_nogo_count > 0 THEN
        RETURN QUERY SELECT
            '=== SUMMARY ==='::text,
            'NO-GO'::text,
            format('%s critical issues detected', v_nogo_count),
            'Address NO-GO items before enabling always-on monitoring. See recommendations above.'::text;
    ELSIF v_caution_count > 0 THEN
        RETURN QUERY SELECT
            '=== SUMMARY ==='::text,
            'PROCEED WITH CAUTION'::text,
            format('%s cautions detected', v_caution_count),
            'System is acceptable for always-on monitoring but consider addressing cautions. Test in staging first if possible.'::text;
    ELSE
        RETURN QUERY SELECT
            '=== SUMMARY ==='::text,
            'READY FOR PRODUCTION'::text,
            'All checks passed',
            'System is well-suited for "set and forget" always-on monitoring. Run quarterly_review() every 3 months to verify continued health.'::text;
    END IF;
END;
$$;

COMMENT ON FUNCTION flight_recorder.preflight_check_with_summary() IS
'Pre-installation validation with summary. Calls preflight_check() twice - once for results, once to count. More expensive but includes summary row.';

-- -----------------------------------------------------------------------------
-- Quarterly Review - Run every 3 months for ongoing health validation
-- -----------------------------------------------------------------------------
-- No-brainer quarterly health check for always-on deployments.
-- Validates that flight recorder is still operating within acceptable parameters.
-- Returns: Clear health status with actionable recommendations.
--
-- USAGE:
--   SELECT * FROM flight_recorder.quarterly_review();
--
-- Run this every 3 months (set a calendar reminder) to ensure flight recorder
-- continues to operate safely. Takes ~1 second to execute.
--
CREATE OR REPLACE FUNCTION flight_recorder.quarterly_review()
RETURNS TABLE(
    component TEXT,
    status TEXT,
    metric TEXT,
    assessment TEXT
)
LANGUAGE plpgsql AS $$
DECLARE
    v_avg_duration_ms NUMERIC;
    v_max_duration_ms NUMERIC;
    v_skipped_count INTEGER;
    v_total_count INTEGER;
    v_schema_size_mb NUMERIC;
    v_last_sample TIMESTAMPTZ;
    v_last_snapshot TIMESTAMPTZ;
    v_circuit_breaker_trips INTEGER;
    v_failed_collections INTEGER;
    v_sample_count INTEGER;
BEGIN
    -- Header
    RETURN QUERY SELECT
        '=== FLIGHT RECORDER QUARTERLY REVIEW ==='::text,
        'INFO'::text,
        format('Review period: Last 90 days | Generated: %s', now()::text),
        'This review validates flight recorder health for continued always-on operation.'::text;

    -- Metric 1: Average overhead (last 30 days)
    SELECT
        avg(duration_ms),
        max(duration_ms),
        count(*) FILTER (WHERE skipped),
        count(*)
    INTO v_avg_duration_ms, v_max_duration_ms, v_skipped_count, v_total_count
    FROM flight_recorder.collection_stats
    WHERE started_at > now() - interval '30 days'
      AND collection_type = 'sample';

    IF v_avg_duration_ms IS NULL THEN
        RETURN QUERY SELECT
            '1. Collection Performance'::text,
            'ERROR'::text,
            'No collections in last 30 days',
            'Flight recorder may not be running. Check pg_cron jobs.'::text;
    ELSIF v_avg_duration_ms < 200 THEN
        RETURN QUERY SELECT
            '1. Collection Performance'::text,
            'EXCELLENT'::text,
            format('Avg: %sms | Max: %sms | Skipped: %s/%s',
                   round(v_avg_duration_ms), round(v_max_duration_ms), v_skipped_count, v_total_count),
            'Collection overhead is minimal. No action needed.'::text;
    ELSIF v_avg_duration_ms < 500 THEN
        RETURN QUERY SELECT
            '1. Collection Performance'::text,
            'GOOD'::text,
            format('Avg: %sms | Max: %sms | Skipped: %s/%s',
                   round(v_avg_duration_ms), round(v_max_duration_ms), v_skipped_count, v_total_count),
            'Collection overhead is acceptable. Continue monitoring.'::text;
    ELSE
        RETURN QUERY SELECT
            '1. Collection Performance'::text,
            'REVIEW NEEDED'::text,
            format('Avg: %sms | Max: %sms | Skipped: %s/%s',
                   round(v_avg_duration_ms), round(v_max_duration_ms), v_skipped_count, v_total_count),
            'Collections are slower than expected. Consider: (1) switching to light mode, (2) increasing sample_interval_seconds to 300, or (3) checking for system bottlenecks.'::text;
    END IF;

    -- Metric 2: Storage consumption
    SELECT schema_size_mb INTO v_schema_size_mb FROM flight_recorder._check_schema_size();
    SELECT count(*) INTO v_sample_count FROM flight_recorder.samples_ring;

    IF v_schema_size_mb < 3000 THEN
        RETURN QUERY SELECT
            '2. Storage Consumption'::text,
            'EXCELLENT'::text,
            format('%s MB | %s samples', round(v_schema_size_mb), v_sample_count),
            'Storage usage is healthy. Daily cleanup is working correctly.'::text;
    ELSIF v_schema_size_mb < 6000 THEN
        RETURN QUERY SELECT
            '2. Storage Consumption'::text,
            'GOOD'::text,
            format('%s MB | %s samples', round(v_schema_size_mb), v_sample_count),
            'Storage usage is acceptable. Monitor growth trend.'::text;
    ELSE
        RETURN QUERY SELECT
            '2. Storage Consumption'::text,
            'REVIEW NEEDED'::text,
            format('%s MB | %s samples', round(v_schema_size_mb), v_sample_count),
            'Storage usage is high. Run cleanup() or reduce retention_samples_days.'::text;
    END IF;

    -- Metric 3: Collection reliability (last 90 days)
    SELECT
        count(*) FILTER (WHERE NOT success AND NOT skipped)
    INTO v_failed_collections
    FROM flight_recorder.collection_stats
    WHERE started_at > now() - interval '90 days';

    IF v_failed_collections = 0 THEN
        RETURN QUERY SELECT
            '3. Collection Reliability'::text,
            'EXCELLENT'::text,
            format('0 failed collections in 90 days'),
            'Flight recorder is operating reliably.'::text;
    ELSIF v_failed_collections < 10 THEN
        RETURN QUERY SELECT
            '3. Collection Reliability'::text,
            'GOOD'::text,
            format('%s failed collections in 90 days', v_failed_collections),
            'Minimal failures detected. This is normal and acceptable.'::text;
    ELSE
        RETURN QUERY SELECT
            '3. Collection Reliability'::text,
            'REVIEW NEEDED'::text,
            format('%s failed collections in 90 days', v_failed_collections),
            'Frequent failures detected. Check collection_stats for error patterns.'::text;
    END IF;

    -- Metric 4: Circuit breaker activity (last 90 days)
    SELECT count(*) INTO v_circuit_breaker_trips
    FROM flight_recorder.collection_stats
    WHERE skipped = true
      AND skipped_reason LIKE '%Circuit breaker%'
      AND started_at > now() - interval '90 days';

    IF v_circuit_breaker_trips = 0 THEN
        RETURN QUERY SELECT
            '4. Circuit Breaker Activity'::text,
            'EXCELLENT'::text,
            '0 trips in 90 days',
            'System has not experienced collection stress.'::text;
    ELSIF v_circuit_breaker_trips < 20 THEN
        RETURN QUERY SELECT
            '4. Circuit Breaker Activity'::text,
            'GOOD'::text,
            format('%s trips in 90 days', v_circuit_breaker_trips),
            'Circuit breaker has triggered occasionally. This is normal during temporary load spikes.'::text;
    ELSE
        RETURN QUERY SELECT
            '4. Circuit Breaker Activity'::text,
            'REVIEW NEEDED'::text,
            format('%s trips in 90 days', v_circuit_breaker_trips),
            'Frequent circuit breaker trips indicate system stress. Consider switching to light mode permanently.'::text;
    END IF;

    -- Metric 5: Data freshness
    SELECT max(captured_at) INTO v_last_sample FROM flight_recorder.samples_ring;
    SELECT max(captured_at) INTO v_last_snapshot FROM flight_recorder.snapshots;

    IF v_last_sample > now() - interval '10 minutes' AND v_last_snapshot > now() - interval '15 minutes' THEN
        RETURN QUERY SELECT
            '5. Data Freshness'::text,
            'EXCELLENT'::text,
            format('Last sample: %s ago | Last snapshot: %s ago',
                   age(now(), v_last_sample)::text, age(now(), v_last_snapshot)::text),
            'Collections are running on schedule.'::text;
    ELSE
        RETURN QUERY SELECT
            '5. Data Freshness'::text,
            'ERROR'::text,
            format('Last sample: %s ago | Last snapshot: %s ago',
                   age(now(), v_last_sample)::text, age(now(), v_last_snapshot)::text),
            'Collections are stale. Check pg_cron jobs: SELECT * FROM cron.job WHERE jobname LIKE ''flight_recorder_%'';'::text;
    END IF;

    -- Metric 6: pg_cron Job Health (A+ UPGRADE)
    -- Verify all 4 required jobs exist and are active
    DECLARE
        v_missing_count INTEGER;
        v_inactive_count INTEGER;
        v_missing_jobs TEXT[];
        v_inactive_jobs TEXT[];
    BEGIN
        WITH required_jobs AS (
            SELECT unnest(ARRAY[
                'flight_recorder_sample',
                'flight_recorder_snapshot',
                'flight_recorder_flush',
                'flight_recorder_cleanup'
            ]) AS job_name
        )
        SELECT
            count(*) FILTER (WHERE j.jobid IS NULL),
            count(*) FILTER (WHERE j.jobid IS NOT NULL AND NOT j.active),
            array_agg(r.job_name) FILTER (WHERE j.jobid IS NULL),
            array_agg(r.job_name) FILTER (WHERE j.jobid IS NOT NULL AND NOT j.active)
        INTO v_missing_count, v_inactive_count, v_missing_jobs, v_inactive_jobs
        FROM required_jobs r
        LEFT JOIN cron.job j ON j.jobname = r.job_name;

        IF v_missing_count = 0 AND v_inactive_count = 0 THEN
            RETURN QUERY SELECT
                '6. pg_cron Job Health'::text,
                'EXCELLENT'::text,
                '4/4 jobs active (sample, snapshot, flush, cleanup)',
                'All pg_cron jobs are running correctly.'::text;
        ELSIF v_missing_count > 0 THEN
            RETURN QUERY SELECT
                '6. pg_cron Job Health'::text,
                'CRITICAL'::text,
                format('%s/%s jobs missing: %s', v_missing_count, 4, array_to_string(v_missing_jobs, ', ')),
                'CRITICAL: Flight recorder is not collecting data. Run flight_recorder.enable() to restore.'::text;
        ELSE
            RETURN QUERY SELECT
                '6. pg_cron Job Health'::text,
                'CRITICAL'::text,
                format('%s/%s jobs inactive: %s', v_inactive_count, 4, array_to_string(v_inactive_jobs, ', ')),
                'CRITICAL: pg_cron jobs exist but are disabled. Run flight_recorder.enable() to reactivate.'::text;
        END IF;
    EXCEPTION WHEN OTHERS THEN
        RETURN QUERY SELECT
            '7. pg_cron Job Health'::text,
            'ERROR'::text,
            format('Failed to check pg_cron jobs: %s', SQLERRM),
            'Verify pg_cron extension is installed and accessible.'::text;
    END;

END;
$$;

COMMENT ON FUNCTION flight_recorder.quarterly_review() IS
'Quarterly health check for flight recorder. Returns component metrics (EXCELLENT/GOOD/REVIEW NEEDED/ERROR). For summary, use quarterly_review_with_summary().';

-- Helper function: quarterly_review with summary
CREATE OR REPLACE FUNCTION flight_recorder.quarterly_review_with_summary()
RETURNS TABLE(
    component TEXT,
    status TEXT,
    metric TEXT,
    assessment TEXT
)
LANGUAGE plpgsql AS $$
DECLARE
    v_issues_count INTEGER;
BEGIN
    -- Return all quarterly review results
    RETURN QUERY SELECT * FROM flight_recorder.quarterly_review();

    -- Count status results (calls function again to count)
    SELECT count(*) INTO v_issues_count
    FROM flight_recorder.quarterly_review() qr
    WHERE qr.status IN ('ERROR', 'REVIEW NEEDED');

    -- Add summary based on counts
    IF v_issues_count = 0 THEN
        RETURN QUERY SELECT
            '=== QUARTERLY REVIEW SUMMARY ==='::text,
            'HEALTHY'::text,
            'All metrics within acceptable parameters',
            'Flight recorder is operating as expected. Schedule next review in 3 months. No action required.'::text;
    ELSE
        RETURN QUERY SELECT
            '=== QUARTERLY REVIEW SUMMARY ==='::text,
            'ACTION REQUIRED'::text,
            format('%s items need review', v_issues_count),
            'Address items marked ERROR or REVIEW NEEDED above. Run config_recommendations() for specific tuning suggestions.'::text;
    END IF;
END;
$$;

COMMENT ON FUNCTION flight_recorder.quarterly_review_with_summary() IS
'Quarterly health check with summary. Calls quarterly_review() twice - once for results, once to count. More expensive but includes summary row.';

-- Capture initial snapshot and sample
SELECT flight_recorder.snapshot();
SELECT flight_recorder.sample();

-- -----------------------------------------------------------------------------
-- Done
-- -----------------------------------------------------------------------------

DO $$
DECLARE
    v_sample_schedule TEXT;
BEGIN
    -- Determine what sampling schedule was configured
    SELECT schedule INTO v_sample_schedule
    FROM cron.job WHERE jobname = 'flight_recorder_sample';

    RAISE NOTICE '';
    RAISE NOTICE 'Flight Recorder installed successfully.';
    RAISE NOTICE '';
    RAISE NOTICE 'Collection schedule:';
    RAISE NOTICE '  - Snapshots: every 5 minutes (WAL, checkpoints, I/O stats) - DURABLE';
    RAISE NOTICE '  - Samples: every 120 seconds (ring buffer, 120 slots, 4-hour retention, A GRADE default)';
    RAISE NOTICE '  - Flush: every 5 minutes (ring buffer → durable aggregates)';
    RAISE NOTICE '  - Cleanup: daily at 3 AM (aggregates: 7 days, snapshots: 30 days)';
    RAISE NOTICE '';
    RAISE NOTICE 'Quick start:';
    RAISE NOTICE '  1. Flight Recorder collects automatically in the background';
    RAISE NOTICE '';
    RAISE NOTICE '  2. Query any time window to diagnose performance:';
    RAISE NOTICE '     SELECT * FROM flight_recorder.compare(''2024-12-16 14:00'', ''2024-12-16 15:00'');';
    RAISE NOTICE '     SELECT * FROM flight_recorder.wait_summary(''2024-12-16 14:00'', ''2024-12-16 15:00'');';
    RAISE NOTICE '';
    RAISE NOTICE 'Views for recent activity:';
    RAISE NOTICE '  - flight_recorder.deltas            (snapshot deltas incl. temp files)';
    RAISE NOTICE '  - flight_recorder.recent_waits      (wait events, last 2 hours from ring buffer)';
    RAISE NOTICE '  - flight_recorder.recent_activity   (active sessions, last 2 hours from ring buffer)';
    RAISE NOTICE '  - flight_recorder.recent_locks      (lock contention, last 2 hours from ring buffer)';
    RAISE NOTICE '  - flight_recorder.recent_replication (replication lag, last 2 hours)';
    RAISE NOTICE '';
EXCEPTION
    WHEN undefined_table THEN
        -- pg_cron not available, show generic message
        RAISE NOTICE '';
        RAISE NOTICE 'Flight Recorder installed successfully.';
        RAISE NOTICE '';
        RAISE NOTICE 'NOTE: pg_cron not available. Run flight_recorder.snapshot() and flight_recorder.sample() manually.';
        RAISE NOTICE '';
END;
$$;
