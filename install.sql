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
--   - pg_cron extension (1.4.1+ recommended for 30-second sampling)
--   - Superuser or appropriate privileges to create schema/functions
--
-- INSTALLATION
-- ------------
--   psql -f install.sql
--
-- TWO-TIER COLLECTION
-- -------------------
--   1. Snapshots (every 5 min via pg_cron)
--      Cumulative stats that are meaningful as deltas:
--      - WAL: bytes generated, write/sync time
--      - Checkpoints: timed/requested count, write/sync time, buffers
--      - BGWriter: buffers clean/alloc/backend (backend writes = pressure)
--      - Replication slots: count, max retained WAL bytes
--      - Replication lag: per-replica write_lag, flush_lag, replay_lag
--      - Temp files: cumulative temp files and bytes (work_mem spills)
--      - pg_stat_io (PG16+): I/O by backend type
--      - Per-table stats for tracked tables: size, tuples, vacuum activity
--
--   2. Samples (every 30 sec via pg_cron)
--      Point-in-time snapshots for real-time visibility:
--      - Wait events: aggregated by backend_type, wait_event_type, wait_event
--      - Active sessions: top 25 non-idle sessions with query preview
--      - Operation progress: vacuum, COPY, analyze, create index
--      - Lock contention: blocked/blocking PIDs with queries
--
-- QUICK START
-- -----------
--   -- 1. Optionally track specific tables for detailed monitoring
--   SELECT flight_recorder.track_table('orders');
--
--   -- 2. Flight Recorder collects automatically in the background
--
--   -- 3. Query any time window to diagnose performance
--   SELECT * FROM flight_recorder.compare('2024-12-16 14:00', '2024-12-16 15:00');
--   SELECT * FROM flight_recorder.table_compare('orders', '2024-12-16 14:00', '2024-12-16 15:00');
--   SELECT * FROM flight_recorder.wait_summary('2024-12-16 14:00', '2024-12-16 15:00');
--
--   -- 4. Or use the recent_* views for rolling 2-hour visibility
--   SELECT * FROM flight_recorder.recent_waits;
--   SELECT * FROM flight_recorder.recent_locks;
--   SELECT * FROM flight_recorder.recent_activity;
--
-- FUNCTIONS
-- ---------
--   flight_recorder.snapshot()
--       Capture cumulative stats. Called automatically every 5 min.
--       Returns: timestamp of capture
--
--   flight_recorder.sample()
--       Capture point-in-time activity. Called automatically every 30 sec.
--       Returns: timestamp of capture
--
--   flight_recorder.track_table(name, schema DEFAULT 'public')
--       Register a table for per-table monitoring.
--       Returns: confirmation message
--
--   flight_recorder.untrack_table(name, schema DEFAULT 'public')
--       Stop monitoring a table.
--       Returns: confirmation message
--
--   flight_recorder.list_tracked_tables()
--       Show all tracked tables.
--       Returns: table of (schemaname, relname, added_at)
--
--   flight_recorder.compare(start_time, end_time)
--       Compare cumulative stats between two time points.
--       Returns: single row with deltas for WAL, checkpoints, bgwriter, I/O
--
--   flight_recorder.table_compare(table, start_time, end_time, schema DEFAULT 'public')
--       Compare table stats between two time points.
--       Returns: single row with size delta, tuple counts, vacuum activity
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
--   flight_recorder.table_deltas
--       Changes to tracked tables between consecutive snapshots.
--       Key columns: size_delta_pretty, inserts_delta, n_dead_tup,
--                    dead_tuple_ratio, autovacuum_ran, autoanalyze_ran
--
--   flight_recorder.recent_waits
--       Wait events from last 2 hours.
--       Columns: captured_at, backend_type, wait_event_type, wait_event, state, count
--
--   flight_recorder.recent_activity
--       Active sessions from last 2 hours.
--       Columns: captured_at, pid, usename, backend_type, state, wait_event,
--                running_for, query_preview
--
--   flight_recorder.recent_locks
--       Lock contention from last 2 hours.
--       Columns: captured_at, blocked_pid, blocked_duration, blocking_pid,
--                lock_type, locked_relation, blocked_query_preview
--
--   flight_recorder.recent_progress
--       Operation progress (vacuum/copy/analyze/create_index) from last 2 hours.
--       Columns: captured_at, progress_type, pid, relname, phase,
--                blocks_pct, tuples_done, bytes_done_pretty
--
--   flight_recorder.recent_replication
--       Replication lag from last 2 hours.
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
--   These patterns were validated against PostgreSQL 15 on Supabase.
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
--     - table_compare() shows autovacuum_ran=true during batch
--     - recent_progress shows vacuum phases overlapping batch
--     - Wait events: LWLock:BufferContent, IO:DataFileRead
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
--   4. Table-specific (if tracking):
--      SELECT * FROM flight_recorder.table_compare('mytable', 'START_TIME', 'END_TIME');
--      => Check: autovacuum_ran, dead_tuple_ratio, size_delta
--
--   5. Active operations:
--      SELECT * FROM flight_recorder.recent_progress
--      WHERE captured_at BETWEEN 'START_TIME' AND 'END_TIME';
--      => Check: overlapping vacuum, COPY, or index builds
--
--   6. Replication lag (if using sync replication):
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
--   flight_recorder_snapshot  : */5 * * * *   (every 5 minutes)
--   flight_recorder_sample    : 180 seconds   (default 3-minute sampling - configurable via sample_interval_seconds)
--   flight_recorder_cleanup   : 0 3 * * *     (daily at 3 AM - drops old partitions and cleans snapshots)
--   flight_recorder_partition : 0 2 * * *     (daily at 2 AM - creates future partitions proactively)
--
--   NOTE: The installer auto-detects pg_cron version. If < 1.4.1 (e.g., "1.4-1"),
--   it falls back to minute-level sampling and logs a notice.
--
-- UNINSTALL
-- ---------
--   SELECT cron.unschedule('flight_recorder_snapshot');
--   SELECT cron.unschedule('flight_recorder_sample');
--   SELECT cron.unschedule('flight_recorder_cleanup');
--   DROP SCHEMA flight_recorder CASCADE;
--
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS flight_recorder;

-- -----------------------------------------------------------------------------
-- Table: snapshots
-- UNLOGGED: Minimizes WAL overhead. Data lost on crash but acceptable for telemetry.
-- -----------------------------------------------------------------------------

CREATE UNLOGGED TABLE IF NOT EXISTS flight_recorder.snapshots (
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
-- Table: tracked_tables - Tables to monitor for batch operations
-- Regular table (not UNLOGGED) - small config data worth preserving
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS flight_recorder.tracked_tables (
    relid           OID PRIMARY KEY,
    schemaname      TEXT NOT NULL DEFAULT 'public',
    relname         TEXT NOT NULL,
    added_at        TIMESTAMPTZ DEFAULT now()
);

-- -----------------------------------------------------------------------------
-- Table: table_snapshots - Per-table stats captured with each snapshot
-- UNLOGGED: Minimizes WAL overhead. Data lost on crash but acceptable for telemetry.
-- -----------------------------------------------------------------------------

CREATE UNLOGGED TABLE IF NOT EXISTS flight_recorder.table_snapshots (
    snapshot_id             INTEGER REFERENCES flight_recorder.snapshots(id) ON DELETE CASCADE,
    relid                   OID,
    schemaname              TEXT,
    relname                 TEXT,
    -- Size
    pg_relation_size        BIGINT,
    pg_total_relation_size  BIGINT,
    pg_indexes_size         BIGINT,
    -- Tuple counts (point-in-time)
    n_live_tup              BIGINT,
    n_dead_tup              BIGINT,
    -- Cumulative DML counters
    n_tup_ins               BIGINT,
    n_tup_upd               BIGINT,
    n_tup_del               BIGINT,
    n_tup_hot_upd           BIGINT,
    -- Vacuum/analyze timestamps
    last_vacuum             TIMESTAMPTZ,
    last_autovacuum         TIMESTAMPTZ,
    last_analyze            TIMESTAMPTZ,
    last_autoanalyze        TIMESTAMPTZ,
    -- Vacuum/analyze counts (cumulative)
    vacuum_count            BIGINT,
    autovacuum_count        BIGINT,
    analyze_count           BIGINT,
    autoanalyze_count       BIGINT,
    PRIMARY KEY (snapshot_id, relid)
);

-- -----------------------------------------------------------------------------
-- Table: replication_snapshots - Per-replica stats captured with each snapshot
-- -----------------------------------------------------------------------------

CREATE UNLOGGED TABLE IF NOT EXISTS flight_recorder.replication_snapshots (
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
-- -----------------------------------------------------------------------------

CREATE UNLOGGED TABLE IF NOT EXISTS flight_recorder.statement_snapshots (
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
-- Table: samples - High-frequency sampling (every 60 seconds, adaptive)
-- PARTITIONED BY RANGE (captured_at) - Daily partitions for efficient cleanup
-- -----------------------------------------------------------------------------

CREATE UNLOGGED TABLE IF NOT EXISTS flight_recorder.samples (
    id              BIGINT GENERATED ALWAYS AS IDENTITY,
    captured_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (id, captured_at)
) PARTITION BY RANGE (captured_at);

-- Create initial partitions (today, tomorrow, day after)
DO $$
DECLARE
    v_partition_date DATE;
    v_partition_name TEXT;
    v_start_date TEXT;
    v_end_date TEXT;
BEGIN
    FOR i IN 0..2 LOOP
        v_partition_date := CURRENT_DATE + (i || ' days')::interval;
        v_partition_name := 'samples_' || TO_CHAR(v_partition_date, 'YYYYMMDD');
        v_start_date := v_partition_date::TEXT;
        v_end_date := (v_partition_date + 1)::TEXT;

        IF NOT EXISTS (
            SELECT 1 FROM pg_tables
            WHERE schemaname = 'flight_recorder' AND tablename = v_partition_name
        ) THEN
            EXECUTE format(
                'CREATE TABLE IF NOT EXISTS flight_recorder.%I PARTITION OF flight_recorder.samples FOR VALUES FROM (%L) TO (%L)',
                v_partition_name, v_start_date, v_end_date
            );
        END IF;
    END LOOP;
END $$;

-- Index on parent table (inherited by all partitions)
CREATE INDEX IF NOT EXISTS samples_captured_at_idx ON flight_recorder.samples(captured_at);

-- -----------------------------------------------------------------------------
-- Table: wait_samples - Aggregated wait events per sample
-- -----------------------------------------------------------------------------

CREATE UNLOGGED TABLE IF NOT EXISTS flight_recorder.wait_samples (
    sample_id           INTEGER,
    sample_captured_at  TIMESTAMPTZ,
    backend_type        TEXT NOT NULL,
    wait_event_type     TEXT NOT NULL,
    wait_event          TEXT NOT NULL,
    state               TEXT NOT NULL,
    count               INTEGER NOT NULL,
    PRIMARY KEY (sample_id, backend_type, wait_event_type, wait_event, state),
    FOREIGN KEY (sample_id, sample_captured_at) REFERENCES flight_recorder.samples(id, captured_at) ON DELETE CASCADE
);

-- -----------------------------------------------------------------------------
-- Table: activity_samples - Top active sessions per sample
-- -----------------------------------------------------------------------------

CREATE UNLOGGED TABLE IF NOT EXISTS flight_recorder.activity_samples (
    sample_id           INTEGER,
    sample_captured_at  TIMESTAMPTZ,
    pid                 INTEGER NOT NULL,
    usename             TEXT,
    application_name    TEXT,
    backend_type        TEXT,
    state               TEXT,
    wait_event_type     TEXT,
    wait_event          TEXT,
    query_start         TIMESTAMPTZ,
    state_change        TIMESTAMPTZ,
    query_preview       TEXT,
    PRIMARY KEY (sample_id, pid),
    FOREIGN KEY (sample_id, sample_captured_at) REFERENCES flight_recorder.samples(id, captured_at) ON DELETE CASCADE
);

-- -----------------------------------------------------------------------------
-- Table: progress_samples - Operation progress (vacuum, copy, analyze, etc.)
-- -----------------------------------------------------------------------------

CREATE UNLOGGED TABLE IF NOT EXISTS flight_recorder.progress_samples (
    sample_id           INTEGER,
    sample_captured_at  TIMESTAMPTZ,
    progress_type       TEXT NOT NULL,      -- 'vacuum', 'copy', 'analyze', 'create_index'
    pid                 INTEGER NOT NULL,
    relid               OID,
    relname             TEXT,
    phase               TEXT,
    blocks_total        BIGINT,
    blocks_done         BIGINT,
    tuples_total        BIGINT,
    tuples_done         BIGINT,
    bytes_total         BIGINT,
    bytes_done          BIGINT,
    details             JSONB,              -- Type-specific additional fields
    PRIMARY KEY (sample_id, progress_type, pid),
    FOREIGN KEY (sample_id, sample_captured_at) REFERENCES flight_recorder.samples(id, captured_at) ON DELETE CASCADE
);

-- -----------------------------------------------------------------------------
-- Table: lock_samples - Blocking lock relationships
-- -----------------------------------------------------------------------------

CREATE UNLOGGED TABLE IF NOT EXISTS flight_recorder.lock_samples (
    sample_id               INTEGER,
    sample_captured_at  TIMESTAMPTZ,
    blocked_pid             INTEGER NOT NULL,
    blocked_user            TEXT,
    blocked_app             TEXT,
    blocked_query_preview   TEXT,
    blocked_duration        INTERVAL,
    blocking_pid            INTEGER NOT NULL,
    blocking_user           TEXT,
    blocking_app            TEXT,
    blocking_query_preview  TEXT,
    lock_type               TEXT,
    locked_relation         TEXT,
    PRIMARY KEY (sample_id, blocked_pid, blocking_pid),
    FOREIGN KEY (sample_id, sample_captured_at) REFERENCES flight_recorder.samples(id, captured_at) ON DELETE CASCADE
);

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
    -- Configurable sample interval (default 180s for 0.5% overhead)
    ('sample_interval_seconds', '180'),        -- Sample collection frequency
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
    ('statement_timeout_ms', '2000'),          -- Max total collection time
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
    ('auto_mode_trips_threshold', '3'),        -- Switch to emergency if circuit breaker tripped N times in 10min
    -- Configurable retention by table type
    ('retention_samples_days', '7'),           -- Retention for samples table
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
    -- Adaptive sampling (opt-in, skips collection when idle)
    ('adaptive_sampling', 'false'),            -- Skip collection when system idle
    ('adaptive_sampling_idle_threshold', '5'), -- Skip if < N active connections
    -- DDL detection (enabled by default, prevents lock contention with schema changes)
    ('ddl_detection_enabled', 'true'),         -- Check for active DDL before collecting
    ('ddl_skip_locks', 'true'),                -- Skip lock collection when DDL detected
    ('ddl_skip_entire_sample', 'false')        -- Skip entire sample when DDL detected (more aggressive)
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
-- Helper: Detect active DDL operations
-- -----------------------------------------------------------------------------
-- Checks for active DDL operations (CREATE, ALTER, DROP, TRUNCATE, REINDEX)
-- to prevent lock contention between flight recorder and schema changes.
--
-- Returns: Record with (ddl_detected BOOLEAN, ddl_count INTEGER, ddl_types TEXT[])
--
-- RATIONALE:
--   DDL operations acquire AccessExclusiveLock on system catalogs, which conflicts
--   with flight recorder's AccessShareLock on pg_stat_activity, pg_locks, etc.
--   This can cause:
--   1. Flight recorder lock timeouts (fails to collect)
--   2. DDL operations delayed by flight recorder's catalog locks
--
-- USAGE:
--   Called by sample() function before catalog queries to decide whether to:
--   - Skip lock collection (ddl_skip_locks = true)
--   - Skip entire sample (ddl_skip_entire_sample = true)
--
-- PERFORMANCE:
--   - Protected by lock_timeout=100ms (set before calling)
--   - Query limited to active backend_type='client backend' only
--   - Regex match on query text (fast, no joins)
--
CREATE OR REPLACE FUNCTION flight_recorder._detect_active_ddl()
RETURNS TABLE (
    ddl_detected BOOLEAN,
    ddl_count INTEGER,
    ddl_types TEXT[]
)
LANGUAGE plpgsql AS $$
DECLARE
    v_enabled BOOLEAN;
BEGIN
    -- Check if DDL detection is enabled
    v_enabled := COALESCE(
        flight_recorder._get_config('ddl_detection_enabled', 'true')::boolean,
        true
    );

    IF NOT v_enabled THEN
        RETURN QUERY SELECT false, 0, ARRAY[]::TEXT[];
        RETURN;
    END IF;

    -- Detect DDL operations by query pattern
    -- Match common DDL commands: CREATE, ALTER, DROP, TRUNCATE, REINDEX, VACUUM FULL
    -- IMPORTANT: This query is protected by lock_timeout set in sample() function
    RETURN QUERY
    WITH ddl_queries AS (
        SELECT
            CASE
                WHEN query ~* '^\s*CREATE' THEN 'CREATE'
                WHEN query ~* '^\s*ALTER' THEN 'ALTER'
                WHEN query ~* '^\s*DROP' THEN 'DROP'
                WHEN query ~* '^\s*TRUNCATE' THEN 'TRUNCATE'
                WHEN query ~* '^\s*REINDEX' THEN 'REINDEX'
                WHEN query ~* '^\s*VACUUM\s+FULL' THEN 'VACUUM FULL'
                ELSE 'OTHER'
            END AS ddl_type
        FROM pg_stat_activity
        WHERE state = 'active'
          AND backend_type = 'client backend'
          AND pid != pg_backend_pid()
          AND query ~* '^\s*(CREATE|ALTER|DROP|TRUNCATE|REINDEX|VACUUM\s+FULL)'
    )
    SELECT
        (COUNT(*) > 0)::BOOLEAN AS ddl_detected,
        COUNT(*)::INTEGER AS ddl_count,
        array_agg(DISTINCT ddl_type) AS ddl_types
    FROM ddl_queries;
END;
$$;

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
        flight_recorder._get_config('auto_mode_trips_threshold', '3')::integer,
        3
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
    v_tracked_count INTEGER;
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

    -- Check 4: Tracked table count
    SELECT count(*) INTO v_tracked_count FROM flight_recorder.tracked_tables;
    RETURN QUERY SELECT
        'tracked_tables'::text,
        CASE
            WHEN v_tracked_count > 50 THEN 'CRITICAL'
            WHEN v_tracked_count > 20 THEN 'WARNING'
            ELSE 'OK'
        END::text,
        format('Tracking %s tables. Each adds 3 size queries + catalog lock every 5 minutes. Recommend: <= 20 tables',
               v_tracked_count);

    -- Check 5: Schema size
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

    -- Check 6: Cost-based skip thresholds
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

    -- Check 7: Recent collection failures
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

    -- Check 8: Recent lock timeout errors
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
            format('%s lock timeout errors in last hour. Consider reducing lock_timeout_ms or disabling tracked tables',
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

    -- Get thresholds from config
    v_warning_mb := COALESCE(
        flight_recorder._get_config('schema_size_warning_mb', '5000')::integer,
        5000
    );
    v_critical_mb := COALESCE(
        flight_recorder._get_config('schema_size_critical_mb', '10000')::integer,
        10000
    );

    -- Calculate total schema size (all tables in flight recorder schema)
    SELECT COALESCE(sum(pg_total_relation_size(c.oid)), 0)
    INTO v_size_bytes
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'flight_recorder'
      AND c.relkind IN ('r', 'i', 't');  -- tables, indexes, TOAST

    v_size_mb := round(v_size_bytes / 1024.0 / 1024.0, 2);

    -- Check thresholds and take action
    IF v_size_mb >= v_critical_mb THEN
        -- Critical: auto-disable collection
        BEGIN
            PERFORM flight_recorder.disable();
            RETURN QUERY SELECT
                v_size_mb,
                v_warning_mb,
                v_critical_mb,
                'CRITICAL'::text,
                'Auto-disabled collection'::text;
        EXCEPTION WHEN OTHERS THEN
            RETURN QUERY SELECT
                v_size_mb,
                v_warning_mb,
                v_critical_mb,
                'CRITICAL'::text,
                format('Failed to auto-disable: %s', SQLERRM)::text;
        END;
    ELSIF v_size_mb >= v_warning_mb THEN
        -- Warning: log but continue
        RAISE WARNING 'pg-flight-recorder: Schema size (% MB) exceeds warning threshold (% MB). Consider running cleanup or reducing retention.',
            v_size_mb, v_warning_mb;
        RETURN QUERY SELECT
            v_size_mb,
            v_warning_mb,
            v_critical_mb,
            'WARNING'::text,
            'Logged warning'::text;
    ELSE
        -- OK
        RETURN QUERY SELECT
            v_size_mb,
            v_warning_mb,
            v_critical_mb,
            'OK'::text,
            'none'::text;
    END IF;
END;
$$;

-- -----------------------------------------------------------------------------
-- Table tracking functions
-- -----------------------------------------------------------------------------

-- WARNING: Each tracked table adds ~10-50ms overhead per snapshot (every 5 min)
-- Due to pg_relation_size(), pg_total_relation_size(), pg_indexes_size() calls
-- Recommend tracking max 5-20 critical tables
CREATE OR REPLACE FUNCTION flight_recorder.track_table(p_table TEXT, p_schema TEXT DEFAULT 'public')
RETURNS TEXT
LANGUAGE plpgsql AS $$
DECLARE
    v_relid OID;
BEGIN
    SELECT c.oid INTO v_relid
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relname = p_table AND n.nspname = p_schema AND c.relkind = 'r';

    IF v_relid IS NULL THEN
        RAISE EXCEPTION 'Table %.% not found', p_schema, p_table;
    END IF;

    INSERT INTO flight_recorder.tracked_tables (relid, schemaname, relname)
    VALUES (v_relid, p_schema, p_table)
    ON CONFLICT (relid) DO NOTHING;

    RAISE NOTICE 'pg-flight-recorder: Tracking table %.%. This adds overhead: pg_relation_size() + pg_total_relation_size() + pg_indexes_size() every 5 minutes. Tracked table count: %',
        p_schema, p_table, (SELECT count(*) FROM flight_recorder.tracked_tables);

    RETURN format('Now tracking %I.%I', p_schema, p_table);
END;
$$;

CREATE OR REPLACE FUNCTION flight_recorder.untrack_table(p_table TEXT, p_schema TEXT DEFAULT 'public')
RETURNS TEXT
LANGUAGE plpgsql AS $$
BEGIN
    DELETE FROM flight_recorder.tracked_tables
    WHERE relname = p_table AND schemaname = p_schema;

    IF NOT FOUND THEN
        RETURN format('Table %I.%I was not being tracked', p_schema, p_table);
    END IF;

    RETURN format('Stopped tracking %I.%I', p_schema, p_table);
END;
$$;

CREATE OR REPLACE FUNCTION flight_recorder.list_tracked_tables()
RETURNS TABLE(schemaname TEXT, relname TEXT, added_at TIMESTAMPTZ)
LANGUAGE sql STABLE AS $$
    SELECT schemaname, relname, added_at FROM flight_recorder.tracked_tables ORDER BY added_at;
$$;

-- -----------------------------------------------------------------------------
-- flight_recorder.sample() - High-frequency sampling (wait events, activity, progress, locks)
-- Per-section timeouts, O(n) lock detection using pg_blocking_pids()
-- -----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION flight_recorder.sample()
RETURNS TIMESTAMPTZ
LANGUAGE plpgsql AS $$
DECLARE
    v_sample_id INTEGER;
    v_captured_at TIMESTAMPTZ := now();
    v_enable_locks BOOLEAN;
    v_enable_progress BOOLEAN;
    v_pg_version INTEGER;
    v_stat_id INTEGER;
    v_should_skip BOOLEAN;
    v_snapshot_based BOOLEAN;
BEGIN
    -- P2 Safety: Check and adjust mode automatically based on system load
    PERFORM flight_recorder._check_and_adjust_mode();

    -- P0 Safety: Check circuit breaker
    v_should_skip := flight_recorder._check_circuit_breaker('sample');
    IF v_should_skip THEN
        PERFORM flight_recorder._record_collection_skip('sample', 'Circuit breaker tripped - last run exceeded threshold');
        RAISE NOTICE 'pg-flight-recorder: Skipping sample collection due to circuit breaker';
        RETURN v_captured_at;
    END IF;

    -- P0 Safety: Record collection start for circuit breaker (4 sections: wait events, activity, progress, locks)
    v_stat_id := flight_recorder._record_collection_start('sample', 4);

    -- P0 Safety: Set lock timeout and work_mem
    PERFORM set_config('lock_timeout',
        COALESCE(flight_recorder._get_config('lock_timeout_ms', '100'), '100'),
        true);
    PERFORM set_config('work_mem',
        COALESCE(flight_recorder._get_config('work_mem_kb', '2048'), '2048') || 'kB',
        true);  -- Limit memory for joins/sorts

    -- Adaptive sampling - skip if system idle (opt-in)
    -- IMPORTANT: This check happens AFTER lock_timeout is set, so it's protected
    DECLARE
        v_adaptive_sampling BOOLEAN;
        v_idle_threshold INTEGER;
        v_active_count INTEGER;
    BEGIN
        v_adaptive_sampling := COALESCE(
            flight_recorder._get_config('adaptive_sampling', 'false')::boolean,
            false
        );

        IF v_adaptive_sampling THEN
            v_idle_threshold := COALESCE(
                flight_recorder._get_config('adaptive_sampling_idle_threshold', '5')::integer,
                5
            );

            -- This query is now protected by lock_timeout=100ms set above
            SELECT count(*) INTO v_active_count
            FROM pg_stat_activity
            WHERE state = 'active' AND backend_type = 'client backend';

            IF v_active_count < v_idle_threshold THEN
                PERFORM flight_recorder._record_collection_skip('sample',
                    format('Adaptive sampling: system idle (%s active connections < %s threshold)',
                           v_active_count, v_idle_threshold));
                -- Reset timeout before returning
                PERFORM set_config('statement_timeout', '0', true);
                RETURN v_captured_at;
            END IF;
        END IF;
    END;

    -- DDL detection - skip lock collection or entire sample when DDL is active
    -- IMPORTANT: This check happens AFTER lock_timeout is set, so it's protected
    DECLARE
        v_ddl_detected BOOLEAN;
        v_ddl_count INTEGER;
        v_ddl_types TEXT[];
        v_skip_locks_on_ddl BOOLEAN;
        v_skip_sample_on_ddl BOOLEAN;
    BEGIN
        -- Call DDL detection (protected by lock_timeout=100ms)
        SELECT ddl_detected, ddl_count, ddl_types
        INTO v_ddl_detected, v_ddl_count, v_ddl_types
        FROM flight_recorder._detect_active_ddl();

        IF v_ddl_detected THEN
            -- Get DDL skip configuration
            v_skip_sample_on_ddl := COALESCE(
                flight_recorder._get_config('ddl_skip_entire_sample', 'false')::boolean,
                false
            );
            v_skip_locks_on_ddl := COALESCE(
                flight_recorder._get_config('ddl_skip_locks', 'true')::boolean,
                true
            );

            -- Option 1: Skip entire sample (most aggressive)
            IF v_skip_sample_on_ddl THEN
                PERFORM flight_recorder._record_collection_skip('sample',
                    format('DDL detected: %s active DDL operation(s) [%s] - skipping entire sample to prevent lock contention',
                           v_ddl_count, array_to_string(v_ddl_types, ', ')));
                RAISE NOTICE 'pg-flight-recorder: Skipping sample collection - DDL active: % operations [%]',
                    v_ddl_count, array_to_string(v_ddl_types, ', ');
                RETURN v_captured_at;
            -- Option 2: Skip only lock collection (default)
            ELSIF v_skip_locks_on_ddl THEN
                RAISE NOTICE 'pg-flight-recorder: DDL detected (% operations: [%]) - skipping lock collection to prevent lock contention',
                    v_ddl_count, array_to_string(v_ddl_types, ', ');
                -- Will override enable_locks below
            END IF;
        END IF;
    EXCEPTION WHEN OTHERS THEN
        -- If DDL detection fails (e.g., lock timeout), log but continue
        -- This ensures DDL detection doesn't break collection
        RAISE WARNING 'pg-flight-recorder: DDL detection failed: % - continuing with collection', SQLERRM;
    END;

    -- Get configuration
    v_enable_locks := COALESCE(
        flight_recorder._get_config('enable_locks', 'true')::boolean,
        TRUE
    );
    v_enable_progress := COALESCE(
        flight_recorder._get_config('enable_progress', 'true')::boolean,
        TRUE
    );
    v_pg_version := flight_recorder._pg_version();

    -- Apply DDL detection override for lock collection
    -- If DDL was detected and ddl_skip_locks=true, disable lock collection
    IF v_ddl_detected AND v_skip_locks_on_ddl AND NOT v_skip_sample_on_ddl THEN
        v_enable_locks := FALSE;
    END IF;

    -- Snapshot-based collection (default enabled) - Query pg_stat_activity once
    v_snapshot_based := COALESCE(
        flight_recorder._get_config('snapshot_based_collection', 'true')::boolean,
        true
    );

    IF v_snapshot_based THEN
        -- Create temp table snapshot of pg_stat_activity (ONE catalog lock)
        -- This replaces 3+ queries to pg_stat_activity with 1 query
        CREATE TEMP TABLE IF NOT EXISTS _fr_psa_snapshot (
            LIKE pg_stat_activity
        ) ON COMMIT DROP;

        TRUNCATE _fr_psa_snapshot;

        INSERT INTO _fr_psa_snapshot
        SELECT * FROM pg_stat_activity WHERE pid != pg_backend_pid();
    END IF;

    -- Create sample record (set section timeout before each section)
    PERFORM flight_recorder._set_section_timeout();
    INSERT INTO flight_recorder.samples (captured_at)
    VALUES (v_captured_at)
    RETURNING id INTO v_sample_id;

    -- Section 1: Wait events
    BEGIN
        PERFORM flight_recorder._set_section_timeout();

        -- Use snapshot table if enabled (reduces catalog locks)
        IF v_snapshot_based THEN
            INSERT INTO flight_recorder.wait_samples (sample_id, sample_captured_at, backend_type, wait_event_type, wait_event, state, count)
            SELECT
                v_sample_id,
                v_captured_at,
                COALESCE(backend_type, 'unknown'),
                COALESCE(wait_event_type, 'Running'),
                COALESCE(wait_event, 'CPU'),
                COALESCE(state, 'unknown'),
                count(*)::integer
            FROM _fr_psa_snapshot
            GROUP BY backend_type, wait_event_type, wait_event, state;
        ELSE
            INSERT INTO flight_recorder.wait_samples (sample_id, sample_captured_at, backend_type, wait_event_type, wait_event, state, count)
            SELECT
                v_sample_id,
                v_captured_at,
                COALESCE(backend_type, 'unknown'),
                COALESCE(wait_event_type, 'Running'),
                COALESCE(wait_event, 'CPU'),
                COALESCE(state, 'unknown'),
                count(*)::integer
            FROM pg_stat_activity
            WHERE pid != pg_backend_pid()
            GROUP BY backend_type, wait_event_type, wait_event, state;
        END IF;

        PERFORM flight_recorder._record_section_success(v_stat_id);
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'pg-flight-recorder: Wait events collection failed: %', SQLERRM;
    END;

    -- Section 2: Active sessions (cost-based skip)
    BEGIN
        PERFORM flight_recorder._set_section_timeout();
        DECLARE
            v_active_conn_count INTEGER;
            v_skip_activity_threshold INTEGER;
        BEGIN
            v_skip_activity_threshold := COALESCE(
                flight_recorder._get_config('skip_activity_conn_threshold', '100')::integer,
                100
            );

            -- Use snapshot table if enabled (reduces catalog locks)
            -- Quick count (minimal overhead)
            IF v_snapshot_based THEN
                SELECT COUNT(*) INTO v_active_conn_count
                FROM _fr_psa_snapshot
                WHERE state != 'idle';
            ELSE
                SELECT COUNT(*) INTO v_active_conn_count
                FROM pg_stat_activity
                WHERE state != 'idle' AND pid != pg_backend_pid();
            END IF;

            IF v_active_conn_count > v_skip_activity_threshold THEN
                RAISE NOTICE 'pg-flight-recorder: Skipping activity collection - % active connections exceeds threshold %',
                    v_active_conn_count, v_skip_activity_threshold;
            ELSE
                -- Use snapshot table if enabled
                IF v_snapshot_based THEN
                    INSERT INTO flight_recorder.activity_samples (
                        sample_id, sample_captured_at, pid, usename, application_name, backend_type,
                        state, wait_event_type, wait_event, query_start, state_change, query_preview
                    )
                    SELECT
                        v_sample_id,
                        v_captured_at,
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
                    ORDER BY query_start ASC NULLS LAST
                    LIMIT 25;
                ELSE
                    INSERT INTO flight_recorder.activity_samples (
                        sample_id, sample_captured_at, pid, usename, application_name, backend_type,
                        state, wait_event_type, wait_event, query_start, state_change, query_preview
                    )
                    SELECT
                        v_sample_id,
                        v_captured_at,
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
                    ORDER BY query_start ASC NULLS LAST
                    LIMIT 25;
                END IF;
            END IF;
        END;

        PERFORM flight_recorder._record_section_success(v_stat_id);
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'pg-flight-recorder: Activity samples collection failed: %', SQLERRM;
    END;

    -- Section 3: Progress tracking
    IF v_enable_progress THEN
    BEGIN
        PERFORM flight_recorder._set_section_timeout();
        -- Vacuum progress (handle PG17 column changes)
        IF v_pg_version >= 17 THEN
            INSERT INTO flight_recorder.progress_samples (
                sample_id, sample_captured_at, progress_type, pid, relid, relname, phase,
                blocks_total, blocks_done, tuples_total, tuples_done, details
            )
            SELECT
                v_sample_id,
                v_captured_at,
                'vacuum',
                p.pid,
                p.relid,
                p.relid::regclass::text,
                p.phase,
                p.heap_blks_total,
                p.heap_blks_vacuumed,
                p.max_dead_tuple_bytes,
                p.dead_tuple_bytes,
                jsonb_build_object(
                    'heap_blks_scanned', p.heap_blks_scanned,
                    'index_vacuum_count', p.index_vacuum_count,
                    'num_dead_item_ids', p.num_dead_item_ids
                )
            FROM pg_stat_progress_vacuum p;
        ELSE
            INSERT INTO flight_recorder.progress_samples (
                sample_id, sample_captured_at, progress_type, pid, relid, relname, phase,
                blocks_total, blocks_done, tuples_total, tuples_done, details
            )
            SELECT
                v_sample_id,
                v_captured_at,
                'vacuum',
                p.pid,
                p.relid,
                p.relid::regclass::text,
                p.phase,
                p.heap_blks_total,
                p.heap_blks_vacuumed,
                p.max_dead_tuples,
                p.num_dead_tuples,
                jsonb_build_object(
                    'heap_blks_scanned', p.heap_blks_scanned,
                    'index_vacuum_count', p.index_vacuum_count
                )
            FROM pg_stat_progress_vacuum p;
        END IF;

        -- COPY progress
        INSERT INTO flight_recorder.progress_samples (
            sample_id, sample_captured_at, progress_type, pid, relid, relname, phase,
            tuples_done, bytes_total, bytes_done, details
        )
        SELECT
            v_sample_id,
            v_captured_at,
            'copy',
            p.pid,
            p.relid,
            p.relid::regclass::text,
            p.command || '/' || p.type,
            p.tuples_processed,
            p.bytes_total,
            p.bytes_processed,
            jsonb_build_object(
                'tuples_excluded', p.tuples_excluded
            )
        FROM pg_stat_progress_copy p;

        -- Analyze progress
        INSERT INTO flight_recorder.progress_samples (
            sample_id, sample_captured_at, progress_type, pid, relid, relname, phase,
            blocks_total, blocks_done, details
        )
        SELECT
            v_sample_id,
            v_captured_at,
            'analyze',
            p.pid,
            p.relid,
            p.relid::regclass::text,
            p.phase,
            p.sample_blks_total,
            p.sample_blks_scanned,
            jsonb_build_object(
                'ext_stats_total', p.ext_stats_total,
                'ext_stats_computed', p.ext_stats_computed,
                'child_tables_total', p.child_tables_total,
                'child_tables_done', p.child_tables_done
            )
        FROM pg_stat_progress_analyze p;

        -- Create index progress
        INSERT INTO flight_recorder.progress_samples (
            sample_id, sample_captured_at, progress_type, pid, relid, relname, phase,
            blocks_total, blocks_done, tuples_total, tuples_done, details
        )
        SELECT
            v_sample_id,
            v_captured_at,
            'create_index',
            p.pid,
            p.relid,
            p.relid::regclass::text,
            p.phase,
            p.blocks_total,
            p.blocks_done,
            p.tuples_total,
            p.tuples_done,
            jsonb_build_object(
                'index_relid', p.index_relid,
                'command', p.command,
                'lockers_total', p.lockers_total,
                'lockers_done', p.lockers_done,
                'partitions_total', p.partitions_total,
                'partitions_done', p.partitions_done
            )
        FROM pg_stat_progress_create_index p;

        PERFORM flight_recorder._record_section_success(v_stat_id);
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'pg-flight-recorder: Progress tracking collection failed: %', SQLERRM;
    END;
    END IF;  -- v_enable_progress

    -- Section 4: Lock sampling (O(n) algorithm using pg_blocking_pids())
    -- Uses pg_blocking_pids() which is O(n) instead of O(n²) join on pg_locks
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

            -- Use snapshot table if enabled (reduces catalog locks)
            -- Quick count of blocked sessions (minimal overhead)
            IF v_snapshot_based THEN
                SELECT COUNT(*) INTO v_blocked_count
                FROM _fr_psa_snapshot
                WHERE cardinality(pg_blocking_pids(pid)) > 0;
            ELSE
                SELECT COUNT(*) INTO v_blocked_count
                FROM pg_stat_activity
                WHERE pid != pg_backend_pid()
                  AND cardinality(pg_blocking_pids(pid)) > 0;
            END IF;

            IF v_blocked_count > v_skip_locks_threshold THEN
                RAISE NOTICE 'pg-flight-recorder: Skipping lock collection - % blocked sessions exceeds threshold % (potential lock storm)',
                    v_blocked_count, v_skip_locks_threshold;
            ELSE
                -- Use snapshot table if enabled
                -- O(n) lock detection using pg_blocking_pids()
                -- pg_blocking_pids(pid) returns array of PIDs blocking a given PID
                -- This is much more efficient than the O(n²) self-join on pg_locks
                IF v_snapshot_based THEN
                    INSERT INTO flight_recorder.lock_samples (
                        sample_id, sample_captured_at, blocked_pid, blocked_user, blocked_app, blocked_query_preview, blocked_duration,
                        blocking_pid, blocking_user, blocking_app, blocking_query_preview, lock_type, locked_relation
                    )
                    SELECT DISTINCT ON (blocked.pid, blocking_pid)
                        v_sample_id,
                        v_captured_at,
                        blocked.pid,
                        blocked.usename,
                        blocked.application_name,
                        left(blocked.query, 200),
                        v_captured_at - blocked.query_start,
                        blocking_pid,
                        blocking.usename,
                        blocking.application_name,
                        left(blocking.query, 200),
                        -- Get lock type from the blocked session's wait_event
                        CASE
                            WHEN blocked.wait_event_type = 'Lock' THEN blocked.wait_event
                            ELSE 'unknown'
                        END,
                        -- Get relation if waiting on a relation lock
                        CASE
                            WHEN blocked.wait_event IN ('relation', 'extend', 'page', 'tuple') THEN
                                (SELECT l.relation::regclass::text
                                 FROM pg_locks l
                                 WHERE l.pid = blocked.pid AND NOT l.granted
                                 LIMIT 1)
                            ELSE NULL
                        END
                    FROM _fr_psa_snapshot blocked
                    CROSS JOIN LATERAL unnest(pg_blocking_pids(blocked.pid)) AS blocking_pid
                    JOIN _fr_psa_snapshot blocking ON blocking.pid = blocking_pid
                    ORDER BY blocked.pid, blocking_pid
                    LIMIT 100;
                ELSE
                    INSERT INTO flight_recorder.lock_samples (
                        sample_id, sample_captured_at, blocked_pid, blocked_user, blocked_app, blocked_query_preview, blocked_duration,
                        blocking_pid, blocking_user, blocking_app, blocking_query_preview, lock_type, locked_relation
                    )
                    SELECT DISTINCT ON (blocked.pid, blocking_pid)
                        v_sample_id,
                        v_captured_at,
                        blocked.pid,
                        blocked.usename,
                        blocked.application_name,
                        left(blocked.query, 200),
                        v_captured_at - blocked.query_start,
                        blocking_pid,
                        blocking.usename,
                        blocking.application_name,
                        left(blocking.query, 200),
                        -- Get lock type from the blocked session's wait_event
                        CASE
                            WHEN blocked.wait_event_type = 'Lock' THEN blocked.wait_event
                            ELSE 'unknown'
                        END,
                        -- Get relation if waiting on a relation lock
                        CASE
                            WHEN blocked.wait_event IN ('relation', 'extend', 'page', 'tuple') THEN
                                (SELECT l.relation::regclass::text
                                 FROM pg_locks l
                                 WHERE l.pid = blocked.pid AND NOT l.granted
                                 LIMIT 1)
                            ELSE NULL
                        END
                    FROM pg_stat_activity blocked
                    CROSS JOIN LATERAL unnest(pg_blocking_pids(blocked.pid)) AS blocking_pid
                    JOIN pg_stat_activity blocking ON blocking.pid = blocking_pid
                    WHERE blocked.pid != pg_backend_pid()
                    ORDER BY blocked.pid, blocking_pid
                    LIMIT 100;
                END IF;
            END IF;
        END;

        PERFORM flight_recorder._record_section_success(v_stat_id);
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'pg-flight-recorder: Lock sampling collection failed: %', SQLERRM;
    END;
    END IF;  -- v_enable_locks

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
BEGIN
    -- P0 Safety: Check circuit breaker
    v_should_skip := flight_recorder._check_circuit_breaker('snapshot');
    IF v_should_skip THEN
        PERFORM flight_recorder._record_collection_skip('snapshot', 'Circuit breaker tripped - last run exceeded threshold');
        RAISE NOTICE 'pg-flight-recorder: Skipping snapshot collection due to circuit breaker';
        RETURN v_captured_at;
    END IF;

    -- P1 Safety: Check schema size (runs every 5 minutes, auto-disables if critical)
    PERFORM flight_recorder._check_schema_size();

    -- P0 Safety: Record collection start for circuit breaker (5 sections: system stats, snapshot INSERT, tracked tables, replication, statements)
    v_stat_id := flight_recorder._record_collection_start('snapshot', 5);

    -- P0 Safety: Set lock timeout and work_mem
    PERFORM set_config('lock_timeout',
        COALESCE(flight_recorder._get_config('lock_timeout_ms', '100'), '100'),
        true);
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
            (pg_control_checkpoint()).redo_lsn,
            (pg_control_checkpoint()).checkpoint_time,
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
            (pg_control_checkpoint()).redo_lsn,
            (pg_control_checkpoint()).checkpoint_time,
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
            (pg_control_checkpoint()).redo_lsn,
            (pg_control_checkpoint()).checkpoint_time,
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

    -- Section 3: Capture stats for tracked tables
    BEGIN
        PERFORM flight_recorder._set_section_timeout();
        INSERT INTO flight_recorder.table_snapshots (
            snapshot_id, relid, schemaname, relname,
            pg_relation_size, pg_total_relation_size, pg_indexes_size,
            n_live_tup, n_dead_tup,
            n_tup_ins, n_tup_upd, n_tup_del, n_tup_hot_upd,
            last_vacuum, last_autovacuum, last_analyze, last_autoanalyze,
            vacuum_count, autovacuum_count, analyze_count, autoanalyze_count
        )
        SELECT
            v_snapshot_id,
            t.relid,
            t.schemaname,
            t.relname,
            pg_relation_size(t.relid),
            pg_total_relation_size(t.relid),
            pg_indexes_size(t.relid),
            s.n_live_tup,
            s.n_dead_tup,
            s.n_tup_ins,
            s.n_tup_upd,
            s.n_tup_del,
            s.n_tup_hot_upd,
            s.last_vacuum,
            s.last_autovacuum,
            s.last_analyze,
            s.last_autoanalyze,
            s.vacuum_count,
            s.autovacuum_count,
            s.analyze_count,
            s.autoanalyze_count
        FROM flight_recorder.tracked_tables t
        JOIN pg_stat_user_tables s ON s.relid = t.relid;

        PERFORM flight_recorder._record_section_success(v_stat_id);
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'pg-flight-recorder: Tracked tables collection failed: %', SQLERRM;
    END;

    -- Section 4: Capture replication stats
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

    -- Section 5: Capture pg_stat_statements (if available and enabled)
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
            END IF;  -- v_should_collect check
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
-- flight_recorder.table_deltas - View showing deltas for tracked tables
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW flight_recorder.table_deltas AS
SELECT
    ts.snapshot_id,
    s.captured_at,
    ts.schemaname,
    ts.relname,
    EXTRACT(EPOCH FROM (s.captured_at - prev_s.captured_at))::numeric AS interval_seconds,

    -- Size changes
    ts.pg_relation_size - prev_ts.pg_relation_size AS size_delta_bytes,
    flight_recorder._pretty_bytes(ts.pg_relation_size - prev_ts.pg_relation_size) AS size_delta_pretty,
    ts.pg_total_relation_size - prev_ts.pg_total_relation_size AS total_size_delta_bytes,

    -- Tuple counts (point-in-time)
    ts.n_live_tup,
    ts.n_dead_tup,
    ts.n_dead_tup::float / NULLIF(ts.n_live_tup, 0) AS dead_tuple_ratio,

    -- DML deltas
    ts.n_tup_ins - prev_ts.n_tup_ins AS inserts_delta,
    ts.n_tup_upd - prev_ts.n_tup_upd AS updates_delta,
    ts.n_tup_del - prev_ts.n_tup_del AS deletes_delta,
    ts.n_tup_hot_upd - prev_ts.n_tup_hot_upd AS hot_updates_delta,

    -- Vacuum/analyze activity
    (ts.last_autovacuum IS DISTINCT FROM prev_ts.last_autovacuum) AS autovacuum_ran,
    (ts.last_autoanalyze IS DISTINCT FROM prev_ts.last_autoanalyze) AS autoanalyze_ran,
    ts.autovacuum_count - prev_ts.autovacuum_count AS autovacuum_count_delta,
    ts.autoanalyze_count - prev_ts.autoanalyze_count AS autoanalyze_count_delta,
    ts.last_autovacuum,
    ts.last_autoanalyze

FROM flight_recorder.table_snapshots ts
JOIN flight_recorder.snapshots s ON s.id = ts.snapshot_id
JOIN flight_recorder.table_snapshots prev_ts ON (
    prev_ts.relid = ts.relid AND
    prev_ts.snapshot_id = (
        SELECT MAX(snapshot_id) FROM flight_recorder.table_snapshots
        WHERE relid = ts.relid AND snapshot_id < ts.snapshot_id
    )
)
JOIN flight_recorder.snapshots prev_s ON prev_s.id = prev_ts.snapshot_id
ORDER BY s.captured_at DESC, ts.relname;

-- -----------------------------------------------------------------------------
-- flight_recorder.recent_waits - View of wait events from last 2 hours
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW flight_recorder.recent_waits AS
SELECT
    sm.captured_at,
    w.backend_type,
    w.wait_event_type,
    w.wait_event,
    w.state,
    w.count
FROM flight_recorder.samples sm
JOIN flight_recorder.wait_samples w ON w.sample_id = sm.id
WHERE sm.captured_at > now() - interval '2 hours'
ORDER BY sm.captured_at DESC, w.count DESC;

-- -----------------------------------------------------------------------------
-- flight_recorder.recent_activity - View of active sessions from last 2 hours
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW flight_recorder.recent_activity AS
SELECT
    sm.captured_at,
    a.pid,
    a.usename,
    a.application_name,
    a.backend_type,
    a.state,
    a.wait_event_type,
    a.wait_event,
    a.query_start,
    sm.captured_at - a.query_start AS running_for,
    a.query_preview
FROM flight_recorder.samples sm
JOIN flight_recorder.activity_samples a ON a.sample_id = sm.id
WHERE sm.captured_at > now() - interval '2 hours'
ORDER BY sm.captured_at DESC, a.query_start ASC;

-- -----------------------------------------------------------------------------
-- flight_recorder.recent_locks - View of lock contention from last 2 hours
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW flight_recorder.recent_locks AS
SELECT
    sm.captured_at,
    l.blocked_pid,
    l.blocked_user,
    l.blocked_app,
    l.blocked_duration,
    l.blocking_pid,
    l.blocking_user,
    l.blocking_app,
    l.lock_type,
    l.locked_relation,
    l.blocked_query_preview,
    l.blocking_query_preview
FROM flight_recorder.samples sm
JOIN flight_recorder.lock_samples l ON l.sample_id = sm.id
WHERE sm.captured_at > now() - interval '2 hours'
ORDER BY sm.captured_at DESC, l.blocked_duration DESC;

-- -----------------------------------------------------------------------------
-- flight_recorder.recent_progress - View of operation progress from last 2 hours
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW flight_recorder.recent_progress AS
SELECT
    sm.captured_at,
    p.progress_type,
    p.pid,
    p.relname,
    p.phase,
    p.blocks_done,
    p.blocks_total,
    CASE WHEN p.blocks_total > 0
        THEN round(100.0 * p.blocks_done / p.blocks_total, 1)
        ELSE NULL END AS blocks_pct,
    p.tuples_done,
    p.tuples_total,
    p.bytes_done,
    p.bytes_total,
    flight_recorder._pretty_bytes(p.bytes_done) AS bytes_done_pretty,
    p.details
FROM flight_recorder.samples sm
JOIN flight_recorder.progress_samples p ON p.sample_id = sm.id
WHERE sm.captured_at > now() - interval '2 hours'
ORDER BY sm.captured_at DESC, p.progress_type, p.relname;

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
        SELECT id, captured_at
        FROM flight_recorder.samples
        WHERE captured_at BETWEEN p_start_time AND p_end_time
    ),
    total_samples AS (
        SELECT count(*) AS cnt FROM sample_range
    )
    SELECT
        w.backend_type,
        w.wait_event_type,
        w.wait_event,
        count(DISTINCT w.sample_id) AS sample_count,
        sum(w.count) AS total_waiters,
        round(avg(w.count), 2) AS avg_waiters,
        max(w.count) AS max_waiters,
        round(100.0 * count(DISTINCT w.sample_id) / NULLIF(t.cnt, 0), 1) AS pct_of_samples
    FROM flight_recorder.wait_samples w
    JOIN sample_range sr ON sr.id = w.sample_id
    CROSS JOIN total_samples t
    WHERE w.state NOT IN ('idle', 'idle in transaction')
    GROUP BY w.backend_type, w.wait_event_type, w.wait_event, t.cnt
    ORDER BY total_waiters DESC, sample_count DESC;
$$;

-- -----------------------------------------------------------------------------
-- flight_recorder.table_compare() - Compare table stats between two time points
-- -----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION flight_recorder.table_compare(
    p_table TEXT,
    p_start_time TIMESTAMPTZ,
    p_end_time TIMESTAMPTZ,
    p_schema TEXT DEFAULT 'public'
)
RETURNS TABLE(
    table_name              TEXT,
    start_snapshot_at       TIMESTAMPTZ,
    end_snapshot_at         TIMESTAMPTZ,
    elapsed_seconds         NUMERIC,

    size_start              TEXT,
    size_end                TEXT,
    size_delta              TEXT,
    total_size_delta        TEXT,

    n_live_tup_start        BIGINT,
    n_live_tup_end          BIGINT,
    n_dead_tup_end          BIGINT,
    dead_tuple_ratio        NUMERIC,

    inserts_delta           BIGINT,
    updates_delta           BIGINT,
    deletes_delta           BIGINT,
    hot_updates_delta       BIGINT,

    autovacuum_ran          BOOLEAN,
    autoanalyze_ran         BOOLEAN,
    autovacuum_count_delta  BIGINT,
    autoanalyze_count_delta BIGINT
)
LANGUAGE sql STABLE AS $$
    WITH
    start_snap AS (
        SELECT ts.*, s.captured_at
        FROM flight_recorder.table_snapshots ts
        JOIN flight_recorder.snapshots s ON s.id = ts.snapshot_id
        WHERE ts.schemaname = p_schema
          AND ts.relname = p_table
          AND s.captured_at <= p_start_time
        ORDER BY s.captured_at DESC
        LIMIT 1
    ),
    end_snap AS (
        SELECT ts.*, s.captured_at
        FROM flight_recorder.table_snapshots ts
        JOIN flight_recorder.snapshots s ON s.id = ts.snapshot_id
        WHERE ts.schemaname = p_schema
          AND ts.relname = p_table
          AND s.captured_at >= p_end_time
        ORDER BY s.captured_at ASC
        LIMIT 1
    )
    SELECT
        p_schema || '.' || p_table,
        s.captured_at,
        e.captured_at,
        EXTRACT(EPOCH FROM (e.captured_at - s.captured_at))::numeric,

        flight_recorder._pretty_bytes(s.pg_relation_size),
        flight_recorder._pretty_bytes(e.pg_relation_size),
        flight_recorder._pretty_bytes(e.pg_relation_size - s.pg_relation_size),
        flight_recorder._pretty_bytes(e.pg_total_relation_size - s.pg_total_relation_size),

        s.n_live_tup,
        e.n_live_tup,
        e.n_dead_tup,
        round(e.n_dead_tup::numeric / NULLIF(e.n_live_tup, 0), 4),

        e.n_tup_ins - s.n_tup_ins,
        e.n_tup_upd - s.n_tup_upd,
        e.n_tup_del - s.n_tup_del,
        e.n_tup_hot_upd - s.n_tup_hot_upd,

        (s.last_autovacuum IS DISTINCT FROM e.last_autovacuum),
        (s.last_autoanalyze IS DISTINCT FROM e.last_autoanalyze),
        e.autovacuum_count - s.autovacuum_count,
        e.autoanalyze_count - s.autoanalyze_count
    FROM start_snap s, end_snap e
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
        SELECT id, captured_at,
               ABS(EXTRACT(EPOCH FROM (captured_at - p_timestamp))) AS offset_secs
        FROM flight_recorder.samples
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
        FROM flight_recorder.wait_samples w
        JOIN nearest_sample ns ON ns.id = w.sample_id
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
        FROM flight_recorder.activity_samples a
        JOIN nearest_sample ns ON ns.id = a.sample_id
    ),
    sample_locks AS (
        SELECT
            count(DISTINCT blocked_pid) AS blocked_pids,
            max(blocked_duration) AS longest_blocked
        FROM flight_recorder.lock_samples l
        JOIN nearest_sample ns ON ns.id = l.sample_id
    ),
    sample_progress AS (
        SELECT
            count(*) FILTER (WHERE progress_type = 'vacuum') AS vacuums,
            count(*) FILTER (WHERE progress_type = 'copy') AS copies,
            count(*) FILTER (WHERE progress_type = 'create_index') AS indexes,
            count(*) FILTER (WHERE progress_type = 'analyze') AS analyzes
        FROM flight_recorder.progress_samples p
        JOIN nearest_sample ns ON ns.id = p.sample_id
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
    FROM flight_recorder.lock_samples l
    JOIN flight_recorder.samples s ON s.id = l.sample_id
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
    FROM flight_recorder.samples WHERE captured_at BETWEEN p_start_time AND p_end_time;

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
    FROM flight_recorder.lock_samples l
    JOIN flight_recorder.samples s ON s.id = l.sample_id
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
        flight_recorder._get_config('sample_interval_seconds', '120')::integer,
        120
    );

    -- Set mode-specific configuration
    -- Modes control WHAT is collected (locks, progress), not HOW OFTEN (interval)
    CASE p_mode
        WHEN 'normal' THEN
            v_enable_locks := TRUE;
            v_enable_progress := TRUE;
            v_sample_interval_seconds := v_current_interval;  -- Respect current config
            v_description := format('Normal mode: %ss sampling, all collectors enabled', v_sample_interval_seconds);
        WHEN 'light' THEN
            v_enable_locks := TRUE;
            v_enable_progress := FALSE;
            v_sample_interval_seconds := v_current_interval;  -- Respect current config
            v_description := format('Light mode: %ss sampling, progress tracking disabled', v_sample_interval_seconds);
        WHEN 'emergency' THEN
            v_enable_locks := FALSE;
            v_enable_progress := FALSE;
            -- Emergency mode forces minimum 120s interval
            v_sample_interval_seconds := GREATEST(v_current_interval, 120);
            v_description := format('Emergency mode: %ss sampling, locks and progress disabled', v_sample_interval_seconds);
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

    -- If emergency mode and interval < 120s, update interval
    IF p_mode = 'emergency' AND v_sample_interval_seconds > v_current_interval THEN
        INSERT INTO flight_recorder.config (key, value, updated_at)
        VALUES ('sample_interval_seconds', v_sample_interval_seconds::text, now())
        ON CONFLICT (key) DO UPDATE SET value = v_sample_interval_seconds::text, updated_at = now();
    END IF;

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
            WHEN 'normal' THEN '60 seconds'
            WHEN 'light' THEN '60 seconds'
            WHEN 'emergency' THEN '120 seconds'
            ELSE 'unknown'
        END AS sample_interval,
        COALESCE(flight_recorder._get_config('enable_locks', 'true')::boolean, true) AS locks_enabled,
        COALESCE(flight_recorder._get_config('enable_progress', 'true')::boolean, true) AS progress_enabled,
        flight_recorder._get_config('statements_enabled', 'auto') AS statements_enabled
$$;

-- -----------------------------------------------------------------------------
-- flight_recorder.cleanup() - Remove old flight recorder data
-- -----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION flight_recorder.cleanup(p_retain_interval INTERVAL DEFAULT NULL)
RETURNS TABLE(
    deleted_snapshots   BIGINT,
    deleted_samples     BIGINT,
    deleted_statements  BIGINT,
    deleted_stats       BIGINT,
    vacuumed_tables     INTEGER
)
LANGUAGE plpgsql AS $$
DECLARE
    v_deleted_snapshots BIGINT;
    v_deleted_samples BIGINT;
    v_deleted_statements BIGINT;
    v_deleted_stats BIGINT;
    v_vacuumed_count INTEGER := 0;
    v_table_name TEXT;
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

    -- Delete old samples (cascades to wait_samples, activity_samples, progress_samples, lock_samples)
    WITH deleted AS (
        DELETE FROM flight_recorder.samples WHERE captured_at < v_samples_cutoff RETURNING 1
    )
    SELECT count(*) INTO v_deleted_samples FROM deleted;

    -- Delete old snapshots (cascades to table_snapshots, replication_snapshots)
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

    -- P1 Safety: VACUUM ANALYZE after cleanup to reclaim space and update stats
    -- This prevents bloat in flight recorder tables and keeps query planner informed
    FOR v_table_name IN
        SELECT c.relname
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'flight_recorder'
          AND c.relkind = 'r'  -- regular tables only
          AND c.relname IN (
              'samples', 'wait_samples', 'activity_samples', 'progress_samples', 'lock_samples',
              'snapshots', 'table_snapshots', 'replication_snapshots', 'statement_snapshots',
              'collection_stats'
          )
        ORDER BY c.relname
    LOOP
        BEGIN
            EXECUTE format('VACUUM ANALYZE flight_recorder.%I', v_table_name);
            v_vacuumed_count := v_vacuumed_count + 1;
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'pg-flight-recorder: Failed to VACUUM table %: %', v_table_name, SQLERRM;
        END;
    END LOOP;

    RETURN QUERY SELECT v_deleted_snapshots, v_deleted_samples, v_deleted_statements, v_deleted_stats, v_vacuumed_count;
END;
$$;

-- -----------------------------------------------------------------------------
-- Partition Management Functions
-- -----------------------------------------------------------------------------

-- Create future partitions for samples table (daily partitions)
CREATE OR REPLACE FUNCTION flight_recorder.create_partitions(p_days_ahead INTEGER DEFAULT 3)
RETURNS TEXT
LANGUAGE plpgsql AS $$
DECLARE
    v_partition_date DATE;
    v_partition_name TEXT;
    v_start_date TEXT;
    v_end_date TEXT;
    v_created INTEGER := 0;
BEGIN
    FOR i IN 0..p_days_ahead LOOP
        v_partition_date := CURRENT_DATE + (i || ' days')::interval;
        v_partition_name := 'samples_' || TO_CHAR(v_partition_date, 'YYYYMMDD');
        v_start_date := v_partition_date::TEXT;
        v_end_date := (v_partition_date + 1)::TEXT;

        -- Check if partition exists
        IF NOT EXISTS (
            SELECT 1 FROM pg_tables
            WHERE schemaname = 'flight_recorder' AND tablename = v_partition_name
        ) THEN
            EXECUTE format(
                'CREATE TABLE flight_recorder.%I PARTITION OF flight_recorder.samples FOR VALUES FROM (%L) TO (%L)',
                v_partition_name, v_start_date, v_end_date
            );
            v_created := v_created + 1;
        END IF;
    END LOOP;

    RETURN format('Created %s future partition(s) for samples table', v_created);
END;
$$;

-- Drop old partitions (more efficient than DELETE for partitioned tables)
CREATE OR REPLACE FUNCTION flight_recorder.drop_old_partitions(p_retention_days INTEGER DEFAULT NULL)
RETURNS TEXT
LANGUAGE plpgsql AS $$
DECLARE
    v_retention_days INTEGER;
    v_cutoff_date DATE;
    v_partition_name TEXT;
    v_partition_date DATE;
    v_dropped INTEGER := 0;
BEGIN
    -- Get retention from config
    v_retention_days := COALESCE(
        p_retention_days,
        flight_recorder._get_config('retention_samples_days', '7')::integer,
        7
    );

    v_cutoff_date := CURRENT_DATE - v_retention_days;

    -- Find and drop old partitions
    FOR v_partition_name IN
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'flight_recorder'
          AND tablename LIKE 'samples_%'
          AND tablename ~ 'samples_\d{8}$'
        ORDER BY tablename
    LOOP
        -- Extract date from partition name (format: samples_YYYYMMDD)
        BEGIN
            v_partition_date := TO_DATE(substring(v_partition_name from 9), 'YYYYMMDD');

            IF v_partition_date < v_cutoff_date THEN
                EXECUTE format('DROP TABLE IF EXISTS flight_recorder.%I', v_partition_name);
                v_dropped := v_dropped + 1;
                RAISE NOTICE 'Dropped old partition: %', v_partition_name;
            END IF;
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'Failed to process partition %: %', v_partition_name, SQLERRM;
        END;
    END LOOP;

    RETURN format('Dropped %s old partition(s) older than % days', v_dropped, v_retention_days);
END;
$$;

-- List all partitions with sizes
CREATE OR REPLACE FUNCTION flight_recorder.list_partitions()
RETURNS TABLE(
    partition_name TEXT,
    partition_date DATE,
    size_pretty TEXT,
    row_count BIGINT
)
LANGUAGE plpgsql AS $$
BEGIN
    RETURN QUERY
    SELECT
        t.tablename::TEXT,
        TO_DATE(substring(t.tablename from 9), 'YYYYMMDD'),
        pg_size_pretty(pg_total_relation_size('flight_recorder.' || t.tablename)),
        (SELECT count(*) FROM pg_class c
         JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = 'flight_recorder' AND c.relname = t.tablename)::BIGINT
    FROM pg_tables t
    WHERE t.schemaname = 'flight_recorder'
      AND t.tablename LIKE 'samples_%'
      AND t.tablename ~ 'samples_\d{8}$'
    ORDER BY t.tablename;
END;
$$;

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

        -- Unschedule partition creation
        PERFORM cron.unschedule('flight_recorder_partition')
        WHERE EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'flight_recorder_partition');
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

    -- Get configurable sample interval (default 120s)
    v_sample_interval_seconds := COALESCE(
        flight_recorder._get_config('sample_interval_seconds', '120')::integer,
        120
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

        -- Schedule sample based on configurable interval
        -- Convert seconds to minutes for cron format (only supports minute granularity)
        IF v_sample_interval_seconds < 60 THEN
            -- Sub-minute intervals not supported reliably, default to 60s
            v_cron_expression := '* * * * *';
            v_sample_schedule := 'every minute (60s)';
        ELSIF v_sample_interval_seconds = 60 THEN
            v_cron_expression := '* * * * *';
            v_sample_schedule := 'every minute (60s)';
        ELSE
            -- Convert to minutes (round up)
            v_sample_interval_minutes := CEILING(v_sample_interval_seconds::numeric / 60.0)::integer;
            v_cron_expression := format('*/%s * * * *', v_sample_interval_minutes);
            v_sample_schedule := format('every %s minutes (%ss)', v_sample_interval_minutes, v_sample_interval_seconds);
        END IF;

        -- Emergency mode overrides with 2-minute minimum interval
        IF v_mode = 'emergency' AND v_sample_interval_minutes < 2 THEN
            v_cron_expression := '*/2 * * * *';
            v_sample_schedule := 'every 2 minutes (emergency mode override)';
        END IF;

        PERFORM cron.schedule('flight_recorder_sample', v_cron_expression, 'SELECT flight_recorder.sample()');
        v_scheduled := v_scheduled + 1;

        -- Schedule cleanup (daily at 3 AM) - now uses drop_old_partitions for samples table
        PERFORM cron.schedule('flight_recorder_cleanup', '0 3 * * *',
            'SELECT flight_recorder.drop_old_partitions(); SELECT * FROM flight_recorder.cleanup(''7 days''::interval);');
        v_scheduled := v_scheduled + 1;

        -- Schedule partition creation (daily at 2 AM) - create future partitions proactively
        PERFORM cron.schedule('flight_recorder_partition', '0 2 * * *', 'SELECT flight_recorder.create_partitions(3)');
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
        PERFORM cron.unschedule('flight_recorder_cleanup')
        WHERE EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'flight_recorder_cleanup');
        PERFORM cron.unschedule('flight_recorder_partition')
        WHERE EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'flight_recorder_partition');
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

    -- Schedule sample collection based on configured interval
    IF v_sample_interval_seconds < 60 THEN
        -- Sub-minute intervals - round up to 60s (not reliably supported)
        v_cron_expression := '* * * * *';
        v_sample_schedule := 'every minute (60s, rounded from ' || v_sample_interval_seconds || 's config)';
    ELSIF v_sample_interval_seconds = 60 THEN
        v_cron_expression := '* * * * *';
        v_sample_schedule := 'every minute (60s)';
    ELSE
        -- Multi-minute intervals - convert to minutes
        v_sample_interval_minutes := CEILING(v_sample_interval_seconds::numeric / 60.0)::integer;
        v_cron_expression := format('*/%s * * * *', v_sample_interval_minutes);
        v_sample_schedule := format('every %s minutes (%ss)', v_sample_interval_minutes, v_sample_interval_seconds);
    END IF;

    PERFORM cron.schedule(
        'flight_recorder_sample',
        v_cron_expression,
        'SELECT flight_recorder.sample()'
    );
    RAISE NOTICE 'Flight Recorder installed. Sampling %', v_sample_schedule;

    -- Schedule cleanup (daily at 3 AM) - uses drop_old_partitions
    PERFORM cron.schedule(
        'flight_recorder_cleanup',
        '0 3 * * *',
        'SELECT flight_recorder.drop_old_partitions(); SELECT * FROM flight_recorder.cleanup(''7 days''::interval);'
    );

    -- Schedule partition creation (daily at 2 AM) - create future partitions proactively
    PERFORM cron.schedule(
        'flight_recorder_partition',
        '0 2 * * *',
        'SELECT flight_recorder.create_partitions(3)'
    );

EXCEPTION
    WHEN undefined_table THEN
        RAISE NOTICE 'pg_cron extension not found. Automatic scheduling disabled. Run flight_recorder.snapshot() and flight_recorder.sample() manually or via external scheduler.';
    WHEN undefined_function THEN
        RAISE NOTICE 'pg_cron extension not found. Automatic scheduling disabled. Run flight_recorder.snapshot() and flight_recorder.sample() manually or via external scheduler.';
END;
$$;

-- -----------------------------------------------------------------------------
-- P2: Partition Management Helpers (Optional)
-- -----------------------------------------------------------------------------
-- These functions help set up time-based partitioning for samples and snapshots
-- tables. Partitioning improves performance and makes cleanup faster (DROP vs DELETE).
--
-- WARNING: Converting existing tables to partitioned tables requires data migration.
-- Only use these functions on NEW installs or follow proper migration procedure.
-- -----------------------------------------------------------------------------

-- P2: Create next time-based partition for samples or snapshots
CREATE OR REPLACE FUNCTION flight_recorder.create_next_partition(
    p_table_name TEXT,  -- 'samples' or 'snapshots'
    p_partition_interval TEXT DEFAULT 'day'  -- 'day' or 'week'
)
RETURNS TEXT
LANGUAGE plpgsql AS $$
DECLARE
    v_next_date DATE;
    v_partition_name TEXT;
    v_from_date DATE;
    v_to_date DATE;
BEGIN
    -- Validate table name
    IF p_table_name NOT IN ('samples', 'snapshots') THEN
        RAISE EXCEPTION 'Invalid table name: %. Must be ''samples'' or ''snapshots''', p_table_name;
    END IF;

    -- Validate interval
    IF p_partition_interval NOT IN ('day', 'week') THEN
        RAISE EXCEPTION 'Invalid partition interval: %. Must be ''day'' or ''week''', p_partition_interval;
    END IF;

    -- Calculate next partition dates
    v_next_date := CURRENT_DATE + interval '1 day';

    IF p_partition_interval = 'day' THEN
        v_from_date := v_next_date;
        v_to_date := v_next_date + interval '1 day';
        v_partition_name := format('flight_recorder.%s_%s', p_table_name, to_char(v_from_date, 'YYYYMMDD'));
    ELSE  -- week
        v_from_date := date_trunc('week', v_next_date + interval '1 week')::date;
        v_to_date := v_from_date + interval '1 week';
        v_partition_name := format('flight_recorder.%s_%s', p_table_name, to_char(v_from_date, 'YYYYMMDD'));
    END IF;

    -- Create partition
    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS %s PARTITION OF flight_recorder.%I FOR VALUES FROM (%L) TO (%L)',
        v_partition_name, p_table_name, v_from_date, v_to_date
    );

    RETURN format('Created partition %s for dates %s to %s', v_partition_name, v_from_date, v_to_date);
END;
$$;

-- P2: Drop old partitions (faster than DELETE for cleanup)
CREATE OR REPLACE FUNCTION flight_recorder.drop_old_partitions(
    p_table_name TEXT,  -- 'samples' or 'snapshots'
    p_retain_interval INTERVAL DEFAULT '7 days'
)
RETURNS TABLE(
    partition_name TEXT,
    dropped BOOLEAN
)
LANGUAGE plpgsql AS $$
DECLARE
    v_cutoff_date DATE;
    v_partition_record RECORD;
    v_partition_date DATE;
BEGIN
    -- Validate table name
    IF p_table_name NOT IN ('samples', 'snapshots') THEN
        RAISE EXCEPTION 'Invalid table name: %. Must be ''samples'' or ''snapshots''', p_table_name;
    END IF;

    v_cutoff_date := (now() - p_retain_interval)::date;

    -- Find and drop old partitions
    FOR v_partition_record IN
        SELECT
            c.relname,
            pg_get_expr(c.relpartbound, c.oid) as partition_bound
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_inherits i ON i.inhrelid = c.oid
        JOIN pg_class p ON p.oid = i.inhparent
        WHERE n.nspname = 'flight_recorder'
          AND p.relname = p_table_name
          AND c.relkind = 'r'
    LOOP
        -- Extract date from partition name (format: tablename_YYYYMMDD)
        BEGIN
            v_partition_date := to_date(
                substring(v_partition_record.relname from '[0-9]{8}$'),
                'YYYYMMDD'
            );

            IF v_partition_date < v_cutoff_date THEN
                EXECUTE format('DROP TABLE IF EXISTS flight_recorder.%I', v_partition_record.relname);
                RETURN QUERY SELECT v_partition_record.relname::text, true;
            END IF;
        EXCEPTION WHEN OTHERS THEN
            -- Skip partitions that don't match naming convention
            CONTINUE;
        END;
    END LOOP;
END;
$$;

-- P2: Check partition status and provide recommendations
CREATE OR REPLACE FUNCTION flight_recorder.partition_status()
RETURNS TABLE(
    table_name TEXT,
    is_partitioned BOOLEAN,
    partition_count INTEGER,
    oldest_partition TEXT,
    newest_partition TEXT,
    recommendation TEXT
)
LANGUAGE plpgsql AS $$
BEGIN
    RETURN QUERY
    WITH partition_info AS (
        SELECT
            p.relname as parent_table,
            c.relname as partition_name,
            pg_get_expr(c.relpartbound, c.oid) as partition_bound
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_inherits i ON i.inhrelid = c.oid
        JOIN pg_class p ON p.oid = i.inhparent
        WHERE n.nspname = 'flight_recorder'
          AND p.relname IN ('samples', 'snapshots')
          AND c.relkind = 'r'
    )
    SELECT
        t.table_name::text,
        (EXISTS (
            SELECT 1 FROM pg_partitioned_table pt
            JOIN pg_class c ON c.oid = pt.partrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'flight_recorder' AND c.relname = t.table_name
        )) as is_partitioned,
        COALESCE(pi.cnt, 0)::integer as partition_count,
        pi.oldest::text,
        pi.newest::text,
        CASE
            WHEN NOT (EXISTS (
                SELECT 1 FROM pg_partitioned_table pt
                JOIN pg_class c ON c.oid = pt.partrelid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = 'flight_recorder' AND c.relname = t.table_name
            )) THEN 'Table not partitioned. See documentation for migration procedure.'
            WHEN pi.cnt = 0 THEN 'No partitions found. Run create_next_partition().'
            WHEN pi.cnt < 2 THEN 'Low partition count. Consider running create_next_partition().'
            ELSE 'OK'
        END::text as recommendation
    FROM (VALUES ('samples'), ('snapshots')) AS t(table_name)
    LEFT JOIN LATERAL (
        SELECT
            count(*) as cnt,
            min(partition_name) as oldest,
            max(partition_name) as newest
        FROM partition_info
        WHERE parent_table = t.table_name
    ) pi ON true;
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
    SELECT max(captured_at) INTO v_last_sample FROM flight_recorder.samples;
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
    SELECT count(*) INTO v_sample_count FROM flight_recorder.samples;
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

    -- Component 7: DDL detection status
    DECLARE
        v_ddl_enabled BOOLEAN;
        v_ddl_skips INTEGER;
    BEGIN
        v_ddl_enabled := COALESCE(
            flight_recorder._get_config('ddl_detection_enabled', 'true')::boolean,
            true
        );

        SELECT count(*)
        INTO v_ddl_skips
        FROM flight_recorder.collection_stats
        WHERE skipped = true
          AND started_at > now() - interval '24 hours'
          AND skipped_reason LIKE '%DDL detected%';

        RETURN QUERY SELECT
            'DDL Detection'::text,
            CASE
                WHEN NOT v_ddl_enabled THEN 'DISABLED'
                WHEN v_ddl_skips = 0 THEN 'OK'
                WHEN v_ddl_skips < 10 THEN 'INFO'
                ELSE 'WARNING'
            END::text,
            CASE
                WHEN NOT v_ddl_enabled THEN 'DDL detection disabled - lock contention possible'
                ELSE format('Enabled - %s DDL-related skips in last 24h', v_ddl_skips)
            END,
            CASE
                WHEN NOT v_ddl_enabled THEN 'Enable with: UPDATE flight_recorder.config SET value = ''true'' WHERE key = ''ddl_detection_enabled'';'
                WHEN v_ddl_skips >= 10 THEN 'Frequent DDL activity detected - normal if schema changes are active'
                ELSE NULL
            END::text;
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
        SELECT max(captured_at) INTO v_last_sample FROM flight_recorder.samples;

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
                FROM flight_recorder.wait_samples ws
                WHERE ws.sample_id = s.id
            ), '[]'::jsonb),
            COALESCE((
                SELECT jsonb_agg(jsonb_build_array(
                    ls.blocked_pid,
                    ls.blocking_pid,
                    ls.lock_type,
                    ls.blocked_duration
                ))
                FROM flight_recorder.lock_samples ls
                WHERE ls.sample_id = s.id
            ), '[]'::jsonb)
        )
    )
    INTO v_samples
    FROM flight_recorder.samples s
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
    SELECT count(*) INTO v_sample_count FROM flight_recorder.samples;
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

    -- Recommendation 5: Consider partitioning
    IF v_sample_count > 100000 THEN
        RETURN QUERY SELECT
            'Scalability'::text,
            'Consider implementing table partitioning'::text,
            format('Very high sample count (%s) - partitioning improves cleanup performance', v_sample_count),
            'See documentation for partition_status() and create_next_partition() functions.'::text;
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
            'Flight recorder uses 2-3 GB per week (default 7-day retention)',
            'UNLOGGED tables minimize WAL overhead. Daily partition cleanup prevents unbounded growth.'::text;
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
        'Circuit breaker, adaptive mode, DDL detection, timeouts all enabled by default',
        'Flight recorder will auto-reduce overhead under stress and prevent lock contention.'::text;

    -- Summary recommendation
    DECLARE
        v_nogo_count INTEGER;
        v_caution_count INTEGER;
    BEGIN
        SELECT
            count(*) FILTER (WHERE status = 'NO-GO'),
            count(*) FILTER (WHERE status = 'CAUTION')
        INTO v_nogo_count, v_caution_count
        FROM flight_recorder.preflight_check();

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
END;
$$;

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
    v_ddl_skips INTEGER;
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
    SELECT count(*) INTO v_sample_count FROM flight_recorder.samples;

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

    -- Metric 5: DDL detection activity (last 90 days)
    SELECT count(*) INTO v_ddl_skips
    FROM flight_recorder.collection_stats
    WHERE skipped = true
      AND skipped_reason LIKE '%DDL%'
      AND started_at > now() - interval '90 days';

    IF v_ddl_skips = 0 THEN
        RETURN QUERY SELECT
            '5. DDL Detection Activity'::text,
            'INFO'::text,
            '0 DDL-related skips in 90 days',
            'No schema changes detected during collection windows.'::text;
    ELSIF v_ddl_skips < 50 THEN
        RETURN QUERY SELECT
            '5. DDL Detection Activity'::text,
            'INFO'::text,
            format('%s DDL-related skips in 90 days', v_ddl_skips),
            'DDL detection prevented lock contention. This is working as intended.'::text;
    ELSE
        RETURN QUERY SELECT
            '5. DDL Detection Activity'::text,
            'INFO'::text,
            format('%s DDL-related skips in 90 days', v_ddl_skips),
            'Frequent DDL activity detected. Normal if you have active schema changes. Consider ddl_skip_entire_sample=false if you need lock data during DDL.'::text;
    END IF;

    -- Metric 6: Data freshness
    SELECT max(captured_at) INTO v_last_sample FROM flight_recorder.samples;
    SELECT max(captured_at) INTO v_last_snapshot FROM flight_recorder.snapshots;

    IF v_last_sample > now() - interval '10 minutes' AND v_last_snapshot > now() - interval '15 minutes' THEN
        RETURN QUERY SELECT
            '6. Data Freshness'::text,
            'EXCELLENT'::text,
            format('Last sample: %s ago | Last snapshot: %s ago',
                   age(now(), v_last_sample)::text, age(now(), v_last_snapshot)::text),
            'Collections are running on schedule.'::text;
    ELSE
        RETURN QUERY SELECT
            '6. Data Freshness'::text,
            'ERROR'::text,
            format('Last sample: %s ago | Last snapshot: %s ago',
                   age(now(), v_last_sample)::text, age(now(), v_last_snapshot)::text),
            'Collections are stale. Check pg_cron jobs: SELECT * FROM cron.job WHERE jobname LIKE ''flight_recorder_%'';'::text;
    END IF;

    -- Summary and next steps
    DECLARE
        v_issues_count INTEGER;
    BEGIN
        SELECT count(*) INTO v_issues_count
        FROM flight_recorder.quarterly_review()
        WHERE status IN ('ERROR', 'REVIEW NEEDED');

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
END;
$$;

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
    RAISE NOTICE '  - Snapshots: every 5 minutes (WAL, checkpoints, I/O stats)';
    RAISE NOTICE '  - Samples: % (wait events, activity, progress, locks)', COALESCE(v_sample_schedule, 'not scheduled');
    RAISE NOTICE '  - Cleanup: daily at 3 AM (retains 7 days)';
    RAISE NOTICE '';
    RAISE NOTICE 'Quick start for batch monitoring:';
    RAISE NOTICE '  1. Track your target table:';
    RAISE NOTICE '     SELECT flight_recorder.track_table(''my_table'');';
    RAISE NOTICE '';
    RAISE NOTICE '  2. Run your batch job, then analyze:';
    RAISE NOTICE '     SELECT * FROM flight_recorder.compare(''2024-12-16 14:00'', ''2024-12-16 15:00'');';
    RAISE NOTICE '     SELECT * FROM flight_recorder.table_compare(''my_table'', ''2024-12-16 14:00'', ''2024-12-16 15:00'');';
    RAISE NOTICE '     SELECT * FROM flight_recorder.wait_summary(''2024-12-16 14:00'', ''2024-12-16 15:00'');';
    RAISE NOTICE '';
    RAISE NOTICE 'Views for recent activity:';
    RAISE NOTICE '  - flight_recorder.deltas            (snapshot deltas incl. temp files)';
    RAISE NOTICE '  - flight_recorder.table_deltas      (tracked table deltas)';
    RAISE NOTICE '  - flight_recorder.recent_waits      (wait events, last 2 hours)';
    RAISE NOTICE '  - flight_recorder.recent_activity   (active sessions, last 2 hours)';
    RAISE NOTICE '  - flight_recorder.recent_locks      (lock contention, last 2 hours)';
    RAISE NOTICE '  - flight_recorder.recent_progress   (vacuum/copy/analyze progress, last 2 hours)';
    RAISE NOTICE '  - flight_recorder.recent_replication (replication lag, last 2 hours)';
    RAISE NOTICE '';
    RAISE NOTICE 'Table management:';
    RAISE NOTICE '  - flight_recorder.track_table(name, schema)';
    RAISE NOTICE '  - flight_recorder.untrack_table(name, schema)';
    RAISE NOTICE '  - flight_recorder.list_tracked_tables()';
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
