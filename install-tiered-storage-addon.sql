-- =============================================================================
-- pg-flight-recorder: TIERED STORAGE ADD-ON
-- =============================================================================
-- This add-on implements a 3-tier storage architecture for crash resistance:
--
-- TIER 1 (Hot):  UNLOGGED ring buffer - high-frequency (60s), volatile
-- TIER 2 (Warm): REGULAR aggregates - flushed every 5min, durable
-- TIER 3 (Cold): REGULAR snapshots - every 5min, durable (already exists)
--
-- Trade-off: Lose last ~2 hours of raw samples on crash (acceptable)
--            Keep aggregated diagnostics forever (critical)
--
-- INSTALLATION:
--   psql -f install-tiered-storage-addon.sql
--
-- DEPENDENCIES:
--   - Must be run AFTER install.sql
--   - Requires PostgreSQL 15, 16, or 17
--   - Requires pg_cron extension
-- =============================================================================

-- =============================================================================
-- TIER 1: Hot Ring Buffer Tables (UNLOGGED, fixed size)
-- =============================================================================
-- Uses modular arithmetic to create circular buffers
-- Fixed memory footprint: N slots × row size
-- No DELETE needed - UPSERT automatically overwrites old data

-- Ring buffer master table (120 slots = 2 hours at 60s intervals)
CREATE UNLOGGED TABLE IF NOT EXISTS flight_recorder.samples_ring (
    slot_id             INTEGER PRIMARY KEY CHECK (slot_id >= 0 AND slot_id < 120),
    captured_at         TIMESTAMPTZ NOT NULL,
    epoch_seconds       BIGINT NOT NULL
);

COMMENT ON TABLE flight_recorder.samples_ring IS 'TIER 1: Ring buffer master (120 slots, 60s intervals, 2 hours retention)';

-- Wait events ring buffer
CREATE UNLOGGED TABLE IF NOT EXISTS flight_recorder.wait_samples_ring (
    slot_id             INTEGER REFERENCES flight_recorder.samples_ring(slot_id) ON DELETE CASCADE,
    backend_type        TEXT NOT NULL,
    wait_event_type     TEXT NOT NULL,
    wait_event          TEXT NOT NULL,
    state               TEXT NOT NULL,
    count               INTEGER NOT NULL,
    PRIMARY KEY (slot_id, backend_type, wait_event_type, wait_event, state)
);

COMMENT ON TABLE flight_recorder.wait_samples_ring IS 'TIER 1: Wait events ring buffer (aggregated by type/event/state)';

-- Activity samples ring buffer
CREATE UNLOGGED TABLE IF NOT EXISTS flight_recorder.activity_samples_ring (
    slot_id             INTEGER REFERENCES flight_recorder.samples_ring(slot_id) ON DELETE CASCADE,
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
    PRIMARY KEY (slot_id, pid)
);

COMMENT ON TABLE flight_recorder.activity_samples_ring IS 'TIER 1: Active sessions ring buffer (top 25 per sample)';

-- Lock samples ring buffer
CREATE UNLOGGED TABLE IF NOT EXISTS flight_recorder.lock_samples_ring (
    slot_id                 INTEGER REFERENCES flight_recorder.samples_ring(slot_id) ON DELETE CASCADE,
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
    PRIMARY KEY (slot_id, blocked_pid, blocking_pid)
);

COMMENT ON TABLE flight_recorder.lock_samples_ring IS 'TIER 1: Lock contention ring buffer (blocked/blocking relationships)';

-- Initialize ring buffer slots (0 to 119)
INSERT INTO flight_recorder.samples_ring (slot_id, captured_at, epoch_seconds)
SELECT
    generate_series AS slot_id,
    '1970-01-01'::timestamptz,  -- Placeholder, will be overwritten
    0
FROM generate_series(0, 119)
ON CONFLICT (slot_id) DO NOTHING;

-- =============================================================================
-- TIER 2: Warm Aggregate Tables (REGULAR, durable)
-- =============================================================================
-- These tables survive crashes and contain sufficient detail for diagnosis
-- Flushed every 5 minutes from the ring buffer

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
-- Ring Buffer Write Function
-- =============================================================================
-- Replaces the existing sample() function to write to ring buffer instead

CREATE OR REPLACE FUNCTION flight_recorder.sample_to_ring()
RETURNS TIMESTAMPTZ
LANGUAGE plpgsql AS $$
DECLARE
    v_captured_at TIMESTAMPTZ := now();
    v_epoch BIGINT := extract(epoch from v_captured_at)::bigint;
    v_slot_id INTEGER := (v_epoch / 60) % 120;  -- 60-second intervals, 120 slots
    v_enable_locks BOOLEAN;
    v_snapshot_based BOOLEAN;
    v_blocked_count INTEGER;
    v_skip_locks_threshold INTEGER;
BEGIN
    -- Get configuration
    v_enable_locks := COALESCE(
        flight_recorder._get_config('enable_locks', 'true')::boolean,
        TRUE
    );
    v_snapshot_based := COALESCE(
        flight_recorder._get_config('snapshot_based_collection', 'true')::boolean,
        true
    );
    v_skip_locks_threshold := COALESCE(
        flight_recorder._get_config('skip_locks_threshold', '50')::integer,
        50
    );

    -- Set lock timeout
    PERFORM set_config('lock_timeout',
        COALESCE(flight_recorder._get_config('lock_timeout_ms', '100'), '100'),
        true);

    -- Update slot metadata (UPSERT pattern)
    INSERT INTO flight_recorder.samples_ring (slot_id, captured_at, epoch_seconds)
    VALUES (v_slot_id, v_captured_at, v_epoch)
    ON CONFLICT (slot_id) DO UPDATE SET
        captured_at = EXCLUDED.captured_at,
        epoch_seconds = EXCLUDED.epoch_seconds;

    -- Clear old data in child tables for this slot
    DELETE FROM flight_recorder.wait_samples_ring WHERE slot_id = v_slot_id;
    DELETE FROM flight_recorder.activity_samples_ring WHERE slot_id = v_slot_id;
    DELETE FROM flight_recorder.lock_samples_ring WHERE slot_id = v_slot_id;

    -- Create snapshot of pg_stat_activity if enabled
    IF v_snapshot_based THEN
        CREATE TEMP TABLE IF NOT EXISTS _fr_psa_snapshot (
            LIKE pg_stat_activity
        ) ON COMMIT DROP;
        TRUNCATE _fr_psa_snapshot;
        INSERT INTO _fr_psa_snapshot
        SELECT * FROM pg_stat_activity WHERE pid != pg_backend_pid();
    END IF;

    -- Section 1: Wait events
    BEGIN
        IF v_snapshot_based THEN
            INSERT INTO flight_recorder.wait_samples_ring (slot_id, backend_type, wait_event_type, wait_event, state, count)
            SELECT
                v_slot_id,
                COALESCE(backend_type, 'unknown'),
                COALESCE(wait_event_type, 'Running'),
                COALESCE(wait_event, 'CPU'),
                COALESCE(state, 'unknown'),
                count(*)::integer
            FROM _fr_psa_snapshot
            GROUP BY backend_type, wait_event_type, wait_event, state;
        ELSE
            INSERT INTO flight_recorder.wait_samples_ring (slot_id, backend_type, wait_event_type, wait_event, state, count)
            SELECT
                v_slot_id,
                COALESCE(backend_type, 'unknown'),
                COALESCE(wait_event_type, 'Running'),
                COALESCE(wait_event, 'CPU'),
                COALESCE(state, 'unknown'),
                count(*)::integer
            FROM pg_stat_activity
            WHERE pid != pg_backend_pid()
            GROUP BY backend_type, wait_event_type, wait_event, state;
        END IF;
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'pg-flight-recorder: Wait events collection failed: %', SQLERRM;
    END;

    -- Section 2: Activity samples (top 25)
    BEGIN
        IF v_snapshot_based THEN
            INSERT INTO flight_recorder.activity_samples_ring (
                slot_id, pid, usename, application_name, backend_type,
                state, wait_event_type, wait_event, query_start, state_change, query_preview
            )
            SELECT
                v_slot_id,
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
            INSERT INTO flight_recorder.activity_samples_ring (
                slot_id, pid, usename, application_name, backend_type,
                state, wait_event_type, wait_event, query_start, state_change, query_preview
            )
            SELECT
                v_slot_id,
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
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'pg-flight-recorder: Activity samples collection failed: %', SQLERRM;
    END;

    -- Section 3: Lock samples (if enabled)
    IF v_enable_locks THEN
    BEGIN
        -- Materialize blocked sessions (OPTIMIZATION 1)
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

        IF v_blocked_count <= v_skip_locks_threshold THEN
            INSERT INTO flight_recorder.lock_samples_ring (
                slot_id, blocked_pid, blocked_user, blocked_app,
                blocked_query_preview, blocked_duration, blocking_pid, blocking_user,
                blocking_app, blocking_query_preview, lock_type, locked_relation_oid
            )
            SELECT DISTINCT ON (bs.pid, blocking_pid)
                v_slot_id,
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
            FROM _fr_blocked_sessions bs
            CROSS JOIN LATERAL unnest(bs.blocking_pids) AS blocking_pid
            JOIN _fr_psa_snapshot blocking ON blocking.pid = blocking_pid
            ORDER BY bs.pid, blocking_pid
            LIMIT 100;
        END IF;
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'pg-flight-recorder: Lock sampling collection failed: %', SQLERRM;
    END;
    END IF;

    RETURN v_captured_at;
END;
$$;

COMMENT ON FUNCTION flight_recorder.sample_to_ring() IS 'TIER 1: Write samples to ring buffer (60s intervals, overwrites old data automatically)';

-- =============================================================================
-- Flush Function (Tier 1 → Tier 2)
-- =============================================================================
-- Aggregates ring buffer data and flushes to durable tables every 5 minutes

CREATE OR REPLACE FUNCTION flight_recorder.flush_ring_to_aggregates()
RETURNS VOID
LANGUAGE plpgsql AS $$
DECLARE
    v_start_time TIMESTAMPTZ;
    v_end_time TIMESTAMPTZ;
    v_total_samples INTEGER;
    v_last_flush TIMESTAMPTZ;
BEGIN
    -- Find the last flush time
    SELECT COALESCE(max(end_time), '1970-01-01')
    INTO v_last_flush
    FROM flight_recorder.wait_event_aggregates;

    -- Find the time range covered by current ring buffer (samples newer than last flush)
    SELECT min(captured_at), max(captured_at), count(*)
    INTO v_start_time, v_end_time, v_total_samples
    FROM flight_recorder.samples_ring
    WHERE captured_at > v_last_flush;

    -- If no new data, exit early
    IF v_start_time IS NULL OR v_total_samples = 0 THEN
        RETURN;
    END IF;

    -- Aggregate and flush wait events
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
    GROUP BY w.backend_type, w.wait_event_type, w.wait_event, w.state;

    -- Aggregate and flush lock patterns
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
    GROUP BY l.blocked_user, l.blocking_user, l.lock_type, l.locked_relation_oid;

    -- Aggregate and flush query patterns
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
      AND a.query_start IS NOT NULL
    GROUP BY a.query_preview;

    RAISE NOTICE 'pg-flight-recorder: Flushed ring buffer (% to %, % samples)',
        v_start_time, v_end_time, v_total_samples;
END;
$$;

COMMENT ON FUNCTION flight_recorder.flush_ring_to_aggregates() IS 'TIER 2: Flush ring buffer to durable aggregates every 5 minutes';

-- =============================================================================
-- Schedule Jobs with pg_cron
-- =============================================================================

-- Ring buffer collection (every 60 seconds)
SELECT cron.schedule('flight_recorder_ring_sample', '60 seconds',
    'SELECT flight_recorder.sample_to_ring()');

-- Flush to aggregates (every 5 minutes)
SELECT cron.schedule('flight_recorder_flush', '*/5 * * * *',
    'SELECT flight_recorder.flush_ring_to_aggregates()');

-- =============================================================================
-- Migration Notes
-- =============================================================================
-- After installing this add-on:
--
-- 1. DISABLE old sample() collection:
--    SELECT cron.unschedule('flight_recorder_sample');
--
-- 2. Old partitioned samples table is no longer used (ring buffer replaces it)
--    You can keep it for historical data or drop it:
--    DROP TABLE IF EXISTS flight_recorder.samples CASCADE;
--
-- 3. The old views (recent_waits, recent_locks, etc.) will need updates to
--    query the ring buffer instead of the partitioned samples table.
--
-- =============================================================================

RAISE NOTICE 'pg-flight-recorder: Tiered storage add-on installed successfully';
RAISE NOTICE 'NEXT STEPS: Disable old sample() job and update views';
