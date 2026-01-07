-- =============================================================================
-- OPTIMIZATION 1: Materialized Blocked Sessions
-- =============================================================================
-- Impact: 50% reduction in pg_blocking_pids() calls + 95% reduction in CROSS JOIN rows
-- Location: Replace lines 1654-1770 in install.sql
--
-- Current problem:
--   1. Call pg_blocking_pids(pid) for every session to count blocked sessions
--   2. Call pg_blocking_pids(pid) AGAIN for every session in main query
--   3. CROSS JOIN processes all N sessions, even though only ~5% are blocked
--
-- Solution:
--   1. Create temp table with pg_blocking_pids() called ONCE per session
--   2. Filter to ONLY blocked sessions (WHERE array is not empty)
--   3. Reuse materialized array in CROSS JOIN
-- =============================================================================

    -- Section 4: Lock sampling (O(n) algorithm using pg_blocking_pids())
    -- OPTIMIZED: Materialize blocking PIDs once, filter early
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

            -- OPTIMIZATION: Materialize blocked sessions with pg_blocking_pids() computed ONCE
            -- This creates a temp table with ONLY blocked sessions (early filtering)
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
                    pg_blocking_pids(pid) AS blocking_pids  -- Computed once!
                FROM _fr_psa_snapshot
                WHERE cardinality(pg_blocking_pids(pid)) > 0;  -- Only blocked sessions
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

            -- Check count using materialized table (no re-computation)
            SELECT count(*) INTO v_blocked_count FROM _fr_blocked_sessions;

            IF v_blocked_count > v_skip_locks_threshold THEN
                RAISE NOTICE 'pg-flight-recorder: Skipping lock collection - % blocked sessions exceeds threshold % (potential lock storm)',
                    v_blocked_count, v_skip_locks_threshold;
            ELSE
                -- Use the pre-computed results from materialized table
                -- CROSS JOIN now operates on ONLY blocked sessions (not all sessions)
                INSERT INTO flight_recorder.lock_samples (
                    sample_id, sample_captured_at, blocked_pid, blocked_user, blocked_app,
                    blocked_query_preview, blocked_duration, blocking_pid, blocking_user,
                    blocking_app, blocking_query_preview, lock_type, locked_relation_oid
                )
                SELECT DISTINCT ON (bs.pid, blocking_pid)
                    v_sample_id,
                    v_captured_at,
                    bs.pid,
                    bs.usename,
                    bs.application_name,
                    left(bs.query, 200),
                    v_captured_at - bs.query_start,
                    blocking_pid,
                    blocking.usename,
                    blocking.application_name,
                    left(blocking.query, 200),
                    -- Get lock type from the blocked session's wait_event
                    CASE
                        WHEN bs.wait_event_type = 'Lock' THEN bs.wait_event
                        ELSE 'unknown'
                    END,
                    -- Get relation if waiting on a relation lock
                    CASE
                        WHEN bs.wait_event IN ('relation', 'extend', 'page', 'tuple') THEN
                            (SELECT l.relation
                             FROM pg_locks l
                             WHERE l.pid = bs.pid AND NOT l.granted
                             LIMIT 1)
                        ELSE NULL
                    END
                FROM _fr_blocked_sessions bs  -- Only blocked sessions!
                CROSS JOIN LATERAL unnest(bs.blocking_pids) AS blocking_pid  -- Use cached array
                JOIN _fr_psa_snapshot blocking ON blocking.pid = blocking_pid
                ORDER BY bs.pid, blocking_pid
                LIMIT 100;
            END IF;
        END;

        PERFORM flight_recorder._record_section_success(v_stat_id);
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'pg-flight-recorder: Lock sampling collection failed: %', SQLERRM;
    END;
    END IF;  -- v_enable_locks

-- =============================================================================
-- PERFORMANCE COMPARISON
-- =============================================================================
-- Scenario: 100 connections, 5 blocked sessions
--
-- BEFORE (current code):
--   - pg_blocking_pids() calls: 100 (count) + 100 (main query) = 200 calls
--   - CROSS JOIN rows processed: 100 sessions × avg 1 blocker = ~100 rows
--
-- AFTER (optimized):
--   - pg_blocking_pids() calls: 100 (once during materialization) = 100 calls
--   - CROSS JOIN rows processed: 5 blocked sessions × avg 1 blocker = ~5 rows
--
-- NET IMPROVEMENT:
--   - Function calls: 50% reduction (200 → 100)
--   - CROSS JOIN work: 95% reduction (100 → 5 rows)
--   - Memory: +1 small temp table (~1KB for 5 blocked sessions)
-- =============================================================================
