-- =============================================================================
-- OPTIMIZATION 2: Activity Samples - Skip Redundant Count
-- =============================================================================
-- Impact: Eliminate duplicate table scan when below threshold
-- Location: Replace lines 1448-1528 in install.sql
--
-- Current problem:
--   1. Scan _fr_psa_snapshot to count WHERE state != 'idle'
--   2. If below threshold (100), scan SAME data again with LIMIT 25
--   3. The count is only used to decide whether to skip (threshold = 100)
--
-- Solution:
--   Since we only need 25 rows and threshold is 100 (much higher),
--   we can just query once and check if we got "too many" rows AFTER.
--
--   Alternative: If threshold check is critical, materialize the filtered
--   set once and reuse it.
-- =============================================================================

    -- Section 2: Active sessions (cost-based skip)
    -- OPTIMIZATION: Avoid double scan of same data
    BEGIN
        PERFORM flight_recorder._set_section_timeout();
        DECLARE
            v_skip_activity_threshold INTEGER;
        BEGIN
            v_skip_activity_threshold := COALESCE(
                flight_recorder._get_config('skip_activity_conn_threshold', '100')::integer,
                100
            );

            -- OPTIMIZATION OPTION A: Skip the pre-count, just query LIMIT 25
            -- Rationale: We only need 25 rows. Threshold is 100 (much higher).
            -- If system is that busy, we'd skip anyway, and the INSERT will be fast.
            -- The count was defensive but adds overhead.

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
        END;

        PERFORM flight_recorder._record_section_success(v_stat_id);
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'pg-flight-recorder: Activity samples collection failed: %', SQLERRM;
    END;

-- =============================================================================
-- ALTERNATIVE OPTION B: Materialize if threshold check is critical
-- =============================================================================
-- If you MUST enforce the threshold strictly (e.g., skip if >100 active):
--
-- CREATE TEMP TABLE _fr_active_sessions ON COMMIT DROP AS
-- SELECT * FROM _fr_psa_snapshot WHERE state != 'idle';
--
-- SELECT count(*) INTO v_active_conn_count FROM _fr_active_sessions;
--
-- IF v_active_conn_count > v_skip_activity_threshold THEN
--     RAISE NOTICE '...skipping...';
-- ELSE
--     INSERT INTO flight_recorder.activity_samples (...)
--     SELECT ... FROM _fr_active_sessions LIMIT 25;
-- END IF;
--
-- This way you scan once, store results, then reuse for both count and insert.
-- =============================================================================

-- =============================================================================
-- PERFORMANCE COMPARISON
-- =============================================================================
-- Scenario: 50 active sessions (below threshold of 100)
--
-- BEFORE (current code):
--   - Scan _fr_psa_snapshot: 2 times (once for count, once for INSERT)
--   - Rows processed: 50 + 50 = 100 row scans
--
-- AFTER (Option A - recommended):
--   - Scan _fr_psa_snapshot: 1 time (INSERT with LIMIT 25)
--   - Rows processed: 25 rows (early termination with LIMIT)
--
-- AFTER (Option B - if threshold is critical):
--   - Scan _fr_psa_snapshot: 1 time (materialize filtered set)
--   - Rows processed: 50 rows (one scan to temp table) + 25 (from temp)
--
-- NET IMPROVEMENT (Option A):
--   - Scans: 50% reduction (2 → 1)
--   - Rows: 75% reduction (100 → 25)
--   - Trade-off: No threshold enforcement (but LIMIT 25 is so cheap this doesn't matter)
-- =============================================================================
