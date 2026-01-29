-- =============================================================================
-- Migration: 2.6 to 2.7
-- =============================================================================
-- Description: Autovacuum Observer Enhancements
--
-- Changes:
--   - Add n_mod_since_analyze column to table_snapshots
--   - Add table_stats_mode config parameter
--   - Add table_stats_activity_threshold config parameter
--   - Add rate calculation functions for autovacuum analysis
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
    v_expected TEXT := '2.6';
    v_target TEXT := '2.7';
BEGIN
    SELECT value INTO v_current
    FROM flight_recorder.config WHERE key = 'schema_version';

    IF v_current IS NULL THEN
        RAISE EXCEPTION 'schema_version not found. Is Flight Recorder installed?';
    END IF;

    IF v_current != v_expected THEN
        RAISE EXCEPTION 'Migration 2.6->2.7 requires version %, found %', v_expected, v_current;
    END IF;

    RAISE NOTICE 'Migrating from % to %...', v_expected, v_target;
END $$;

-- =============================================================================
-- Step 2: Schema Changes (Data Preserving)
-- =============================================================================

-- Add n_mod_since_analyze column to table_snapshots
ALTER TABLE flight_recorder.table_snapshots
    ADD COLUMN IF NOT EXISTS n_mod_since_analyze BIGINT;

-- =============================================================================
-- Step 3: Configuration Parameters
-- =============================================================================

-- Add new config parameters for sampling strategy
INSERT INTO flight_recorder.config (key, value)
VALUES
    ('table_stats_mode', 'top_n'),
    ('table_stats_activity_threshold', '0')
ON CONFLICT (key) DO NOTHING;

-- =============================================================================
-- Step 4: Rate Calculation Functions
-- =============================================================================

-- Calculates the rate of dead tuple accumulation over a time window
CREATE OR REPLACE FUNCTION flight_recorder.dead_tuple_growth_rate(
    p_relid OID,
    p_window INTERVAL
)
RETURNS NUMERIC
LANGUAGE plpgsql STABLE AS $$
DECLARE
    v_first_snapshot RECORD;
    v_last_snapshot RECORD;
    v_delta_tuples BIGINT;
    v_delta_seconds NUMERIC;
BEGIN
    SELECT ts.n_dead_tup, s.captured_at
    INTO v_first_snapshot
    FROM flight_recorder.table_snapshots ts
    JOIN flight_recorder.snapshots s ON s.id = ts.snapshot_id
    WHERE ts.relid = p_relid
      AND s.captured_at >= now() - p_window
    ORDER BY s.captured_at ASC
    LIMIT 1;

    SELECT ts.n_dead_tup, s.captured_at
    INTO v_last_snapshot
    FROM flight_recorder.table_snapshots ts
    JOIN flight_recorder.snapshots s ON s.id = ts.snapshot_id
    WHERE ts.relid = p_relid
      AND s.captured_at >= now() - p_window
    ORDER BY s.captured_at DESC
    LIMIT 1;

    IF v_first_snapshot.captured_at IS NULL OR v_last_snapshot.captured_at IS NULL
       OR v_first_snapshot.captured_at = v_last_snapshot.captured_at THEN
        RETURN NULL;
    END IF;

    v_delta_tuples := COALESCE(v_last_snapshot.n_dead_tup, 0) - COALESCE(v_first_snapshot.n_dead_tup, 0);
    v_delta_seconds := EXTRACT(EPOCH FROM (v_last_snapshot.captured_at - v_first_snapshot.captured_at));

    IF v_delta_seconds <= 0 THEN
        RETURN NULL;
    END IF;

    RETURN ROUND(v_delta_tuples::numeric / v_delta_seconds, 4);
END;
$$;
COMMENT ON FUNCTION flight_recorder.dead_tuple_growth_rate(OID, INTERVAL) IS 'Returns dead tuple growth rate (tuples/second) for a table over a time window';

-- Calculates the rate of row modifications over a time window
CREATE OR REPLACE FUNCTION flight_recorder.modification_rate(
    p_relid OID,
    p_window INTERVAL
)
RETURNS NUMERIC
LANGUAGE plpgsql STABLE AS $$
DECLARE
    v_first_snapshot RECORD;
    v_last_snapshot RECORD;
    v_delta_mods BIGINT;
    v_delta_seconds NUMERIC;
BEGIN
    SELECT ts.n_tup_ins, ts.n_tup_upd, ts.n_tup_del, s.captured_at
    INTO v_first_snapshot
    FROM flight_recorder.table_snapshots ts
    JOIN flight_recorder.snapshots s ON s.id = ts.snapshot_id
    WHERE ts.relid = p_relid
      AND s.captured_at >= now() - p_window
    ORDER BY s.captured_at ASC
    LIMIT 1;

    SELECT ts.n_tup_ins, ts.n_tup_upd, ts.n_tup_del, s.captured_at
    INTO v_last_snapshot
    FROM flight_recorder.table_snapshots ts
    JOIN flight_recorder.snapshots s ON s.id = ts.snapshot_id
    WHERE ts.relid = p_relid
      AND s.captured_at >= now() - p_window
    ORDER BY s.captured_at DESC
    LIMIT 1;

    IF v_first_snapshot.captured_at IS NULL OR v_last_snapshot.captured_at IS NULL
       OR v_first_snapshot.captured_at = v_last_snapshot.captured_at THEN
        RETURN NULL;
    END IF;

    v_delta_mods := (COALESCE(v_last_snapshot.n_tup_ins, 0) + COALESCE(v_last_snapshot.n_tup_upd, 0) + COALESCE(v_last_snapshot.n_tup_del, 0))
                  - (COALESCE(v_first_snapshot.n_tup_ins, 0) + COALESCE(v_first_snapshot.n_tup_upd, 0) + COALESCE(v_first_snapshot.n_tup_del, 0));
    v_delta_seconds := EXTRACT(EPOCH FROM (v_last_snapshot.captured_at - v_first_snapshot.captured_at));

    IF v_delta_seconds <= 0 THEN
        RETURN NULL;
    END IF;

    RETURN ROUND(v_delta_mods::numeric / v_delta_seconds, 4);
END;
$$;
COMMENT ON FUNCTION flight_recorder.modification_rate(OID, INTERVAL) IS 'Returns row modification rate (modifications/second) for a table over a time window';

-- Calculates the HOT update ratio for a table
CREATE OR REPLACE FUNCTION flight_recorder.hot_update_ratio(
    p_relid OID
)
RETURNS NUMERIC
LANGUAGE plpgsql STABLE AS $$
DECLARE
    v_n_tup_upd BIGINT;
    v_n_tup_hot_upd BIGINT;
BEGIN
    SELECT ts.n_tup_upd, ts.n_tup_hot_upd
    INTO v_n_tup_upd, v_n_tup_hot_upd
    FROM flight_recorder.table_snapshots ts
    JOIN flight_recorder.snapshots s ON s.id = ts.snapshot_id
    WHERE ts.relid = p_relid
    ORDER BY s.captured_at DESC
    LIMIT 1;

    IF v_n_tup_upd IS NULL OR v_n_tup_upd = 0 THEN
        RETURN NULL;
    END IF;

    RETURN ROUND((COALESCE(v_n_tup_hot_upd, 0)::numeric / v_n_tup_upd) * 100, 2);
END;
$$;
COMMENT ON FUNCTION flight_recorder.hot_update_ratio(OID) IS 'Returns HOT update percentage (0-100) for a table based on latest snapshot';

-- Estimates time until dead tuple budget is exhausted
CREATE OR REPLACE FUNCTION flight_recorder.time_to_budget_exhaustion(
    p_relid OID,
    p_budget BIGINT
)
RETURNS INTERVAL
LANGUAGE plpgsql STABLE AS $$
DECLARE
    v_current_dead_tuples BIGINT;
    v_growth_rate NUMERIC;
    v_remaining_budget BIGINT;
    v_seconds_to_exhaustion NUMERIC;
BEGIN
    SELECT ts.n_dead_tup
    INTO v_current_dead_tuples
    FROM flight_recorder.table_snapshots ts
    JOIN flight_recorder.snapshots s ON s.id = ts.snapshot_id
    WHERE ts.relid = p_relid
    ORDER BY s.captured_at DESC
    LIMIT 1;

    IF v_current_dead_tuples IS NULL THEN
        RETURN NULL;
    END IF;

    v_growth_rate := flight_recorder.dead_tuple_growth_rate(p_relid, '1 hour'::interval);

    IF v_growth_rate IS NULL OR v_growth_rate <= 0 THEN
        RETURN NULL;
    END IF;

    v_remaining_budget := p_budget - v_current_dead_tuples;

    IF v_remaining_budget <= 0 THEN
        RETURN '0 seconds'::interval;
    END IF;

    v_seconds_to_exhaustion := v_remaining_budget::numeric / v_growth_rate;

    RETURN make_interval(secs => v_seconds_to_exhaustion);
END;
$$;
COMMENT ON FUNCTION flight_recorder.time_to_budget_exhaustion(OID, BIGINT) IS 'Estimates time until dead tuple budget is exhausted based on growth rate';

-- =============================================================================
-- Step 5: Update Version
-- =============================================================================
UPDATE flight_recorder.config
SET value = '2.7', updated_at = now()
WHERE key = 'schema_version';

COMMIT;

-- =============================================================================
-- Step 6: Post-migration verification
-- =============================================================================
DO $$
DECLARE
    v_version TEXT;
    v_n_mod_since_analyze_exists BOOLEAN;
    v_mode_exists BOOLEAN;
    v_threshold_exists BOOLEAN;
    v_dead_tuple_rate_exists BOOLEAN;
    v_mod_rate_exists BOOLEAN;
    v_hot_ratio_exists BOOLEAN;
    v_budget_exists BOOLEAN;
BEGIN
    SELECT value INTO v_version
    FROM flight_recorder.config WHERE key = 'schema_version';

    -- Verify n_mod_since_analyze column exists
    SELECT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'flight_recorder'
          AND table_name = 'table_snapshots'
          AND column_name = 'n_mod_since_analyze'
    ) INTO v_n_mod_since_analyze_exists;

    -- Verify config parameters exist
    SELECT EXISTS (
        SELECT 1 FROM flight_recorder.config WHERE key = 'table_stats_mode'
    ) INTO v_mode_exists;

    SELECT EXISTS (
        SELECT 1 FROM flight_recorder.config WHERE key = 'table_stats_activity_threshold'
    ) INTO v_threshold_exists;

    -- Verify functions exist
    SELECT EXISTS (
        SELECT 1 FROM pg_proc p
        JOIN pg_namespace n ON p.pronamespace = n.oid
        WHERE n.nspname = 'flight_recorder' AND p.proname = 'dead_tuple_growth_rate'
    ) INTO v_dead_tuple_rate_exists;

    SELECT EXISTS (
        SELECT 1 FROM pg_proc p
        JOIN pg_namespace n ON p.pronamespace = n.oid
        WHERE n.nspname = 'flight_recorder' AND p.proname = 'modification_rate'
    ) INTO v_mod_rate_exists;

    SELECT EXISTS (
        SELECT 1 FROM pg_proc p
        JOIN pg_namespace n ON p.pronamespace = n.oid
        WHERE n.nspname = 'flight_recorder' AND p.proname = 'hot_update_ratio'
    ) INTO v_hot_ratio_exists;

    SELECT EXISTS (
        SELECT 1 FROM pg_proc p
        JOIN pg_namespace n ON p.pronamespace = n.oid
        WHERE n.nspname = 'flight_recorder' AND p.proname = 'time_to_budget_exhaustion'
    ) INTO v_budget_exists;

    IF NOT v_n_mod_since_analyze_exists THEN
        RAISE WARNING 'n_mod_since_analyze column not found in table_snapshots';
    END IF;

    IF NOT v_mode_exists THEN
        RAISE WARNING 'table_stats_mode config parameter not found';
    END IF;

    IF NOT v_threshold_exists THEN
        RAISE WARNING 'table_stats_activity_threshold config parameter not found';
    END IF;

    IF NOT v_dead_tuple_rate_exists THEN
        RAISE WARNING 'dead_tuple_growth_rate function not found';
    END IF;

    IF NOT v_mod_rate_exists THEN
        RAISE WARNING 'modification_rate function not found';
    END IF;

    IF NOT v_hot_ratio_exists THEN
        RAISE WARNING 'hot_update_ratio function not found';
    END IF;

    IF NOT v_budget_exists THEN
        RAISE WARNING 'time_to_budget_exhaustion function not found';
    END IF;

    RAISE NOTICE '';
    RAISE NOTICE '=== Migration Complete ===';
    RAISE NOTICE 'Now at version: %', v_version;
    RAISE NOTICE 'Added autovacuum observer enhancements:';
    RAISE NOTICE '  - table_snapshots.n_mod_since_analyze (row modifications since last ANALYZE)';
    RAISE NOTICE '';
    RAISE NOTICE 'New configuration parameters:';
    RAISE NOTICE '  - table_stats_mode: Collection mode (top_n, all, threshold)';
    RAISE NOTICE '  - table_stats_activity_threshold: Minimum activity for threshold mode';
    RAISE NOTICE '';
    RAISE NOTICE 'New rate calculation functions:';
    RAISE NOTICE '  - dead_tuple_growth_rate(relid, window): Dead tuples/second';
    RAISE NOTICE '  - modification_rate(relid, window): Modifications/second';
    RAISE NOTICE '  - hot_update_ratio(relid): HOT update percentage';
    RAISE NOTICE '  - time_to_budget_exhaustion(relid, budget): Time until budget exceeded';
    RAISE NOTICE '';
END $$;
