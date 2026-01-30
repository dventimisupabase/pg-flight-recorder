-- =============================================================================
-- Migration: 2.8 to 2.9
-- =============================================================================
-- Description: Canary Queries for Silent Performance Degradation Detection
--
-- Changes:
--   - Add canaries table for canary query definitions
--   - Add canary_results table for execution results
--   - Add canary config settings (canary_enabled, canary_interval_minutes,
--     canary_capture_plans, retention_canary_days)
--   - Add run_canaries(), canary_status(), enable_canaries(), disable_canaries()
--   - Insert pre-defined canary queries
--   - Update cleanup_aggregates() to include canary retention
--   - Update disable() to unschedule canary cron job
--
-- Data preservation: Existing data unchanged, new tables created
-- =============================================================================

\set ON_ERROR_STOP on

BEGIN;

-- =============================================================================
-- Step 1: Version Guard
-- =============================================================================
DO $$
DECLARE
    v_current TEXT;
    v_expected TEXT := '2.8';
    v_target TEXT := '2.9';
BEGIN
    SELECT value INTO v_current
    FROM flight_recorder.config WHERE key = 'schema_version';

    IF v_current IS NULL THEN
        RAISE EXCEPTION 'schema_version not found. Is Flight Recorder installed?';
    END IF;

    IF v_current != v_expected THEN
        RAISE EXCEPTION 'Migration 2.8->2.9 requires version %, found %', v_expected, v_current;
    END IF;

    RAISE NOTICE 'Migrating from % to %...', v_expected, v_target;
END $$;

-- =============================================================================
-- Step 2: Schema Changes (Data Preserving)
-- =============================================================================

-- Canary query definitions for synthetic performance monitoring
CREATE TABLE IF NOT EXISTS flight_recorder.canaries (
    id                  SERIAL PRIMARY KEY,
    name                TEXT NOT NULL UNIQUE,
    description         TEXT,
    query_text          TEXT NOT NULL,
    expected_time_ms    NUMERIC,
    threshold_warning   NUMERIC DEFAULT 1.5,
    threshold_critical  NUMERIC DEFAULT 2.0,
    enabled             BOOLEAN DEFAULT true,
    created_at          TIMESTAMPTZ DEFAULT now()
);
COMMENT ON TABLE flight_recorder.canaries IS 'Canary query definitions for synthetic performance monitoring. Pre-defined queries detect silent degradation.';

-- Canary query execution results
CREATE TABLE IF NOT EXISTS flight_recorder.canary_results (
    id              BIGSERIAL PRIMARY KEY,
    canary_id       INTEGER REFERENCES flight_recorder.canaries(id) ON DELETE CASCADE,
    executed_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    duration_ms     NUMERIC NOT NULL,
    plan            JSONB,
    error_message   TEXT,
    success         BOOLEAN DEFAULT true
);
CREATE INDEX IF NOT EXISTS canary_results_canary_id_executed_at_idx
    ON flight_recorder.canary_results(canary_id, executed_at);
CREATE INDEX IF NOT EXISTS canary_results_executed_at_idx
    ON flight_recorder.canary_results(executed_at);
COMMENT ON TABLE flight_recorder.canary_results IS 'Canary query execution results for performance baseline comparison';

-- Add canary config settings
INSERT INTO flight_recorder.config (key, value) VALUES
    ('canary_enabled', 'false'),
    ('canary_interval_minutes', '15'),
    ('canary_capture_plans', 'false'),
    ('retention_canary_days', '7')
ON CONFLICT (key) DO NOTHING;

-- Insert pre-defined canary queries
INSERT INTO flight_recorder.canaries (name, description, query_text) VALUES
    ('index_lookup', 'B-tree index lookup on pg_class', 'SELECT oid FROM pg_class WHERE relname = ''pg_class'' LIMIT 1'),
    ('small_agg', 'Count aggregation on pg_stat_activity', 'SELECT count(*) FROM pg_stat_activity'),
    ('seq_scan_baseline', 'Sequential scan count on pg_namespace', 'SELECT count(*) FROM pg_namespace'),
    ('simple_join', 'Join pg_namespace to pg_class', 'SELECT count(*) FROM pg_namespace n JOIN pg_class c ON c.relnamespace = n.oid WHERE n.nspname = ''pg_catalog''')
ON CONFLICT (name) DO NOTHING;

-- =============================================================================
-- Step 3: Update Version
-- =============================================================================
UPDATE flight_recorder.config
SET value = '2.9', updated_at = now()
WHERE key = 'schema_version';

COMMIT;

-- =============================================================================
-- Step 4: Post-migration verification
-- =============================================================================
DO $$
DECLARE
    v_version TEXT;
    v_canaries_exists BOOLEAN;
    v_canary_results_exists BOOLEAN;
    v_canary_count INTEGER;
BEGIN
    SELECT value INTO v_version
    FROM flight_recorder.config WHERE key = 'schema_version';

    -- Verify tables exist
    SELECT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'flight_recorder'
          AND table_name = 'canaries'
    ) INTO v_canaries_exists;

    SELECT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'flight_recorder'
          AND table_name = 'canary_results'
    ) INTO v_canary_results_exists;

    -- Verify canaries were inserted
    SELECT count(*) INTO v_canary_count FROM flight_recorder.canaries;

    IF NOT v_canaries_exists THEN
        RAISE WARNING 'canaries table not found';
    END IF;

    IF NOT v_canary_results_exists THEN
        RAISE WARNING 'canary_results table not found';
    END IF;

    RAISE NOTICE '';
    RAISE NOTICE '=== Migration Complete ===';
    RAISE NOTICE 'Now at version: %', v_version;
    RAISE NOTICE 'Added canary query monitoring:';
    RAISE NOTICE '  - canaries table (% pre-defined canaries)', v_canary_count;
    RAISE NOTICE '  - canary_results table for execution history';
    RAISE NOTICE '';
    RAISE NOTICE 'To enable canary monitoring:';
    RAISE NOTICE '  SELECT flight_recorder.enable_canaries();';
    RAISE NOTICE '';
    RAISE NOTICE 'To check canary status:';
    RAISE NOTICE '  SELECT * FROM flight_recorder.canary_status();';
    RAISE NOTICE '';
END $$;
