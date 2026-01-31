-- =============================================================================
-- Migration: 2.16 to 2.17
-- =============================================================================
-- Description: Buffer-Based Performance Metrics
--
-- Replaces timing-based behavior with buffer-processing metrics for more
-- stable, predictable performance analysis. Buffer counts are deterministic;
-- timing varies with system load.
--
-- Schema changes:
--   - canary_results: Add shared_blks_hit, shared_blks_read, temp_blks_read,
--                     temp_blks_written, total_buffers columns
--   - query_regressions: Add baseline_avg_buffers, current_avg_buffers,
--                        buffer_change_pct, detection_metric columns
--
-- Config additions:
--   - statements_ranking_metric: 'buffers' (default) or 'time'
--   - regression_detection_metric: 'buffers' (default) or 'time'
--   - canary_comparison_metric: 'buffers' (default) or 'time'
--
-- NOTE: After running this migration, reinstall install.sql to get updated functions.
-- =============================================================================

\set ON_ERROR_STOP on

BEGIN;

-- =============================================================================
-- Step 1: Version Guard
-- =============================================================================
DO $$
DECLARE
    v_current TEXT;
    v_expected TEXT := '2.16';
    v_target TEXT := '2.17';
BEGIN
    SELECT value INTO v_current
    FROM flight_recorder.config WHERE key = 'schema_version';

    IF v_current IS NULL THEN
        RAISE EXCEPTION 'schema_version not found. Is Flight Recorder installed?';
    END IF;

    IF v_current != v_expected THEN
        RAISE EXCEPTION 'Migration 2.16->2.17 requires version %, found %', v_expected, v_current;
    END IF;

    RAISE NOTICE 'Migrating from % to %...', v_expected, v_target;
END $$;

-- =============================================================================
-- Step 2: Schema Changes
-- =============================================================================

-- Add buffer columns to canary_results
ALTER TABLE flight_recorder.canary_results
    ADD COLUMN IF NOT EXISTS shared_blks_hit BIGINT,
    ADD COLUMN IF NOT EXISTS shared_blks_read BIGINT,
    ADD COLUMN IF NOT EXISTS temp_blks_read BIGINT,
    ADD COLUMN IF NOT EXISTS temp_blks_written BIGINT,
    ADD COLUMN IF NOT EXISTS total_buffers BIGINT;

COMMENT ON COLUMN flight_recorder.canary_results.shared_blks_hit IS 'Shared buffer cache hits during canary execution';
COMMENT ON COLUMN flight_recorder.canary_results.shared_blks_read IS 'Shared buffer cache misses (disk reads) during canary execution';
COMMENT ON COLUMN flight_recorder.canary_results.temp_blks_read IS 'Temporary file blocks read during canary execution';
COMMENT ON COLUMN flight_recorder.canary_results.temp_blks_written IS 'Temporary file blocks written during canary execution';
COMMENT ON COLUMN flight_recorder.canary_results.total_buffers IS 'Total buffer operations (shared_blks_hit + shared_blks_read + temp_blks_read + temp_blks_written)';

-- Add buffer columns to query_regressions
ALTER TABLE flight_recorder.query_regressions
    ADD COLUMN IF NOT EXISTS baseline_avg_buffers NUMERIC,
    ADD COLUMN IF NOT EXISTS current_avg_buffers NUMERIC,
    ADD COLUMN IF NOT EXISTS buffer_change_pct NUMERIC,
    ADD COLUMN IF NOT EXISTS detection_metric TEXT DEFAULT 'buffers';

COMMENT ON COLUMN flight_recorder.query_regressions.baseline_avg_buffers IS 'Average total buffer operations during baseline period';
COMMENT ON COLUMN flight_recorder.query_regressions.current_avg_buffers IS 'Average total buffer operations during recent period';
COMMENT ON COLUMN flight_recorder.query_regressions.buffer_change_pct IS 'Percentage change in buffer operations (positive = regression)';
COMMENT ON COLUMN flight_recorder.query_regressions.detection_metric IS 'Metric used for detection: ''buffers'' (default) or ''time''';

-- =============================================================================
-- Step 3: Configuration Updates
-- =============================================================================

-- Add new config options (default to buffers for deterministic behavior)
INSERT INTO flight_recorder.config (key, value)
VALUES
    ('statements_ranking_metric', 'buffers'),
    ('regression_detection_metric', 'buffers'),
    ('canary_comparison_metric', 'buffers')
ON CONFLICT (key) DO NOTHING;

-- =============================================================================
-- Step 4: Update Version
-- =============================================================================
UPDATE flight_recorder.config
SET value = '2.17', updated_at = now()
WHERE key = 'schema_version';

COMMIT;

-- =============================================================================
-- Step 5: Post-migration verification
-- =============================================================================
DO $$
DECLARE
    v_version TEXT;
BEGIN
    SELECT value INTO v_version
    FROM flight_recorder.config WHERE key = 'schema_version';

    RAISE NOTICE '';
    RAISE NOTICE '=== Migration Complete ===';
    RAISE NOTICE 'Now at version: %', v_version;
    RAISE NOTICE '';
    RAISE NOTICE 'Buffer-based performance metrics are now the default.';
    RAISE NOTICE '';
    RAISE NOTICE 'New columns added:';
    RAISE NOTICE '  canary_results: shared_blks_hit, shared_blks_read, temp_blks_read,';
    RAISE NOTICE '                  temp_blks_written, total_buffers';
    RAISE NOTICE '  query_regressions: baseline_avg_buffers, current_avg_buffers,';
    RAISE NOTICE '                     buffer_change_pct, detection_metric';
    RAISE NOTICE '';
    RAISE NOTICE 'New config options (default to ''buffers''):';
    RAISE NOTICE '  - statements_ranking_metric';
    RAISE NOTICE '  - regression_detection_metric';
    RAISE NOTICE '  - canary_comparison_metric';
    RAISE NOTICE '';
    RAISE NOTICE 'IMPORTANT: Reinstall install.sql to get the updated functions:';
    RAISE NOTICE '  psql -f install.sql';
    RAISE NOTICE '';
    RAISE NOTICE 'To revert to timing-based behavior:';
    RAISE NOTICE '  UPDATE flight_recorder.config SET value = ''time''';
    RAISE NOTICE '  WHERE key IN (''statements_ranking_metric'',';
    RAISE NOTICE '                ''regression_detection_metric'',';
    RAISE NOTICE '                ''canary_comparison_metric'');';
    RAISE NOTICE '';
END $$;
