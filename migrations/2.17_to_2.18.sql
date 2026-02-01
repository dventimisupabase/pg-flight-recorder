-- =============================================================================
-- Migration: 2.17 to 2.18
-- =============================================================================
-- Description: Version bump for SQLite export enhancements (now removed)
--
-- This migration originally enhanced the export_sql() function. Since that
-- function has been removed in 2.19, this migration is now a no-op that
-- simply advances the version number.
-- =============================================================================

\set ON_ERROR_STOP on

BEGIN;

DO $$
DECLARE
    v_current TEXT;
BEGIN
    SELECT value INTO v_current FROM flight_recorder.config WHERE key = 'schema_version';
    IF v_current != '2.17' THEN
        RAISE EXCEPTION 'Migration 2.17->2.18 requires version 2.17, found %', v_current;
    END IF;
    RAISE NOTICE 'Migrating from 2.17 to 2.18...';
END $$;

-- No schema changes - this was a function-only update that is now removed

UPDATE flight_recorder.config SET value = '2.18', updated_at = now()
WHERE key = 'schema_version';

COMMIT;

DO $$
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE '=== Migration Complete ===';
    RAISE NOTICE 'Now at version: 2.18';
    RAISE NOTICE '';
END $$;
