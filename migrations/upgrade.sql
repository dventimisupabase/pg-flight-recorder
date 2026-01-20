-- =============================================================================
-- pg-flight-recorder upgrade framework
-- =============================================================================
-- Detects current version and runs necessary migrations.
-- Run with: psql -f migrations/upgrade.sql
-- =============================================================================

\set ON_ERROR_STOP on

-- Detect current version and validate installation
DO $$
DECLARE
    v_current_version TEXT;
    v_target_version TEXT := '2.0';  -- Update this when adding migrations
BEGIN
    -- Check if flight_recorder schema exists
    IF NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'flight_recorder') THEN
        RAISE EXCEPTION E'\n\nFlight Recorder is not installed.\nRun: psql -f install.sql\n';
    END IF;

    -- Check if config table exists
    IF NOT EXISTS (
        SELECT 1 FROM pg_tables
        WHERE schemaname = 'flight_recorder' AND tablename = 'config'
    ) THEN
        RAISE EXCEPTION E'\n\nFlight Recorder config table not found.\nThis installation may be corrupted. Consider reinstalling.\n';
    END IF;

    -- Get current version
    SELECT value INTO v_current_version
    FROM flight_recorder.config WHERE key = 'schema_version';

    IF v_current_version IS NULL THEN
        -- Pre-versioning installation detected
        RAISE NOTICE E'\n=== Pre-versioning installation detected ===';
        RAISE NOTICE 'Adding schema_version tracking...';
        INSERT INTO flight_recorder.config (key, value) VALUES ('schema_version', '1.0')
        ON CONFLICT (key) DO UPDATE SET value = '1.0', updated_at = now();
        v_current_version := '1.0';
    END IF;

    RAISE NOTICE E'\n=== Flight Recorder Upgrade ===';
    RAISE NOTICE 'Current version: %', v_current_version;
    RAISE NOTICE 'Target version:  %', v_target_version;

    IF v_current_version = v_target_version THEN
        RAISE NOTICE 'Already at target version. No migration needed.';
        RAISE NOTICE E'===\n';
        RETURN;
    END IF;

    RAISE NOTICE 'Migrations will be applied...';
    RAISE NOTICE E'===\n';
END $$;

-- =============================================================================
-- Migration chain
-- =============================================================================
-- Each migration file is responsible for:
-- 1. Checking its required source version
-- 2. Making schema changes (preserving data!)
-- 3. Updating the schema_version
--
-- Add new migrations here as they're created:
-- \i migrations/1.0_to_1.1.sql
-- \i migrations/1.1_to_2.0.sql
-- =============================================================================

-- Currently no migrations needed (fresh 2.0 installations)
-- When you need to migrate from 2.0 to 2.1, add:
-- \i migrations/2.0_to_2.1.sql

-- =============================================================================
-- Post-upgrade verification
-- =============================================================================
DO $$
DECLARE
    v_final_version TEXT;
BEGIN
    SELECT value INTO v_final_version
    FROM flight_recorder.config WHERE key = 'schema_version';

    RAISE NOTICE E'\n=== Upgrade Complete ===';
    RAISE NOTICE 'Final version: %', v_final_version;
    RAISE NOTICE E'===\n';
END $$;
