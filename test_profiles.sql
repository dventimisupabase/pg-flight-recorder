-- =============================================================================
-- Quick validation script for configuration profiles
-- =============================================================================
-- Run with: psql -f test_profiles.sql
-- =============================================================================

\echo '=== Testing Configuration Profiles ==='
\echo ''

\echo '1. List all available profiles:'
SELECT profile_name, overhead_level, sample_interval
FROM flight_recorder.list_profiles()
ORDER BY profile_name;
\echo ''

\echo '2. Explain default profile (should show current settings):'
SELECT setting_key, current_value, profile_value, will_change
FROM flight_recorder.explain_profile('default')
WHERE will_change = true
LIMIT 5;
\echo ''

\echo '3. Get current profile match:'
SELECT * FROM flight_recorder.get_current_profile();
\echo ''

\echo '4. Apply production_safe profile:'
SELECT setting_key, old_value, new_value, changed
FROM flight_recorder.apply_profile('production_safe')
WHERE changed = true
LIMIT 10;
\echo ''

\echo '5. Verify changes were applied:'
SELECT key, value
FROM flight_recorder.config
WHERE key IN ('sample_interval_seconds', 'enable_locks', 'load_shedding_active_pct')
ORDER BY key;
\echo ''

\echo '6. Check current profile again (should be production_safe):'
SELECT closest_profile, match_percentage, recommendation
FROM flight_recorder.get_current_profile();
\echo ''

\echo '7. Switch to troubleshooting profile:'
SELECT setting_key, new_value, changed
FROM flight_recorder.apply_profile('troubleshooting')
WHERE changed = true
LIMIT 5;
\echo ''

\echo '8. Verify troubleshooting settings:'
SELECT key, value
FROM flight_recorder.config
WHERE key IN ('sample_interval_seconds', 'adaptive_sampling', 'load_shedding_enabled')
ORDER BY key;
\echo ''

\echo '9. Test invalid profile (should error):'
\set ON_ERROR_STOP off
SELECT * FROM flight_recorder.apply_profile('nonexistent_profile');
\set ON_ERROR_STOP on
\echo ''

\echo '10. Reset to default profile:'
SELECT count(*) as settings_changed
FROM flight_recorder.apply_profile('default')
WHERE changed = true;
\echo ''

\echo '=== Profile Testing Complete ==='
