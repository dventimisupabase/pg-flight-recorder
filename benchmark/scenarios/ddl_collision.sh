#!/bin/bash
# DDL collision scenario - ultra-simple version that definitely works
# Usage: ./ddl_collision_simple.sh <duration_seconds> <output_file>

set -e

DURATION=${1:-60}
OUTPUT_FILE=${2:-/tmp/ddl_timings.json}
PGHOST=${PGHOST:-localhost}
PGPORT=${PGPORT:-5432}
PGUSER=${PGUSER:-postgres}
PGDATABASE=${PGDATABASE:-postgres}

echo "Starting DDL collision test for ${DURATION} seconds"
echo "Output: ${OUTPUT_FILE}"

# Create test table
psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" > /dev/null 2>&1 <<'SQL'
    DROP TABLE IF EXISTS ddl_test_table CASCADE;
    CREATE TABLE ddl_test_table (
        id SERIAL PRIMARY KEY,
        data TEXT,
        created_at TIMESTAMPTZ DEFAULT now()
    );
    CREATE INDEX idx_ddl_test_data ON ddl_test_table(data);
SQL

# Create results table to store timings (not TEMP - we need it across sessions)
psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" > /dev/null 2>&1 <<'SQL'
    DROP TABLE IF EXISTS ddl_timing_results;
    CREATE UNLOGGED TABLE ddl_timing_results (
        operation_id INTEGER,
        ddl_type TEXT,
        start_time TIMESTAMPTZ,
        end_time TIMESTAMPTZ,
        duration_ms NUMERIC,
        was_blocked BOOLEAN,
        blocked_by TEXT
    );
SQL

START_TIME=$(date +%s)
END_TIME=$((START_TIME + DURATION))
OPERATION_COUNT=0

echo "Running DDL operations until $(date -r $END_TIME '+%Y-%m-%d %H:%M:%S' 2>/dev/null || date -d @$END_TIME '+%Y-%m-%d %H:%M:%S')"

while [ $(date +%s) -lt $END_TIME ]; do
    OPERATION_COUNT=$((OPERATION_COUNT + 1))

    # Rotate through different DDL operations
    case $((OPERATION_COUNT % 6)) in
        0)
            DDL_TYPE="ALTER_ADD_COLUMN"
            DDL_CMD="ALTER TABLE ddl_test_table ADD COLUMN IF NOT EXISTS col_${OPERATION_COUNT} TEXT"
            ;;
        1)
            DDL_TYPE="ALTER_DROP_COLUMN"
            DROP_COL=$((OPERATION_COUNT - 6))
            DDL_CMD="ALTER TABLE ddl_test_table DROP COLUMN IF EXISTS col_${DROP_COL}"
            ;;
        2)
            DDL_TYPE="CREATE_INDEX"
            DDL_CMD="CREATE INDEX IF NOT EXISTS idx_ddl_test_${OPERATION_COUNT} ON ddl_test_table(id)"
            ;;
        3)
            DDL_TYPE="DROP_INDEX"
            DROP_IDX=$((OPERATION_COUNT - 6))
            DDL_CMD="DROP INDEX IF EXISTS idx_ddl_test_${DROP_IDX}"
            ;;
        4)
            DDL_TYPE="ALTER_TYPE"
            DDL_CMD="ALTER TABLE ddl_test_table ALTER COLUMN data TYPE TEXT"
            ;;
        5)
            DDL_TYPE="VACUUM"
            DDL_CMD="VACUUM ddl_test_table"
            ;;
    esac

    # Execute DDL with timing captured in PostgreSQL
    psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" > /dev/null 2>&1 <<SQL
        DO \$\$
        DECLARE
            v_start TIMESTAMPTZ;
            v_end TIMESTAMPTZ;
            v_duration NUMERIC;
            v_blockers TEXT;
        BEGIN
            -- Check for flight recorder activity
            SELECT string_agg(
                CASE
                    WHEN query ILIKE '%flight_recorder%' THEN 'flight_recorder:' || pid::text
                    ELSE 'other:' || pid::text
                END, ', ')
            INTO v_blockers
            FROM pg_stat_activity
            WHERE state = 'active'
            AND pid != pg_backend_pid()
            AND (query ILIKE '%pg_stat_activity%'
                 OR query ILIKE '%pg_locks%'
                 OR query ILIKE '%pg_class%'
                 OR query ILIKE '%flight_recorder%');

            v_start := clock_timestamp();

            -- Execute DDL
            EXECUTE '${DDL_CMD}';

            v_end := clock_timestamp();
            v_duration := EXTRACT(EPOCH FROM (v_end - v_start)) * 1000;

            -- Store result
            INSERT INTO ddl_timing_results VALUES (
                ${OPERATION_COUNT},
                '${DDL_TYPE}',
                v_start,
                v_end,
                v_duration,
                v_blockers IS NOT NULL,
                v_blockers
            );
        END;
        \$\$;
SQL

    # Progress indicator
    if [ $((OPERATION_COUNT % 10)) -eq 0 ]; then
        echo "  Completed ${OPERATION_COUNT} DDL operations..."
    fi

    sleep 0.1
done

echo ""
echo "DDL collision test complete: ${OPERATION_COUNT} operations"
echo "Extracting results to JSON..."

# Export results to JSON
psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -qtA -c "
    SELECT json_build_object(
        'ddl_operations',
        json_agg(
            json_build_object(
                'operation_id', operation_id,
                'ddl_type', ddl_type,
                'start_time', to_char(start_time, 'YYYY-MM-DD HH24:MI:SS.MS'),
                'end_time', to_char(end_time, 'YYYY-MM-DD HH24:MI:SS.MS'),
                'duration_ms', round(duration_ms, 2),
                'was_blocked', was_blocked,
                'blocked_by', blocked_by,
                'lock_wait_ms', CASE WHEN was_blocked THEN round(duration_ms, 2) ELSE NULL END
            )
            ORDER BY operation_id
        )
    )
    FROM ddl_timing_results;
" > "${OUTPUT_FILE}"

# Cleanup
psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" > /dev/null 2>&1 <<'SQL'
    DROP TABLE IF EXISTS ddl_test_table CASCADE;
    DROP TABLE IF EXISTS ddl_timing_results;
SQL

echo "Results written to: ${OUTPUT_FILE}"
