#!/bin/bash
# DDL collision scenario - simplified and robust version
# Usage: ./ddl_collision_v2.sh <duration_seconds> <output_file>

set -e

DURATION=${1:-60}
OUTPUT_FILE=${2:-/tmp/ddl_timings.json}
PGHOST=${PGHOST:-localhost}
PGPORT=${PGPORT:-5432}
PGUSER=${PGUSER:-postgres}
PGDATABASE=${PGDATABASE:-postgres}

echo "Starting DDL collision test for ${DURATION} seconds"
echo "Output: ${OUTPUT_FILE}"

# Initialize output file
echo '{"ddl_operations": []}' > "${OUTPUT_FILE}"

# Create test table
psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -c "
    DROP TABLE IF EXISTS ddl_test_table CASCADE;
    CREATE TABLE ddl_test_table (
        id SERIAL PRIMARY KEY,
        data TEXT,
        created_at TIMESTAMPTZ DEFAULT now()
    );
    CREATE INDEX idx_ddl_test_data ON ddl_test_table(data);
" > /dev/null 2>&1

START_TIME=$(date +%s)
END_TIME=$((START_TIME + DURATION))
OPERATION_COUNT=0

echo "Running DDL operations until $(date -r $END_TIME '+%Y-%m-%d %H:%M:%S')"

while [ "$(date +%s)" -lt "$END_TIME" ]; do
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

    # Check for concurrent flight_recorder activity before DDL
    BLOCKERS=$(psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -qtA -c "
        SELECT string_agg(
            CASE
                WHEN query ILIKE '%flight_recorder%' THEN 'flight_recorder:' || pid::text
                ELSE 'other:' || pid::text
            END, ', ')
        FROM pg_stat_activity
        WHERE state = 'active'
        AND pid != pg_backend_pid()
        AND (query ILIKE '%pg_stat_activity%'
             OR query ILIKE '%pg_locks%'
             OR query ILIKE '%pg_class%'
             OR query ILIKE '%flight_recorder%');
    ")

    # Execute DDL with timing
    START_MS=$(date +%s%3N 2>/dev/null || echo $(($(date +%s) * 1000)))
    START_TIME=$(date -u +"%Y-%m-%d %H:%M:%S.%3N" 2>/dev/null || date -u +"%Y-%m-%d %H:%M:%S.000")

    psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -c "${DDL_CMD};" > /dev/null 2>&1

    END_MS=$(date +%s%3N 2>/dev/null || echo $(($(date +%s) * 1000)))
    END_TIME=$(date -u +"%Y-%m-%d %H:%M:%S.%3N" 2>/dev/null || date -u +"%Y-%m-%d %H:%M:%S.000")
    DURATION_MS=$((END_MS - START_MS))

    # Build JSON result
    RESULT=$(jq -n \
        --argjson op_id "${OPERATION_COUNT}" \
        --arg ddl_type "${DDL_TYPE}" \
        --arg start "${START_TIME}" \
        --arg end "${END_TIME}" \
        --argjson duration "${DURATION_MS}" \
        --arg blocked "${BLOCKERS}" \
        '{
            operation_id: $op_id,
            ddl_type: $ddl_type,
            start_time: $start,
            end_time: $end,
            duration_ms: $duration,
            was_blocked: ($blocked != ""),
            blocked_by: (if $blocked != "" then $blocked else null end),
            lock_wait_ms: (if $blocked != "" then $duration else null end)
        }'
    )

    # Append to output file using jq
    if [ -n "$RESULT" ]; then
        TMP_FILE=$(mktemp)
        jq ".ddl_operations += [${RESULT}]" "${OUTPUT_FILE}" > "${TMP_FILE}" && mv "${TMP_FILE}" "${OUTPUT_FILE}"
    fi

    # Progress indicator
    if [ $((OPERATION_COUNT % 10)) -eq 0 ]; then
        echo "  Completed ${OPERATION_COUNT} DDL operations..."
    fi

    # Small delay
    sleep 0.1
done

# Cleanup
psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -c "
    DROP TABLE IF EXISTS ddl_test_table CASCADE;
" > /dev/null 2>&1

echo "DDL collision test complete: ${OPERATION_COUNT} operations"
echo "Results written to: ${OUTPUT_FILE}"
