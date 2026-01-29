#!/usr/bin/env bash
set -Eeuo pipefail
IFS=$'\n\t'

# measure_storage.sh: Track storage growth of flight_recorder tables
# Measures row sizes, projections, and actual growth over time

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULTS_DIR="${SCRIPT_DIR}/results/storage_$(date +%Y%m%d_%H%M%S)"

# Configuration
DURATION_HOURS=${DURATION_HOURS:-4}
SAMPLE_INTERVAL=${SAMPLE_INTERVAL:-300}  # 5 minutes

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

log() { echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $*"; }
info() { echo -e "${BLUE}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $*"; }

check_prerequisites() {
    log "Checking prerequisites..."

    if ! psql -c "select 1" &> /dev/null; then
        echo "ERROR: Cannot connect to PostgreSQL" >&2
        exit 1
    fi

    if ! psql -tAc "select 1 from pg_namespace where nspname = 'flight_recorder'" | grep -q 1; then
        echo "ERROR: flight_recorder not installed" >&2
        exit 1
    fi

    log "Prerequisites OK"
}

measure_storage() {
    local output_file="$1"
    local timestamp
    timestamp=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

    psql -tAF',' -c "
        select
            '${timestamp}' as captured_at,
            c.relname as table_name,
            c.reltuples::int8 as row_count,
            pg_relation_size(c.oid) as heap_size_bytes,
            pg_indexes_size(c.oid) as index_size_bytes,
            pg_total_relation_size(c.oid) as total_size_bytes
        from pg_class as c
        inner join pg_namespace as n
            on n.oid = c.relnamespace
        where
            n.nspname = 'flight_recorder'
            and c.relkind = 'r'
        order by c.relname
    " >> "${output_file}"
}

measure_row_sizes() {
    local output_file="$1"

    log "Measuring actual row sizes..."

    # Ring buffers (exact count, full scan OK)
    psql -tAF',' -c "
        select
            'samples_ring' as table_name,
            count(*) as row_count,
            coalesce(avg(pg_column_size(t.*)), 0)::numeric(10, 2) as avg_row_bytes,
            pg_relation_size('flight_recorder.samples_ring') as heap_bytes,
            pg_indexes_size('flight_recorder.samples_ring') as index_bytes
        from flight_recorder.samples_ring as t
    " >> "${output_file}" 2>/dev/null || true

    psql -tAF',' -c "
        select
            'activity_samples_ring' as table_name,
            count(*) as row_count,
            coalesce(avg(pg_column_size(t.*)), 0)::numeric(10, 2) as avg_row_bytes,
            pg_relation_size('flight_recorder.activity_samples_ring') as heap_bytes,
            pg_indexes_size('flight_recorder.activity_samples_ring') as index_bytes
        from flight_recorder.activity_samples_ring as t
    " >> "${output_file}" 2>/dev/null || true

    psql -tAF',' -c "
        select
            'wait_samples_ring' as table_name,
            count(*) as row_count,
            coalesce(avg(pg_column_size(t.*)), 0)::numeric(10, 2) as avg_row_bytes,
            pg_relation_size('flight_recorder.wait_samples_ring') as heap_bytes,
            pg_indexes_size('flight_recorder.wait_samples_ring') as index_bytes
        from flight_recorder.wait_samples_ring as t
    " >> "${output_file}" 2>/dev/null || true

    psql -tAF',' -c "
        select
            'lock_samples_ring' as table_name,
            count(*) as row_count,
            coalesce(avg(pg_column_size(t.*)), 0)::numeric(10, 2) as avg_row_bytes,
            pg_relation_size('flight_recorder.lock_samples_ring') as heap_bytes,
            pg_indexes_size('flight_recorder.lock_samples_ring') as index_bytes
        from flight_recorder.lock_samples_ring as t
    " >> "${output_file}" 2>/dev/null || true
}

compute_projections() {
    local row_sizes_file="$1"
    local output_file="$2"

    log "Computing storage projections..."

    # Read row sizes and compute projections
    {
        echo "table_name,avg_row_bytes,samples_per_day,heap_mb_per_day,total_mb_per_day"
        while IFS=',' read -r table_name _row_count avg_row_bytes _heap_bytes _index_bytes; do
            if [[ -z "${avg_row_bytes}" ]] || [[ "${avg_row_bytes}" == "0" ]]; then
                continue
            fi
            # Samples per day at 180s interval
            local samples_per_day=480
            local heap_mb_per_day
            local total_mb_per_day
            heap_mb_per_day=$(echo "scale=4; ${samples_per_day} * ${avg_row_bytes} / 1024 / 1024" | bc)
            total_mb_per_day=$(echo "scale=4; ${heap_mb_per_day} * 1.4" | bc)  # 40% index overhead
            echo "${table_name},${avg_row_bytes},${samples_per_day},${heap_mb_per_day},${total_mb_per_day}"
        done < "${row_sizes_file}"
    } > "${output_file}"
}

generate_report() {
    local storage_file="$1"
    local row_sizes_file="$2"
    local projections_file="$3"

    log "Generating storage report..."

    local total_start total_end growth_bytes growth_mb

    # Get first and last data rows (skip header)
    # Sum total_size_bytes for first timestamp
    total_start=$(sed -n '2p' "${storage_file}" | cut -d',' -f6)
    total_end=$(tail -1 "${storage_file}" | cut -d',' -f6)

    # Handle empty or header-only file
    if [[ -z "${total_start}" ]] || [[ "${total_start}" == "total_size_bytes" ]]; then
        total_start=0
    fi
    if [[ -z "${total_end}" ]] || [[ "${total_end}" == "total_size_bytes" ]]; then
        total_end=0
    fi

    if [[ "${total_start}" -gt 0 ]] && [[ "${total_end}" -gt 0 ]]; then
        growth_bytes=$((total_end - total_start))
        growth_mb=$(echo "scale=2; ${growth_bytes} / 1024 / 1024" | bc)
    else
        growth_bytes=0
        growth_mb=0
    fi

    cat > "${RESULTS_DIR}/storage_report.md" <<EOF
# Storage Benchmark Results

**Date:** $(date -u +"%Y-%m-%d %H:%M:%S UTC")
**Duration:** ${DURATION_HOURS} hours
**Sample interval:** ${SAMPLE_INTERVAL} seconds

## Summary

- **Total growth:** ${growth_mb} MB over ${DURATION_HOURS} hours
- **Projected daily growth:** $(echo "scale=2; ${growth_mb} * 24 / ${DURATION_HOURS}" | bc) MB/day

## Row Sizes

\`\`\`
$(cat "${row_sizes_file}")
\`\`\`

## Projections

\`\`\`
$(cat "${projections_file}")
\`\`\`

## Raw Data

See:
- \`storage_timeline.csv\` - Storage measurements over time
- \`row_sizes.csv\` - Actual row sizes
- \`projections.csv\` - Growth projections
EOF

    log "Report saved to ${RESULTS_DIR}/storage_report.md"
}

main() {
    log "=== Storage Benchmark ==="
    log "Duration: ${DURATION_HOURS} hours"
    log "Sample interval: ${SAMPLE_INTERVAL} seconds"
    log ""

    check_prerequisites
    mkdir -p "${RESULTS_DIR}"

    # Enable flight recorder
    psql -c "select flight_recorder.enable()" &> /dev/null

    # Initialize files
    local storage_file="${RESULTS_DIR}/storage_timeline.csv"
    local row_sizes_file="${RESULTS_DIR}/row_sizes.csv"
    local projections_file="${RESULTS_DIR}/projections.csv"

    echo "captured_at,table_name,row_count,heap_size_bytes,index_size_bytes,total_size_bytes" > "${storage_file}"
    echo "table_name,row_count,avg_row_bytes,heap_bytes,index_bytes" > "${row_sizes_file}"

    # Run ANALYZE first
    log "Running ANALYZE on flight_recorder schema..."
    psql -c "analyze flight_recorder.samples_ring" &> /dev/null || true
    psql -c "analyze flight_recorder.activity_samples_ring" &> /dev/null || true

    # Measure initial row sizes
    measure_row_sizes "${row_sizes_file}"

    # Compute projections
    compute_projections "${row_sizes_file}" "${projections_file}"

    # Run storage tracking
    local duration_seconds
    duration_seconds=$(echo "${DURATION_HOURS} * 3600" | bc | cut -d. -f1)
    local end_time=$(($(date +%s) + duration_seconds))
    local sample_count=0

    log "Starting storage tracking (Ctrl+C to stop early)..."

    while [[ $(date +%s) -lt ${end_time} ]]; do
        measure_storage "${storage_file}"
        sample_count=$((sample_count + 1))

        if (( sample_count % 12 == 0 )); then
            info "Progress: ${sample_count} samples collected"
        fi

        sleep "${SAMPLE_INTERVAL}"
    done

    # Final row size measurement
    measure_row_sizes "${row_sizes_file}"

    # Generate report
    generate_report "${storage_file}" "${row_sizes_file}" "${projections_file}"

    log ""
    log "=== Storage Benchmark Complete ==="
    log "Results: ${RESULTS_DIR}"
}

main "$@"
