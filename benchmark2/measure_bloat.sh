#!/usr/bin/env bash
set -Eeuo pipefail
IFS=$'\n\t'

# measure_bloat.sh: Track bloat and HOT update ratios for flight_recorder tables
# Uses pg_stat_user_tables for lightweight tracking, pgstattuple_approx at end

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULTS_DIR="${SCRIPT_DIR}/results/bloat_$(date +%Y%m%d_%H%M%S)"

# Configuration
DURATION_HOURS=${DURATION_HOURS:-4}
SAMPLE_INTERVAL=${SAMPLE_INTERVAL:-300}  # 5 minutes

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

log() { echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $*"; }
info() { echo -e "${BLUE}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $*"; }
warn() { echo -e "${YELLOW}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $*"; }
error() { echo -e "${RED}[$(date +'%Y-%m-%d %H:%M:%S')] ERROR:${NC} $*"; }

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

    # Check pgstattuple extension
    if ! psql -tAc "select 1 from pg_extension where extname = 'pgstattuple'" | grep -q 1; then
        warn "pgstattuple extension not installed - precise bloat measurement unavailable"
        warn "Run: CREATE EXTENSION pgstattuple"
    fi

    log "Prerequisites OK"
}

capture_baseline() {
    local output_file="$1"

    log "Capturing baseline statistics..."

    psql -tAF',' -c "
        select
            relname,
            n_tup_upd,
            n_tup_hot_upd,
            n_dead_tup,
            n_live_tup
        from pg_stat_user_tables
        where schemaname = 'flight_recorder'
        order by relname
    " > "${output_file}"
}

measure_bloat() {
    local output_file="$1"
    local timestamp
    timestamp=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

    psql -tAF',' -c "
        select
            '${timestamp}' as captured_at,
            relname,
            n_live_tup,
            n_dead_tup,
            round(100.0 * n_dead_tup / nullif(n_live_tup + n_dead_tup, 1), 2) as dead_pct,
            n_tup_upd,
            n_tup_hot_upd,
            round(100.0 * n_tup_hot_upd / nullif(n_tup_upd, 1), 2) as hot_pct,
            last_autovacuum
        from pg_stat_user_tables
        where
            schemaname = 'flight_recorder'
            and relname like '%\_ring'
        order by relname
    " >> "${output_file}"
}

compute_deltas() {
    local baseline_file="$1"
    local output_file="$2"

    log "Computing delta-based HOT statistics..."

    psql -tAF',' -c "
        with baseline as (
            select
                split_part(line, ',', 1) as relname,
                split_part(line, ',', 2)::bigint as n_tup_upd,
                split_part(line, ',', 3)::bigint as n_tup_hot_upd,
                split_part(line, ',', 4)::bigint as n_dead_tup
            from (
                $(while IFS= read -r line; do echo "select '${line}' as line union all"; done < "${baseline_file}" | sed '$ s/union all$//')
            ) as data
        ),
        current_stats as (
            select
                relname,
                n_tup_upd,
                n_tup_hot_upd,
                n_dead_tup
            from pg_stat_user_tables
            where schemaname = 'flight_recorder'
        )
        select
            c.relname,
            c.n_tup_upd - coalesce(b.n_tup_upd, 0) as updates_during_test,
            c.n_tup_hot_upd - coalesce(b.n_tup_hot_upd, 0) as hot_updates_during_test,
            round(
                100.0 * (c.n_tup_hot_upd - coalesce(b.n_tup_hot_upd, 0))
                / nullif(c.n_tup_upd - coalesce(b.n_tup_upd, 0), 0),
                2
            ) as hot_pct_during_test
        from current_stats as c
        left join baseline as b on c.relname = b.relname
        where c.relname like '%\_ring'
        order by c.relname
    " > "${output_file}" 2>/dev/null || {
        # Fallback: simple delta calculation without CTE
        psql -tAF',' -c "
            select
                relname,
                n_tup_upd as updates_during_test,
                n_tup_hot_upd as hot_updates_during_test,
                round(100.0 * n_tup_hot_upd / nullif(n_tup_upd, 1), 2) as hot_pct_during_test
            from pg_stat_user_tables
            where schemaname = 'flight_recorder'
                and relname like '%\_ring'
            order by relname
        " > "${output_file}"
    }
}

measure_precise_bloat() {
    local output_file="$1"

    log "Measuring precise bloat with pgstattuple_approx..."

    # Check if pgstattuple is available
    if ! psql -tAc "select 1 from pg_extension where extname = 'pgstattuple'" | grep -q 1; then
        echo "pgstattuple not installed - skipping precise measurement" > "${output_file}"
        return
    fi

    {
        echo "table_name,tuple_count,dead_tuple_count,dead_tuple_pct,free_space,free_percent"

        for table in samples_ring activity_samples_ring wait_samples_ring lock_samples_ring; do
            psql -tAF',' -c "
                select
                    '${table}' as table_name,
                    approx_tuple_count,
                    dead_tuple_count,
                    round(100.0 * dead_tuple_count / nullif(approx_tuple_count + dead_tuple_count, 1), 2) as dead_tuple_pct,
                    approx_free_space,
                    approx_free_percent
                from pgstattuple_approx('flight_recorder.${table}')
            " 2>/dev/null || echo "${table},error,error,error,error,error"
        done
    } > "${output_file}"
}

check_sample_duration() {
    local output_file="$1"

    log "Checking sample() duration vs interval..."

    psql -tAF',' -c "
        select
            started_at,
            duration_ms,
            case
                when duration_ms > 180000 * 0.8 then 'CRITICAL'
                when duration_ms > 180000 * 0.5 then 'WARNING'
                else 'OK'
            end as status
        from flight_recorder.collection_stats
        where collection_type = 'sample'
        order by started_at desc
        limit 20
    " > "${output_file}" 2>/dev/null || echo "collection_stats not available" > "${output_file}"
}

generate_report() {
    local bloat_file="$1"
    local deltas_file="$2"
    local precise_file="$3"
    local duration_file="$4"

    log "Generating bloat report..."

    cat > "${RESULTS_DIR}/bloat_report.md" <<EOF
# Bloat Benchmark Results

**Date:** $(date -u +"%Y-%m-%d %H:%M:%S UTC")
**Duration:** ${DURATION_HOURS} hours
**Sample interval:** ${SAMPLE_INTERVAL} seconds

## HOT Update Ratio (Delta-Based)

\`\`\`
$(cat "${deltas_file}")
\`\`\`

**Thresholds:**
- Healthy: > 85%
- Warning: < 70%

## Dead Tuple Percentage (Latest)

\`\`\`
$(tail -4 "${bloat_file}" | column -t -s',')
\`\`\`

**Thresholds:**
- Healthy: < 10%
- Warning: > 20%

## Precise Bloat (pgstattuple_approx)

\`\`\`
$(cat "${precise_file}")
\`\`\`

## Sample Duration Check

\`\`\`
$(cat "${duration_file}")
\`\`\`

**Thresholds:**
- OK: < 50% of interval
- WARNING: 50-80% of interval
- CRITICAL: > 80% of interval

## Raw Data

See:
- \`bloat_timeline.csv\` - Bloat measurements over time
- \`deltas.csv\` - Delta-based HOT statistics
- \`precise_bloat.csv\` - pgstattuple_approx results
- \`sample_duration.csv\` - Collection duration checks
EOF

    log "Report saved to ${RESULTS_DIR}/bloat_report.md"
}

main() {
    log "=== Bloat Benchmark ==="
    log "Duration: ${DURATION_HOURS} hours"
    log "Sample interval: ${SAMPLE_INTERVAL} seconds"
    log ""

    check_prerequisites
    mkdir -p "${RESULTS_DIR}"

    # Enable flight recorder
    psql -c "select flight_recorder.enable()" &> /dev/null

    # Initialize files
    local baseline_file="${RESULTS_DIR}/baseline.csv"
    local bloat_file="${RESULTS_DIR}/bloat_timeline.csv"
    local deltas_file="${RESULTS_DIR}/deltas.csv"
    local precise_file="${RESULTS_DIR}/precise_bloat.csv"
    local duration_file="${RESULTS_DIR}/sample_duration.csv"

    echo "captured_at,relname,n_live_tup,n_dead_tup,dead_pct,n_tup_upd,n_tup_hot_upd,hot_pct,last_autovacuum" > "${bloat_file}"

    # Capture baseline
    capture_baseline "${baseline_file}"

    # Run bloat tracking
    local duration_seconds
    duration_seconds=$(echo "${DURATION_HOURS} * 3600" | bc | cut -d. -f1)
    local end_time=$(($(date +%s) + duration_seconds))
    local sample_count=0

    log "Starting bloat tracking (Ctrl+C to stop early)..."

    while [[ $(date +%s) -lt ${end_time} ]]; do
        measure_bloat "${bloat_file}"
        sample_count=$((sample_count + 1))

        if (( sample_count % 12 == 0 )); then
            info "Progress: ${sample_count} samples collected"
        fi

        sleep "${SAMPLE_INTERVAL}"
    done

    # Final measurements
    compute_deltas "${baseline_file}" "${deltas_file}"
    measure_precise_bloat "${precise_file}"
    check_sample_duration "${duration_file}"

    # Generate report
    generate_report "${bloat_file}" "${deltas_file}" "${precise_file}" "${duration_file}"

    log ""
    log "=== Bloat Benchmark Complete ==="
    log "Results: ${RESULTS_DIR}"
}

main "$@"
