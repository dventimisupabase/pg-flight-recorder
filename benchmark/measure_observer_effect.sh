#!/usr/bin/env bash
set -Eeuo pipefail
IFS=$'\n\t'

# measure_observer_effect.sh: A-B interleaved benchmark for flight recorder impact
# Measures TPS and latency with/without flight recorder enabled

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULTS_DIR="${SCRIPT_DIR}/results/observer_effect_$(date +%Y%m%d_%H%M%S)"

# Configuration
CLIENTS=${CLIENTS:-50}
WARMUP_DURATION=${WARMUP_DURATION:-120}
TEST_DURATION=${TEST_DURATION:-900}
ITERATIONS=${ITERATIONS:-5}
WORKLOADS=${WORKLOADS:-"oltp_balanced oltp_read_heavy oltp_write_heavy"}
RANDOM_SEED=${RANDOM_SEED:-42}

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

log() { echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $*"; }
info() { echo -e "${BLUE}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $*"; }
warn() { echo -e "${YELLOW}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $*"; }
err() { echo -e "${RED}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $*" >&2; }

cleanup() {
    local ec=$?
    # Re-enable flight recorder if we exit early
    psql -c "select flight_recorder.enable()" &> /dev/null || true
    exit "${ec}"
}
trap cleanup EXIT

check_prerequisites() {
    log "Checking prerequisites..."

    # Check commands
    for cmd in psql pgbench python3; do
        if ! command -v "${cmd}" &> /dev/null; then
            err "ERROR: ${cmd} not found"
            exit 1
        fi
    done

    # Check database connection
    if ! psql -c "select 1" &> /dev/null; then
        err "ERROR: Cannot connect to PostgreSQL"
        exit 1
    fi

    # Check flight_recorder installed
    if ! psql -tAc "select 1 from pg_namespace where nspname = 'flight_recorder'" | grep -q 1; then
        err "ERROR: flight_recorder not installed"
        exit 1
    fi

    # Check pgbench tables exist
    if ! psql -tAc "select 1 from pg_tables where tablename = 'pgbench_accounts'" | grep -q 1; then
        err "ERROR: pgbench tables not found. Run: ./setup.sh"
        exit 1
    fi

    # Disable collection jitter for accurate measurement
    psql -c "update flight_recorder.config set value = 'false' where key = 'collection_jitter_enabled'" &> /dev/null || true

    log "Prerequisites OK"
}

measure_wal_overhead() {
    log "Measuring WAL overhead per sample()..."

    local wal_result
    wal_result=$(psql -tAc "
        do \$\$
        declare
            v_wal_before pg_lsn;
            v_wal_after pg_lsn;
            v_samples int := 20;
        begin
            v_wal_before := pg_current_wal_lsn();
            for i in 1..v_samples loop
                perform flight_recorder.sample();
            end loop;
            v_wal_after := pg_current_wal_lsn();
            raise notice '%', pg_wal_lsn_diff(v_wal_after, v_wal_before) / v_samples;
        end \$\$;
    " 2>&1 | grep "NOTICE" | sed 's/.*NOTICE:  //')

    echo "${wal_result}"
}

run_workload() {
    local workload="$1"
    local mode="$2"
    local iteration="$3"
    local output_dir="$4"

    local log_prefix="${output_dir}/${mode}_${iteration}"

    # Toggle flight recorder
    if [[ "${mode}" == "baseline" ]]; then
        psql -c "select flight_recorder.disable()" &> /dev/null
    else
        psql -c "select flight_recorder.enable()" &> /dev/null
    fi

    # Warmup run (discard)
    info "  Warmup: ${mode} iteration ${iteration}..."
    pgbench -n -M prepared --random-seed="${RANDOM_SEED}" \
        -c "${CLIENTS}" -j "${CLIENTS}" -T "${WARMUP_DURATION}" \
        -f "${SCRIPT_DIR}/scenarios/${workload}.sql" > /dev/null 2>&1

    # Measured run
    info "  Measuring: ${mode} iteration ${iteration}..."
    pgbench -n -M prepared --random-seed="${RANDOM_SEED}" \
        -c "${CLIENTS}" -j "${CLIENTS}" -T "${TEST_DURATION}" \
        -P 10 -r --log --log-prefix="${log_prefix}" \
        -f "${SCRIPT_DIR}/scenarios/${workload}.sql" > "${log_prefix}_summary.txt" 2>&1
}

run_benchmark() {
    local workload="$1"
    local output_dir="${RESULTS_DIR}/${workload}"

    mkdir -p "${output_dir}"
    log "Running workload: ${workload}"

    for i in $(seq 1 "${ITERATIONS}"); do
        log "Iteration ${i}/${ITERATIONS}"

        # Alternate order to eliminate systematic bias
        if (( i % 2 == 1 )); then
            first_mode="baseline"
            second_mode="enabled"
        else
            first_mode="enabled"
            second_mode="baseline"
        fi

        run_workload "${workload}" "${first_mode}" "${i}" "${output_dir}"
        run_workload "${workload}" "${second_mode}" "${i}" "${output_dir}"
    done

    log "Workload ${workload} complete"
}

generate_report() {
    log "Generating analysis report..."

    python3 "${SCRIPT_DIR}/lib/statistical_analysis.py" \
        --results-dir "${RESULTS_DIR}" \
        --output "${RESULTS_DIR}/analysis_report.json"

    # Generate summary
    cat > "${RESULTS_DIR}/summary.md" <<EOF
# Observer Effect Benchmark Results

**Date:** $(date -u +"%Y-%m-%d %H:%M:%S UTC")
**Configuration:**
- Clients: ${CLIENTS}
- Warmup: ${WARMUP_DURATION}s
- Test duration: ${TEST_DURATION}s
- Iterations: ${ITERATIONS}
- Workloads: ${WORKLOADS}

## WAL Overhead

$(cat "${RESULTS_DIR}/wal_overhead.txt" 2>/dev/null || echo "Not measured")

## Results

See \`analysis_report.json\` for detailed statistics.

### Files

$(find "${RESULTS_DIR}" -name "*.txt" -o -name "*.json" | sort)
EOF

    log "Report saved to ${RESULTS_DIR}/summary.md"
}

main() {
    log "=== Observer Effect Benchmark ==="
    log "Configuration:"
    log "  Clients: ${CLIENTS}"
    log "  Warmup: ${WARMUP_DURATION}s"
    log "  Test duration: ${TEST_DURATION}s"
    log "  Iterations: ${ITERATIONS}"
    log "  Workloads: ${WORKLOADS}"
    log ""

    check_prerequisites
    mkdir -p "${RESULTS_DIR}"

    # Measure WAL overhead first (idle system)
    local wal_bytes
    wal_bytes=$(measure_wal_overhead)
    echo "WAL per sample(): ${wal_bytes} bytes" > "${RESULTS_DIR}/wal_overhead.txt"
    log "WAL overhead: ${wal_bytes} bytes per sample()"

    # Run benchmarks for each workload
    for workload in ${WORKLOADS}; do
        run_benchmark "${workload}"
    done

    # Re-enable flight recorder
    psql -c "select flight_recorder.enable()" &> /dev/null

    generate_report

    log ""
    log "=== Benchmark Complete ==="
    log "Results: ${RESULTS_DIR}"
}

main "$@"
