#!/usr/bin/env bash
# Run pg-flight-recorder benchmarks

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
RESULTS_DIR="$SCRIPT_DIR/results/$(date +%Y%m%d_%H%M%S)"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log() {
    echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $*"
}

warn() {
    echo -e "${YELLOW}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $*"
}

error() {
    echo -e "${RED}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $*" >&2
}

info() {
    echo -e "${BLUE}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $*"
}

# Check if flight recorder is installed
check_flight_recorder() {
    if psql -c "SELECT flight_recorder.get_mode()" &> /dev/null; then
        log "✓ Flight recorder is installed"
        return 0
    else
        error "Flight recorder is not installed"
        return 1
    fi
}

# Run benchmark with flight recorder disabled (baseline)
run_baseline() {
    local scenario=$1
    local duration=$2
    local clients=$3

    log "Running baseline (flight recorder DISABLED)..."

    # Disable flight recorder
    psql -c "SELECT flight_recorder.disable()" &> /dev/null || true

    # Wait for any in-flight collections to complete
    sleep 5

    # Run scenario
    "$SCRIPT_DIR/scenarios/$scenario.sh" \
        --duration "$duration" \
        --clients "$clients" \
        --output "$RESULTS_DIR/baseline_${scenario}.json"

    log "✓ Baseline complete"
}

# Run benchmark with flight recorder enabled
run_with_flight_recorder() {
    local scenario=$1
    local duration=$2
    local clients=$3
    local mode=${4:-normal}

    log "Running with flight recorder ENABLED (mode: $mode)..."

    # Enable flight recorder
    psql -c "SELECT flight_recorder.enable()" &> /dev/null
    psql -c "SELECT flight_recorder.set_mode('$mode')" &> /dev/null

    # Wait for flight recorder to stabilize
    sleep 5

    # Run scenario
    "$SCRIPT_DIR/scenarios/$scenario.sh" \
        --duration "$duration" \
        --clients "$clients" \
        --output "$RESULTS_DIR/with_fr_${mode}_${scenario}.json"

    log "✓ Flight recorder run complete"
}

# Compare results
compare_results() {
    local scenario=$1
    local mode=${2:-normal}

    log "Comparing results for $scenario..."

    "$SCRIPT_DIR/lib/compare.py" \
        --baseline "$RESULTS_DIR/baseline_${scenario}.json" \
        --test "$RESULTS_DIR/with_fr_${mode}_${scenario}.json" \
        --output "$RESULTS_DIR/comparison_${mode}_${scenario}.md"

    log "✓ Comparison saved to comparison_${mode}_${scenario}.md"
}

# Run single scenario
run_scenario() {
    local scenario=$1
    local duration=${2:-30}  # minutes
    local clients=${3:-10}
    local mode=${4:-normal}

    info ""
    info "========================================="
    info "Scenario: $scenario"
    info "Duration: ${duration}m"
    info "Clients: $clients"
    info "Mode: $mode"
    info "========================================="
    info ""

    # Run baseline
    run_baseline "$scenario" "$duration" "$clients"

    # Cool down period
    log "Cooling down for 60 seconds..."
    sleep 60

    # Run with flight recorder
    run_with_flight_recorder "$scenario" "$duration" "$clients" "$mode"

    # Compare
    compare_results "$scenario" "$mode"

    info ""
    info "✓ Scenario $scenario complete"
    info ""
}

# Run all scenarios
run_all_scenarios() {
    local mode=${1:-normal}

    log "=== Running ALL Benchmark Scenarios ==="
    log "Mode: $mode"
    log "Results will be saved to: $RESULTS_DIR"
    log ""

    # Light OLTP: 10 clients, 30 min
    if [[ -f "$SCRIPT_DIR/scenarios/light_oltp.sh" ]]; then
        run_scenario "light_oltp" 30 10 "$mode"
    else
        warn "Scenario light_oltp.sh not found, skipping"
    fi

    # Heavy OLTP: 100 clients, 30 min
    if [[ -f "$SCRIPT_DIR/scenarios/heavy_oltp.sh" ]]; then
        run_scenario "heavy_oltp" 30 100 "$mode"
    else
        warn "Scenario heavy_oltp.sh not found, skipping"
    fi

    # Analytical: 5 clients, 30 min
    if [[ -f "$SCRIPT_DIR/scenarios/analytical.sh" ]]; then
        run_scenario "analytical" 30 5 "$mode"
    else
        warn "Scenario analytical.sh not found, skipping"
    fi

    # Mixed: 50 clients, 30 min
    if [[ -f "$SCRIPT_DIR/scenarios/mixed.sh" ]]; then
        run_scenario "mixed" 30 50 "$mode"
    else
        warn "Scenario mixed.sh not found, skipping"
    fi

    log ""
    log "=== All Scenarios Complete ==="
    log "Results directory: $RESULTS_DIR"
    log ""
    log "Generate final report:"
    log "  ./lib/report.py --results-dir $RESULTS_DIR"
    log ""
}

# Main
main() {
    local scenario=""
    local duration=30
    local clients=10
    local mode="normal"
    local run_all=false

    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            --scenario)
                scenario="$2"
                shift 2
                ;;
            --duration)
                duration="$2"
                shift 2
                ;;
            --clients)
                clients="$2"
                shift 2
                ;;
            --mode)
                mode="$2"
                shift 2
                ;;
            --all)
                run_all=true
                shift
                ;;
            --help)
                cat <<EOF
Run pg-flight-recorder benchmarks

Usage: $0 [OPTIONS]

Options:
    --scenario NAME    Run specific scenario (light_oltp, heavy_oltp, analytical, mixed)
    --duration N       Duration in minutes (default: 30)
    --clients N        Number of concurrent clients (default: 10)
    --mode MODE        Flight recorder mode: normal, light, emergency (default: normal)
    --all              Run all scenarios
    --help             Show this help

Examples:
    $0 --scenario light_oltp --duration 30 --clients 10
    $0 --all --mode normal
    $0 --scenario heavy_oltp --duration 60 --clients 200

Environment:
    Use standard libpq environment variables:
    PGHOST, PGPORT, PGUSER, PGDATABASE, PGPASSWORD

Output:
    Results saved to: results/YYYYMMDD_HHMMSS/
    - baseline_*.json: Baseline measurements (flight recorder OFF)
    - with_fr_*.json: Test measurements (flight recorder ON)
    - comparison_*.md: Statistical comparison and impact analysis
EOF
                exit 0
                ;;
            *)
                error "Unknown option: $1"
                error "Run with --help for usage"
                exit 1
                ;;
        esac
    done

    # Create results directory
    mkdir -p "$RESULTS_DIR"

    log "=== pg-flight-recorder Benchmark Runner ==="
    log "Results directory: $RESULTS_DIR"
    log ""

    # Check prerequisites
    if ! check_flight_recorder; then
        error "Install flight recorder first: psql -f install.sql"
        exit 1
    fi

    # Run benchmarks
    if [[ "$run_all" == "true" ]]; then
        run_all_scenarios "$mode"
    elif [[ -n "$scenario" ]]; then
        run_scenario "$scenario" "$duration" "$clients" "$mode"
    else
        error "Specify --scenario NAME or --all"
        error "Run with --help for usage"
        exit 1
    fi
}

main "$@"
