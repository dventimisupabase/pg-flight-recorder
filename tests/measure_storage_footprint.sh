#!/usr/bin/env bash
# Measure storage footprint of pg-flight-recorder over time
# Shows actual storage consumption per hour at different frequencies

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DURATION_SECONDS=${1:-600}       # Default: 10 minutes
SAMPLE_INTERVAL=${2:-30}         # Default: measure every 30 seconds

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

log() { echo -e "${GREEN}[$(date +'%H:%M:%S')]${NC} $*"; }
info() { echo -e "${BLUE}[$(date +'%H:%M:%S')]${NC} $*"; }
warn() { echo -e "${YELLOW}[$(date +'%H:%M:%S')]${NC} $*"; }

# Check prerequisites
if ! psql -c "SELECT 1 FROM flight_recorder.config LIMIT 1" &> /dev/null; then
    echo "Error: Flight recorder not installed"
    echo "Run: psql -f install.sql"
    exit 1
fi

OUTPUT_DIR="${SCRIPT_DIR}/../benchmark/results"
mkdir -p "$OUTPUT_DIR"
REPORT_FILE="$OUTPUT_DIR/storage_footprint_$(date +%Y%m%d_%H%M%S).txt"

log "=========================================="
log "pg-flight-recorder Storage Footprint Test"
log "=========================================="
log ""
log "Duration:         $DURATION_SECONDS seconds ($(echo "scale=1; $DURATION_SECONDS/60" | bc) minutes)"
log "Sample interval:  $SAMPLE_INTERVAL seconds"
log "Report:           $REPORT_FILE"
log ""

# Get current configuration
COLLECTION_INTERVAL=$(psql -t -c "SELECT value FROM flight_recorder.config WHERE key = 'sample_interval_seconds'" | xargs)
PROFILE=$(psql -t -c "SELECT flight_recorder.get_current_profile()" 2>/dev/null | xargs || echo "custom")

info "Current Configuration:"
info "  Collection interval: ${COLLECTION_INTERVAL}s"
info "  Profile: $PROFILE"
info ""

# Capture initial sizes
get_schema_size() {
    psql -t -c "
        SELECT COALESCE(sum(pg_total_relation_size(c.oid)), 0)
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'flight_recorder'
    " | xargs
}

get_table_sizes() {
    psql -t -c "
        SELECT
            c.relname,
            pg_total_relation_size(c.oid) as size_bytes,
            pg_size_pretty(pg_total_relation_size(c.oid)) as size_pretty
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'flight_recorder'
          AND c.relkind IN ('r', 'i')
        ORDER BY pg_total_relation_size(c.oid) DESC
    "
}

get_row_counts() {
    psql -t -c "
        SELECT 'samples_ring', count(*) FROM flight_recorder.samples_ring
        UNION ALL SELECT 'wait_samples_ring', count(*) FROM flight_recorder.wait_samples_ring
        UNION ALL SELECT 'activity_samples_ring', count(*) FROM flight_recorder.activity_samples_ring
        UNION ALL SELECT 'lock_samples_ring', count(*) FROM flight_recorder.lock_samples_ring
        UNION ALL SELECT 'wait_event_aggregates', count(*) FROM flight_recorder.wait_event_aggregates
        UNION ALL SELECT 'activity_aggregates', count(*) FROM flight_recorder.activity_aggregates
        UNION ALL SELECT 'lock_aggregates', count(*) FROM flight_recorder.lock_aggregates
        UNION ALL SELECT 'wait_samples_archive', count(*) FROM flight_recorder.wait_samples_archive
        UNION ALL SELECT 'activity_samples_archive', count(*) FROM flight_recorder.activity_samples_archive
        UNION ALL SELECT 'lock_samples_archive', count(*) FROM flight_recorder.lock_samples_archive
        UNION ALL SELECT 'snapshots', count(*) FROM flight_recorder.snapshots
        UNION ALL SELECT 'statement_snapshots', count(*) FROM flight_recorder.statement_snapshots
        ORDER BY 1
    "
}

# Enable flight recorder
psql -c "SELECT flight_recorder.enable()" &> /dev/null 2>&1 || true

# Disable jitter for consistent timing
psql -c "UPDATE flight_recorder.config SET value = 'false' WHERE key = 'collection_jitter_enabled'" &> /dev/null

INITIAL_SIZE=$(get_schema_size)
START_TIME=$(date +%s)

log "Initial schema size: $(psql -t -c "SELECT pg_size_pretty($INITIAL_SIZE)" | xargs)"
log ""
log "Starting measurement (Ctrl+C to stop early)..."
log ""

# Header for CSV-like output
echo "timestamp,elapsed_sec,schema_size_bytes,delta_bytes,samples_taken,snapshots_taken" > "$REPORT_FILE.csv"

# Track collections
PREV_SAMPLES=$(psql -t -c "SELECT count(*) FROM flight_recorder.collection_stats WHERE collection_type = 'sample'" | xargs)
PREV_SNAPSHOTS=$(psql -t -c "SELECT count(*) FROM flight_recorder.collection_stats WHERE collection_type = 'snapshot'" | xargs)
PREV_SIZE=$INITIAL_SIZE

# Cleanup handler
cleanup() {
    log ""
    log "Interrupted - generating final report..."
    generate_report
    exit 0
}
trap cleanup INT TERM

generate_report() {
    END_TIME=$(date +%s)
    ELAPSED=$((END_TIME - START_TIME))
    FINAL_SIZE=$(get_schema_size)
    GROWTH=$((FINAL_SIZE - INITIAL_SIZE))

    # Calculate samples and snapshots taken
    FINAL_SAMPLES=$(psql -t -c "SELECT count(*) FROM flight_recorder.collection_stats WHERE collection_type = 'sample'" | xargs)
    FINAL_SNAPSHOTS=$(psql -t -c "SELECT count(*) FROM flight_recorder.collection_stats WHERE collection_type = 'snapshot'" | xargs)
    SAMPLES_TAKEN=$((FINAL_SAMPLES - PREV_SAMPLES))
    SNAPSHOTS_TAKEN=$((FINAL_SNAPSHOTS - PREV_SNAPSHOTS))

    # Calculate hourly rate
    if [ $ELAPSED -gt 0 ]; then
        GROWTH_PER_HOUR=$(echo "scale=0; ($GROWTH * 3600) / $ELAPSED" | bc)
    else
        GROWTH_PER_HOUR=0
    fi

    {
        echo ""
        echo "=========================================="
        echo "STORAGE FOOTPRINT REPORT"
        echo "=========================================="
        echo ""
        echo "Test Duration:      $ELAPSED seconds ($(echo "scale=1; $ELAPSED/60" | bc) minutes)"
        echo "Collection Interval: ${COLLECTION_INTERVAL}s"
        echo "Profile:            $PROFILE"
        echo ""
        echo "STORAGE GROWTH"
        echo "--------------"
        echo "Initial size:       $(psql -t -c "SELECT pg_size_pretty($INITIAL_SIZE)" | xargs)"
        echo "Final size:         $(psql -t -c "SELECT pg_size_pretty($FINAL_SIZE)" | xargs)"
        echo "Total growth:       $(psql -t -c "SELECT pg_size_pretty($GROWTH)" | xargs)"
        echo ""
        echo "PROJECTED STORAGE (extrapolated)"
        echo "---------------------------------"
        echo "Per hour:           $(psql -t -c "SELECT pg_size_pretty($GROWTH_PER_HOUR)" | xargs)"
        echo "Per day:            $(psql -t -c "SELECT pg_size_pretty($GROWTH_PER_HOUR * 24)" | xargs)"
        echo "Per week:           $(psql -t -c "SELECT pg_size_pretty($GROWTH_PER_HOUR * 24 * 7)" | xargs)"
        echo "Per 30 days:        $(psql -t -c "SELECT pg_size_pretty($GROWTH_PER_HOUR * 24 * 30)" | xargs)"
        echo ""
        echo "COLLECTIONS DURING TEST"
        echo "-----------------------"
        echo "Samples taken:      $SAMPLES_TAKEN"
        echo "Snapshots taken:    $SNAPSHOTS_TAKEN"
        echo ""
        echo "TABLE SIZES (current)"
        echo "---------------------"
        get_table_sizes
        echo ""
        echo "ROW COUNTS"
        echo "----------"
        get_row_counts
        echo ""
        echo "RING BUFFER STATUS"
        echo "------------------"
        psql -t -c "
            SELECT
                'Slots used: ' || count(DISTINCT slot_id) || '/120',
                'Oldest: ' || min(captured_at)::text,
                'Newest: ' || max(captured_at)::text
            FROM flight_recorder.samples_ring
            WHERE captured_at > '2000-01-01'
        "
        echo ""
        echo "=========================================="
        echo "FREQUENCY COMPARISON (theoretical)"
        echo "=========================================="
        echo ""
        echo "Profile            | Samples/hr | Estimated Storage/hr"
        echo "-------------------|------------|---------------------"
        echo "troubleshooting    | 60         | $(psql -t -c "SELECT pg_size_pretty(($GROWTH_PER_HOUR * 60) / GREATEST($SAMPLES_TAKEN,1) * $ELAPSED / 3600)" | xargs)"
        echo "default (180s)     | 20         | $(psql -t -c "SELECT pg_size_pretty(($GROWTH_PER_HOUR * 20) / GREATEST($SAMPLES_TAKEN,1) * $ELAPSED / 3600)" | xargs)"
        echo "production_safe    | 12         | $(psql -t -c "SELECT pg_size_pretty(($GROWTH_PER_HOUR * 12) / GREATEST($SAMPLES_TAKEN,1) * $ELAPSED / 3600)" | xargs)"
        echo ""
    } | tee "$REPORT_FILE"

    log ""
    log "Full report saved to: $REPORT_FILE"
    log "CSV data saved to: $REPORT_FILE.csv"
}

# Main measurement loop
ELAPSED=0
while [ $ELAPSED -lt $DURATION_SECONDS ]; do
    sleep $SAMPLE_INTERVAL

    CURRENT_TIME=$(date +%s)
    ELAPSED=$((CURRENT_TIME - START_TIME))

    CURRENT_SIZE=$(get_schema_size)
    DELTA=$((CURRENT_SIZE - PREV_SIZE))

    CURRENT_SAMPLES=$(psql -t -c "SELECT count(*) FROM flight_recorder.collection_stats WHERE collection_type = 'sample'" | xargs)
    CURRENT_SNAPSHOTS=$(psql -t -c "SELECT count(*) FROM flight_recorder.collection_stats WHERE collection_type = 'snapshot'" | xargs)
    SAMPLES_DELTA=$((CURRENT_SAMPLES - PREV_SAMPLES))
    SNAPSHOTS_DELTA=$((CURRENT_SNAPSHOTS - PREV_SNAPSHOTS))

    # Log progress
    SIZE_PRETTY=$(psql -t -c "SELECT pg_size_pretty($CURRENT_SIZE)" | xargs)
    DELTA_PRETTY=$(psql -t -c "SELECT pg_size_pretty($DELTA)" | xargs)

    info "[$ELAPSED/${DURATION_SECONDS}s] Size: $SIZE_PRETTY (+$DELTA_PRETTY) | Samples: +$SAMPLES_DELTA | Snapshots: +$SNAPSHOTS_DELTA"

    # Record to CSV
    echo "$(date -Iseconds),$ELAPSED,$CURRENT_SIZE,$DELTA,$CURRENT_SAMPLES,$CURRENT_SNAPSHOTS" >> "$REPORT_FILE.csv"

    PREV_SIZE=$CURRENT_SIZE
    PREV_SAMPLES=$CURRENT_SAMPLES
    PREV_SNAPSHOTS=$CURRENT_SNAPSHOTS
done

generate_report
