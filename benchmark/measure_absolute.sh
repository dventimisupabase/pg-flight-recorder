#!/usr/bin/env bash
# Measure absolute costs of pg-flight-recorder collections
# This is the PRIMARY benchmark - measures constant costs independent of load

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ITERATIONS=${1:-100}
OUTPUT_FILE="$SCRIPT_DIR/results/absolute_costs_$(date +%Y%m%d_%H%M%S).json"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log() {
    echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $*"
}

info() {
    echo -e "${BLUE}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $*"
}

# Check prerequisites
if ! psql -c "SELECT flight_recorder.get_mode()" &> /dev/null; then
    echo "Error: Flight recorder not installed" >&2
    echo "Run: psql -f install.sql" >&2
    exit 1
fi

mkdir -p "$SCRIPT_DIR/results"

log "=== Measuring Absolute Costs of pg-flight-recorder ==="
log "Iterations: $ITERATIONS"
log "Output: $OUTPUT_FILE"
log ""

# Get environment info
PG_VERSION=$(psql -t -c "SHOW server_version" | xargs)
DB_SIZE=$(psql -t -c "SELECT pg_size_pretty(pg_database_size(current_database()))" | xargs)
TABLE_COUNT=$(psql -t -c "SELECT count(*) FROM information_schema.tables WHERE table_schema NOT IN ('pg_catalog', 'information_schema')" | xargs)

info "Environment:"
info "  PostgreSQL: $PG_VERSION"
info "  Database: ${PGDATABASE:-$(psql -t -c "SELECT current_database()" | xargs)}"
info "  DB Size: $DB_SIZE"
info "  Tables: $TABLE_COUNT"
info ""

# Enable flight recorder
psql -c "SELECT flight_recorder.enable()" &> /dev/null
psql -c "SELECT flight_recorder.set_mode('normal')" &> /dev/null

# Disable collection jitter for accurate timing measurement
# (Jitter adds 0-10s random sleep to avoid thundering herd in SaaS environments)
log "Disabling collection jitter for accurate measurement..."
ORIGINAL_JITTER=$(psql -t -c "SELECT value FROM flight_recorder.config WHERE key = 'collection_jitter_enabled'" | xargs)
psql -c "UPDATE flight_recorder.config SET value = 'false' WHERE key = 'collection_jitter_enabled'" &> /dev/null

log "Warming up (5 collections)..."
for i in {1..5}; do
    psql -c "SELECT flight_recorder.sample()" &> /dev/null
    sleep 1
done

log "Starting measurement..."
log ""

# Measure timing for each collection
TIMINGS_FILE=$(mktemp)
MEMORY_FILE=$(mktemp)
IO_FILE=$(mktemp)

for i in $(seq 1 $ITERATIONS); do
    # Show progress every 10 iterations
    if (( i % 10 == 0 )); then
        info "Progress: $i/$ITERATIONS"
    fi

    # Capture before stats
    psql -t -c "
        SELECT
            blks_read + blks_hit as total_blocks,
            xact_commit + xact_rollback as total_xacts,
            extract(epoch from now()) as ts
        FROM pg_stat_database
        WHERE datname = current_database()
    " > /tmp/before_stats.txt

    # Run collection with timing
    TIMING=$(psql -t -c "\timing on" -c "SELECT flight_recorder.sample()" 2>&1 | grep "Time:" | sed 's/Time: \([0-9.]*\).*/\1/')

    # Capture after stats
    psql -t -c "
        SELECT
            blks_read + blks_hit as total_blocks,
            xact_commit + xact_rollback as total_xacts,
            extract(epoch from now()) as ts
        FROM pg_stat_database
        WHERE datname = current_database()
    " > /tmp/after_stats.txt

    # Calculate I/O delta
    BEFORE_BLOCKS=$(awk '{print $1}' /tmp/before_stats.txt | xargs)
    AFTER_BLOCKS=$(awk '{print $1}' /tmp/after_stats.txt | xargs)
    IO_DELTA=$((AFTER_BLOCKS - BEFORE_BLOCKS))

    # Record measurements
    echo "$TIMING" >> "$TIMINGS_FILE"
    echo "$IO_DELTA" >> "$IO_FILE"

    # Brief sleep to avoid overwhelming the system
    sleep 0.5
done

log ""
log "Analyzing results..."

# Calculate statistics using Python
if command -v python3 &> /dev/null; then
    STATS=$(python3 <<PYTHON
import sys

# Read timings
with open('$TIMINGS_FILE') as f:
    timings = [float(line.strip()) for line in f if line.strip()]

# Read I/O
with open('$IO_FILE') as f:
    io_ops = [int(line.strip()) for line in f if line.strip()]

import statistics
import json

if timings:
    # Sort for percentiles
    timings.sort()
    io_ops.sort()

    n = len(timings)

    stats = {
        'timing_ms': {
            'mean': statistics.mean(timings),
            'median': statistics.median(timings),
            'stddev': statistics.stdev(timings) if n > 1 else 0,
            'min': min(timings),
            'max': max(timings),
            'p50': timings[int(n * 0.50)],
            'p95': timings[int(n * 0.95)],
            'p99': timings[int(n * 0.99)]
        },
        'io_blocks': {
            'mean': statistics.mean(io_ops),
            'median': statistics.median(io_ops),
            'stddev': statistics.stdev(io_ops) if n > 1 else 0,
            'min': min(io_ops),
            'max': max(io_ops),
            'p95': io_ops[int(n * 0.95)]
        }
    }

    print(json.dumps(stats, indent=2))
else:
    print('{}', file=sys.stderr)
    sys.exit(1)
PYTHON
)
else
    # Fallback if no Python
    STATS='{"error": "Python3 required for statistical analysis"}'
fi

# Get catalog lock info
LOCK_INFO=$(psql -t -c "
    SELECT
        count(*) as lock_count,
        array_agg(DISTINCT relation::regclass::text) as tables_locked
    FROM pg_locks
    WHERE locktype = 'relation'
    AND relation IN (
        SELECT oid FROM pg_class
        WHERE relnamespace = 'pg_catalog'::regnamespace
        AND relkind = 'r'
    )
    AND granted = true
    LIMIT 1
")

# Calculate sustained CPU percentage at different intervals
MEAN_TIME=$(echo "$STATS" | python3 -c "import json, sys; print(json.load(sys.stdin)['timing_ms']['mean'])")

CPU_180s=$(python3 -c "print(f'{($MEAN_TIME / 180000) * 100:.4f}')")
CPU_120s=$(python3 -c "print(f'{($MEAN_TIME / 120000) * 100:.4f}')")
CPU_60s=$(python3 -c "print(f'{($MEAN_TIME / 60000) * 100:.4f}')")

# Build JSON report
cat > "$OUTPUT_FILE" <<EOF
{
  "measurement_date": "$(date -u +"%Y-%m-%d %H:%M:%S UTC")",
  "environment": {
    "postgresql_version": "$PG_VERSION",
    "database": "${PGDATABASE:-$(psql -t -c "SELECT current_database()" | xargs)}",
    "database_size": "$DB_SIZE",
    "table_count": $TABLE_COUNT,
    "hardware": "$(uname -m)",
    "os": "$(uname -s)"
  },
  "methodology": {
    "iterations": $ITERATIONS,
    "flight_recorder_mode": "normal",
    "warmup_collections": 5
  },
  "absolute_costs": $STATS,
  "sustained_cpu_pct": {
    "at_60s_intervals": $CPU_60s,
    "at_120s_intervals": $CPU_120s,
    "at_180s_intervals": $CPU_180s,
    "note": "Sustained percentage = (collection_time_ms / interval_ms) * 100"
  },
  "peak_impact": {
    "note": "Collections cause brief CPU spike every N seconds",
    "spike_duration_ms": $MEAN_TIME,
    "recommendation": "Systems need this much CPU headroom available"
  }
}
EOF

# Cleanup
rm -f "$TIMINGS_FILE" "$MEMORY_FILE" "$IO_FILE" /tmp/before_stats.txt /tmp/after_stats.txt

# Display results
log ""
log "=== Results ==="
log ""
log "Collection Timing (milliseconds):"
echo "$STATS" | python3 -c "
import json, sys
stats = json.load(sys.stdin)
t = stats['timing_ms']
print(f'  Mean:   {t[\"mean\"]:.1f} ms ± {t[\"stddev\"]:.1f} ms')
print(f'  Median: {t[\"median\"]:.1f} ms')
print(f'  P95:    {t[\"p95\"]:.1f} ms')
print(f'  P99:    {t[\"p99\"]:.1f} ms')
print(f'  Range:  {t[\"min\"]:.1f} - {t[\"max\"]:.1f} ms')
"

log ""
log "I/O Operations (blocks per collection):"
echo "$STATS" | python3 -c "
import json, sys
stats = json.load(sys.stdin)
io = stats['io_blocks']
print(f'  Mean:   {io[\"mean\"]:.0f} blocks')
print(f'  Median: {io[\"median\"]:.0f} blocks')
print(f'  P95:    {io[\"p95\"]:.0f} blocks')
"

log ""
log "Sustained CPU Impact:"
log "  At 60s intervals:  ${CPU_60s}%"
log "  At 120s intervals: ${CPU_120s}%"
log "  At 180s intervals: ${CPU_180s}%"

log ""
log "Peak Impact:"
log "  Brief ${MEAN_TIME}ms CPU spike every N seconds"
log "  Systems need this much CPU headroom available"

log ""
log "=== Headroom Assessment ==="
log ""

# Provide assessment
python3 <<PYTHON
mean_ms = $MEAN_TIME

print("For 180s intervals (current default):")
print("")

# 1 vCPU scenarios
if mean_ms < 50:
    print("  ✓ 1 vCPU system: SAFE - very low overhead")
elif mean_ms < 150:
    print("  ✓ 1 vCPU system: ACCEPTABLE - test in staging first")
elif mean_ms < 300:
    print("  ⚠ 1 vCPU system: MONITOR - may cause brief spikes")
else:
    print("  ✗ 1 vCPU system: RISKY - consider emergency mode (300s)")

print("")

# 2+ vCPU scenarios
if mean_ms < 100:
    print("  ✓ 2+ vCPU system: SAFE - negligible impact")
elif mean_ms < 200:
    print("  ✓ 2+ vCPU system: SAFE - minimal impact")
else:
    print("  ✓ 2+ vCPU system: ACCEPTABLE - brief spikes")

print("")
print("Under heavy load:")
print("  - Load shedding: skips if >70% connections active")
print("  - Load throttling: skips if >1000 txn/sec or >10K blocks/sec")
print("  - These protect against observer effect amplification")
PYTHON

# Restore original jitter setting
if [[ -n "$ORIGINAL_JITTER" ]]; then
    psql -c "UPDATE flight_recorder.config SET value = '$ORIGINAL_JITTER' WHERE key = 'collection_jitter_enabled'" &> /dev/null
    log "Restored collection jitter setting: $ORIGINAL_JITTER"
fi

log ""
log "Full report saved to: $OUTPUT_FILE"
log ""
log "=== Next Steps ==="
log ""
log "1. Review absolute costs above"
log "2. Compare to your available CPU headroom"
log "3. For tiny systems (1 vCPU), test in staging"
log "4. For production, monitor with: SELECT * FROM flight_recorder.recent_activity"
log ""
