#!/bin/bash
# Measure DDL blocking impact from flight recorder
# Usage: ./measure_ddl_impact.sh [duration_seconds] [interval_seconds]

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DURATION=${1:-300}  # 5 minutes default
INTERVAL=${2:-180}  # 180s default (normal mode)
PGHOST=${PGHOST:-localhost}
PGPORT=${PGPORT:-5432}
PGUSER=${PGUSER:-postgres}
PGDATABASE=${PGDATABASE:-postgres}

TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
RESULTS_DIR="${SCRIPT_DIR}/results/ddl_impact_${TIMESTAMP}"
mkdir -p "${RESULTS_DIR}"

DDL_TIMINGS="${RESULTS_DIR}/ddl_timings.json"
COLLECTION_LOG="${RESULTS_DIR}/collection_log.json"
REPORT="${RESULTS_DIR}/ddl_impact_report.md"

echo "========================================"
echo "DDL Blocking Impact Measurement"
echo "========================================"
echo "Duration: ${DURATION}s"
echo "Flight recorder interval: ${INTERVAL}s"
echo "Results directory: ${RESULTS_DIR}"
echo ""

# Check if flight recorder is installed
echo "Checking flight recorder installation..."
FR_INSTALLED=$(psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -qtA -c "
    SELECT EXISTS (
        SELECT 1 FROM pg_namespace WHERE nspname = 'flight_recorder'
    );
")

if [ "$FR_INSTALLED" != "t" ]; then
    echo "ERROR: flight_recorder not installed. Installing..."
    psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -f "${SCRIPT_DIR}/../install.sql"
fi

# Configure flight recorder for the test
echo "Configuring flight recorder (${INTERVAL}s interval)..."
psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -c "
    -- Set sampling interval
    UPDATE flight_recorder.config
    SET value = '${INTERVAL}'
    WHERE key = 'sample_interval_seconds';

    -- Disable adaptive sampling (we want consistent timing)
    UPDATE flight_recorder.config
    SET value = 'false'
    WHERE key = 'adaptive_sampling';

    -- Enable snapshot-based collection (reduces locks from 3 to 1)
    UPDATE flight_recorder.config
    SET value = 'true'
    WHERE key = 'snapshot_based_collection';

    -- Enable flight recorder
    SELECT flight_recorder.enable();
"

# Wait for first collection to ensure jobs are running
echo "Waiting for first collection..."
sleep $((INTERVAL + 5))

# Clear collection stats to start fresh
psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -c "
    TRUNCATE flight_recorder.collection_stats;
"

# Start background process to log collections
echo "Starting collection monitor..."
{
    while true; do
        psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -qtA -c "
            SELECT json_agg(row_to_json(t))
            FROM (
                SELECT
                    started_at,
                    collection_type,
                    duration_ms,
                    success
                FROM flight_recorder.collection_stats
                ORDER BY started_at DESC
                LIMIT 1
            ) t;
        " >> "${COLLECTION_LOG}"
        sleep 10
    done
} &
MONITOR_PID=$!

echo "Starting DDL collision test..."
echo "Running ${DURATION}s of DDL operations..."
echo ""

# Run the DDL collision scenario
bash "${SCRIPT_DIR}/scenarios/ddl_collision.sh" "${DURATION}" "${DDL_TIMINGS}"

# Stop collection monitor
kill $MONITOR_PID 2>/dev/null || true

echo ""
echo "Test complete. Analyzing results..."

# Analyze results with Python
python3 - <<EOF
import json
import sys
from datetime import datetime

# Load DDL timings
with open('${DDL_TIMINGS}', 'r') as f:
    ddl_data = json.load(f)

operations = ddl_data['ddl_operations']

if not operations:
    print("ERROR: No DDL operations recorded")
    sys.exit(1)

# Calculate statistics
total_ops = len(operations)
blocked_ops = [op for op in operations if op.get('was_blocked', False)]
blocked_count = len(blocked_ops)

durations = [op['duration_ms'] for op in operations]
durations.sort()

# Percentiles
def percentile(data, p):
    if not data:
        return 0
    k = (len(data) - 1) * p / 100
    f = int(k)
    c = f + 1 if f + 1 < len(data) else f
    return data[f] + (k - f) * (data[c] - data[f])

p50 = percentile(durations, 50)
p95 = percentile(durations, 95)
p99 = percentile(durations, 99)
mean = sum(durations) / len(durations) if durations else 0
max_duration = max(durations) if durations else 0
min_duration = min(durations) if durations else 0

# Analyze blocked operations
blocked_durations = [op['duration_ms'] for op in blocked_ops]
blocked_durations.sort()

blocked_p50 = percentile(blocked_durations, 50) if blocked_durations else 0
blocked_p95 = percentile(blocked_durations, 95) if blocked_durations else 0
blocked_p99 = percentile(blocked_durations, 99) if blocked_durations else 0
blocked_mean = sum(blocked_durations) / len(blocked_durations) if blocked_durations else 0

# Calculate collision rate
collision_rate = (blocked_count / total_ops * 100) if total_ops > 0 else 0

# Check for flight recorder in blocked_by
fr_blocked = 0
for op in blocked_ops:
    blocked_by = op.get('blocked_by', '')
    if blocked_by and 'flight_recorder' in blocked_by.lower():
        fr_blocked += 1

fr_collision_rate = (fr_blocked / total_ops * 100) if total_ops > 0 else 0

# Generate report
report = f"""# DDL Blocking Impact Report

**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
**Test Duration:** ${DURATION}s
**Flight Recorder Interval:** ${INTERVAL}s

## Summary

- **Total DDL Operations:** {total_ops:,}
- **Blocked Operations:** {blocked_count:,} ({collision_rate:.2f}%)
- **Blocked by Flight Recorder:** {fr_blocked:,} ({fr_collision_rate:.2f}%)

## All DDL Operations (Duration)

| Metric | Duration (ms) |
|--------|---------------|
| Minimum | {min_duration:.2f} |
| Mean | {mean:.2f} |
| Median (P50) | {p50:.2f} |
| P95 | {p95:.2f} |
| P99 | {p99:.2f} |
| Maximum | {max_duration:.2f} |

## Blocked Operations Only

"""

if blocked_count > 0:
    report += f"""| Metric | Duration (ms) |
|--------|---------------|
| Mean | {blocked_mean:.2f} |
| Median (P50) | {blocked_p50:.2f} |
| P95 | {blocked_p95:.2f} |
| P99 | {blocked_p99:.2f} |

**Average Delay from Blocking:** {blocked_mean - mean:.2f} ms
"""
else:
    report += "No operations were blocked during the test.\n"

# Add collision analysis
expected_collisions_per_day = (480 / ${INTERVAL}) * (${DURATION} / 3600) * fr_collision_rate / 100 * (86400 / ${DURATION})

report += f"""
## Impact Assessment

**At {${INTERVAL}}s intervals (normal mode):**
- Flight recorder runs: {480 * ${INTERVAL} // 180:.0f} collections/day
- Expected DDL collisions: ~{expected_collisions_per_day:.1f} per day
- If you run 100 DDL ops/hour: ~{expected_collisions_per_day * 100 / 24:.1f} will encounter blocking

**Risk Level:**
"""

if fr_collision_rate < 1:
    report += "- **LOW** - Minimal DDL impact\n"
elif fr_collision_rate < 5:
    report += "- **MODERATE** - Acceptable for most workloads\n"
else:
    report += "- **HIGH** - Consider emergency mode (300s) for DDL-heavy workloads\n"

report += f"""
**Recommendation:**
"""

if fr_collision_rate < 2:
    report += "- Safe for production use with high DDL workloads\n"
elif fr_collision_rate < 5:
    report += "- Safe for typical workloads\n- Monitor if >50 DDL ops/hour\n"
else:
    report += "- Use emergency mode (300s intervals) for DDL-heavy periods\n- Consider scheduling DDL operations during low-traffic windows\n"

# DDL type breakdown
ddl_types = {{}}
for op in operations:
    ddl_type = op.get('ddl_type', 'unknown')
    if ddl_type not in ddl_types:
        ddl_types[ddl_type] = {{'total': 0, 'blocked': 0, 'durations': []}}
    ddl_types[ddl_type]['total'] += 1
    ddl_types[ddl_type]['durations'].append(op['duration_ms'])
    if op.get('was_blocked', False):
        ddl_types[ddl_type]['blocked'] += 1

report += f"""
## DDL Type Breakdown

| DDL Type | Count | Blocked | Block Rate | Avg Duration (ms) |
|----------|-------|---------|------------|-------------------|
"""

for ddl_type, stats in sorted(ddl_types.items()):
    avg_dur = sum(stats['durations']) / len(stats['durations']) if stats['durations'] else 0
    block_rate = (stats['blocked'] / stats['total'] * 100) if stats['total'] > 0 else 0
    report += f"| {ddl_type} | {stats['total']} | {stats['blocked']} | {block_rate:.1f}% | {avg_dur:.2f} |\n"

report += f"""
## Raw Data

- DDL timings: \`{DDL_TIMINGS}\`
- Collection log: \`{COLLECTION_LOG}\`

## Methodology

This benchmark measures actual DDL blocking by:
1. Running flight recorder at {${INTERVAL}}s intervals
2. Continuously executing DDL operations (ALTER, CREATE INDEX, DROP, etc.)
3. Measuring when DDL operations wait for locks
4. Detecting if flight recorder is the blocker

The collision rate represents the probability that a DDL operation will encounter
an AccessShareLock held by flight recorder's catalog queries.
"""

# Write report
with open('${REPORT}', 'w') as f:
    f.write(report)

print(report)

EOF

echo ""
echo "========================================"
echo "Results saved to:"
echo "  Report: ${REPORT}"
echo "  DDL timings: ${DDL_TIMINGS}"
echo "  Collection log: ${COLLECTION_LOG}"
echo "========================================"
