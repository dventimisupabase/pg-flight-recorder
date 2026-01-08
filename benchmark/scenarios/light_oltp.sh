#!/usr/bin/env bash
# Light OLTP workload scenario

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Default parameters
DURATION=30  # minutes
CLIENTS=10
OUTPUT=""

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --duration)
            DURATION="$2"
            shift 2
            ;;
        --clients)
            CLIENTS="$2"
            shift 2
            ;;
        --output)
            OUTPUT="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1" >&2
            exit 1
            ;;
    esac
done

if [[ -z "$OUTPUT" ]]; then
    echo "Error: --output required" >&2
    exit 1
fi

# Workload definition: Light OLTP
# 80% SELECT, 15% UPDATE, 5% INSERT
# Simulates typical e-commerce read-heavy workload

SECONDS_DURATION=$((DURATION * 60))

echo "[$(date +'%Y-%m-%d %H:%M:%S')] Starting light_oltp workload..."
echo "  Duration: ${DURATION}m (${SECONDS_DURATION}s)"
echo "  Clients: $CLIENTS"
echo "  Output: $OUTPUT"

# Custom pgbench script for realistic mixed workload
cat > /tmp/light_oltp.sql <<'EOF'
-- 80% probability: SELECT (point lookup)
\set aid random(1, 100000 * :scale)
SELECT abalance FROM pgbench_accounts WHERE aid = :aid;

-- 15% probability: UPDATE
\set aid random(1, 100000 * :scale)
\set delta random(-5000, 5000)
UPDATE pgbench_accounts SET abalance = abalance + :delta WHERE aid = :aid;

-- 5% probability: INSERT (new transaction record)
\set aid random(1, 100000 * :scale)
\set tid random(1, 10 * :scale)
\set bid random(1, 1 * :scale)
\set delta random(-5000, 5000)
INSERT INTO pgbench_history (tid, bid, aid, delta, mtime) VALUES (:tid, :bid, :aid, :delta, CURRENT_TIMESTAMP);
EOF

# Record start time
START_TIME=$(date +%s)
START_TIMESTAMP=$(date -u +"%Y-%m-%d %H:%M:%S UTC")

# Capture initial pg_stat_database metrics
psql -t -c "
SELECT row_to_json(t)
FROM (
    SELECT
        current_database() as database,
        xact_commit,
        xact_rollback,
        blks_read,
        blks_hit,
        tup_returned,
        tup_fetched,
        tup_inserted,
        tup_updated,
        tup_deleted
    FROM pg_stat_database
    WHERE datname = current_database()
) t
" | jq '.' > /tmp/light_oltp_start.json

# Run pgbench
pgbench \
    --file=/tmp/light_oltp.sql \
    --client=$CLIENTS \
    --jobs=$CLIENTS \
    --time=$SECONDS_DURATION \
    --progress=60 \
    --log \
    --log-prefix=/tmp/light_oltp_log \
    2>&1 | tee /tmp/light_oltp_output.txt

# Record end time
END_TIME=$(date +%s)
END_TIMESTAMP=$(date -u +"%Y-%m-%d %H:%M:%S UTC")
ACTUAL_DURATION=$((END_TIME - START_TIME))

# Capture final pg_stat_database metrics
psql -t -c "
SELECT row_to_json(t)
FROM (
    SELECT
        current_database() as database,
        xact_commit,
        xact_rollback,
        blks_read,
        blks_hit,
        tup_returned,
        tup_fetched,
        tup_inserted,
        tup_updated,
        tup_deleted
    FROM pg_stat_database
    WHERE datname = current_database()
) t
" | jq '.' > /tmp/light_oltp_end.json

# Parse pgbench output
TPS=$(grep "^tps = " /tmp/light_oltp_output.txt | sed 's/tps = \([0-9.]*\).*/\1/')
LATENCY_AVG=$(grep "^latency average = " /tmp/light_oltp_output.txt | sed 's/latency average = \([0-9.]*\).*/\1/')
LATENCY_STDDEV=$(grep "^latency stddev = " /tmp/light_oltp_output.txt | sed 's/latency stddev = \([0-9.]*\).*/\1/' || echo "0")

# Calculate percentiles from pgbench log files
# pgbench creates per-client log files: light_oltp_log.{client_id}
if command -v python3 &> /dev/null; then
    PERCENTILES=$(python3 <<PYTHON
import glob
import numpy as np

latencies = []
for logfile in glob.glob("/tmp/light_oltp_log.*"):
    with open(logfile) as f:
        for line in f:
            parts = line.strip().split()
            if len(parts) >= 3:
                # Format: client_id txn_time timestamp latency_us
                latency_ms = float(parts[2]) / 1000.0
                latencies.append(latency_ms)

if latencies:
    p50 = np.percentile(latencies, 50)
    p95 = np.percentile(latencies, 95)
    p99 = np.percentile(latencies, 99)
    pmax = max(latencies)
    print(f"{p50:.2f},{p95:.2f},{p99:.2f},{pmax:.2f}")
else:
    print("0,0,0,0")
PYTHON
)
    IFS=',' read -r P50 P95 P99 PMAX <<< "$PERCENTILES"
else
    P50="0"
    P95="0"
    P99="0"
    PMAX="0"
fi

# Build JSON output
cat > "$OUTPUT" <<EOF
{
  "scenario": "light_oltp",
  "start_time": "$START_TIMESTAMP",
  "end_time": "$END_TIMESTAMP",
  "duration_seconds": $ACTUAL_DURATION,
  "clients": $CLIENTS,
  "throughput": {
    "tps": $TPS,
    "transactions_total": $(echo "$TPS * $ACTUAL_DURATION" | bc)
  },
  "latency_ms": {
    "mean": $LATENCY_AVG,
    "stddev": $LATENCY_STDDEV,
    "p50": $P50,
    "p95": $P95,
    "p99": $P99,
    "max": $PMAX
  },
  "database_stats": {
    "start": $(cat /tmp/light_oltp_start.json),
    "end": $(cat /tmp/light_oltp_end.json)
  }
}
EOF

# Cleanup
rm -f /tmp/light_oltp.sql /tmp/light_oltp_output.txt /tmp/light_oltp_log.* /tmp/light_oltp_start.json /tmp/light_oltp_end.json

echo "[$(date +'%Y-%m-%d %H:%M:%S')] light_oltp workload complete"
echo "  TPS: $TPS"
echo "  Latency (mean): ${LATENCY_AVG}ms"
echo "  Latency (p95): ${P95}ms"
echo "  Results: $OUTPUT"
