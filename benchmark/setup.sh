#!/usr/bin/env bash
# Setup benchmark environment for pg-flight-recorder

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log() {
    echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $*"
}

warn() {
    echo -e "${YELLOW}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $*"
}

error() {
    echo -e "${RED}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $*" >&2
}

# Check prerequisites
check_prerequisites() {
    log "Checking prerequisites..."

    # Check psql
    if ! command -v psql &> /dev/null; then
        error "psql not found. Install PostgreSQL client."
        exit 1
    fi

    # Check pgbench
    if ! command -v pgbench &> /dev/null; then
        error "pgbench not found. Install PostgreSQL contrib package."
        exit 1
    fi

    # Check database connection
    if ! psql -c "SELECT 1" &> /dev/null; then
        error "Cannot connect to PostgreSQL."
        error "Set PGHOST, PGPORT, PGUSER, PGDATABASE, PGPASSWORD or use .pgpass"
        exit 1
    fi

    log "✓ Prerequisites satisfied"
}

# Get PostgreSQL version
get_pg_version() {
    psql -t -c "SHOW server_version" | xargs
}

# Get system info
get_system_info() {
    log "Collecting system information..."

    local pg_version
    pg_version=$(get_pg_version)

    cat > "$SCRIPT_DIR/results/system_info.txt" <<EOF
Benchmark Environment Information
Generated: $(date -u +"%Y-%m-%d %H:%M:%S UTC")

PostgreSQL Version: $pg_version
Database: ${PGDATABASE:-$(psql -t -c "SELECT current_database()" | xargs)}
Host: ${PGHOST:-localhost}
Port: ${PGPORT:-5432}

PostgreSQL Configuration:
$(psql -t -c "SELECT name, setting, unit FROM pg_settings WHERE name IN (
    'shared_buffers',
    'work_mem',
    'maintenance_work_mem',
    'effective_cache_size',
    'max_connections',
    'max_parallel_workers',
    'max_parallel_workers_per_gather',
    'random_page_cost',
    'effective_io_concurrency'
) ORDER BY name")

System:
$(uname -a)

CPU Info:
$(if [[ "$OSTYPE" == "darwin"* ]]; then
    sysctl -n machdep.cpu.brand_string
    sysctl -n hw.ncpu | awk '{print $0 " logical cores"}'
else
    lscpu | grep -E "Model name|CPU\(s\):"
fi)

Memory:
$(if [[ "$OSTYPE" == "darwin"* ]]; then
    sysctl hw.memsize | awk '{print $2/1024/1024/1024 " GB"}'
else
    free -h | grep Mem
fi)
EOF

    log "✓ System info saved to results/system_info.txt"
}

# Initialize pgbench schema
init_pgbench() {
    local scale=${1:-100}  # Default scale factor = 100 (15MB)

    log "Initializing pgbench with scale factor $scale..."
    log "This creates ~$((scale * 15))MB of test data"

    # Drop existing pgbench tables if they exist
    psql -c "DROP TABLE IF EXISTS pgbench_accounts, pgbench_branches, pgbench_tellers, pgbench_history CASCADE" || true

    # Initialize pgbench
    pgbench -i -s "$scale" --quiet

    # Create indexes
    log "Creating indexes..."
    psql <<EOF
-- Ensure indexes exist
CREATE INDEX IF NOT EXISTS pgbench_accounts_aid ON pgbench_accounts(aid);
CREATE INDEX IF NOT EXISTS pgbench_branches_bid ON pgbench_branches(bid);
CREATE INDEX IF NOT EXISTS pgbench_tellers_tid ON pgbench_tellers(tid);

-- Analyze for better query plans
ANALYZE pgbench_accounts;
ANALYZE pgbench_branches;
ANALYZE pgbench_tellers;
ANALYZE pgbench_history;
EOF

    log "✓ pgbench initialized"
}

# Create analytical tables for analytical workload scenario
create_analytical_schema() {
    log "Creating analytical tables..."

    psql <<'EOF'
-- Drop existing analytical schema
DROP TABLE IF EXISTS analytics.user_events CASCADE;
DROP TABLE IF EXISTS analytics.aggregations CASCADE;
DROP SCHEMA IF EXISTS analytics CASCADE;

-- Create schema
CREATE SCHEMA analytics;

-- User events table (fact table)
CREATE TABLE analytics.user_events (
    event_id BIGSERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL DEFAULT now(),
    session_id UUID NOT NULL,
    properties JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Generate sample data (1M rows ~ 100MB)
INSERT INTO analytics.user_events (user_id, event_type, event_timestamp, session_id, properties)
SELECT
    (random() * 100000)::integer,
    (ARRAY['page_view', 'click', 'purchase', 'signup', 'logout'])[floor(random() * 5 + 1)],
    now() - (random() * interval '90 days'),
    gen_random_uuid(),
    jsonb_build_object(
        'page', '/page-' || floor(random() * 100),
        'duration_ms', floor(random() * 10000),
        'browser', (ARRAY['chrome', 'firefox', 'safari'])[floor(random() * 3 + 1)]
    )
FROM generate_series(1, 1000000);

-- Create indexes
CREATE INDEX idx_user_events_user_id ON analytics.user_events(user_id);
CREATE INDEX idx_user_events_timestamp ON analytics.user_events(event_timestamp);
CREATE INDEX idx_user_events_type ON analytics.user_events(event_type);
CREATE INDEX idx_user_events_properties ON analytics.user_events USING gin(properties);

-- Pre-aggregation table
CREATE TABLE analytics.aggregations (
    date DATE NOT NULL,
    event_type TEXT NOT NULL,
    user_count BIGINT,
    event_count BIGINT,
    PRIMARY KEY (date, event_type)
);

ANALYZE analytics.user_events;
ANALYZE analytics.aggregations;
EOF

    log "✓ Analytical schema created"
}

# Main setup
main() {
    local scale=100
    local skip_data=false

    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            --scale)
                scale="$2"
                shift 2
                ;;
            --skip-data)
                skip_data=true
                shift
                ;;
            --help)
                cat <<EOF
Setup benchmark environment for pg-flight-recorder

Usage: $0 [OPTIONS]

Options:
    --scale N       pgbench scale factor (default: 100 = ~15MB)
    --skip-data     Skip data initialization (use existing tables)
    --help          Show this help

Examples:
    $0                    # Setup with default scale
    $0 --scale 1000       # Larger dataset (~1.5GB)
    $0 --skip-data        # Only check prerequisites

Environment:
    Use standard libpq environment variables:
    PGHOST, PGPORT, PGUSER, PGDATABASE, PGPASSWORD
    Or configure ~/.pgpass
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

    log "=== pg-flight-recorder Benchmark Setup ==="
    log ""

    check_prerequisites
    get_system_info

    if [[ "$skip_data" == "false" ]]; then
        init_pgbench "$scale"
        create_analytical_schema
    fi

    log ""
    log "=== Setup Complete ==="
    log ""
    log "Next steps:"
    log "  1. Review system info: cat results/system_info.txt"
    log "  2. Run benchmarks: ./run.sh"
    log ""
}

main "$@"
