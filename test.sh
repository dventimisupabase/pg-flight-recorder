#!/bin/bash
set -e

# Test runner for pg-flight-recorder
# Usage: ./test.sh [version]
#   version: 15, 16, 17, or "all" (default: 16)
#
# Examples:
#   ./test.sh        # Test on PostgreSQL 16
#   ./test.sh 15     # Test on PostgreSQL 15
#   ./test.sh all    # Test on all versions (15, 16, 17)
#
# Note: PostgreSQL 18 support pending (requires Docker volume layout changes)

VERSION="${1:-16}"

run_tests() {
    local pg_version=$1
    echo ""
    echo "========================================="
    echo "Testing on PostgreSQL $pg_version"
    echo "========================================="

    # Clean up any existing container
    docker-compose down -v 2>/dev/null || true

    # Build image with pg_cron
    echo "Building PostgreSQL $pg_version image with pg_cron..."
    PG_VERSION=$pg_version docker-compose build --quiet

    # Start PostgreSQL with specified version
    echo "Starting PostgreSQL $pg_version with pg_cron..."
    PG_VERSION=$pg_version docker-compose up -d

    echo "Waiting for PostgreSQL to be ready..."
    for _ in {1..30}; do
        if docker-compose exec -T postgres pg_isready -U postgres > /dev/null 2>&1; then
            break
        fi
        sleep 1
    done

    echo "Installing pg_cron extension..."
    docker-compose exec -T postgres psql -U postgres -d postgres -c "CREATE EXTENSION IF NOT EXISTS pg_cron;" > /dev/null

    echo "Installing pg-flight-recorder..."
    docker-compose exec -T postgres psql -U postgres -d postgres -f /install.sql > /dev/null

    echo "Installing pgTAP extension..."
    docker-compose exec -T postgres psql -U postgres -d postgres -c "CREATE EXTENSION IF NOT EXISTS pgtap;" > /dev/null

    echo "Running tests..."
    docker-compose exec -T postgres pg_prove -U postgres -d postgres /flight_recorder_test.sql

    echo "PostgreSQL $pg_version: PASS"
}

if [ "$VERSION" = "all" ]; then
    for v in 15 16 17; do
        run_tests $v
    done
    echo ""
    echo "========================================="
    echo "All versions passed!"
    echo "========================================="
else
    run_tests $VERSION
fi

# Clean up
docker-compose down -v
