#!/bin/bash
set -e

# Test runner for pg-flight-recorder
# Usage: ./test.sh [version]
#   version: 15, 16, 17, "all", or "parallel" (default: 16)
#
# Examples:
#   ./test.sh           # Test on PostgreSQL 16
#   ./test.sh 15        # Test on PostgreSQL 15
#   ./test.sh all       # Test on all versions sequentially (15, 16, 17)
#   ./test.sh parallel  # Test on all versions in parallel (fastest)
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

    echo "Disabling scheduled jobs for testing..."
    docker-compose exec -T postgres psql -U postgres -d postgres -c "SELECT flight_recorder.disable();" > /dev/null

    echo "Running tests with per-file timing..."
    docker-compose exec -T postgres sh -c 'pg_prove --timer -U postgres -d postgres /tests/*.sql'

    echo "PostgreSQL $pg_version: PASS"
}

run_parallel_tests() {
    echo ""
    echo "========================================="
    echo "Running parallel tests on PG 15, 16, 17"
    echo "========================================="

    # Clean up any existing containers
    docker-compose -f docker-compose.parallel.yml down -v 2>/dev/null || true

    # Build all images in parallel
    echo "Building PostgreSQL images with pg_cron..."
    docker-compose -f docker-compose.parallel.yml build --quiet --parallel

    # Start all PostgreSQL instances
    echo "Starting all PostgreSQL instances..."
    docker-compose -f docker-compose.parallel.yml up -d

    # Wait for all instances to be ready
    echo "Waiting for all PostgreSQL instances to be ready..."
    for service in postgres15 postgres16 postgres17; do
        for _ in {1..30}; do
            if docker-compose -f docker-compose.parallel.yml exec -T $service pg_isready -U postgres > /dev/null 2>&1; then
                break
            fi
            sleep 1
        done
    done

    # Setup all instances in parallel
    echo "Setting up extensions on all instances..."
    for service in postgres15 postgres16 postgres17; do
        (
            docker-compose -f docker-compose.parallel.yml exec -T $service psql -U postgres -d postgres -c "CREATE EXTENSION IF NOT EXISTS pg_cron;" > /dev/null
            docker-compose -f docker-compose.parallel.yml exec -T $service psql -U postgres -d postgres -f /install.sql > /dev/null
            docker-compose -f docker-compose.parallel.yml exec -T $service psql -U postgres -d postgres -c "CREATE EXTENSION IF NOT EXISTS pgtap;" > /dev/null
            docker-compose -f docker-compose.parallel.yml exec -T $service psql -U postgres -d postgres -c "SELECT flight_recorder.disable();" > /dev/null
        ) &
    done
    wait

    # Run tests in parallel, capturing output
    echo "Running tests in parallel..."
    echo ""

    PIDS=()
    RESULTS_DIR=$(mktemp -d)

    for service in postgres15 postgres16 postgres17; do
        version="${service#postgres}"
        (
            echo "=========================================" > "$RESULTS_DIR/$version.log"
            echo "PostgreSQL $version" >> "$RESULTS_DIR/$version.log"
            echo "=========================================" >> "$RESULTS_DIR/$version.log"
            if docker-compose -f docker-compose.parallel.yml exec -T $service sh -c 'pg_prove --timer -U postgres -d postgres /tests/*.sql' >> "$RESULTS_DIR/$version.log" 2>&1; then
                echo "PASS" > "$RESULTS_DIR/$version.status"
            else
                echo "FAIL" > "$RESULTS_DIR/$version.status"
            fi
        ) &
        PIDS+=($!)
    done

    # Wait for all tests to complete
    FAILED=0
    for pid in "${PIDS[@]}"; do
        wait $pid || FAILED=1
    done

    # Display results
    for version in 15 16 17; do
        cat "$RESULTS_DIR/$version.log"
        echo ""
        STATUS=$(cat "$RESULTS_DIR/$version.status")
        if [ "$STATUS" = "FAIL" ]; then
            echo "PostgreSQL $version: FAIL"
            FAILED=1
        else
            echo "PostgreSQL $version: PASS"
        fi
        echo ""
    done

    rm -rf "$RESULTS_DIR"

    # Clean up
    docker-compose -f docker-compose.parallel.yml down -v

    if [ $FAILED -eq 1 ]; then
        echo "========================================="
        echo "Some tests failed!"
        echo "========================================="
        exit 1
    fi

    echo "========================================="
    echo "All parallel tests passed!"
    echo "========================================="
}

if [ "$VERSION" = "parallel" ]; then
    run_parallel_tests
elif [ "$VERSION" = "all" ]; then
    for v in 15 16 17; do
        run_tests $v
    done
    echo ""
    echo "========================================="
    echo "All versions passed!"
    echo "========================================="
    # Clean up
    docker-compose down -v
else
    run_tests $VERSION
    # Clean up
    docker-compose down -v
fi
