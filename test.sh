#!/bin/bash
set -e

# Test runner for pg-flight-recorder
# Usage: ./test.sh [version]
#   version: 15, 16, 17 (runs single version)
#   no args: runs all versions in parallel (default)
#
# Examples:
#   ./test.sh           # Test on PostgreSQL 15, 16, 17 in parallel
#   ./test.sh 16        # Test on PostgreSQL 16 only

VERSION="${1:-all}"

run_single_version() {
    local pg_version=$1
    local service="postgres${pg_version}"
    local profile="pg${pg_version}"

    echo ""
    echo "========================================="
    echo "Testing on PostgreSQL $pg_version"
    echo "========================================="

    # Clean up any existing containers
    docker compose --profile $profile down -v 2>/dev/null || true

    # Build and start
    echo "Building PostgreSQL $pg_version image with pg_cron..."
    docker compose --profile $profile build --quiet

    echo "Starting PostgreSQL $pg_version..."
    docker compose --profile $profile up -d

    echo "Waiting for PostgreSQL to be ready..."
    for _ in {1..30}; do
        if docker compose --profile $profile exec -T $service pg_isready -U postgres > /dev/null 2>&1; then
            break
        fi
        sleep 1
    done

    echo "Installing pg_cron extension..."
    docker compose --profile $profile exec -T $service psql -U postgres -d postgres -c "CREATE EXTENSION IF NOT EXISTS pg_cron;" > /dev/null

    echo "Installing pg-flight-recorder..."
    docker compose --profile $profile exec -T $service psql -U postgres -d postgres -f /install.sql > /dev/null

    echo "Installing pgTAP extension..."
    docker compose --profile $profile exec -T $service psql -U postgres -d postgres -c "CREATE EXTENSION IF NOT EXISTS pgtap;" > /dev/null

    echo "Disabling scheduled jobs for testing..."
    docker compose --profile $profile exec -T $service psql -U postgres -d postgres -c "SELECT flight_recorder.disable();" > /dev/null

    echo "Running tests with per-file timing..."
    docker compose --profile $profile exec -T $service sh -c 'pg_prove --timer -U postgres -d postgres /tests/*.sql'

    echo "PostgreSQL $pg_version: PASS"

    # Clean up
    docker compose --profile $profile down -v
}

run_all_parallel() {
    echo ""
    echo "========================================="
    echo "Running parallel tests on PG 15, 16, 17"
    echo "========================================="

    # Clean up any existing containers
    docker compose --profile all down -v 2>/dev/null || true

    # Build all images in parallel
    echo "Building PostgreSQL images with pg_cron..."
    docker compose --profile all build --quiet --parallel

    # Start all PostgreSQL instances
    echo "Starting all PostgreSQL instances..."
    docker compose --profile all up -d

    # Wait for all instances to be ready
    echo "Waiting for all PostgreSQL instances to be ready..."
    for service in postgres15 postgres16 postgres17; do
        for _ in {1..30}; do
            if docker compose --profile all exec -T $service pg_isready -U postgres > /dev/null 2>&1; then
                break
            fi
            sleep 1
        done
    done

    # Setup all instances in parallel
    echo "Setting up extensions on all instances..."
    for service in postgres15 postgres16 postgres17; do
        (
            docker compose --profile all exec -T $service psql -U postgres -d postgres -c "CREATE EXTENSION IF NOT EXISTS pg_cron;" > /dev/null
            docker compose --profile all exec -T $service psql -U postgres -d postgres -f /install.sql > /dev/null
            docker compose --profile all exec -T $service psql -U postgres -d postgres -c "CREATE EXTENSION IF NOT EXISTS pgtap;" > /dev/null
            docker compose --profile all exec -T $service psql -U postgres -d postgres -c "SELECT flight_recorder.disable();" > /dev/null
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
            if docker compose --profile all exec -T $service sh -c 'pg_prove --timer -U postgres -d postgres /tests/*.sql' >> "$RESULTS_DIR/$version.log" 2>&1; then
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
    docker compose --profile all down -v

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

if [ "$VERSION" = "all" ]; then
    run_all_parallel
elif [ "$VERSION" = "15" ] || [ "$VERSION" = "16" ] || [ "$VERSION" = "17" ]; then
    run_single_version $VERSION
else
    echo "Usage: ./test.sh [version]"
    echo "  version: 15, 16, 17 (single version) or omit for all versions in parallel"
    exit 1
fi
