# Pathological Data Generators for pg-flight-recorder

## Purpose

This test suite validates that pg-flight-recorder can detect real-world database pathologies by:

1. **Generating** authentic problematic conditions in the database
2. **Capturing** the data using pg-flight-recorder's snapshot/sample functions
3. **Verifying** that diagnostic functions correctly identify the issues

These tests serve as both validation and living documentation, proving that the diagnostic playbooks work end-to-end.

## Test Files

- **`07_pathology_generators.sql`** - 48 pgTAP tests covering all 9 pathologies

## All 9 Pathologies Covered

### 1. Lock Contention

**Based on:** DIAGNOSTIC_PLAYBOOKS.md Section 5

**Pathology Generated:**

- Advisory locks held across transactions
- Simulates scenarios where one session blocks others

**Detection Verified:**

- `recent_locks_current()` function executes
- `lock_samples_ring` captures lock events
- `pg_locks` system view is accessible

**Real-world analogue:**

- Long-running transactions holding row/table locks
- Multiple sessions competing for same resources
- Idle-in-transaction sessions blocking others

### 2. Memory Pressure / work_mem Issues

**Based on:** DIAGNOSTIC_PLAYBOOKS.md Section 9

**Pathology Generated:**

- Large dataset (10,000 rows with substantial text data)
- Low `work_mem` setting (64kB)
- Complex sorts and aggregations that exceed available memory
- Forces PostgreSQL to spill to temporary files

**Detection Verified:**

- `snapshots.temp_files` and `temp_bytes` are captured
- `statement_snapshots` contains query statistics
- `anomaly_report()` can analyze the time period

**Real-world analogue:**

- Under-provisioned `work_mem` causing performance degradation
- Large reporting queries spilling to disk
- Memory-intensive operations during peak load

### 3. High CPU Usage

**Based on:** DIAGNOSTIC_PLAYBOOKS.md Section 4

**Pathology Generated:**

- CPU-intensive mathematical calculations (sqrt, ln, exp, cos, power)
- Large aggregations over 50,000 rows
- Queries that are compute-bound (no disk I/O)

**Detection Verified:**

- `statement_snapshots` captures query statistics
- `recent_activity_current()` works for real-time monitoring
- Snapshots capture the test period

**Real-world analogue:**

- Complex analytical queries burning CPU
- Inefficient queries doing lots of computation
- JSON/text processing operations

### 4. Database Slow - Real-time

**Based on:** DIAGNOSTIC_PLAYBOOKS.md Section 1

**Pathology Generated:**

- Long-running operations using `pg_sleep()`
- Activity during the slow period captured via `sample()`

**Detection Verified:**

- `activity_samples_ring` captures activity
- `recent_activity_current()` shows current sessions
- `recent_waits_current()` shows wait events

**Real-world analogue:**

- Long-running transactions blocking resources
- Queries stuck waiting on locks or I/O
- Real-time performance issues requiring immediate triage

### 5. Queries Timing Out / Taking Forever

**Based on:** DIAGNOSTIC_PLAYBOOKS.md Section 3

**Pathology Generated:**

- Large table (20,000 rows) without indexes
- Sequential scans forced by queries on unindexed columns
- LIKE patterns and aggregations requiring full table scans

**Detection Verified:**

- `statement_snapshots` captures query execution statistics
- `compare()` function works for before/after analysis
- Can query `mean_exec_time` and `shared_blks_read`

**Real-world analogue:**

- Missing indexes causing slow queries
- Plan regressions after statistics changes
- Queries timing out under load

### 6. Connection Exhaustion

**Based on:** DIAGNOSTIC_PLAYBOOKS.md Section 6

**Pathology Generated:**

- Uses `dblink` extension to create 15 real concurrent connections
- Simulates connection pressure approaching `max_connections`

**Detection Verified:**

- `pg_stat_activity` shows multiple sessions
- `snapshots.connections_total` captures connection count
- `recent_activity_current()` shows active connections

**Real-world analogue:**

- Connection pool exhaustion
- Applications not releasing connections
- "Too many connections" errors

### 7. Database Slow - Historical

**Based on:** DIAGNOSTIC_PLAYBOOKS.md Section 2

**Pathology Generated:**

- Generates activity and captures multiple snapshots
- Creates time-range for historical analysis

**Detection Verified:**

- `summary_report()` executes for time range analysis
- `wait_summary()` executes for historical wait analysis
- `activity_samples_archive` is queryable

**Real-world analogue:**

- Investigating past performance incidents
- "Database was slow yesterday between 10-11am"
- Post-incident forensic analysis

### 8. Disk I/O Problems

**Based on:** DIAGNOSTIC_PLAYBOOKS.md Section 7

**Pathology Generated:**

- Large table (10,000 rows) with text padding
- Sequential scans forced by disabling index usage
- Cross joins to amplify I/O

**Detection Verified:**

- `wait_summary()` can filter by IO wait events
- `shared_blks_read` metrics queryable from statement_snapshots
- Buffer cache hit ratio calculations work

**Real-world analogue:**

- Slow storage causing query delays
- Missing indexes leading to sequential scans
- Cache misses requiring disk reads

### 9. Checkpoint Storms

**Based on:** DIAGNOSTIC_PLAYBOOKS.md Section 8

**Pathology Generated:**

- Triggers `CHECKPOINT` command
- Captures checkpoint metrics in snapshots

**Detection Verified:**

- Checkpoint timing data queryable from snapshots
- `anomaly_report()` can check for checkpoint anomalies
- WAL and buffer metrics captured

**Real-world analogue:**

- Performance dips during checkpoints
- Forced checkpoints due to WAL pressure
- Backend fsync interference

## How to Run

```bash
# Run all tests (including pathology tests)
./test.sh 16

# Run on all PostgreSQL versions
./test.sh all

# Run in parallel for speed
./test.sh parallel

# Run only pathology tests (if extracted separately)
docker-compose exec postgres pg_prove -U postgres -d postgres /tests/07_pathology_generators.sql
```

## Test Pattern

Each pathology follows this pattern:

```sql
-- =============================================================================
-- PATHOLOGY N: [NAME] (X tests)
-- Based on: DIAGNOSTIC_PLAYBOOKS.md - Section Y "[Title]"
--
-- Real-world scenario: [Description]
-- Expected detection: [What pg-flight-recorder should find]
-- =============================================================================

-- Setup: Create test objects
-- ... table creation, data insertion ...

SELECT ok(
    [setup verification],
    'PATHOLOGY [NAME]: Test setup should succeed'
);

-- Generate pathology
DO $$
BEGIN
    -- ... code to create the problematic condition ...
    PERFORM flight_recorder.snapshot(); -- or sample()
END;
$$;

-- Verify detection
SELECT ok(
    (SELECT ... FROM flight_recorder.[diagnostic_function]() WHERE ...),
    'PATHOLOGY [NAME]: Pathology should be detected'
);

-- Cleanup
DROP TABLE ...;
```

## Pathology Checklist

All 9 DIAGNOSTIC_PLAYBOOKS.md scenarios are now covered:

- [x] Database Slow (Real-time) - Section 1
- [x] Database Slow (Historical) - Section 2
- [x] Queries Timing Out - Section 3
- [x] High CPU Usage - Section 4
- [x] Lock Contention - Section 5
- [x] Connection Exhaustion - Section 6
- [x] Disk I/O Problems - Section 7
- [x] Checkpoint Storms - Section 8
- [x] Memory Pressure - Section 9

## Benefits of Pathology Testing

1. **Validation**: Proves pg-flight-recorder works on real problems
2. **Confidence**: CSAs and users know the tool detects what it claims
3. **Documentation**: Living examples of what pathologies look like
4. **Regression Prevention**: Ensures future changes don't break detection
5. **Playbook Verification**: Confirms diagnostic queries work as documented

## Questions or Issues?

For questions about pathology tests or ideas for new pathologies:

- Review DIAGNOSTIC_PLAYBOOKS.md for inspiration
- Check existing tests for patterns
- File an issue in the repository

---

**Last Updated**: 2026-01-20
**Maintainer**: Flight Recorder Team
**Coverage**: 9/9 pathologies (100%)
