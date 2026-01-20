# Pathological Data Generators for pg-flight-recorder

## Purpose

This test suite validates that pg-flight-recorder can detect real-world database pathologies by:

1. **Generating** authentic problematic conditions in the database
2. **Capturing** the data using pg-flight-recorder's snapshot/sample functions
3. **Verifying** that diagnostic functions correctly identify the issues

These tests serve as both validation and living documentation, proving that the diagnostic playbooks work end-to-end.

## Test Files

- **`07_pathology_generators.sql`** - 33 pgTAP tests covering 6 pathologies

## Current Pathologies Covered

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

## Adding New Pathologies

To add a new pathology based on DIAGNOSTIC_PLAYBOOKS.md:

### Step 1: Choose a Playbook Section

Review DIAGNOSTIC_PLAYBOOKS.md and pick an uncovered pathology:

- [x] Database Slow (Real-time)
- [ ] Database Slow (Historical)
- [x] Queries Timing Out
- [x] High CPU Usage
- [x] Lock Contention
- [x] Connection Exhaustion
- [ ] Disk I/O Problems
- [ ] Checkpoint Storms
- [x] Memory Pressure

### Step 2: Design the Generator

For each pathology, identify:

1. **Setup**: What database objects are needed?
   - Tables, indexes, data volume

2. **Pathology Generation**: How to create the problem?
   - SQL commands, configuration changes, workload patterns

3. **Capture**: When to call flight_recorder functions?
   - `flight_recorder.snapshot()` for system metrics
   - `flight_recorder.sample()` for current activity

4. **Verification**: What should be detected?
   - Which tables should contain data?
   - Which functions should return specific results?
   - What anomalies should be flagged?

### Step 3: Write the Test Section

Template for new pathology:

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

### Step 4: Update Test Count

Don't forget to update `SELECT plan(N)` at the top of the file with the new total test count.

## Example: High CPU Pathology

Here's how you'd add High CPU based on Playbook Section 4:

```sql
-- PATHOLOGY 3: HIGH CPU USAGE (4 tests)
-- Based on: DIAGNOSTIC_PLAYBOOKS.md - Section 4

CREATE TABLE test_cpu_intensive (
    id int,
    value numeric
);

INSERT INTO test_cpu_intensive
SELECT i, random() FROM generate_series(1, 100000) i;

-- Generate pathology: CPU-intensive calculations
DO $$
DECLARE
    v_result numeric;
BEGIN
    -- Capture before
    PERFORM flight_recorder.snapshot();

    -- CPU-intensive query (lots of mathematical operations)
    SELECT sum(
        sqrt(value) * ln(value + 1) * exp(value / 1000000.0)
    ) INTO v_result
    FROM test_cpu_intensive
    WHERE value > 0;

    -- Capture after
    PERFORM pg_sleep(0.1);
    PERFORM flight_recorder.snapshot();
END;
$$;

-- Verify: Should see high total_exec_time with blk_read_time = 0
SELECT ok(
    EXISTS (
        SELECT 1 FROM flight_recorder.statement_snapshots
        WHERE blk_read_time = 0
          AND total_exec_time > 0
    ),
    'CPU PATHOLOGY: Should capture CPU-bound queries'
);
```

## Pathology Testing Checklist

When implementing a new pathology test:

- [ ] Add clear comment header with playbook section reference
- [ ] Document expected detection behavior
- [ ] Use descriptive test names: `'PATHOLOGY [NAME]: description'`
- [ ] Clean up all test objects (tables, data)
- [ ] Verify test passes in all PostgreSQL versions (15, 16, 17)
- [ ] Update this documentation with the new pathology

## Benefits of Pathology Testing

1. **Validation**: Proves pg-flight-recorder works on real problems
2. **Confidence**: CSAs and users know the tool detects what it claims
3. **Documentation**: Living examples of what pathologies look like
4. **Regression Prevention**: Ensures future changes don't break detection
5. **Playbook Verification**: Confirms diagnostic queries work as documented

## Future Pathologies to Implement

Remaining pathologies to add (3 of 9):

1. **Database Slow - Historical** (Section 2)
   - Test archive tables and historical analysis
   - Verify `activity_samples_archive` and `summary_report()`

2. **Disk I/O Problems** (Section 7)
   - Large sequential scans
   - Verify `IO:DataFileRead` wait events

3. **Checkpoint Storms** (Section 8)
   - Generate heavy WAL traffic
   - Verify `FORCED_CHECKPOINT` anomalies
   - **Challenge:** May need config changes

## Questions or Issues?

For questions about pathology tests or ideas for new pathologies:

- Review DIAGNOSTIC_PLAYBOOKS.md for inspiration
- Check existing tests for patterns
- File an issue in the repository

---

**Last Updated**: 2026-01-20
**Maintainer**: Flight Recorder Team
