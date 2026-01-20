# Pathological Data Generators for pg-flight-recorder

## Purpose

This test suite validates that pg-flight-recorder can detect real-world database pathologies by:

1. **Generating** authentic problematic conditions in the database
2. **Capturing** the data using pg-flight-recorder's snapshot/sample functions
3. **Verifying** that diagnostic functions correctly identify the issues

These tests serve as both validation and living documentation, proving that the diagnostic playbooks work end-to-end.

## Test Files

- **`07_pathology_generators.sql`** - Initial proof-of-concept with 2 pathologies

## Current Pathologies Covered

### 1. Lock Contention (Test #1)

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

### 2. Memory Pressure / work_mem Issues (Test #2)

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

- [ ] Database Slow (Real-time)
- [ ] Database Slow (Historical)
- [ ] Queries Timing Out
- [ ] High CPU Usage
- [x] Lock Contention
- [ ] Connection Exhaustion
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

Priority pathologies to add next:

1. **High CPU Usage** (Section 4)
   - CPU-bound queries with complex calculations
   - Verify `blk_read_time = 0` and high `total_exec_time`

2. **Connection Exhaustion** (Section 6)
   - Create many connections approaching `max_connections`
   - Verify `connections_total` near `connections_max`

3. **Checkpoint Storms** (Section 8)
   - Generate heavy WAL traffic
   - Verify `FORCED_CHECKPOINT` anomalies

4. **Disk I/O Problems** (Section 7)
   - Large sequential scans
   - Verify `IO:DataFileRead` wait events

5. **Queries Timing Out** (Section 3)
   - Missing indexes causing slow queries
   - Verify high `mean_exec_time` in statement_snapshots

## Questions or Issues?

For questions about pathology tests or ideas for new pathologies:

- Review DIAGNOSTIC_PLAYBOOKS.md for inspiration
- Check existing tests for patterns
- File an issue in the repository

---

**Last Updated**: 2026-01-20
**Maintainer**: Flight Recorder Team
