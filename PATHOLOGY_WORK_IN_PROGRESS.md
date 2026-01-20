# Pathology Generators - Work in Progress

## Session Context & Goal

**Original Question:** Can we use DIAGNOSTIC_PLAYBOOKS.md as a guide to write generators of pathological data, feed them into pg-flight-recorder, and verify they come out the other side correctly?

**Answer:** Yes! And we started implementing it.

## What We Accomplished

### 1. Created Pathology Test Framework

**New Files:**
- `tests/07_pathology_generators.sql` - 12 pgTAP tests covering 2 pathologies
- `tests/PATHOLOGY_TESTS.md` - Comprehensive documentation and guide
- `PR_DESCRIPTION.md` - Ready-to-use PR description

**Git Status:**
- Branch: `claude/pathological-data-generators-9ZPGg`
- All changes committed and pushed
- PR needs to be created at: https://github.com/dventimisupabase/pg-flight-recorder/pull/new/claude/pathological-data-generators-9ZPGg

### 2. Pathologies Implemented (2 of 9)

#### ✅ Pathology 1: Lock Contention
**Location:** `tests/07_pathology_generators.sql` lines 20-99
**Based on:** DIAGNOSTIC_PLAYBOOKS.md Section 5

**Approach:**
- Uses advisory locks (`pg_advisory_lock(12345)`) to simulate blocking
- Advisory locks are ideal because:
  - Explicit and controllable
  - Show up in `pg_locks` like row/table locks
  - Don't require multiple database connections
- Captures state with `flight_recorder.sample()`
- Tests that `recent_locks_current()` and `lock_samples_ring` work

**Tests (6 total):**
1. Test table created
2. `sample()` executes after lock operations
3. `lock_samples_ring` is queryable
4. `recent_locks_current()` executes without error
5. `pg_locks` system view is accessible
6. Test cleanup

**Known Limitation:**
- Advisory locks in a single transaction don't create *blocked* sessions
- Tests verify infrastructure works, but may not capture actual blocking
- **POTENTIAL IMPROVEMENT:** Need multi-session test to create real blocking

#### ✅ Pathology 2: Memory Pressure / work_mem
**Location:** `tests/07_pathology_generators.sql` lines 101-203
**Based on:** DIAGNOSTIC_PLAYBOOKS.md Section 9

**Approach:**
- Creates table with 10,000 rows of substantial text data
- Sets `work_mem = '64kB'` (very low)
- Runs large sorts and aggregations to force temp file spills
- Captures before/after snapshots
- Verifies temp file metrics are captured

**Tests (6 total):**
1. Test table has 10,000 rows
2. At least 2 snapshots exist from test period
3. Snapshots capture `temp_files` metric
4. `statement_snapshots` contains query statistics
5. `anomaly_report()` executes on pathology time range
6. Test cleanup

**What We're Testing:**
- `snapshots.temp_files` and `temp_bytes` increase
- `statement_snapshots.temp_blks_written > 0`
- `anomaly_report()` can analyze the period

## What We Haven't Done Yet

### 🎯 CRITICAL NEXT STEP: Review Test Output

**Need to:**
1. Create the PR on GitHub
2. Wait for GitHub Actions to run
3. Review the test output to see:
   - Do tests pass? ✅ or ❌
   - Did memory pressure actually create temp files?
   - Did lock contention get captured?
   - Any unexpected failures or edge cases?

**Why this matters:**
- We created tests in a non-Docker environment
- Tests may reveal issues when run in real PostgreSQL
- We need to iterate based on actual behavior

### 📋 Remaining Pathologies (7 of 9)

Based on DIAGNOSTIC_PLAYBOOKS.md, we still need:

1. **High CPU Usage** (Section 4)
   - CPU-intensive queries (complex calculations, aggregates)
   - Verify high `total_exec_time` with `blk_read_time = 0`
   - Example: `sqrt(value) * ln(value + 1) * exp(value / 1000000.0)`

2. **Connection Exhaustion** (Section 6)
   - Open many connections near `max_connections`
   - Verify `snapshots.connections_total` near `connections_max`
   - **Challenge:** pgTAP runs in single connection, may need different approach

3. **Disk I/O Problems** (Section 7)
   - Large sequential scans on tables
   - Clear cache to force disk reads
   - Verify `wait_summary()` shows `IO:DataFileRead` events
   - Check high `shared_blks_read` in `statement_snapshots`

4. **Checkpoint Storms** (Section 8)
   - Generate heavy WAL traffic
   - Force checkpoints
   - Verify `FORCED_CHECKPOINT` anomalies
   - Check high `ckpt_requested` in snapshots
   - **Challenge:** May need to manipulate `max_wal_size` config

5. **Database Slow (Real-time)** (Section 1)
   - Long-running queries with `pg_sleep()`
   - Verify `recent_activity_current()` shows long `query_start`
   - Check `wait_event` patterns

6. **Database Slow (Historical)** (Section 2)
   - Similar to #5 but query archive tables
   - Verify `activity_samples_archive` for time range
   - Test `summary_report()` and `compare()` functions

7. **Queries Timing Out** (Section 3)
   - Queries with missing indexes (force sequential scans)
   - High `work_mem` usage
   - Verify high `mean_exec_time` in `statement_snapshots`

## Design Decisions & Patterns

### Pattern for Each Pathology

```sql
-- =============================================================================
-- PATHOLOGY N: [NAME] (X tests)
-- Based on: DIAGNOSTIC_PLAYBOOKS.md - Section Y
--
-- Real-world scenario: [Description]
-- Expected detection: [What pg-flight-recorder should find]
-- =============================================================================

-- 1. Setup: Create test objects
CREATE TABLE test_[pathology] ...;

-- 2. Test setup
SELECT ok([setup check], '[NAME] PATHOLOGY: Setup description');

-- 3. Generate pathology
DO $$
BEGIN
    -- Create problematic condition
    -- Call flight_recorder.snapshot() or sample()
END;
$$;

-- 4. Verify detection (multiple tests)
SELECT ok([detection check], '[NAME] PATHOLOGY: Detection description');

-- 5. Cleanup
DROP TABLE test_[pathology];
SELECT ok([cleanup check], '[NAME] PATHOLOGY: Cleanup description');
```

### Why This Approach Works

1. **Real Validation:** Tests prove pg-flight-recorder detects actual problems
2. **Living Documentation:** Shows what pathologies look like in the data
3. **Regression Prevention:** Ensures future changes don't break detection
4. **Playbook Verification:** Confirms diagnostic queries work as documented

### Test Philosophy

- **Positive Tests:** Verify data is captured correctly
- **Infrastructure Tests:** Confirm functions execute without error
- **Cleanup:** Always drop test objects
- **Naming:** Use `'[PATHOLOGY NAME]: description'` format

## Known Issues & Questions

### 1. Lock Contention Test Limitation
**Issue:** Advisory locks in single transaction don't create blocked sessions

**Possible Solutions:**
- Use dblink to create second connection
- Use pg_background extension
- Accept limitation and document it
- Create integration test that runs outside pgTAP

### 2. Connection Exhaustion Challenge
**Issue:** pgTAP runs in single connection, hard to test connection limits

**Possible Solutions:**
- Use dblink to open many connections
- Test just that we can query the metrics, not generate the pathology
- Skip this pathology in unit tests, cover in integration tests

### 3. Checkpoint Storm Control
**Issue:** Forcing checkpoints requires config changes or heavy WAL load

**Possible Solutions:**
- Use `CHECKPOINT` command (but may not trigger all anomalies)
- Generate actual WAL traffic (large transactions)
- Temporarily lower `max_wal_size` in test

## Files Modified

```
tests/07_pathology_generators.sql        # NEW - 203 lines
tests/PATHOLOGY_TESTS.md                 # NEW - 308 lines
PR_DESCRIPTION.md                        # NEW - 81 lines
PATHOLOGY_WORK_IN_PROGRESS.md            # NEW - This file
```

## Commands to Continue

### Review Test Output
```bash
# After PR is created and CI runs, check results
# Share output with Claude for analysis
```

### Run Tests Locally
```bash
cd /home/user/pg-flight-recorder
./test.sh 16                    # Test on PostgreSQL 16
./test.sh all                   # Test all versions (15, 16, 17)
./test.sh parallel              # Run in parallel
```

### Continue Development
```bash
# Add next pathology (e.g., High CPU)
# Edit tests/07_pathology_generators.sql
# Add new section following the pattern
# Update test count in SELECT plan(N)
```

## Next Session Checklist

When you resume:

1. ✅ Read this file to get context
2. ⬜ Check if PR was created and review test output
3. ⬜ Fix any failures found in CI
4. ⬜ Decide which pathology to implement next
5. ⬜ Consider improving lock contention test with real blocking
6. ⬜ Update PATHOLOGY_TESTS.md with learnings
7. ⬜ Plan roadmap for remaining 7 pathologies

## Questions for User (When They Return)

1. Did the PR get created? What's the URL?
2. What did the CI test output show?
3. Did the pathology tests pass?
4. Did we actually generate temp files in the memory pressure test?
5. Which pathology should we tackle next?
6. Should we improve the lock contention test to use real blocking?

## References

- **DIAGNOSTIC_PLAYBOOKS.md** - Source of truth for pathologies
- **tests/01_foundation.sql** - Example of pgTAP test structure
- **tests/02_ring_buffer_analysis.sql** - Example of analysis function tests
- **test.sh** - Test runner script

## Success Criteria

This work is successful when:
- ✅ All 9 playbook pathologies have generator tests
- ✅ Tests actually generate the pathological conditions
- ✅ pg-flight-recorder captures the data correctly
- ✅ Diagnostic functions detect the pathologies
- ✅ Tests pass in CI on PostgreSQL 15, 16, 17
- ✅ Documentation is complete and clear

## Final Notes

This is a really valuable addition to pg-flight-recorder. Instead of just testing that functions exist, we're proving the entire diagnostic workflow works end-to-end. It gives CSAs and users confidence that when they follow the playbooks, they'll actually find the problems.

The foundation is solid with 2 pathologies done. The pattern is clear. Just need to iterate based on real test output and then systematically add the remaining 7.

---

**Session ended:** 2026-01-20
**Ready to resume:** Waiting for test results from GitHub Actions
**Next step:** Review CI output and iterate
