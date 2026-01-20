## Summary

This PR introduces **pathological data generators** - a new testing approach that validates pg-flight-recorder can actually detect the real-world database problems documented in DIAGNOSTIC_PLAYBOOKS.md.

Instead of just testing that functions exist and run without errors, these tests:

1. **Generate** authentic problematic database conditions
2. **Capture** the data using pg-flight-recorder
3. **Verify** that diagnostic functions correctly identify the issues

## What's Included

### New Test File: `tests/07_pathology_generators.sql`

- **27 pgTAP tests** covering 5 pathologies
- Uses the same pgTAP framework as existing tests
- Clean setup and teardown of test objects

### New Documentation: `tests/PATHOLOGY_TESTS.md`

- Comprehensive guide for understanding pathology tests
- Step-by-step instructions for adding new pathologies
- Templates and examples for future contributors

## Pathologies Covered (6/9)

### 1. Lock Contention (Section 5)

- Simulates blocked queries using advisory locks
- Verifies `recent_locks_current()` works
- Tests `lock_samples_ring` captures events

### 2. Memory Pressure / work_mem Issues (Section 9)

- Creates large dataset (10K rows)
- Forces temp file spills with low work_mem (64kB)
- Verifies `temp_files` and `temp_bytes` are captured

### 3. High CPU Usage (Section 4)

- CPU-intensive mathematical calculations
- 50K rows with sqrt, ln, exp, cos, power operations
- Verifies `statement_snapshots` captures query stats

### 4. Database Slow - Real-time (Section 1)

- Simulates long-running operations with `pg_sleep()`
- Verifies `recent_activity_current()` and `recent_waits_current()` work
- Tests `activity_samples_ring` captures activity

### 5. Queries Timing Out (Section 3)

- Large unindexed table (20K rows) forcing sequential scans
- LIKE patterns and aggregations requiring full table scans
- Verifies `compare()` and `statement_snapshots` work

### 6. Connection Exhaustion (Section 6)

- Uses **dblink** extension to create multiple real connections from within pgTAP
- Opens 15 concurrent connections to simulate connection pressure
- Verifies `connections_total` and connection utilization metrics

## Remaining Pathologies (3/9)

These can be added in future PRs:

- [ ] Database Slow (Historical) (Section 2)
- [ ] Disk I/O Problems (Section 7)
- [ ] Checkpoint Storms (Section 8) - may need config changes

## Why This Matters

1. **Validation**: Proves pg-flight-recorder detects what it claims
2. **Confidence**: CSAs know the diagnostic playbooks work end-to-end
3. **Living Documentation**: Shows exactly what pathologies look like
4. **Regression Prevention**: Ensures future changes don't break detection
5. **Foundation**: Establishes pattern for covering all playbook scenarios

## Testing

The tests run as part of the existing test suite:

```bash
./test.sh 16          # Test on PostgreSQL 16
./test.sh all         # Test on all versions (15, 16, 17)
./test.sh parallel    # Run all versions in parallel
```

GitHub Actions should run these automatically and we can verify they pass!

## Next Steps

If this approach looks good:

1. Wait for CI to validate tests pass
2. Review test output in GitHub Actions
3. Iterate based on findings
4. Plan PRs for remaining 7 pathologies

---

**Related:** DIAGNOSTIC_PLAYBOOKS.md
**Test Count:** 33 new tests covering 6 pathologies
