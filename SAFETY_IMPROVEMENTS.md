# Safety Improvement Plan: B+ → A Grade

## Current Status: B+ (Observer Effect ~7% CPU)

This document outlines specific changes needed to achieve A-grade safety with minimal observer effect.

---

## Priority 1: Reduce Per-Section Timeout (High Impact)

**Current State:**
- `section_timeout_ms` = 1000ms (config table)
- `section_timeout_ms` = 400ms (function fallback)
- 4 sections in sample() = 4000ms worst case
- 5 sections in snapshot() = 5000ms worst case
- Sample frequency: 60s → 6.7% CPU overhead worst case

**Target:**
- `section_timeout_ms` = 250ms maximum
- 4 sections × 250ms = 1000ms total worst case
- 60s interval → 1.7% CPU overhead worst case

**Changes Required:**

```sql
-- In install.sql config table (line 661):
('section_timeout_ms', '250'),  -- REDUCED from 1000ms to 250ms

-- In _set_section_timeout() function (line 853):
v_timeout_ms := COALESCE(
    flight_recorder._get_config('section_timeout_ms', '250')::integer,
    250  -- MATCH config table value
);
```

**Expected Impact:** Reduces worst-case CPU overhead from 6.7% to 1.7%

---

## Priority 1: Fix Dangerous Fallback Defaults (Critical)

**Current State:**
Multiple functions have fallbacks that don't match config table values:

| Parameter | Config Table | Function Fallback | Risk Level |
|-----------|--------------|-------------------|------------|
| `circuit_breaker_threshold_ms` | 1000ms | 5000ms | CRITICAL |
| `section_timeout_ms` | 1000ms | 400ms | Medium |
| `auto_mode_connections_threshold` | 60% | 80% | Medium |

**Target:**
All function fallbacks must match config table defaults.

**Changes Required:**

```sql
-- In _check_circuit_breaker() (line 733):
v_threshold_ms := COALESCE(
    flight_recorder._get_config('circuit_breaker_threshold_ms', '1000')::integer,
    1000  -- MATCH config table: changed from 5000
);

-- In _check_and_adjust_mode() (line 897):
v_connections_threshold := COALESCE(
    flight_recorder._get_config('auto_mode_connections_threshold', '60')::integer,
    60  -- MATCH config table: changed from 80
);
```

**Expected Impact:** Eliminates risk of degraded safety after config loss

---

## Priority 1: Increase Default Sample Interval (Balanced)

**Current State:**
- Sample interval: 60 seconds
- With 1000ms per-section timeout: 6.7% overhead
- With 250ms per-section timeout: 1.7% overhead

**Target:**
- Sample interval: 120 seconds
- With 250ms per-section timeout: 0.8% overhead

**Changes Required:**

```sql
-- In enable() function, change cron schedule:
-- FROM:
'* * * * *'  -- every 60 seconds

-- TO:
'*/2 * * * *'  -- every 120 seconds (every 2 minutes)
```

**Trade-offs:**
- ✓ Halves CPU overhead
- ✗ Reduces temporal resolution from 60s to 120s
- Decision: User choice based on workload

**Recommendation:** Make this configurable via `sample_interval_seconds` config parameter.

---

## Priority 2: Lower Cost-Based Skip Thresholds (Medium Impact)

**Current State:**
```sql
('skip_activity_conn_threshold', '400'),  -- Skip if > 400 active connections
('skip_locks_threshold', '200'),          -- Skip if > 200 blocked sessions
```

**Target:**
```sql
('skip_activity_conn_threshold', '100'),  -- Skip if > 100 active connections
('skip_locks_threshold', '50'),           -- Skip if > 50 blocked sessions
```

**Rationale:**
By the time you hit 400 active connections, the system is already severely degraded. Observer should back off much earlier.

**Expected Impact:** Earlier reduction of overhead under load

---

## Priority 2: Reduce lock_timeout (Medium Impact)

**Current State:**
```sql
('lock_timeout_ms', '500'),  -- Wait up to 500ms for catalog locks
```

**Target:**
```sql
('lock_timeout_ms', '100'),  -- Wait up to 100ms for catalog locks
```

**Rationale:**
Every query to `pg_stat_activity`, `pg_locks`, etc. acquires AccessShareLock on system catalogs. Under heavy DDL, flight recorder should fail fast (100ms) rather than contribute to lock queue buildup (500ms).

**Expected Impact:** Reduces catalog lock contention on DDL-heavy workloads

---

## Priority 3: Document Observer Effect (Critical for Transparency)

**Current State:**
README.md claims "minimal observer effect" but provides no quantitative estimates.

**Target:**
Add "Observer Effect" section to README.md with measured estimates:

```markdown
## Observer Effect

pg-flight-recorder has measurable overhead. Exact cost depends on configuration:

| Config | Sample Interval | Timeout/Section | Worst-Case CPU | Notes |
|--------|-----------------|-----------------|----------------|-------|
| **Conservative** | 120s | 250ms | 0.8% | Recommended for production |
| **Default** | 60s | 250ms | 1.7% | Balanced monitoring |
| **Aggressive** | 60s | 1000ms | 6.7% | High temporal resolution |

Additional considerations:
- **Catalog locks**: Every collection acquires AccessShareLock on system catalogs
- **Memory**: 2MB work_mem per collection (8MB total with 4 concurrent sections)
- **Storage**: ~2-3 GB for 7 days retention (UNLOGGED, no WAL overhead)
- **pg_stat_statements**: 50 queries × 288 snapshots/day = 14,400 rows/day

### Reducing Overhead

```sql
-- Increase sample interval to 120 seconds
SELECT flight_recorder.set_mode('light');  -- Disables progress tracking

-- Or stop completely
SELECT flight_recorder.disable();
```

**Target Environments:**
- ✓ Production troubleshooting (enable during incidents)
- ✓ Staging/dev (always-on monitoring)
- ✗ Resource-constrained databases (< 2 CPU cores)
```

**Expected Impact:** Sets accurate expectations, prevents surprises

---

## Priority 3: Add Tracked Table Monitoring Warning

**Current State:**
No warning about tracked table monitoring overhead.

**Target:**
Add to `track_table()` function:

```sql
RAISE NOTICE 'pg-flight-recorder: Tracking table %.%. This adds overhead: pg_relation_size() + pg_total_relation_size() + pg_indexes_size() every 5 minutes. Tracked table count: %',
    p_schema, p_table, (SELECT count(*) FROM flight_recorder.tracked_tables);
```

**Rationale:**
Each tracked table adds 3 × pg_*_size() calls + AccessShareLock acquisition every 5 minutes. User should know the cost.

**Expected Impact:** Informed decisions about table tracking

---

## Priority 3: Add Configuration Validation Function

**Current State:**
No way to verify config consistency after changes.

**Target:**
Create `flight_recorder.validate_config()` function:

```sql
CREATE FUNCTION flight_recorder.validate_config()
RETURNS TABLE(
    check_name TEXT,
    status TEXT,  -- 'OK', 'WARNING', 'CRITICAL'
    message TEXT
)
LANGUAGE plpgsql AS $$
BEGIN
    -- Check 1: section_timeout_ms should be <= 500ms for production safety
    RETURN QUERY SELECT
        'section_timeout_ms',
        CASE
            WHEN value::integer > 500 THEN 'WARNING'
            WHEN value::integer > 1000 THEN 'CRITICAL'
            ELSE 'OK'
        END,
        format('Current: %s ms. Recommended: <= 250ms for minimal overhead', value)
    FROM flight_recorder.config WHERE key = 'section_timeout_ms';

    -- Check 2: circuit_breaker should be enabled
    RETURN QUERY SELECT
        'circuit_breaker_enabled',
        CASE WHEN value::boolean THEN 'OK' ELSE 'CRITICAL' END,
        format('Current: %s. Circuit breaker provides automatic protection', value)
    FROM flight_recorder.config WHERE key = 'circuit_breaker_enabled';

    -- Check 3: Tracked table count
    RETURN QUERY SELECT
        'tracked_tables',
        CASE
            WHEN count(*) > 50 THEN 'CRITICAL'
            WHEN count(*) > 20 THEN 'WARNING'
            ELSE 'OK'
        END,
        format('Tracking %s tables. Each adds 3 size queries every 5 minutes', count(*))
    FROM flight_recorder.tracked_tables;

    -- Check 4: Schema size
    RETURN QUERY SELECT
        'schema_size',
        CASE
            WHEN schema_size_mb > 8000 THEN 'CRITICAL'
            WHEN schema_size_mb > 5000 THEN 'WARNING'
            ELSE 'OK'
        END,
        format('flight_recorder schema: %s MB (warning: 5000 MB, critical: 10000 MB)', schema_size_mb)
    FROM flight_recorder._check_schema_size();
END;
$$;
```

**Expected Impact:** Proactive identification of risky configurations

---

## Priority 4: Add Catalog Lock Contention Documentation

**Current State:**
No documentation of catalog lock behavior.

**Target:**
Add to REFERENCE.md:

```markdown
## Catalog Lock Contention

Every collection acquires AccessShareLock on system catalogs:

| System View | Lock Target | Acquired By |
|-------------|-------------|-------------|
| pg_stat_activity | pg_stat_activity | Both sample() and snapshot() |
| pg_stat_replication | pg_stat_replication | snapshot() only |
| pg_locks | pg_locks | sample() only (lock detection) |
| pg_stat_statements | pg_stat_statements | snapshot() only |
| pg_relation_size() | Target relation | snapshot() (tracked tables only) |

### Lock Timeout Behavior

`lock_timeout` = 500ms (default) means:
- If system catalog is locked by DDL > 500ms, collection fails
- If collection starts before DDL, DDL waits up to 500ms behind flight recorder

### High-DDL Workloads

Multi-tenant SaaS with frequent CREATE/DROP/ALTER:
- Consider `lock_timeout` = 100ms (fail faster)
- Consider disabling tracked table monitoring (eliminates relation locks)
- Monitor `collection_stats` table for frequent lock_timeout failures

```sql
-- Check for lock timeout failures
SELECT count(*) AS lock_failures
FROM flight_recorder.collection_stats
WHERE error_message LIKE '%lock_timeout%'
  AND started_at > now() - interval '1 hour';
```
```

**Expected Impact:** Users understand lock interaction risks

---

## Summary: Before/After Comparison

| Metric | Current (B+) | After Changes (A) | Improvement |
|--------|--------------|-------------------|-------------|
| Per-section timeout | 1000ms | 250ms | 75% reduction |
| Sample worst-case time | 4000ms | 1000ms | 75% reduction |
| CPU overhead (60s interval) | 6.7% | 1.7% | 75% reduction |
| CPU overhead (120s interval) | 3.3% | 0.8% | 76% reduction |
| lock_timeout | 500ms | 100ms | 80% reduction |
| Skip activity threshold | 400 | 100 | Earlier protection |
| Skip locks threshold | 200 | 50 | Earlier protection |
| Fallback mismatches | 3 critical | 0 | Risk eliminated |
| Observer effect docs | None | Complete | Transparency |

---

## Implementation Order

1. **Phase 1: Safety fixes (no behavior change)**
   - Fix fallback defaults to match config table
   - Commit: "fix: align function fallbacks with config defaults"

2. **Phase 2: Threshold reductions**
   - `section_timeout_ms`: 1000ms → 250ms
   - `lock_timeout_ms`: 500ms → 100ms
   - `skip_activity_conn_threshold`: 400 → 100
   - `skip_locks_threshold`: 200 → 50
   - Commit: "perf: reduce timeouts and skip thresholds for minimal overhead"

3. **Phase 3: Documentation**
   - Add "Observer Effect" section to README.md
   - Add catalog lock contention docs to REFERENCE.md
   - Add tracked table warning to track_table()
   - Commit: "docs: quantify observer effect and catalog lock behavior"

4. **Phase 4: Validation tooling**
   - Create `validate_config()` function
   - Commit: "feat: add configuration validation function"

5. **Phase 5: Optional interval increase**
   - Make sample interval configurable
   - Default: 120s (user can override to 60s)
   - Commit: "perf: increase default sample interval to 120s"

---

## Testing Plan

After each phase:

```sql
-- 1. Verify configuration
SELECT * FROM flight_recorder.config ORDER BY key;

-- 2. Trigger manual collection
SELECT flight_recorder.sample();
SELECT flight_recorder.snapshot();

-- 3. Check collection stats
SELECT
    collection_type,
    avg(duration_ms) AS avg_duration_ms,
    max(duration_ms) AS max_duration_ms,
    count(*) FILTER (WHERE success) AS successful,
    count(*) FILTER (WHERE NOT success) AS failed
FROM flight_recorder.collection_stats
WHERE started_at > now() - interval '1 hour'
GROUP BY collection_type;

-- 4. Verify circuit breaker doesn't trip
SELECT * FROM flight_recorder.collection_stats
WHERE skipped = true
  AND started_at > now() - interval '1 hour';

-- 5. Validate config (after Phase 4)
SELECT * FROM flight_recorder.validate_config();
```

---

## Rollback Plan

If issues arise:

```sql
-- Emergency: disable entirely
SELECT flight_recorder.disable();

-- Revert to old thresholds
UPDATE flight_recorder.config
SET value = '1000'
WHERE key = 'section_timeout_ms';

UPDATE flight_recorder.config
SET value = '500'
WHERE key = 'lock_timeout_ms';

-- Restart with old config
SELECT flight_recorder.enable();
```

---

## Grade Justification: A

With these changes:
- ✓ Worst-case overhead: 0.8-1.7% CPU (depending on interval)
- ✓ Function fallbacks match config (no surprises after restore)
- ✓ Circuit breaker trips earlier (100ms catalog lock wait)
- ✓ Cost-based skips trigger earlier (100/50 thresholds)
- ✓ Quantified observer effect in documentation
- ✓ Catalog lock behavior documented
- ✓ Configuration validation function
- ✓ User warnings for tracked tables

**Remaining limitations (why not A+):**
- Still measurable overhead (~1% minimum)
- Catalog lock contention still possible under heavy DDL
- pg_stat_statements memory impact not eliminated
- NUMA effects not addressed (platform-specific)

**A+ would require:**
- < 0.5% overhead (requires 250s+ sample interval or sampling)
- Async collection (separate connection pool)
- Elimination of catalog locks (snapshots of system views)
- pg_stat_statements bypass (custom query tracking)

These would require architectural changes beyond config tuning.

