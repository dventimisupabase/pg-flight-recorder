# Safety Plan Phase 5: Address Remaining 1.7% Overhead and Catalog Lock Contention

## Current State (Grade A-)

**Overhead:** 1.7% CPU worst-case (4 sections × 250ms / 60s)
**Catalog locks:** AccessShareLock acquired every 60s on pg_stat_activity, pg_locks, etc.

---

## Goal: Achieve < 1% overhead with minimal catalog lock contention

---

## Strategy 1: Increase Default Sample Interval (Immediate, No Code Change)

### Change
```sql
-- Current: sample every 60 seconds
'* * * * *'

-- Proposed: sample every 120 seconds (2 minutes)
'*/2 * * * *'
```

### Impact
- Overhead: 1.7% → **0.8%** (halved)
- Catalog locks: Halved (every 120s instead of 60s)
- Trade-off: Temporal resolution reduced from 60s to 120s
- Still adequate for: incident troubleshooting, anomaly detection
- May miss: Very brief spikes (< 2 min duration)

### Implementation
**Option A: Change default (breaking change)**
- Update enable() function to use 120s interval
- Document in upgrade notes
- Users can override back to 60s if needed

**Option B: Add config parameter (backward compatible)**
```sql
INSERT INTO flight_recorder.config (key, value) VALUES
    ('sample_interval_seconds', '120');  -- Default to 120s, configurable

-- User can set to 60s for higher resolution:
UPDATE flight_recorder.config SET value = '60'
WHERE key = 'sample_interval_seconds';
```

**Recommendation:** Option B (backward compatible, configurable)

---

## Strategy 2: Adaptive Sampling Based on Activity (Low Code Change)

### Concept
Sample more frequently when system is busy, less when idle.

### Implementation
```sql
-- Add to sample() function before collection:
DECLARE
    v_recent_activity INTEGER;
    v_adaptive_enabled BOOLEAN;
BEGIN
    v_adaptive_enabled := flight_recorder._get_config('adaptive_sampling', 'false')::boolean;

    IF v_adaptive_enabled THEN
        -- Check recent activity level
        SELECT count(*) INTO v_recent_activity
        FROM pg_stat_activity
        WHERE state = 'active' AND backend_type = 'client backend';

        -- If < 5 active connections, skip this sample (system is idle)
        IF v_recent_activity < 5 THEN
            PERFORM flight_recorder._record_collection_skip('sample',
                'Adaptive sampling: system idle (' || v_recent_activity || ' active connections)');
            RETURN now();
        END IF;
    END IF;

    -- Proceed with normal collection...
END;
```

### Impact
- Overhead during idle: **~0%** (skips collection)
- Overhead during busy: 1.7% (unchanged)
- Average overhead: **0.5-1.0%** depending on workload
- Catalog locks during idle: **0** (no queries)

### Trade-offs
- May miss the *start* of an incident (first busy sample after idle period)
- Introduces non-uniform sampling (analysis must account for gaps)
- Adds small overhead to every sample() call (1 quick query)

---

## Strategy 3: Snapshot-Based Collection (Medium Code Change)

### Current Problem
Each section queries pg_stat_activity independently:
```sql
-- Section 1: Wait events - queries pg_stat_activity
SELECT ... FROM pg_stat_activity ...

-- Section 2: Active sessions - queries pg_stat_activity AGAIN
SELECT ... FROM pg_stat_activity ...

-- Section 4: Lock detection - queries pg_stat_activity AGAIN
SELECT ... FROM pg_stat_activity ...
```

**Result:** 3 catalog lock acquisitions per sample

### Proposed Solution
Query pg_stat_activity ONCE, snapshot to temp table:

```sql
CREATE OR REPLACE FUNCTION flight_recorder.sample()
...
BEGIN
    -- ONE query to pg_stat_activity (ONE catalog lock)
    CREATE TEMP TABLE IF NOT EXISTS _psa_snapshot ON COMMIT DROP AS
    SELECT * FROM pg_stat_activity
    WHERE pid != pg_backend_pid();

    -- Section 1: Wait events from snapshot
    INSERT INTO flight_recorder.wait_samples ...
    SELECT ... FROM _psa_snapshot ...

    -- Section 2: Active sessions from snapshot
    INSERT INTO flight_recorder.activity_samples ...
    SELECT ... FROM _psa_snapshot WHERE state != 'idle' ...

    -- Section 4: Lock detection from snapshot
    INSERT INTO flight_recorder.lock_samples ...
    SELECT ... FROM _psa_snapshot ...
END;
```

### Impact
- Catalog lock acquisitions: **3 → 1** (67% reduction)
- Overhead: Negligible change (temp table creation is fast)
- Consistency: All sections see same snapshot (more accurate)

### Trade-offs
- Temp table overhead (minimal, ~100KB for 100 connections)
- Slightly more complex code

---

## Strategy 4: Disable Tracked Tables By Default (Immediate, Breaking Change)

### Current Problem
Every tracked table adds:
- 3 × pg_relation_size() calls every 5 minutes
- AccessShareLock on each relation
- 10-50ms overhead per table

### Proposed Change
```sql
-- Don't auto-track any tables by default
-- Users must explicitly track tables they care about

-- Update README.md:
## Table Tracking (Opt-In)

Table tracking is **disabled by default** due to overhead. Only track
5-10 critical tables maximum:

```sql
SELECT flight_recorder.track_table('orders');
SELECT flight_recorder.track_table('users');
```

**Overhead:** 3 size queries + relation lock per table every 5 minutes.
```

### Impact
- Default overhead: **Eliminates tracked table overhead entirely**
- Users who need it: Must explicitly opt-in
- Catalog locks: Eliminates pg_relation_size() locks

---

## Strategy 5: Lock-Free Activity Check (Low Code Change)

### Current Problem
Cost-based skip thresholds query pg_stat_activity to decide if skip:

```sql
-- Queries pg_stat_activity to count connections
SELECT COUNT(*) FROM pg_stat_activity WHERE state != 'idle';

-- Then if not skipping, queries AGAIN for actual data
```

This doubles the catalog lock acquisitions.

### Proposed Solution
Use pg_stat_database.numbackends (no catalog lock):

```sql
-- Fast check without querying pg_stat_activity
SELECT numbackends FROM pg_stat_database WHERE datname = current_database();

-- Only if < threshold, proceed to query pg_stat_activity
```

### Impact
- Catalog locks: Reduced when skip thresholds trigger
- Overhead: Negligible (pg_stat_database is very fast)

---

## Strategy 6: Reduce pg_stat_statements Memory Pressure (Low Code Change)

### Current Problem
Collection inserts top 50 queries every 5 minutes:
- 50 queries × 288 snapshots/day = **14,400 rows/day**
- Each query to pg_stat_statements can trigger query eviction
- Memory churn in pg_stat_statements hash table
- No deduplication (same query captured repeatedly)

### Proposed Solutions

**Option A: Reduce collection frequency (Easy)**
```sql
-- Collect pg_stat_statements every 15 minutes instead of 5
-- In snapshot() function, add:
DECLARE
    v_last_statements_collection TIMESTAMPTZ;
    v_statements_interval INTEGER;
BEGIN
    v_statements_interval := COALESCE(
        flight_recorder._get_config('statements_interval_minutes', '15')::integer,
        15
    );

    SELECT max(captured_at) INTO v_last_statements_collection
    FROM flight_recorder.snapshots
    WHERE EXISTS (
        SELECT 1 FROM flight_recorder.statement_snapshots
        WHERE snapshot_id = snapshots.id
    );

    -- Skip if collected within interval
    IF v_last_statements_collection > now() - (v_statements_interval || ' minutes')::interval THEN
        RETURN; -- Skip this collection
    END IF;
END;
```

Impact: 288 snapshots/day → **96 snapshots/day** (67% reduction)

**Option B: Reduce top N queries (Easy)**
```sql
-- Current: top 50 queries
('statements_top_n', '50')

-- Proposed: top 20 queries
('statements_top_n', '20')
```

Impact: 50 × 288 = 14,400 rows/day → **20 × 288 = 5,760 rows/day** (60% reduction)

**Option C: Query deduplication (Medium)**
```sql
-- Only insert if query text changed significantly since last snapshot
-- Store hash of query text, skip if same queryid + similar metrics

CREATE TABLE flight_recorder.statement_fingerprints (
    queryid BIGINT PRIMARY KEY,
    last_collected_at TIMESTAMPTZ,
    query_hash TEXT,  -- hash of normalized query
    last_calls BIGINT,
    last_total_exec_time NUMERIC
);

-- In snapshot collection:
-- 1. Calculate hash of current query stats
-- 2. Compare with previous fingerprint
-- 3. Only INSERT if:
--    - New queryid, OR
--    - Calls increased by > 10%, OR
--    - Exec time changed by > 20%
```

Impact: Dramatic reduction (maybe 70-90% fewer rows), only capture when queries change

**Option D: Disable by default (Breaking change)**
```sql
-- Change default:
('statements_enabled', 'false')  -- Was 'auto'

-- Users must explicitly enable:
UPDATE flight_recorder.config
SET value = 'true'
WHERE key = 'statements_enabled';
```

Impact: **Zero overhead** unless opted-in

**Recommendation:** Option A + Option B
- Collect every 15 minutes (not 5)
- Top 20 queries (not 50)
- Combined: 14,400 → **1,920 rows/day** (87% reduction)
- Still captures query performance trends
- Dramatically reduces pg_stat_statements pressure

---

## Strategy 7: Intelligent pg_cron Scheduling (No Code Change)

### Current Problem
pg_cron fires at :00 seconds of each minute.
If snapshot (every 5 min) and sample (every 60s) align, both run simultaneously.

### Proposed Solution
Offset sample collection to avoid overlap:

```sql
-- Snapshot: runs at :00 seconds (0, 5, 10, 15, 20...)
'*/5 * * * *'

-- Sample: runs at :30 seconds (offset by 30s)
-- NOTE: pg_cron doesn't support sub-minute scheduling in cron format
-- BUT: Can reschedule after each run to create offset

-- Alternative: Change to 2-minute intervals to naturally avoid overlap
'*/2 * * * *'  -- Runs at even minutes (0, 2, 4, 6, 8, 10...)
-- Snapshot at: 0, 5, 10, 15, 20, 25...
-- Only overlap: minute 0, 10, 20, 30... (1 in 5 samples)
```

### Impact
- Reduces simultaneous catalog lock contention
- Spreads load more evenly

---

---

## Strategy 8: Asynchronous Collection (Eliminated - Requires Extensions)

### Considered but rejected

**Problem:** Synchronous collection blocks pg_cron during execution.

**Potential solution:** Use background workers, dblink, or pg_background.

**Decision:** **REJECTED** - All async solutions require extensions:
- pg_background: Extension required
- dblink: Extension required
- Separate connection: Would need connection pooler extension

**Rationale:** Project goal is to remain **extension-free** except for pg_cron (required for scheduling). Synchronous collection with proper timeouts and circuit breakers is acceptable.

---

## Recommended Implementation Plan

### Phase 5A: Quick Wins (2-3 hours)

1. **Increase default sample interval from 60s to 120s**
   - Config parameter: `sample_interval_seconds` = 120
   - Update enable() to read config and schedule accordingly
   - Update README: document 60s vs 120s trade-offs
   - **Impact: 1.7% → 0.8% overhead** (halved)
   - **Catalog locks: Halved** (every 120s instead of 60s)

2. **Reduce pg_stat_statements pressure**
   - Increase statements collection interval: 5 min → 15 min
   - Reduce top N queries: 50 → 20
   - Config: `statements_interval_minutes` = 15, `statements_top_n` = 20
   - **Impact: 14,400 rows/day → 1,920 rows/day** (87% reduction)
   - **Reduces hash table churn in pg_stat_statements**

3. **Disable tracked tables by default**
   - Remove any default tracked tables
   - Update README: opt-in model with overhead warning
   - **Impact: Eliminates relation lock overhead entirely**

4. **Document configuration options**
   - Add to README: how to tune sample interval
   - Trade-off table: 60s vs 120s vs 180s overhead comparison
   - Document pg_stat_statements collection frequency tuning

**Expected Result:**
- CPU: 0.8% overhead (was 1.7%)
- Catalog locks: Halved frequency
- pg_stat_statements: 87% less pressure
- No breaking changes (all configurable)

---

### Phase 5B: Snapshot-Based Collection (4-6 hours)

1. **Implement pg_stat_activity snapshot**
   - Modify sample() to create temp table
   - Update all sections to query snapshot
   - Impact: 3 catalog locks → 1

2. **Lock-free skip checks**
   - Use pg_stat_database.numbackends for skip logic
   - Impact: Fewer catalog locks when skip triggers

**Expected Result:** 0.8% overhead, 67% fewer catalog locks

---

### Phase 5C: Adaptive Sampling (Optional, 2-3 hours)

1. **Implement idle detection**
   - Add adaptive_sampling config parameter
   - Skip collection when < 5 active connections
   - Impact: 0.8% → 0.3-0.5% average (workload dependent)

**Expected Result:** 0.3-0.8% overhead depending on workload, minimal locks during idle

---

## Final Target State

| Metric | Current (A-) | After 5A | After 5B | After 5C |
|--------|--------------|----------|----------|----------|
| **Worst-case CPU** | 1.7% | 0.8% | 0.8% | 0.8% |
| **Average CPU** | 1.7% | 0.8% | 0.8% | 0.3-0.5% |
| **Catalog locks/min** | 3 locks/60s | 3 locks/120s | 1 lock/120s | 0-1 lock/120s |
| **Sample interval** | 60s | **120s** | 120s | 120s (when active) |
| **Tracked tables** | User opt-in | **Disabled default** | Disabled default | Disabled default |
| **pg_stat_statements rows/day** | 14,400 | **1,920** | 1,920 | 1,920 |
| **Statements collection** | Every 5 min | **Every 15 min** | Every 15 min | Every 15 min |
| **Top N queries** | 50 | **20** | 20 | 20 |
| **Grade** | A- | **A** | A | A+ |

### Key Improvements in Phase 5A

1. ✓ **60s → 120s sample interval** (configurable, backward compatible)
2. ✓ **pg_stat_statements: 87% less pressure** (15min interval, top 20 queries)
3. ✓ **Catalog locks: Halved** (every 120s instead of 60s)
4. ✓ **Tracked tables: Disabled by default** (opt-in model)
5. ✓ **No extensions required** (async collection not pursued)
6. ✓ **Fully backward compatible** (all config-based)

---

## Grade Justification After Phase 5

**After 5A (120s interval, no default tracking):**
- Grade: **A**
- Overhead: 0.8% worst-case (acceptable)
- Catalog locks: Halved
- 2 hours of work

**After 5B (snapshot-based collection):**
- Grade: **A**
- Overhead: 0.8% worst-case
- Catalog locks: 67% reduction (1 per sample vs 3)
- 6 hours total work

**After 5C (adaptive sampling):**
- Grade: **A+**
- Overhead: 0.3-0.5% average (depending on workload)
- Catalog locks: Minimal during idle periods
- 8-9 hours total work

---

## Risk Assessment

### Low Risk
- Phase 5A: Config changes only, fully backward compatible
- Users can override sample interval if needed

### Medium Risk
- Phase 5B: Code changes to snapshot logic
- Must ensure temp table cleanup (ON COMMIT DROP)
- Test with high connection counts (1000+ connections)

### Low Risk
- Phase 5C: Adaptive sampling is opt-in via config
- Default behavior unchanged if not enabled

---

## Testing Requirements

For each phase:

1. **Functional testing**
   ```sql
   SELECT flight_recorder.sample();
   SELECT flight_recorder.snapshot();
   SELECT * FROM flight_recorder.collection_stats ORDER BY started_at DESC LIMIT 10;
   ```

2. **Performance testing**
   - Measure actual duration_ms from collection_stats
   - Verify < 1000ms per sample at 120s interval

3. **Catalog lock testing**
   ```sql
   -- Run in one session:
   BEGIN;
   LOCK TABLE pg_catalog.pg_database IN ACCESS EXCLUSIVE MODE;
   -- Hold for 2 minutes

   -- Monitor in another session:
   SELECT * FROM flight_recorder.collection_stats
   WHERE error_message LIKE '%lock_timeout%';
   ```

4. **Idle workload testing (5C only)**
   - Let system idle (< 5 active connections)
   - Verify sample() skips collection
   - Check collection_stats for skip_reason

---

## Decision Point

**Recommend:** Implement Phase 5A immediately (2-3 hours, safe, backward compatible)

**Phase 5A includes:**
- ✓ 120s sample interval (was 60s) → halves overhead
- ✓ pg_stat_statements: 15min interval, top 20 queries → 87% less pressure
- ✓ Disable tracked tables by default → eliminates relation locks
- ✓ No extensions required (async collection rejected)
- ✓ All configurable (users can override to 60s if needed)

Then evaluate:
- If 0.8% overhead is acceptable → **STOP** (Grade A achieved)
- If catalog lock contention observed → Implement 5B (Grade A, snapshot-based)
- If further optimization needed → Implement 5C (Grade A+, adaptive sampling)

**Do NOT implement all phases unless needed. Measure first, optimize only if required.**

### Why Phase 5A is sufficient for most use cases

- 0.8% CPU overhead is acceptable for production troubleshooting
- 120s temporal resolution still captures incidents (most issues last > 5 minutes)
- Halved catalog lock frequency reduces DDL interaction risk
- pg_stat_statements pressure reduced by 87%
- Users can tune back to 60s if they need higher resolution

**Grade A is the target. Grade A+ (Phase 5C) is overkill unless you have severe resource constraints.**
