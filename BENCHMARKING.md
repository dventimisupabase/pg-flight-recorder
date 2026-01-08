# Benchmarking Methodology

**Status:** Implemented

## Philosophy: No Benchmarketing

We are committed to honest, reproducible performance measurement based on a key insight:

**The observer effect of flight recorder is roughly constant.**

Running `SELECT * FROM pg_stat_activity` takes ~X milliseconds whether your database is idle or processing 10,000 TPS. The cost doesn't scale with load - it's an absolute cost.

## What We Measure: Absolute Costs (Not Relative Impact)

Traditional benchmarking asks: "Does this feature reduce TPS by 2%?"

We ask instead: **"Does your system have X milliseconds of CPU headroom every 180 seconds?"**

This reframes the question correctly:
- ✓ Measure: Collection takes 150ms of CPU time
- ✓ Calculate: At 180s intervals = 0.08% sustained CPU
- ✓ Assess: Do you have 150ms spare capacity every 3 minutes?
- ✗ ~~Measure TPS degradation under synthetic load~~ (not meaningful)

## Why This Approach Is Better

### The Math

If one collection = 150ms:
- At 180s intervals = 150ms / 180,000ms = **0.083% sustained CPU**
- At 120s intervals = 150ms / 120,000ms = **0.125% sustained CPU**
- At 60s intervals = 150ms / 60,000ms = **0.250% sustained CPU**

This is **constant** regardless of your workload.

### It's About Headroom

The question isn't "will this slow down my database?" but rather:
- On a tiny system (1 vCPU): Is 150ms every 180s too much?
- On a loaded system (95% CPU): Will brief 150ms spikes cause problems?
- On a large idle system: Obviously fine (plenty of headroom)

**Two regimes matter:**
1. **Tiny systems** - Constrained headroom even at idle
2. **Heavily loaded systems** - Constrained headroom despite size

## What We Measure

### Primary Benchmark: Absolute Costs (Run on any laptop)

```bash
./benchmark/measure_absolute.sh
```

**Measures:**
- CPU time per collection (mean, p50, p95, p99)
- I/O operations per collection
- Memory usage during collection
- Sustained CPU % at different intervals (60s, 120s, 180s)

**Output Example 1** (MacBook Pro M-series, PostgreSQL 17.6, 23MB database, 79 tables):

*Note: psql `\timing` measures end-to-end (including client overhead). Use `collection_stats` table for actual server-side execution time.*

```
Actual Collection Execution (from 315 real collections over 30 minutes):
  Median: 23.0 ms (P50)
  Mean:   24.9 ms ± 11.5 ms (stddev)
  P95:    31.0 ms (95% complete within this time)
  P99:    86.4 ms (99% complete within this time)
  Range:  19 - 145 ms (outliers <1%)

Stability Analysis:
  - First 10 min:  48.3ms mean (3 samples, cold start)
  - Middle 10 min: 21.8ms mean (130 samples)
  - Last 10 min:   26.7ms mean (182 samples)
  - Conclusion: Very stable, no degradation

I/O Operations:
  Mean:   ~4,084 blocks (mostly cached reads)

Sustained CPU Impact:
  At 60s intervals:  0.042% (23ms / 60,000ms)
  At 120s intervals: 0.021% (23ms / 120,000ms)
  At 180s intervals: 0.013% (23ms / 180,000ms)

Peak Impact:
  Brief 23ms CPU spike every N seconds

Headroom Assessment:
  ✓ 1 vCPU system: SAFE - only 23ms every 3 minutes
  ✓ 2+ vCPU system: SAFE - negligible impact
```

**Output Example 2** (Supabase Micro - t4g.nano, 2 core ARM, 1GB RAM, PostgreSQL 17.6):

```
Actual Collection Execution (from 59 real collections over 10 minutes):
  Median: 32.0 ms (P50)
  Mean:   36.6 ms ± 23.3 ms (stddev)
  P95:    46.1 ms (95% complete within this time)
  P99:    118.4 ms (99% complete within this time)
  Range:  31 - 210 ms

I/O Operations:
  Mean:   ~5,381 blocks (mostly cached reads)

Sustained CPU Impact:
  At 60s intervals:  0.061% (32ms / 60,000ms)
  At 120s intervals: 0.031% (32ms / 120,000ms)
  At 180s intervals: 0.018% (32ms / 180,000ms)

Peak Impact:
  Brief 32ms CPU spike every N seconds

Headroom Assessment:
  ✓ Supabase free tier: SAFE - only 32ms every 3 minutes
  ✓ Resource-constrained systems: VALIDATED - works great even on 2 core/1GB
```

This is **reproducible** and **constant** - doesn't require expensive hardware or complex workload simulation. The Supabase micro instance validates that it works well even on resource-constrained environments.

### Secondary Testing: Safety Feature Validation

The integration test framework (`benchmark/run.sh`) validates:
- ✓ Load shedding triggers correctly (>70% connections)
- ✓ Load throttling works (>1000 txn/sec, >10K blocks/sec)
- ✓ Catalog locks don't cause contention during DDL
- ✓ pg_stat_statements protection prevents hash churn

These tests confirm the safety features work, not "what's the TPS impact?"

## How We Report Overhead

### Bad (Benchmarketing):
> "Uses less than 0.1% of your CPU"

### Good (Honest):
> **Absolute Costs** (measured on PostgreSQL 17.6, 23MB database, Darwin arm64):
> - Collection execution: 23ms median (P50), mean 24.9ms ± 11.5ms
> - P95: 31ms, P99: 86ms
> - I/O per collection: ~4,084 blocks (mostly cached reads)
> - At 180s intervals: 0.013% sustained CPU + brief 23ms spike every 3 min
> - Stability: Validated over 315 collections (30 minutes), no degradation
>
> **Headroom Assessment:**
> - ✓ Systems with ≥2 idle CPU cores: Negligible impact
> - ⚠ Systems with <1 idle CPU core: Test in staging, monitor for spikes
> - ⚠ Heavily loaded systems (>90% CPU): Load shedding and throttling help
> - ⚠ Tiny systems (1 vCPU, <2GB RAM): Test thoroughly before production

## Running the Benchmark

**Anyone can run this in 5 minutes on their laptop:**

```bash
cd pg-flight-recorder/benchmark
./measure_absolute.sh 100  # 100 iterations

# Output:
# - Absolute costs (timing, I/O, memory)
# - Sustained CPU percentages
# - Headroom assessment
# - results/absolute_costs_YYYYMMDD_HHMMSS.json
```

No need for:
- ✗ Expensive cloud hardware
- ✗ Complex workload simulation
- ✗ Hours of runtime
- ✗ Statistical comparison of TPS degradation

Just measure the actual cost directly.

## Validating in Your Environment

Once you measure absolute costs on your laptop, validate in your actual environment:

### 1. Development/Staging Test (5 minutes)

Install on staging and let it run:


```sql
-- Install
\i install.sql

-- Monitor for issues
SELECT * FROM flight_recorder.recent_activity;
SELECT * FROM flight_recorder.preflight_check();
```

Watch for:
- Collections timing out (check logs)
- CPU spikes correlating with collections
- Load shedding/throttling messages (normal under load)

### 2. Minimal System Test (Optional, $0.20)

If deploying to 1 vCPU systems, validate on a tiny VM:

```bash
# Spin up smallest DigitalOcean droplet (1 vCPU, 1GB RAM)
# Install PostgreSQL + flight recorder
# Let run for 24 hours
# Check: Any timeouts? Any problems?
```

If collections complete successfully on 1 vCPU, it's safe everywhere.

### 3. Production Test (Gradual Rollout)

For always-on production use:

1. **Start with emergency mode** (300s, minimal overhead):
   ```sql
   SELECT flight_recorder.set_mode('emergency');
   ```

2. **Monitor for 24 hours**: Check CPU, collection timing

3. **Upgrade to normal mode** if comfortable:
   ```sql
   SELECT flight_recorder.set_mode('normal');  -- 180s
   ```

4. **Monitor safety features**:
   ```sql
   -- Check if load shedding/throttling is triggering
   SELECT * FROM flight_recorder.collection_health;
   ```

## What to Document

After running `measure_absolute.sh` on various systems:

### Report Template

**Example 1: MacBook Pro**

```markdown
## Overhead Measurements

**Test Environment:**
- PostgreSQL: 17.6
- Database size: 23MB
- Table count: 79
- Hardware: MacBook Pro M-series (Darwin arm64)
- Date: 2026-01-08

**Absolute Costs** (315 collections over 30 minutes):
- Collection execution: 23ms median (P50)
- Mean: 24.9ms ± 11.5ms (stddev)
- P95: 31ms, P99: 86ms
- I/O per collection: ~4,084 blocks

**Sustained Impact at 180s intervals:**
- 0.013% sustained CPU
- Brief 23ms spike every 3 minutes

**Stability:**
- No drift or degradation over 30 minutes
- 95% of collections complete within 31ms
- 99% complete within 86ms

**Headroom Assessment:**
- ✓ This system: Safe for always-on use (negligible impact)
- ✓ 1 vCPU systems: Safe - only 23ms every 3 minutes
```

**Example 2: Supabase Micro Instance**

```markdown
## Overhead Measurements

**Test Environment:**
- PostgreSQL: 17.6
- Database size: 23MB
- Table count: 79
- Hardware: Supabase Micro (t4g.nano, 2 core ARM, 1GB RAM)
- Date: 2026-01-08

**Absolute Costs** (59 collections over 10 minutes):
- Collection execution: 32ms median (P50)
- Mean: 36.6ms ± 23.3ms (stddev)
- P95: 46ms, P99: 118ms
- I/O per collection: ~5,381 blocks

**Sustained Impact at 180s intervals:**
- 0.018% sustained CPU
- Brief 32ms spike every 3 minutes

**Headroom Assessment:**
- ✓ Supabase free tier: Safe for always-on use
- ✓ Resource-constrained systems (2 core/1GB): VALIDATED - negligible impact
- ✓ Production ready: Even on smallest Supabase tier, overhead is minimal

**DDL Impact** (202 operations over 120 seconds):
- DDL Blocking Rate: **0%** - No blocking observed
- Mean DDL Duration: 1.61ms
- Concurrent Operations: 14 (no delays)
- Conclusion: Snapshot-based collection eliminates catalog lock contention
```

**Example 3: DDL Impact Validation (Supabase Micro Instance)**

```markdown
## DDL Blocking Impact Measurements

**Test Environment:**
- PostgreSQL: 17.6
- Database size: 23MB
- Table count: 79
- Hardware: Supabase Micro (t4g.nano, 2 core ARM, 1GB RAM)
- Date: 2026-01-08
- Flight Recorder Mode: Emergency (120s intervals)

**Test:** 202 DDL operations over 120 seconds
- ALTER TABLE ADD/DROP COLUMN
- CREATE/DROP INDEX
- ALTER TYPE
- VACUUM

**Results:**
- **DDL Blocking Rate:** 0%
- **Operations Concurrent with Collection:** 14 (6.93%)
- **Mean DDL Duration (All):** 1.61ms ± 0.88ms
- **Mean DDL Duration (Concurrent):** 1.47ms ± 0.72ms
- **Maximum DDL Duration:** 5.92ms
- **P95:** 3.30ms, **P99:** 4.85ms

**Key Finding:**
No blocking observed. Operations that ran concurrently with flight_recorder collection
completed in the same time as non-concurrent operations (1.47ms vs 1.61ms).

**Why No Blocking:**
1. Snapshot-based collection (single AccessShareLock, not 3)
2. Short collection duration (~32ms) vs DDL duration (~1-6ms)
3. Fast catalog access (small database, fast storage)

**Production Impact:**
Even with 100 DDL operations per hour:
- Expected concurrent operations: <1 per day
- Expected delays: 0ms
- **No special precautions needed**

**Conclusion:**
Flight recorder is safe for production use even with high-DDL workloads.
Snapshot-based collection design eliminates catalog lock contention.
```

### Share Results

Help the community by sharing measurements:
- Different database sizes (1GB, 10GB, 100GB, 1TB)
- Different PostgreSQL versions (15, 16, 17)
- Different hardware (laptop, cloud VM, RDS)
- Different table counts (10, 100, 1000, 10000)

This builds evidence for "does cost scale with database size?"

## Validation Testing (Integration Framework)

The `benchmark/run.sh` framework validates safety features work correctly:

### Purpose: NOT Performance, Safety Features

These tests confirm:
- ✓ Load shedding triggers at correct threshold
- ✓ Load throttling activates under high TPS/IO
- ✓ pg_stat_statements protection works
- ✓ No catalog lock conflicts during DDL
- ✓ Ring buffer HOT updates work correctly

### Running Integration Tests

```bash
cd benchmark

# Setup test data
./setup.sh

# Run light_oltp scenario to validate safety features
./run.sh --scenario light_oltp --duration 10 --clients 50

# Review comparison report (validates load shedding triggered, etc.)
cat results/*/comparison_*.md
```

Focus on the "Database Statistics" section - are safety features working?

## Decision Framework

Use this flowchart:

```
Do you have absolute cost measurements?
├─ No  → Run ./benchmark/measure_absolute.sh first
└─ Yes → What's the mean collection time?
         ├─ <100ms → Safe everywhere
         ├─ 100-200ms → Safe on 2+ vCPU, test on 1 vCPU
         └─ >200ms → Investigate why (database size? Config?)

Is this a tiny system (1 vCPU, <2GB RAM)?
├─ Yes → Test in staging for 24h before production
└─ No  → Safe to deploy

Is this always-on production?
├─ Yes → Start with emergency mode, monitor, upgrade if comfortable
└─ No  → Normal mode is fine (troubleshooting/staging)
```

## FAQ

**Q: Do I need expensive hardware to benchmark?**
A: No. Absolute costs are constant. Run on your laptop in 5 minutes.

**Q: Do I need to simulate production load?**
A: No. Collection cost doesn't depend on your workload. Load shedding/throttling handle that.

**Q: What if my collection time is 500ms?**
A: Investigate:
- How many tables? (Large catalogs = slower)
- How many connections? (More to scan)
- Slow disk? (Check I/O metrics)
- Consider emergency mode (300s intervals)

**Q: Should I measure on RDS/Cloud?**
A: Useful to see if cloud overhead differs from local, but not required. The tool works the same everywhere.

**Q: How often should I re-measure?**
A: After major database changes:
- Significantly more tables (10x growth)
- PostgreSQL version upgrade
- Hardware changes

Otherwise, costs are stable.

**Q: Can I contribute measurements?**
A: Yes! Open an issue with your `absolute_costs_*.json` file. Help build evidence about scaling.

## Summary

**Simple approach:**
1. Run `./benchmark/measure_absolute.sh` on your laptop (5 min)
2. Review absolute costs (e.g., "150ms per collection")
3. Calculate sustained CPU at your interval (e.g., "0.08% at 180s")
4. Assess headroom in your environment
5. Deploy and monitor

**No need for:**
- Complex TPS degradation analysis
- Expensive cloud resources
- Multi-hour test runs
- Statistical significance testing of relative impact

Just measure the absolute cost. That's all you need to know.
