# pg-flight-recorder Benchmark Framework

**Status:** Implemented (Simplified Approach)

This framework measures the **absolute cost** of pg-flight-recorder collections. The key insight: observer effect is roughly constant, independent of workload.

## Philosophy: No Benchmarketing

We measure **absolute costs** (CPU time per collection), not relative impact (TPS degradation).

**Why?** Because the cost is constant:

- Running `SELECT * FROM pg_stat_activity` takes ~150ms whether your DB is idle or processing 10,000 TPS
- The question isn't "does this slow my queries?" but "do I have 150ms headroom every 180 seconds?"

See [BENCHMARKING.md](../BENCHMARKING.md) for full methodology.

## Quick Start: Measure Absolute Costs (5 minutes)

### Prerequisites

1. **PostgreSQL 15+** with pg_cron installed
2. **pg-flight-recorder** installed: `psql -f ../install.sql`
3. **Python 3**: For statistical analysis
4. **Standard libpq auth**: Set `PGHOST`, `PGUSER`, `PGDATABASE`, `PGPASSWORD` or use `.pgpass`

### Run Measurement

```bash
cd benchmark

# Measure absolute costs (100 collections)
./measure_absolute.sh

# Output:
# - Collection timing (mean, p50, p95, p99)
# - I/O operations per collection
# - Sustained CPU % at different intervals
# - Headroom assessment
# - JSON report: results/absolute_costs_YYYYMMDD_HHMMSS.json
```

Example output:

```
Collection Timing:
  Mean:   127.3 ms ± 23.1 ms
  P95:    168.2 ms

Sustained CPU Impact:
  At 180s intervals: 0.071%

Peak Impact:
  Brief 127ms CPU spike every 180 seconds

Headroom Assessment:
  ✓ 2+ vCPU system: SAFE - minimal impact
  ✓ 1 vCPU system: ACCEPTABLE - test in staging first
```

**That's it.** No complex workload simulation needed.

## DDL Blocking Impact Measurement (NEW)

### Purpose

Measure how often and how long flight recorder blocks DDL operations (ALTER TABLE, CREATE INDEX, etc.) due to AccessShareLock contention on system catalogs.

**Key Questions Answered:**

- What % of DDL operations encounter blocking from flight recorder?
- How long do blocked DDL operations wait?
- What's the expected collision rate at different workload levels?

### Run DDL Impact Test

```bash
cd benchmark

# Run 5-minute test (default: 180s interval, normal mode)
./measure_ddl_impact.sh

# Custom duration and interval
./measure_ddl_impact.sh 600 300  # 10 min test, 300s interval (emergency mode)

# Output:
# - DDL collision rate (% of operations blocked by flight recorder)
# - Wait time distribution (P50, P95, P99)
# - Risk assessment and recommendations
# - Report: results/ddl_impact_YYYYMMDD_HHMMSS/ddl_impact_report.md
```

### Example Output

```
DDL Blocking Impact Report

Total DDL Operations: 1,247
Operations Blocked by Flight Recorder: 28 (2.24%)

All DDL Operations (Duration):
  Median: 12.3 ms
  P95:    45.7 ms
  P99:    78.2 ms

Blocked Operations Only:
  Median: 18.9 ms
  P95:    67.3 ms
  P99:    95.1 ms

Average Delay from Blocking: 6.6 ms

Impact Assessment (180s intervals):
  Flight recorder runs: 480 collections/day
  Expected DDL collisions: ~26.8 per day
  If you run 100 DDL ops/hour: ~2.2 will encounter blocking

Risk Level: LOW - Minimal DDL impact
Recommendation: Safe for production use with high DDL workloads
```

### Interpreting Results

**Collision Rate:**

- **<1%**: Negligible impact - safe for DDL-heavy workloads
- **1-3%**: Low impact - acceptable for most workloads
- **3-5%**: Moderate impact - monitor if >50 DDL ops/hour
- **>5%**: High impact - consider emergency mode (300s) during DDL-heavy periods

**Average Delay:**

- **<10ms**: Minimal impact
- **10-50ms**: Acceptable for most applications
- **50-100ms**: May be noticeable for latency-sensitive DDL
- **>100ms**: Consider scheduling DDL during maintenance windows

### What This Measures

The test:

1. Runs flight recorder at specified interval (default: 180s)
2. Continuously executes DDL operations (ALTER, CREATE INDEX, DROP, VACUUM)
3. Detects when DDL waits for locks via pg_locks
4. Identifies if flight recorder's catalog queries are the blocker

**AccessShareLock Contention:**

- Flight recorder queries: pg_stat_activity, pg_locks, pg_class, etc.
- These acquire AccessShareLock on system catalogs
- DDL operations need AccessExclusiveLock
- When flight recorder holds AccessShareLock, DDL must wait

### Reducing DDL Impact

If collision rate is concerning:

```sql
-- Option 1: Use emergency mode (300s intervals = 40% fewer collections)
SELECT flight_recorder.set_mode('emergency');

-- Option 2: Increase lock_timeout (fail faster, less DDL delay)
UPDATE flight_recorder.config
SET value = '50'
WHERE key = 'lock_timeout_ms';

-- Option 3: Disable during DDL-heavy maintenance
SELECT flight_recorder.disable();
-- ... run DDL operations ...
SELECT flight_recorder.enable();
```

### When to Run This Test

**Required for:**

- High-DDL workloads (multi-tenant SaaS with frequent schema changes)
- Latency-sensitive applications where DDL timing matters
- Before enabling always-on production monitoring

**Less critical for:**

- Low-DDL workloads (<10 DDL ops/hour)
- Troubleshooting/staging use (not always-on)
- Maintenance-window-only DDL

## Integration Testing (Optional)

The scenario-based tests validate **safety features work**, not performance impact.

### Purpose

Confirm that:

- ✓ Load shedding triggers correctly (>70% connections)
- ✓ Load throttling activates under high TPS/IO
- ✓ pg_stat_statements protection works
- ✓ No catalog lock conflicts during DDL
- ✓ Ring buffer HOT updates work correctly

### Available Scenarios

#### light_oltp (Implemented)

- **Pattern**: 80% SELECT, 15% UPDATE, 5% INSERT
- **Purpose**: Validate safety features under moderate OLTP load

#### Others (TODO)

- heavy_oltp: Stress test load shedding/throttling
- analytical: Test during heavy I/O
- high_ddl: Catalog lock contention
- mixed: Realistic production workload

### Running Integration Tests

```bash
cd benchmark

# Setup test data
./setup.sh

# Run scenario
./run.sh --scenario light_oltp --duration 10 --clients 50

# Review report - focus on "Database Statistics" section
cat results/*/comparison_*.md
```

**Note:** These tests measure relative TPS impact, which is less meaningful than absolute costs. Use primarily to validate safety features.

## What the Measurement Tells You

### Absolute Costs

- **Mean timing**: Average CPU time per collection
- **P95 timing**: 95% of collections complete within this time
- **Sustained CPU**: (mean_ms / interval_ms) × 100
- **Peak impact**: Brief spike every N seconds

### Interpreting Results

**Collection time <100ms:**

- ✓ Safe everywhere (1+ vCPU)
- Negligible sustained CPU (<0.06% at 180s)

**Collection time 100-200ms:**

- ✓ Safe on 2+ vCPU systems
- ⚠ Test on 1 vCPU systems first
- Low sustained CPU (<0.11% at 180s)

**Collection time >200ms:**

- ⚠ Investigate: Why so slow?
  - Large catalog (many tables)?
  - Many active connections?
  - Slow disk?
- Consider emergency mode (300s intervals)

### Scaling with Database Size

Help us understand: Does cost scale with DB size?

Run `measure_absolute.sh` on databases of different sizes and share results:

- Empty database
- 1GB, 10GB, 100GB, 1TB
- Different table counts (10, 100, 1000, 10000)

This builds community evidence.

## Share Your Measurements

After running `measure_absolute.sh`, consider sharing your `absolute_costs_*.json` file:

1. Open an issue at: https://github.com/your-org/pg-flight-recorder
2. Title: "Absolute cost measurement: [your environment]"
3. Attach JSON file
4. Include context: database size, table count, hardware

This helps others assess whether flight recorder is appropriate for their environment.

## Deployment Guidance

Based on your absolute cost measurement:

### For Always-On Production

```sql
-- Start conservative
SELECT flight_recorder.set_mode('emergency');  -- 300s

-- Monitor for 24h
SELECT * FROM flight_recorder.collection_health;

-- Upgrade if comfortable
SELECT flight_recorder.set_mode('normal');  -- 180s
```

### For 1 vCPU Systems

Test in staging for 24h first:

- Watch for collection timeouts
- Monitor CPU spikes
- Check if load shedding triggers frequently

### For Troubleshooting

Normal mode is fine (180s) - only runs during incidents.

## FAQ

**Q: Why not measure TPS degradation?**
A: Because observer effect is constant. Whether your DB does 10 TPS or 10,000 TPS, collection takes the same ~150ms. The question is about absolute headroom, not relative impact.

**Q: Can I run this on my laptop?**
A: Yes! Absolute costs don't depend on expensive hardware. Run on any PostgreSQL instance.

**Q: How long does measurement take?**
A: 5-10 minutes for 100 iterations. Quick and simple.

**Q: What if I want to validate safety features?**
A: Use the integration tests (`./run.sh --scenario light_oltp`). But focus on the absolute cost measurement first.

**Q: Should I measure on production?**
A: If you have a staging/dev environment with similar size database, measure there first. Otherwise, production is fine - measurement is non-destructive.

## Contributing

Ways to help:

1. **Share measurements** from different environments
2. **Validate on tiny systems** (1 vCPU) and report
3. **Test on huge databases** (1TB+) and report
4. **Improve measurement script** (better metrics, visualizations)
5. **Document edge cases** (what makes collections slow?)

## Summary

**Simple workflow:**

1. `cd benchmark && ./measure_absolute.sh` (5 min)
2. Review: "Mean: X ms"
3. Calculate: X / 180000 = sustained CPU %
4. Assess: Do I have X ms headroom every 3 minutes?
5. Deploy accordingly

**Philosophy:**

- Measure what's constant (absolute cost)
- Not what's variable (relative TPS impact)
- Reproducible on any laptop
- No complex simulation needed

See [BENCHMARKING.md](../BENCHMARKING.md) for full methodology.
