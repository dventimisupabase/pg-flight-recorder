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
