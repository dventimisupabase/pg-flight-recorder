# Benchmark Testing Plan

This document describes how to test the benchmark suite before merging.

## Prerequisites

```bash
# Required extensions
psql -c "CREATE EXTENSION IF NOT EXISTS pg_cron"
psql -c "CREATE EXTENSION IF NOT EXISTS pgstattuple"

# flight_recorder must be installed
psql -f install.sql

# pgbench tables initialized
cd benchmark && ./setup.sh
```

## Quick Validation (5 minutes)

Run each benchmark with minimal duration to verify scripts work:

```bash
cd benchmark

# Observer effect - 1 iteration, 1 min test
PGDATABASE=your_db ITERATIONS=1 WARMUP_DURATION=30 TEST_DURATION=60 CLIENTS=10 \
  WORKLOADS="oltp_balanced" ./measure_observer_effect.sh

# Storage - 2 minutes
PGDATABASE=your_db DURATION_HOURS=0.03 SAMPLE_INTERVAL=30 ./measure_storage.sh

# Bloat - 2 minutes
PGDATABASE=your_db DURATION_HOURS=0.03 SAMPLE_INTERVAL=30 ./measure_bloat.sh
```

**Expected:** All three scripts complete without errors and generate reports.

## Functional Tests

### 1. Observer Effect Script

```bash
# Test with all workloads
PGDATABASE=your_db ITERATIONS=1 WARMUP_DURATION=30 TEST_DURATION=60 CLIENTS=10 \
  ./measure_observer_effect.sh
```

**Verify:**

- [ ] Creates results directory with timestamp
- [ ] Generates analysis_report.json with TPS and latency data
- [ ] Generates summary.md report
- [ ] WAL measurement completes (check wal_overhead.txt)
- [ ] Baseline and enabled modes alternate correctly

### 2. Storage Script

```bash
PGDATABASE=your_db DURATION_HOURS=0.1 SAMPLE_INTERVAL=30 ./measure_storage.sh
```

**Verify:**

- [ ] Creates storage_timeline.csv with multiple data points
- [ ] Creates row_sizes.csv with actual row measurements
- [ ] Creates projections.csv with daily growth estimates
- [ ] Generates storage_report.md with summary
- [ ] Report shows non-zero row counts for ring tables

### 3. Bloat Script

```bash
PGDATABASE=your_db DURATION_HOURS=0.1 SAMPLE_INTERVAL=30 ./measure_bloat.sh
```

**Verify:**

- [ ] Creates bloat_timeline.csv with tracking data
- [ ] Creates deltas.csv with HOT update calculations
- [ ] Creates precise_bloat.csv with pgstattuple results
- [ ] Creates sample_duration.csv with duration checks
- [ ] Generates bloat_report.md with summary
- [ ] HOT percentages are calculated correctly (not 0/0 = NaN)

### 4. Statistical Analysis

```bash
# Test Python analysis script directly
python3 lib/statistical_analysis.py results/observer_effect_*/
```

**Verify:**

- [ ] Parses pgbench log files correctly
- [ ] Computes percentiles (p50, p95, p99)
- [ ] Generates JSON output with confidence intervals

## Edge Cases

### Decimal Duration

```bash
# Should not fail with float arithmetic error
DURATION_HOURS=0.02 ./measure_storage.sh
DURATION_HOURS=0.02 ./measure_bloat.sh
```

### Empty Results

```bash
# Run with flight_recorder disabled - should still work
psql -c "SELECT flight_recorder.disable()"
DURATION_HOURS=0.02 ./measure_storage.sh
```

### Missing Prerequisites

```bash
# Should fail gracefully with clear error message
PGDATABASE=nonexistent_db ./measure_storage.sh
```

## Full Benchmark Run (Optional, ~1 hour)

For thorough validation before production use:

```bash
# Observer effect with multiple iterations
PGDATABASE=your_db ITERATIONS=3 WARMUP_DURATION=60 TEST_DURATION=300 CLIENTS=20 \
  ./measure_observer_effect.sh

# Storage for 30 minutes
PGDATABASE=your_db DURATION_HOURS=0.5 SAMPLE_INTERVAL=60 ./measure_storage.sh

# Bloat for 30 minutes
PGDATABASE=your_db DURATION_HOURS=0.5 SAMPLE_INTERVAL=60 ./measure_bloat.sh
```

## Success Criteria

| Test | Pass Condition |
|------|----------------|
| Quick validation | All scripts complete without errors |
| Observer effect | TPS impact < 5% (for quick test, noise is expected) |
| Storage | Row sizes measured, projections calculated |
| Bloat | HOT % > 0, dead tuple % calculated |
| Edge cases | Decimal durations work, graceful error handling |

## Known Limitations

1. **Short runs have high variance** - Observer effect needs 3+ iterations for statistical significance
2. **Storage projections inaccurate for short runs** - Ring buffers distort short-term measurements
3. **WAL measurement shows NOTICE messages** - Cosmetic issue, doesn't affect results

## Reporting Issues

If tests fail, include:

1. PostgreSQL version (`psql --version`)
2. OS and architecture
3. Full error output
4. Contents of any generated report files
