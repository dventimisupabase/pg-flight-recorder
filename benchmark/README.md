# pg-flight-recorder Benchmark Framework

**Status:** Initial Implementation

This framework provides reproducible benchmarks to measure the **observer effect** of pg-flight-recorder on PostgreSQL workloads.

## Philosophy: No Benchmarketing

We are committed to **honest, reproducible performance measurement**. See [BENCHMARKING.md](../BENCHMARKING.md) for full methodology.

Key principles:
- ✓ Reproducible setup (you can run the same tests)
- ✓ Statistical rigor (percentiles, confidence intervals)
- ✓ Multiple workloads (not cherry-picked scenarios)
- ✓ Transparent methodology
- ✓ Report worst-case, not just averages

## Quick Start

### Prerequisites

1. **PostgreSQL 15+** with pg_cron installed
2. **pg-flight-recorder** installed: `psql -f install.sql`
3. **pgbench** (comes with PostgreSQL contrib)
4. **Python 3** with numpy: `pip3 install numpy`
5. **Standard libpq auth**: Set `PGHOST`, `PGUSER`, `PGDATABASE`, `PGPASSWORD` or use `.pgpass`

### Run Benchmarks

```bash
# 1. Setup test data and check prerequisites
cd benchmark
./setup.sh

# 2. Run a single scenario (quick test)
./run.sh --scenario light_oltp --duration 10 --clients 10

# 3. Run all scenarios (takes ~4 hours)
./run.sh --all --mode normal
```

### View Results

Results are saved to `results/YYYYMMDD_HHMMSS/`:

```bash
# View comparison report
cat results/20240116_143022/comparison_normal_light_oltp.md

# Summary shows:
# - Throughput impact (TPS degradation)
# - Latency impact (p50, p95, p99)
# - Database statistics deltas
# - Overall assessment
```

## Available Scenarios

### light_oltp (Implemented)
- **Profile**: E-commerce, moderate traffic
- **TPS**: 100-500 transactions/sec
- **Pattern**: 80% SELECT, 15% UPDATE, 5% INSERT
- **Duration**: 30 minutes (configurable)
- **Clients**: 10 (configurable)

### heavy_oltp (TODO)
- **Profile**: High-traffic SaaS
- **TPS**: 1000-5000 transactions/sec
- **Clients**: 100-500

### analytical (TODO)
- **Profile**: Data warehouse queries
- **Pattern**: Complex JOINs, aggregations, scans
- **Clients**: 5-20

### mixed (TODO)
- **Profile**: OLTP + analytical
- **Clients**: 50-100

### high_ddl (TODO)
- **Profile**: Schema changes, migrations
- **Pattern**: CREATE, DROP, ALTER operations

## Understanding Results

### Throughput Impact

**Acceptable:** < 5% degradation
**Moderate:** 5-10% degradation
**High:** > 10% degradation

Example:
```
Baseline: 487.3 TPS
With Flight Recorder: 476.1 TPS
Impact: +2.3% (acceptable)
```

### Latency Impact (P95)

**Acceptable:** < 5% increase
**Moderate:** 5-10% increase
**High:** > 10% increase

Example:
```
Baseline P95: 15.3ms
With Flight Recorder P95: 15.8ms
Impact: +3.3% (acceptable)
```

### Overall Assessment

The framework automatically assesses impact:

- ✓ **NEGLIGIBLE** (<2% impact) - Recommended for production
- ✓ **LOW** (<5% impact) - Acceptable for most workloads
- ⚠ **MODERATE** (<10% impact) - Monitor closely in production
- ⚠ **HIGH** (<20% impact) - Use for troubleshooting only
- ✗ **SEVERE** (>20% impact) - Switch to emergency mode or disable

## Customizing Benchmarks

### Run Specific Duration

```bash
# 60-minute test for more stable measurements
./run.sh --scenario light_oltp --duration 60 --clients 10
```

### Test Different Client Counts

```bash
# Simulate higher concurrency
./run.sh --scenario light_oltp --duration 30 --clients 100
```

### Test Different Modes

```bash
# Test emergency mode (300s sampling)
./run.sh --scenario light_oltp --mode emergency
```

### Larger Dataset

```bash
# Setup with scale=1000 (~1.5GB)
./setup.sh --scale 1000

# Then run benchmarks
./run.sh --scenario light_oltp --duration 30 --clients 50
```

## Creating New Scenarios

Create `scenarios/my_scenario.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail

DURATION=30
CLIENTS=10
OUTPUT=""

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --duration) DURATION="$2"; shift 2 ;;
        --clients) CLIENTS="$2"; shift 2 ;;
        --output) OUTPUT="$2"; shift 2 ;;
        *) echo "Unknown option: $1" >&2; exit 1 ;;
    esac
done

# Run your workload
# Capture metrics
# Write JSON output to $OUTPUT

cat > "$OUTPUT" <<EOF
{
  "scenario": "my_scenario",
  "start_time": "...",
  "end_time": "...",
  "duration_seconds": ...,
  "clients": ...,
  "throughput": {
    "tps": ...,
    "transactions_total": ...
  },
  "latency_ms": {
    "mean": ...,
    "stddev": ...,
    "p50": ...,
    "p95": ...,
    "p99": ...,
    "max": ...
  },
  "database_stats": {
    "start": {...},
    "end": {...}
  }
}
EOF
```

Make executable: `chmod +x scenarios/my_scenario.sh`

Then run: `./run.sh --scenario my_scenario`

## Interpreting Results for Documentation

**DO NOT** make overhead claims in documentation until:

1. ✓ Benchmarks run on reference hardware (document specs)
2. ✓ Multiple scenarios tested (not just best-case)
3. ✓ Statistical significance validated (multiple runs, confidence intervals)
4. ✓ Worst-case scenarios documented
5. ✓ Community validation (others can reproduce)

**When documenting overhead:**

❌ **Bad (Benchmarketing):**
> "Uses less than 0.1% of your CPU"

✓ **Good (Honest):**
> "In our benchmark on AWS RDS db.m6g.xlarge with light OLTP workload (487 TPS),
> we observed 2.3% ± 0.5% throughput impact and 3.3% P95 latency increase.
> Your mileage may vary. Run `./benchmark/run.sh` to measure in your environment."

## Roadmap

- [x] Framework design and methodology
- [x] Setup script (pgbench + analytical schema)
- [x] Run orchestration script
- [x] Light OLTP scenario
- [x] Statistical comparison tool
- [ ] Heavy OLTP scenario
- [ ] Analytical scenario
- [ ] Mixed workload scenario
- [ ] High-DDL scenario
- [ ] Reference hardware benchmarks
- [ ] Multi-run aggregation (confidence intervals)
- [ ] Grafana dashboard for live monitoring
- [ ] Community benchmark submissions

## Contributing

Help us measure flight recorder impact honestly:

1. Run benchmarks in your environment
2. Share results (anonymized if needed)
3. Add new scenarios for different workload patterns
4. Improve statistical analysis
5. Document edge cases we haven't tested

Open issues or PRs at: https://github.com/your-org/pg-flight-recorder

## Support

Questions about benchmarking methodology? Open an issue with:
- Your environment (PG version, hardware, OS)
- Scenario you're running
- Results observed
- Questions about interpretation
