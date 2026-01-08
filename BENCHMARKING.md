# Benchmarking Methodology

**Status:** Work in Progress

## Philosophy: No Benchmarketing

We are committed to honest, reproducible performance measurement. Any overhead claims in documentation MUST be backed by:

1. **Reproducible setup** - You can run the same tests
2. **Statistical rigor** - Means, medians, percentiles, confidence intervals
3. **Multiple workloads** - OLTP, OLAP, mixed, edge cases
4. **Transparent methodology** - Full disclosure of test conditions
5. **Worst-case reporting** - Don't hide the bad numbers

**We will NOT:**
- Cherry-pick best-case scenarios
- Average over idle periods to inflate claims
- Report single-point measurements as "typical"
- Hide methodology details
- Make claims without supporting data

## Benchmark Framework (TODO)

### Goal

Measure the **observer effect** of pg-flight-recorder across realistic PostgreSQL workloads. Specifically:

1. **Throughput impact**: How many fewer transactions/sec?
2. **Latency impact**: How much slower are queries? (p50, p95, p99)
3. **CPU overhead**: Additional CPU consumption during collection
4. **I/O overhead**: Additional disk I/O from UNLOGGED table writes
5. **Lock contention**: Impact of catalog locks on concurrent DDL
6. **Memory overhead**: RSS and shared buffer impact

### Workload Scenarios

We need to test across multiple realistic workloads:

#### 1. Light OLTP
- **Profile**: E-commerce application, moderate traffic
- **TPS**: 100-500 transactions/sec
- **Pattern**: 80% SELECT, 15% UPDATE, 5% INSERT
- **Concurrency**: 10-50 active connections
- **Duration**: 30 minutes
- **Purpose**: Baseline overhead for typical production database

#### 2. Heavy OLTP
- **Profile**: High-traffic SaaS application
- **TPS**: 1000-5000 transactions/sec
- **Pattern**: 70% SELECT, 20% UPDATE, 10% INSERT
- **Concurrency**: 100-500 active connections
- **Duration**: 30 minutes
- **Purpose**: Stress test load shedding and throttling

#### 3. Analytical Workload
- **Profile**: Data warehouse queries
- **TPS**: 10-50 long-running queries/min
- **Pattern**: Complex JOINs, aggregations, sequential scans
- **Concurrency**: 5-20 analytical queries
- **Duration**: 30 minutes
- **Purpose**: Test impact during heavy I/O and CPU usage

#### 4. Mixed Workload
- **Profile**: Production database with both OLTP and analytics
- **TPS**: 500-1000 short + 5-10 long queries
- **Pattern**: 60% OLTP, 40% analytical
- **Concurrency**: 50-100 connections
- **Duration**: 30 minutes
- **Purpose**: Realistic production environment

#### 5. High-DDL Workload
- **Profile**: Migration or development environment
- **TPS**: 10-50 DDL operations/min (CREATE, DROP, ALTER)
- **Concurrency**: 10-20 connections
- **Duration**: 15 minutes
- **Purpose**: Measure catalog lock contention impact

#### 6. Edge Cases
- **Checkpoint storms**: Force frequent checkpoints
- **Vacuum storms**: Heavy autovacuum activity
- **Lock contention**: Deliberate lock waits
- **Connection churn**: Rapid connect/disconnect cycles
- **Statement eviction**: Fill pg_stat_statements to trigger churn

### Metrics Collection

For each scenario, measure **with and without** flight recorder enabled:

#### Primary Metrics
- **Throughput**: Transactions/sec (mean, median, p95, p99)
- **Latency**: Query response time in ms (mean, median, p95, p99, max)
- **CPU**: User+system CPU % (mean, median, p95, max)
- **I/O**: Blocks read+written per second

#### Secondary Metrics
- **Memory**: RSS, shared_buffers utilization
- **Locks**: Lock wait time, lock conflicts
- **Vacuum**: Autovacuum runs, dead tuple accumulation
- **Ring buffer**: HOT update ratio, collection skip rate

### Statistical Analysis

For each metric:

1. **Mean with 95% confidence interval**
2. **Median (50th percentile)**
3. **95th percentile** (worst 5% of measurements)
4. **99th percentile** (worst 1% of measurements)
5. **Standard deviation**
6. **Sample size** (measurement count)

Calculate **impact** as:
```
Impact % = ((with_flight_recorder - baseline) / baseline) × 100
```

Report impact for ALL metrics (don't cherry-pick good ones).

### Test Environment

#### Hardware Requirements
- **CPU**: 4 cores minimum (document exact CPU model)
- **RAM**: 8GB minimum
- **Disk**: SSD with known IOPS capability
- **PostgreSQL**: Version 15, 16, 17 (test all)

#### Configuration
- Document ALL PostgreSQL settings (shared_buffers, work_mem, etc.)
- Document flight recorder mode (normal/light/emergency)
- Document OS and kernel version
- Document any tuning applied

#### Setup Script
Provide reproducible setup:
```bash
./benchmark/setup.sh    # Install pgbench, create schemas
./benchmark/run.sh      # Run all scenarios
./benchmark/analyze.sh  # Generate statistical report
```

### Success Criteria

We can claim "low overhead" ONLY if:

1. **Throughput impact < 5%** across all workloads at p95
2. **Latency impact < 5%** across all workloads at p95
3. **CPU overhead < 2%** sustained during active workload (not idle)
4. **No lock conflicts** detected during normal OLTP workload
5. **Graceful degradation** under extreme load (throttling kicks in)

If these criteria are NOT met, we:
- Document the actual impact honestly
- Do NOT make "low overhead" claims
- Provide guidance on when flight recorder is appropriate

### Output Format

Generate markdown report with:

```markdown
# pg-flight-recorder Benchmark Results

**Date**: YYYY-MM-DD
**PostgreSQL Version**: 16.1
**Hardware**: AWS RDS db.m6g.xlarge (4 vCPU, 16GB RAM)
**Flight Recorder Mode**: normal (180s sampling)

## Summary

| Workload | Throughput Impact | Latency Impact (p95) | CPU Overhead |
|----------|-------------------|----------------------|--------------|
| Light OLTP | +2.3% ± 0.5% | +3.1% ± 0.8% | +1.2% |
| Heavy OLTP | +4.7% ± 1.2% | +5.9% ± 1.5% | +1.8% |
| Analytical | +1.1% ± 0.3% | +1.5% ± 0.4% | +0.8% |
| Mixed      | +3.2% ± 0.7% | +4.2% ± 1.1% | +1.5% |
| High-DDL   | +8.3% ± 2.1% | +12.4% ± 3.2% | +2.1% |

## Detailed Results: Light OLTP

### Throughput (TPS)
- **Baseline**: 487.3 ± 12.1 TPS (mean ± 95% CI)
- **With Flight Recorder**: 476.1 ± 11.8 TPS
- **Impact**: +2.3% ± 0.5%

### Latency (ms)
- **p50**: 8.2ms → 8.4ms (+2.4%)
- **p95**: 15.3ms → 15.8ms (+3.3%)
- **p99**: 23.1ms → 24.7ms (+6.9%)

[Continue for all workloads...]

## Methodology

[Link to this document]

## Reproducibility

Run yourself:
```bash
git clone https://github.com/your-org/pg-flight-recorder
cd pg-flight-recorder/benchmark
./run.sh --scenario light_oltp --duration 30m
```
```

## Current Status: TODO

This framework is **not yet implemented**. Until it is:

1. **No specific overhead claims** should be made in documentation
2. Users should benchmark in their own environment
3. We can describe *mechanisms* (load shedding, throttling) but not *outcomes* (X% CPU)

## Implementation Roadmap

1. **Phase 1**: Create pgbench-based workload generators
2. **Phase 2**: Implement metrics collection (pg_stat_statements, OS metrics)
3. **Phase 3**: Run benchmarks on reference hardware (AWS RDS, standard VM)
4. **Phase 4**: Generate statistical analysis and reports
5. **Phase 5**: Validate with community testing (invite others to run)
6. **Phase 6**: Update documentation with verified claims

## Help Wanted

If you run pg-flight-recorder in production:
- Share your metrics (anonymized)
- Report observed overhead in your environment
- Contribute workload scenarios we should test

This helps us make honest, evidence-based claims.
