# pg-flight-recorder Testing Strategy

This document outlines the testing approach for pg-flight-recorder, including what's currently tested, gaps, and how to measure storage footprint.

## Current Testing Infrastructure

### 1. Existing Test Coverage (What's Working)

| Test File | Tests | Coverage |
|-----------|-------|----------|
| `01_foundation.sql` | 54 | Schema, tables, views, functions exist |
| `02_ring_buffer_analysis.sql` | 25 | Ring buffer mechanics, flush, aggregates |
| `03_safety_features.sql` | 83 | Kill switch, circuit breaker, profiles |
| `04_boundary_critical.sql` | ~50 | Edge cases, limits, data boundaries |
| `05_error_version.sql` | ~80 | Error handling, version detection |
| `06_load_archive_capacity.sql` | 104 | Load shedding, archives, capacity |
| `07_pathology_generators.sql` | ~30 | Test data generation |
| `08_pathology_value_checks.sql` | ~20 | Data validation |

**Total: ~450 test assertions across 8 files**

### 2. Run Existing Tests

```bash
# Run all tests on all supported PostgreSQL versions
./test.sh

# Run on specific version
./test.sh 17
```

### 3. Benchmark Tools

```bash
cd benchmark

# Measure collection timing/overhead
./measure_absolute.sh

# Measure DDL blocking impact
./measure_ddl_impact.sh
```

---

## Collection Frequency Reference

| Profile | Sample Interval | Snapshot Interval | Collections/Hour |
|---------|-----------------|-------------------|------------------|
| `default` | 180s (3 min) | 300s (5 min) | 20 samples + 12 snapshots |
| `production_safe` | 300s (5 min) | 300s (5 min) | 12 samples + 12 snapshots |
| `development` | 180s (3 min) | 300s (5 min) | 20 samples + 12 snapshots |
| `troubleshooting` | 60s (1 min) | 300s (5 min) | 60 samples + 12 snapshots |
| `minimal_overhead` | 300s (5 min) | 300s (5 min) | 12 samples + 12 snapshots |

### Ring Buffer Retention

- **120 fixed slots** in ring buffer
- Retention = 120 slots × sample_interval
  - At 60s: 120 × 60 = 7,200s = **2 hours**
  - At 180s: 120 × 180 = 21,600s = **6 hours**
  - At 300s: 120 × 300 = 36,000s = **10 hours**

---

## Storage Footprint Analysis

### Ring Buffer Sizes (Fixed - UNLOGGED)

| Table | Rows Per Slot | Max Rows | Estimated Size |
|-------|---------------|----------|----------------|
| `samples_ring` | 1 | 120 | ~10 KB |
| `wait_samples_ring` | 100 | 12,000 | ~1.5 MB |
| `activity_samples_ring` | 25 | 3,000 | ~500 KB |
| `lock_samples_ring` | 100 | 12,000 | ~1.5 MB |

**Ring Buffer Total: ~3.5 MB (constant, does not grow)**

### Archive/Aggregate Growth (Per Hour)

Growth depends on activity level. Estimates for moderate workload:

| Profile | Samples/Hour | Wait Archive | Activity Archive | Lock Archive | Aggregates |
|---------|--------------|--------------|------------------|--------------|------------|
| `troubleshooting` (60s) | 60 | ~6 MB/hr | ~1.5 MB/hr | ~6 MB/hr | ~200 KB/hr |
| `default` (180s) | 20 | ~2 MB/hr | ~500 KB/hr | ~2 MB/hr | ~70 KB/hr |
| `production_safe` (300s) | 12 | ~1.2 MB/hr | ~300 KB/hr | ~1.2 MB/hr | ~40 KB/hr |

### Snapshot Growth (Per Hour)

Snapshots collected every 5 minutes = 12/hour

| Table | Estimated Size/Snapshot | Per Hour |
|-------|------------------------|----------|
| `snapshots` | ~1 KB | ~12 KB |
| `table_snapshots` | ~50 bytes × tables | varies |
| `index_snapshots` | ~50 bytes × indexes | varies |
| `statement_snapshots` | ~500 bytes × top_n | ~120 KB |
| `config_snapshots` | ~5 KB | ~60 KB |
| `replication_snapshots` | ~200 bytes | ~2 KB |

### Maximum Storage Per Hour

| Profile | Best Case (Idle) | Typical | Worst Case (Busy) |
|---------|------------------|---------|-------------------|
| `troubleshooting` (60s) | ~2 MB/hr | ~15 MB/hr | ~50 MB/hr |
| `default` (180s) | ~1 MB/hr | ~5 MB/hr | ~15 MB/hr |
| `production_safe` (300s) | ~0.5 MB/hr | ~3 MB/hr | ~10 MB/hr |

### Retention Periods (Default)

| Data Type | Retention | Max Storage (default profile) |
|-----------|-----------|-------------------------------|
| Aggregates | 7 days | ~840 MB |
| Archives | 7 days | ~2.5 GB |
| Snapshots | 30 days | ~3.6 GB |

**Maximum Total: ~7 GB** (with default profile over 30 days)

---

## Test the Storage Footprint

Run the new measurement script:

```bash
# Measure actual storage consumption over time
./tests/measure_storage_footprint.sh

# Or with specific duration and interval
./tests/measure_storage_footprint.sh 3600 60  # 1 hour, measure every 60s
```

---

## Gaps & Areas Not Yet Tested

### Currently Not Covered

1. **Long-running storage growth** - No test runs for hours to measure actual growth
2. **Concurrent access** - Tests run single-threaded
3. **Recovery scenarios** - Crash recovery of UNLOGGED tables
4. **Large catalog impact** - Tests use small schemas (need 1000+ tables test)
5. **Replica lag detection** - Requires replica setup
6. **pg_stat_statements integration** - Partially tested

### Recommended Additional Tests

1. **Storage growth test** - Run for 1 hour, measure growth rate
2. **Stress test** - High concurrency with samples
3. **Version upgrade test** - Install v1, upgrade to v2
4. **Large schema test** - 1000+ tables performance

---

## Quick Testing Checklist

### Before Release

- [ ] `./test.sh` passes on PG 15, 16, 17
- [ ] `./benchmark/measure_absolute.sh` shows <200ms mean
- [ ] `./tests/measure_storage_footprint.sh` runs without error
- [ ] Manual: `SELECT flight_recorder.report('1 hour')` returns data

### For Production Deployment

- [ ] Run `measure_absolute.sh` on target hardware
- [ ] Verify `measure_ddl_impact.sh` shows <3% collision rate
- [ ] Check `SELECT flight_recorder.validate_config()` returns no errors
- [ ] Monitor `SELECT * FROM flight_recorder.collection_stats ORDER BY started_at DESC LIMIT 10`
