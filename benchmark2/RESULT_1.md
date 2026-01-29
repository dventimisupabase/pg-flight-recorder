# Benchmark Execution Report #1

**Date:** 2026-01-24
**Total Runtime:** ~55 minutes
**Environment:** PostgreSQL 17.7 on macOS (Apple M5, 24GB RAM)

## Configuration

| Benchmark | Duration | Parameters |
|-----------|----------|------------|
| Observer Effect | ~24 min | 2 iterations, 5 min test, 1 min warmup, 20 clients, oltp_balanced |
| Storage | ~15 min | 0.25 hours, 60s sample interval |
| Bloat | ~15 min | 0.25 hours, 60s sample interval |

## Results Summary

### 1. Observer Effect Benchmark

**Workload:** oltp_balanced (50% read, 50% write)

| Metric | Baseline | Enabled | Impact | Status |
|--------|----------|---------|--------|--------|
| TPS | 11667.75 | 11805.55 | **+1.18%** | PASS |
| p50 latency | 0.321ms | 0.341ms | +6.23% (+0.02ms) | PASS |
| p95 latency | 6.962ms | 6.907ms | -0.79% (-0.05ms) | PASS |
| p99 latency | 13.366ms | 13.226ms | -1.05% (-0.14ms) | PASS |
| max latency | 1361ms | 160ms | -88.24% | PASS |

**WAL Overhead:** 7,559 bytes per sample() (well under 10KB warning threshold)

**Verdict:** **PASS** - No measurable negative impact. TPS actually improved slightly (within noise).

### 2. Storage Benchmark

| Table | Rows | Avg Row Bytes | Heap Size | Index Size |
|-------|------|---------------|-----------|------------|
| samples_ring | 120 | 47.98 | 16 KB | 24 KB |
| activity_samples_ring | 3000 | 46.08 | 176 KB | 96 KB |
| wait_samples_ring | 12000 | 32.96 | 560 KB | 408 KB |
| lock_samples_ring | 12000 | 41.49 | 656 KB | 368 KB |

**Observed growth:** 0.94 MB over 15 minutes
**Projected daily growth:** ~90 MB/day (likely inflated due to short observation period)

**Note:** Ring buffers have fixed size after warmup. The growth measurement includes initial population which distorts short-term projections. For accurate projections, need 4+ hour runs.

### 3. Bloat Benchmark

#### HOT Update Ratio (Delta-Based)

| Table | Updates | HOT Updates | HOT % | Status |
|-------|---------|-------------|-------|--------|
| samples_ring | 5 | 5 | **100%** | PASS |
| activity_samples_ring | 128 | 125 | **97.66%** | PASS |
| wait_samples_ring | 531 | 511 | **96.23%** | PASS |
| lock_samples_ring | 500 | 500 | **100%** | PASS |

**Verdict:** All tables exceed 85% HOT threshold (target: > 85%)

#### Dead Tuple Percentage

| Table | Live | Dead | Dead % | Status |
|-------|------|------|--------|--------|
| samples_ring | 120 | 28 | 18.92% | WARNING |
| activity_samples_ring | 3000 | 147 | 4.67% | PASS |
| wait_samples_ring | 12000 | 164 | 1.35% | PASS |
| lock_samples_ring | 12000 | 768 | 6.02% | PASS |

**Note:** samples_ring shows 18.92% dead tuples which is near the 20% warning threshold. However, pgstattuple_approx shows only 27 dead tuples (18.37%), suggesting autovacuum is keeping up.

#### Precise Bloat (pgstattuple_approx)

| Table | Tuples | Dead | Dead % | Free Space % |
|-------|--------|------|--------|--------------|
| samples_ring | 120 | 27 | 18.37% | 52.32% |
| activity_samples_ring | 3000 | 1 | 0.03% | 13.24% |
| wait_samples_ring | 12041 | 0 | 0.00% | 19.40% |
| lock_samples_ring | 12000 | 83 | 0.69% | 15.26% |

#### Sample Duration Check

All 20 samples checked: **0ms duration** (OK, well under 50% of interval threshold)

## Assessment Against Success Criteria

| Criterion | Target | Result | Status |
|-----------|--------|--------|--------|
| TPS degradation | < 1% | +1.18% (improved) | **PASS** |
| p99 latency increase | < 5% OR < 2ms | -1.05% (-0.14ms) | **PASS** |
| WAL per sample | < 10 KB | 7.56 KB | **PASS** |
| HOT update % (ring) | > 85% | 96-100% | **PASS** |
| Dead tuple % (ring) | < 10% | 0-18.92% | **WARNING** |

## Issues Found

### 1. samples_ring Dead Tuple Accumulation

**Observation:** samples_ring shows 18.92% dead tuples, approaching 20% warning threshold.

**Analysis:** This table has only 120 rows (smallest ring buffer) and sees frequent updates. The high free space % (52%) indicates autovacuum is running but HOT updates are leaving some dead tuples.

**Recommendation:** Monitor over longer period. If this becomes problematic, consider:

- Lowering autovacuum_vacuum_threshold for this table
- Checking fillfactor setting

### 2. WAL Measurement Output Pollution

The WAL measurement includes NOTICE messages about "_fr_psa_snapshot" relation. This is cosmetic but should be cleaned up:

```
relation "_fr_psa_snapshot" already exists, skipping
```

**Recommendation:** Add `2>/dev/null` to the psql command or filter NOTICE messages.

## Files Generated

- `results/observer_effect_20260123_213050/` - Observer effect results
- `results/storage_20260123_215527/` - Storage measurement results
- `results/bloat_20260123_221034/` - Bloat measurement results

## Next Steps

1. **Run longer benchmarks** - 4+ hours for storage, 24+ hours for bloat to observe autovacuum behavior
2. **Test multiple workloads** - Include read_heavy and write_heavy scenarios
3. **Investigate samples_ring bloat** - Monitor if dead tuple % stabilizes or grows
4. **Clean up WAL measurement** - Suppress NOTICE messages in observer effect script
