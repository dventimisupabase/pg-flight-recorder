# Benchmark Specification

Light benchmark suite measuring observer effect, storage growth, and bloat.

## Quick Start

```bash
cd benchmark

# Prerequisites
psql -c "CREATE EXTENSION IF NOT EXISTS pg_cron"
psql -c "CREATE EXTENSION IF NOT EXISTS pgstattuple"
psql -f ../install.sql
./setup.sh

# Quick validation (~5 min)
ITERATIONS=1 TEST_DURATION=60 ./measure_observer_effect.sh
DURATION_HOURS=0.03 ./measure_storage.sh
DURATION_HOURS=0.03 ./measure_bloat.sh

# Full run (~1 hour)
ITERATIONS=3 TEST_DURATION=300 ./measure_observer_effect.sh
DURATION_HOURS=0.5 ./measure_storage.sh
DURATION_HOURS=0.5 ./measure_bloat.sh
```

## Success Criteria

| Metric | Target |
|--------|--------|
| TPS degradation | < 1% |
| p99 latency increase | < 5% OR < 2ms absolute |
| WAL per sample | < 10 KB |
| HOT update % (ring) | > 85% |
| Dead tuple % (ring) | < 10% |

---

## 1. Observer Effect

### Methodology

A-B interleaved comparison with alternating order:

1. Odd iterations: baseline first, then enabled
2. Even iterations: enabled first, then baseline
3. 2-minute warmup discarded before each measurement
4. Fixed random seed for reproducibility

### Thresholds

| Metric | Warning | Critical |
|--------|---------|----------|
| TPS degradation | > 1% | > 3% |
| p99 latency increase | > 5% AND > 2ms | > 15% AND > 5ms |
| WAL per sample | > 10 KB | > 50 KB |

### WAL Measurement

```sql
do $$
declare
    v_wal_before pg_lsn;
    v_wal_after pg_lsn;
    v_samples int := 20;
begin
    v_wal_before := pg_current_wal_lsn();
    for i in 1..v_samples loop
        perform flight_recorder.sample();
    end loop;
    v_wal_after := pg_current_wal_lsn();
    raise notice 'WAL per sample: % bytes',
        pg_wal_lsn_diff(v_wal_after, v_wal_before) / v_samples;
end $$;
```

---

## 2. Storage

### Measurement

```sql
select
    c.relname as table_name,
    c.reltuples::int8 as row_count,
    pg_relation_size(c.oid) as heap_size_bytes,
    pg_indexes_size(c.oid) as index_size_bytes,
    pg_total_relation_size(c.oid) as total_size_bytes
from pg_class as c
inner join pg_namespace as n on n.oid = c.relnamespace
where n.nspname = 'flight_recorder' and c.relkind = 'r'
order by pg_total_relation_size(c.oid) desc;
```

### Row Size (Data-Driven Projections)

```sql
select
    'samples_ring' as table_name,
    count(*) as row_count,
    avg(pg_column_size(t.*))::numeric(10, 2) as avg_row_bytes
from flight_recorder.samples_ring as t;
```

---

## 3. Bloat

### Lightweight Tracking

```sql
select
    relname,
    n_live_tup,
    n_dead_tup,
    round(100.0 * n_dead_tup / nullif(n_live_tup + n_dead_tup, 1), 2) as dead_pct,
    n_tup_upd,
    n_tup_hot_upd,
    round(100.0 * n_tup_hot_upd / nullif(n_tup_upd, 1), 2) as hot_pct
from pg_stat_user_tables
where schemaname = 'flight_recorder' and relname like '%\_ring';
```

### Thresholds

| Metric | Healthy | Warning |
|--------|---------|---------|
| HOT update % | > 85% | < 70% |
| Dead tuple % | < 10% | > 20% |

### Sample Duration Check

```sql
select
    started_at,
    duration_ms,
    case
        when duration_ms > 180000 * 0.8 then 'CRITICAL'
        when duration_ms > 180000 * 0.5 then 'WARNING'
        else 'OK'
    end as status
from flight_recorder.collection_stats
where collection_type = 'sample'
order by started_at desc limit 10;
```

---

## 4. Files

```
benchmark/
  measure_observer_effect.sh    # A-B interleaved TPS/latency
  measure_storage.sh            # Storage growth tracking
  measure_bloat.sh              # HOT ratio + dead tuples
  scenarios/
    oltp_balanced.sql           # 50% read, 50% write
    oltp_read_heavy.sql         # 90% read, 10% write
    oltp_write_heavy.sql        # 20% read, 80% write
  lib/
    statistical_analysis.py     # Percentiles, CI calculation
```

## 5. Out of Scope

- LWLock contention tracking
- Memory overhead measurement
- Crash recovery testing
- CI integration
- Multi-version PostgreSQL testing
