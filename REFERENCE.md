# pg-flight-recorder Reference

Complete documentation for pg-flight-recorder.

## Requirements

- PostgreSQL 15, 16, or 17
- `pg_cron` extension (1.4.1+ for 30-second sampling, older versions use 60 seconds)
- Superuser privileges
- Optional: `pg_stat_statements` for query analysis

## How It Works

Flight Recorder uses `pg_cron` to run two collection types:

1. **Snapshots** (every 5 minutes): Cumulative stats - WAL, checkpoints, bgwriter, replication, temp files, I/O
2. **Samples** (every 30 seconds): Point-in-time - wait events, active sessions, locks, operation progress

Analysis functions compare snapshots or aggregate samples to diagnose performance issues.

## Functions

### Analysis

| Function                           | Purpose                                                 |
|------------------------------------|---------------------------------------------------------|
| `compare(start, end)`              | Compare system stats between time points                |
| `wait_summary(start, end)`         | Aggregate wait events over time period                  |
| `activity_at(timestamp)`           | What was happening at specific moment                   |
| `anomaly_report(start, end)`       | Auto-detect 6 issue types                               |
| `summary_report(start, end)`       | Comprehensive diagnostic report                         |
| `table_compare(table, start, end)` | Compare table stats (tracked tables only)               |
| `statement_compare(start, end)`    | Compare query performance (requires pg_stat_statements) |

### Control

| Function                                 | Purpose                                   |
|------------------------------------------|-------------------------------------------|
| `enable()`                               | Start collection (schedules pg_cron jobs) |
| `disable()`                              | Stop all collection immediately           |
| `set_mode('normal'/'light'/'emergency')` | Adjust collection intensity               |
| `get_mode()`                             | Show current mode and settings            |
| `cleanup(interval)`                      | Delete old data (default: 7 days)         |

### Table Tracking

| Function                      | Purpose                  |
|-------------------------------|--------------------------|
| `track_table(name, schema)`   | Monitor a specific table |
| `untrack_table(name, schema)` | Stop monitoring          |
| `list_tracked_tables()`       | Show tracked tables      |

**Warning:** Each tracked table adds overhead. Track 5-20 critical tables max.

### Health & Monitoring

| Function                       | Purpose                           |
|--------------------------------|-----------------------------------|
| `health_check()`               | Component status overview         |
| `performance_report(interval)` | Flight recorder's own performance |
| `check_alerts(interval)`       | Active alerts (if enabled)        |
| `config_recommendations()`     | Optimization suggestions          |
| `export_json(start, end)`      | AI-friendly data export           |

## Views

| View                 | Purpose                                     |
|----------------------|---------------------------------------------|
| `recent_waits`       | Wait events (last 2 hours)                  |
| `recent_activity`    | Active sessions (last 2 hours)              |
| `recent_locks`       | Lock contention (last 2 hours)              |
| `recent_progress`    | Vacuum/COPY/analyze progress (last 2 hours) |
| `recent_replication` | Replication lag (last 2 hours)              |
| `deltas`             | Snapshot-over-snapshot changes              |
| `table_deltas`       | Tracked table changes                       |

## Collection Modes

Modes control **what** is collected, not **how often**. Sample interval is configured separately via `sample_interval_seconds` (default: 120s).

| Mode        | Locks | Progress | Interval Behavior | Use Case        |
|-------------|-------|----------|-------------------|-----------------|
| `normal`    | Yes   | Yes      | Uses configured interval | Default (4 sections) |
| `light`     | Yes   | No       | Uses configured interval | Moderate load (3 sections) |
| `emergency` | No    | No       | Forces min 120s | System stressed (2 sections) |

```sql
SELECT flight_recorder.set_mode('light');
SELECT * FROM flight_recorder.get_mode();

-- Sample interval is independent of mode
UPDATE flight_recorder.config SET value = '180' WHERE key = 'sample_interval_seconds';
SELECT flight_recorder.enable();  -- Reschedule with new interval
```

## Anomaly Detection

`anomaly_report()` auto-detects:

| Type                       | Meaning                                     |
|----------------------------|---------------------------------------------|
| `CHECKPOINT_DURING_WINDOW` | Checkpoint occurred (I/O spike)             |
| `FORCED_CHECKPOINT`        | WAL exceeded max_wal_size                   |
| `BUFFER_PRESSURE`          | Backends writing directly to disk           |
| `BACKEND_FSYNC`            | Backends doing fsync (bgwriter overwhelmed) |
| `TEMP_FILE_SPILLS`         | Queries spilling to disk (work_mem too low) |
| `LOCK_CONTENTION`          | Sessions blocked on locks                   |

## Safety Features

### Observer Effect

Flight recorder has measurable overhead. Exact cost depends on configuration:

| Config | Sample Interval | Timeout/Section | Worst-Case CPU | Notes |
|--------|-----------------|-----------------|----------------|-------|
| **Default** | 120s | 250ms | 0.8% | 4 sections: wait, activity, progress, locks |
| **High Resolution** | 60s | 250ms | 1.7% | Set sample_interval_seconds=60 for higher temporal resolution |
| **Light Mode** | 120s | 250ms | 0.6% | 3 sections: wait, activity, locks (progress disabled) |
| **Emergency Mode** | 120s | 250ms | 0.4% | 2 sections: wait, activity (locks and progress disabled) |

**Additional Resource Costs:**

- **Catalog locks**: 1 AccessShareLock per sample (default snapshot-based collection)
- **Lock timeout**: 100ms - fails fast if catalogs are locked by DDL operations
- **Memory**: 2MB work_mem per collection (configurable)
- **Storage**: ~2-3 GB for 7 days retention (UNLOGGED, no WAL overhead)
- **pg_stat_statements**: 20 queries × 96 snapshots/day = 1,920 rows/day

**Target Environments:**

- ✓ Production troubleshooting (enable during incidents)
- ✓ Staging/dev (always-on monitoring)
- ⚠ High-DDL workloads (frequent CREATE/DROP/ALTER may cause catalog lock contention)
- ✗ Resource-constrained databases (< 2 CPU cores, < 4GB RAM)

**Reducing Overhead:**

```sql
-- Switch to light mode (disables progress tracking)
SELECT flight_recorder.set_mode('light');

-- Switch to emergency mode (disables locks and progress)
SELECT flight_recorder.set_mode('emergency');

-- Stop completely
SELECT flight_recorder.disable();

-- Validate your configuration
SELECT * FROM flight_recorder.validate_config();
```

### Observer Effect Prevention

Flight recorder is designed to minimize impact on the database it monitors:

**UNLOGGED Tables**
- 9 telemetry tables use UNLOGGED to eliminate WAL overhead
- Only `config` and `tracked_tables` (small config data) use WAL
- Data lost on crash is acceptable for telemetry

**Per-Section Timeouts**
- Each collection section has independent 250ms timeout (configurable)
- Prevents any single query from monopolizing resources
- Timeout resets between sections and at function end

**O(n) Lock Detection**
- Uses `pg_blocking_pids()` instead of O(n^2) self-join on `pg_locks`
- Scales linearly with connection count

**Result Limits**
- Lock samples: 100 max
- Active sessions: 25 max
- Statement snapshots: 20 max (configurable)

### Circuit Breaker

Automatic protection when collections run slow:

```sql
-- View collection performance
SELECT collection_type, started_at, duration_ms, success, skipped
FROM flight_recorder.collection_stats
ORDER BY started_at DESC LIMIT 10;
```

- Threshold: 1000ms (configurable)
- Window: 15 minutes moving average
- Auto-skips next collection if threshold exceeded
- Auto-resumes when system recovers

```sql
-- Configure
UPDATE flight_recorder.config SET value = '2000' WHERE key = 'circuit_breaker_threshold_ms';
```

### Adaptive Mode

Automatically adjusts collection intensity based on system load:

- **Normal → Light**: When connections reach 60% of max_connections
- **Any → Emergency**: When circuit breaker trips 3 times in 10 minutes
- **Emergency → Light**: After 10 minutes without trips
- **Light → Normal**: When load drops below threshold

Enabled by default (`auto_mode_enabled = 'true'`).

### Schema Size Limits

- Warning at 5GB: Logs warning, continues
- Critical at 10GB: Auto-disables collection
- Check status: `SELECT * FROM flight_recorder._check_schema_size();`

### Graceful Degradation

Each collection section wrapped in exception handlers:
- Wait events fail → activity samples still collected
- Lock detection fails → progress tracking continues
- Partial data better than no data during incidents

## Configuration

All settings in `flight_recorder.config`:

```sql
SELECT * FROM flight_recorder.config;
```

Key settings:

| Key                                 | Default | Purpose                                        |
|-------------------------------------|---------|------------------------------------------------|
| `circuit_breaker_threshold_ms`      | 1000    | Max collection duration                        |
| `circuit_breaker_enabled`           | true    | Enable/disable circuit breaker                 |
| `auto_mode_enabled`                 | true    | Auto-adjust collection mode                    |
| `auto_mode_connections_threshold`   | 60      | % connections to trigger light mode            |
| `section_timeout_ms`                | 250     | Per-section query timeout                      |
| `lock_timeout_ms`                   | 100     | Max wait for catalog locks                     |
| `skip_locks_threshold`              | 50      | Skip lock collection if > N blocked            |
| `skip_activity_conn_threshold`      | 100     | Skip activity if > N active conns              |
| `sample_interval_seconds`           | 120     | Sample collection interval (60, 120, 180, ...) |
| `statements_interval_minutes`       | 15      | pg_stat_statements collection interval         |
| `statements_top_n`                  | 20      | Number of top queries to capture               |
| `snapshot_based_collection`         | true    | Use temp table snapshot (reduces catalog locks)|
| `adaptive_sampling`                 | false   | Skip collection when system idle               |
| `adaptive_sampling_idle_threshold`  | 5       | Skip if < N active connections                 |
| `schema_size_warning_mb`            | 5000    | Warning threshold                              |
| `schema_size_critical_mb`           | 10000   | Auto-disable threshold                         |
| `retention_samples_days`            | 7       | Sample retention                               |
| `retention_snapshots_days`          | 30      | Snapshot retention                             |

## Catalog Lock Contention

Every collection acquires AccessShareLock on system catalogs. This is generally harmless but can interact with DDL operations on high-churn databases.

### System Views Accessed

| System View            | Lock Target          | Acquired By         | Frequency  |
|------------------------|----------------------|---------------------|------------|
| `pg_stat_activity`     | pg_stat_activity     | sample() + snapshot | Every 60s  |
| `pg_stat_replication`  | pg_stat_replication  | snapshot()          | Every 5min |
| `pg_locks`             | pg_locks             | sample()            | Every 60s  |
| `pg_stat_statements`   | pg_stat_statements   | snapshot()          | Every 5min |
| `pg_relation_size()`   | Target relation      | snapshot()          | Every 5min |

### Lock Timeout Behavior

Default `lock_timeout` = 100ms:

- **If catalog is locked by DDL > 100ms**: Collection fails with lock timeout error
- **If collection starts before DDL**: DDL operation waits up to 100ms behind flight recorder
- **Circuit breaker**: After 3 lock timeout failures in 15 minutes, auto-switches to emergency mode

### High-DDL Workloads

Multi-tenant SaaS with frequent CREATE/DROP/ALTER operations:

**Symptoms:**
- `collection_stats` shows frequent `lock_timeout` errors
- Circuit breaker trips repeatedly
- Auto-mode switches to emergency mode

**Mitigations:**
```sql
-- Option 1: Disable tracked table monitoring (eliminates pg_relation_size locks)
SELECT flight_recorder.untrack_table('table_name');

-- Option 2: Reduce lock_timeout further (fail even faster)
UPDATE flight_recorder.config SET value = '50' WHERE key = 'lock_timeout_ms';

-- Option 3: Use emergency mode during high-DDL periods
SELECT flight_recorder.set_mode('emergency');

-- Option 4: Disable during maintenance windows
SELECT flight_recorder.disable();
```

**Check for lock failures:**
```sql
SELECT
    collection_type,
    count(*) AS lock_failures,
    max(started_at) AS last_failure
FROM flight_recorder.collection_stats
WHERE error_message LIKE '%lock_timeout%'
  AND started_at > now() - interval '1 hour'
GROUP BY collection_type;
```

## Advanced Optimizations

Flight recorder includes advanced features to reduce overhead and catalog lock contention. Some are enabled by default, others are opt-in.

### Snapshot-Based Collection (Phase 5B)

**Purpose:** Reduce catalog locks from 3 to 1 per sample

**How it works:** Creates a temp table snapshot of `pg_stat_activity` once per sample, then all sections query from the snapshot instead of querying the catalog 3 separate times.

**Status:** ✓ Enabled by default

**Impact:**
- Catalog locks: 3 → 1 per sample (67% reduction)
- Overhead: Negligible (temp table is ~100KB for 100 connections)
- Consistency: All sections see same snapshot (more accurate)

**To disable (not recommended):**
```sql
UPDATE flight_recorder.config SET value = 'false' WHERE key = 'snapshot_based_collection';
```

### Adaptive Sampling (Phase 5C)

**Purpose:** Reduce average overhead by skipping collection when system is idle

**How it works:** Before each sample, checks active connection count. If fewer than threshold (default: 5), skips collection entirely.

**Enable:**
```sql
UPDATE flight_recorder.config SET value = 'true' WHERE key = 'adaptive_sampling';
UPDATE flight_recorder.config SET value = '5' WHERE key = 'adaptive_sampling_idle_threshold';  -- Optional
```

**Impact:**
- Overhead during idle: ~0% (skips collection)
- Overhead during busy: 0.8% (unchanged)
- Average overhead: 0.3-0.5% depending on workload

**Trade-offs:**
- May miss the *start* of an incident (first sample after idle period)
- Non-uniform sampling (gaps in data during idle periods)

**When to use:**
- Workloads with idle periods (dev/staging, batch processing)
- Systems where you want absolute minimum overhead

### Adjustable Sample Interval

**Purpose:** Trade temporal resolution for lower overhead

**Configure:**
```sql
-- High resolution (1.7% overhead)
UPDATE flight_recorder.config SET value = '60' WHERE key = 'sample_interval_seconds';

-- Default (0.8% overhead)
UPDATE flight_recorder.config SET value = '120' WHERE key = 'sample_interval_seconds';

-- Low overhead (0.4% overhead)
UPDATE flight_recorder.config SET value = '240' WHERE key = 'sample_interval_seconds';
```

**Note:** Must call `flight_recorder.disable()` then `flight_recorder.enable()` to reschedule pg_cron jobs.

### pg_stat_statements Tuning

**Purpose:** Reduce memory pressure on pg_stat_statements hash table

**Configure:**
```sql
-- Collection interval (default: every 15 minutes)
UPDATE flight_recorder.config SET value = '15' WHERE key = 'statements_interval_minutes';

-- Number of top queries to capture (default: 20)
UPDATE flight_recorder.config SET value = '20' WHERE key = 'statements_top_n';
```

**Impact:**
- Default: 20 queries × 96 snapshots/day = 1,920 rows/day
- Old default: 50 queries × 288 snapshots/day = 14,400 rows/day (87% reduction)

## Diagnostic Patterns

### Lock Contention
```sql
-- Symptoms: batch 10x slower than expected
SELECT * FROM flight_recorder.recent_locks
WHERE captured_at BETWEEN '...' AND '...';

SELECT * FROM flight_recorder.wait_summary('...', '...')
WHERE wait_event_type = 'Lock';
```

### Buffer Pressure
```sql
-- Symptoms: compare() shows bgw_buffers_backend_delta > 0
SELECT * FROM flight_recorder.compare('...', '...');
-- Look for: backends writing directly = shared_buffers exhausted
```

### Checkpoint Issues
```sql
-- Symptoms: I/O spikes, slow commits
SELECT * FROM flight_recorder.compare('...', '...');
-- Look for: checkpoint_occurred = true, high ckpt_write_time_ms
```

### Work_mem Exhaustion
```sql
-- Symptoms: slow sorts/joins
SELECT * FROM flight_recorder.compare('...', '...');
-- Look for: temp_files_delta > 0, large temp_bytes_delta
```

## Testing

```bash
# Local development
supabase start
supabase db reset
supabase test db  # 131 tests

# Deploy
supabase link --project-ref <ref>
supabase db push
```

**Note:** VACUUM warnings during tests are expected (tests run in transactions).

## Uninstall

```sql
SELECT flight_recorder.disable();  -- Stop cron jobs
DROP SCHEMA flight_recorder CASCADE;
```

Or use `uninstall.sql`:
```bash
psql -f uninstall.sql
```

## Project Structure

```
pg-flight-recorder/
├── install.sql                  # Standalone install
├── uninstall.sql                # Standalone uninstall
├── README.md                    # Quick start
├── REFERENCE.md                 # This file
└── supabase/
    ├── config.toml
    ├── migrations/
    │   ├── 20260105000000_enable_pg_cron.sql
    │   └── 20260106000000_pg_flight_recorder.sql
    └── tests/
        └── 00001_flight_recorder_test.sql
```
