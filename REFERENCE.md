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

| Mode        | Sample Interval | Locks | Progress | Use Case        |
|-------------|-----------------|-------|----------|-----------------|
| `normal`    | 30 seconds      | Yes   | Yes      | Default         |
| `light`     | 60 seconds      | Yes   | No       | Moderate load   |
| `emergency` | 120 seconds     | No    | No       | System stressed |

```sql
SELECT flight_recorder.set_mode('light');
SELECT * FROM flight_recorder.get_mode();
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

### Observer Effect Prevention

Flight recorder is designed to have minimal impact on the database it monitors:

**UNLOGGED Tables**
- 9 telemetry tables use UNLOGGED to eliminate WAL overhead
- Only `config` and `tracked_tables` (small config data) use WAL
- Data lost on crash is acceptable for telemetry

**Per-Section Timeouts**
- Each collection section has independent 1-second timeout
- Prevents any single query from monopolizing resources
- Timeout resets between sections and at function end

**O(n) Lock Detection**
- Uses `pg_blocking_pids()` instead of O(n^2) self-join on `pg_locks`
- Scales linearly with connection count

**Result Limits**
- Lock samples: 100 max
- Active sessions: 25 max
- Statement snapshots: 50 max (configurable)

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

| Key                               | Default | Purpose                             |
|-----------------------------------|---------|-------------------------------------|
| `circuit_breaker_threshold_ms`    | 1000    | Max collection duration             |
| `circuit_breaker_enabled`         | true    | Enable/disable circuit breaker      |
| `auto_mode_enabled`               | true    | Auto-adjust collection mode         |
| `auto_mode_connections_threshold` | 60      | % connections to trigger light mode |
| `section_timeout_ms`              | 250     | Per-section query timeout           |
| `lock_timeout_ms`                 | 100     | Max wait for catalog locks          |
| `skip_locks_threshold`            | 50      | Skip lock collection if > N blocked |
| `skip_activity_conn_threshold`    | 100     | Skip activity if > N active conns   |
| `schema_size_warning_mb`          | 5000    | Warning threshold                   |
| `schema_size_critical_mb`         | 10000   | Auto-disable threshold              |
| `retention_samples_days`          | 7       | Sample retention                    |
| `retention_snapshots_days`        | 30      | Snapshot retention                  |

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
