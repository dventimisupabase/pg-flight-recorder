# Flight Recorder Diagnostic Playbooks

**Purpose**: Quick-reference guides for diagnosing customer database issues using pg-flight-recorder data.
**Audience**: CSAs, Support Engineers, and AI assistants helping with incident response.

---

## Playbook Index

1. [Database is Slow RIGHT NOW](#1-database-is-slow-right-now)
2. [Database WAS Slow (Historical)](#2-database-was-slow-historical)
3. [Queries Timing Out / Taking Forever](#3-queries-timing-out--taking-forever)
4. [High CPU Usage](#4-high-cpu-usage)
5. [Lock Contention / Blocked Queries](#5-lock-contention--blocked-queries)
6. [Connection Exhaustion](#6-connection-exhaustion)
7. [Disk I/O Problems](#7-disk-io-problems)
8. [Checkpoint Storms](#8-checkpoint-storms)
9. [Memory Pressure / work_mem Issues](#9-memory-pressure--work_mem-issues)

---

## 1. Database is Slow RIGHT NOW

**Scenario**: Customer reports performance issues happening right now. You need real-time visibility.

### Triage Queries

```sql
-- What's happening RIGHT NOW?
SELECT * FROM flight_recorder.recent_activity_current()
ORDER BY query_start NULLS LAST
LIMIT 25;

-- What are sessions waiting on RIGHT NOW?
SELECT * FROM flight_recorder.recent_waits_current()
ORDER BY count DESC;

-- Any lock contention RIGHT NOW?
SELECT * FROM flight_recorder.recent_locks_current()
ORDER BY blocked_duration DESC;
```

### What to Look For

**In `recent_activity_current()`:**

- Long-running queries (`query_start` far in the past)
- State = 'active' with `wait_event_type` not null → blocked on something
- Many queries with same `query_preview` → query storm

**In `recent_waits_current()`:**

- High counts for `Lock` wait events → contention
- `IO:DataFileRead` → disk I/O bottleneck
- `Client:ClientRead` → application not consuming results fast enough
- `LWLock:buffer_mapping` or `buffer_content` → shared_buffers contention

**In `recent_locks_current()`:**

- Look at `blocking_query_preview` to identify the blocker
- `lock_type` tells you what kind of lock (e.g., `relation`, `tuple`, `transactionid`)
- Long `blocked_duration` → urgent intervention needed

### Common Root Causes & Fixes

| Symptom | Root Cause | Fix |
|---------|------------|-----|
| Single query blocking many others | Long transaction holding locks | Kill blocker: `SELECT pg_terminate_backend(blocking_pid)` |
| Many queries waiting on `Lock` | Lock contention on hot table | Identify table from lock OID, optimize queries, consider partitioning |
| Queries waiting on `IO:DataFileRead` | Slow disk or table scan | Check if sequential scans, consider caching/indexing |
| Hundreds of connections active | Connection pooling issue | Check pgbouncer/connection pool config |

---

## 2. Database WAS Slow (Historical)

**Scenario**: "The database was really slow between 10-11am yesterday." You need to reconstruct what happened.

### Triage Queries

```sql
-- Define the incident window
\set incident_start '2025-01-17 10:00:00+00'::timestamptz
\set incident_end '2025-01-17 11:00:00+00'::timestamptz

-- Get the overall summary
SELECT * FROM flight_recorder.summary_report(
    :'incident_start',
    :'incident_end'
);

-- Auto-detect anomalies
SELECT * FROM flight_recorder.anomaly_report(
    :'incident_start',
    :'incident_end'
)
ORDER BY
    CASE severity
        WHEN 'high' THEN 1
        WHEN 'medium' THEN 2
        ELSE 3
    END;

-- Compare system metrics before vs during incident
SELECT * FROM flight_recorder.compare(
    :'incident_start',
    :'incident_end'
);
```

### Deep Dive Queries

```sql
-- What queries changed behavior during the incident?
SELECT
    query_preview,
    calls_delta,
    total_exec_time_delta_ms,
    mean_exec_time_end_ms - mean_exec_time_start_ms AS mean_time_increase_ms,
    temp_blks_written_delta,
    hit_ratio_pct
FROM flight_recorder.statement_compare(
    :'incident_start',
    :'incident_end',
    100,  -- min delta 100ms
    50    -- top 50 queries
)
ORDER BY total_exec_time_delta_ms DESC;

-- What were the wait events during the incident?
SELECT * FROM flight_recorder.wait_summary(
    :'incident_start',
    :'incident_end'
)
ORDER BY total_waiters DESC
LIMIT 20;

-- Get specific activity samples from the incident window
SELECT
    captured_at,
    pid,
    usename,
    application_name,
    state,
    wait_event_type,
    wait_event,
    query_start,
    query_preview
FROM flight_recorder.activity_samples_archive
WHERE captured_at BETWEEN :'incident_start' AND :'incident_end'
    AND state = 'active'
ORDER BY captured_at, query_start;
```

### Interpretation Guide

**From `anomaly_report()`:**

- `FORCED_CHECKPOINT` → WAL size exceeded, increase `max_wal_size`
- `BUFFER_PRESSURE` → `shared_buffers` exhausted
- `TEMP_FILE_SPILLS` → queries spilling to disk, increase `work_mem`
- `LOCK_CONTENTION` → blocking queries identified

**From `statement_compare()`:**

- Large `mean_time_increase_ms` → query regressed
- High `temp_blks_written_delta` → sorts/joins spilling to disk
- Low `hit_ratio_pct` (<90%) → poor cache hit ratio, disk I/O bound

**From `wait_summary()`:**

- Dominant wait event reveals bottleneck type
- `pct_of_samples` shows what % of time was spent waiting

---

## 3. Queries Timing Out / Taking Forever

**Scenario**: Specific queries are timing out or taking much longer than expected.

### Investigation Queries

```sql
-- Find the slowest queries in the last hour
SELECT
    query_preview,
    calls,
    mean_exec_time AS avg_ms,
    max_exec_time AS max_ms,
    total_exec_time / 1000 AS total_seconds,
    temp_blks_written,
    shared_blks_read,
    wal_bytes
FROM flight_recorder.statement_snapshots ss
JOIN flight_recorder.snapshots s ON s.id = ss.snapshot_id
WHERE s.captured_at > now() - interval '1 hour'
ORDER BY max_exec_time DESC
LIMIT 20;

-- Check if query duration correlates with wait events
-- (Compare time windows: when query was fast vs when it was slow)
\set fast_period_start '2025-01-17 09:00:00+00'::timestamptz
\set fast_period_end '2025-01-17 09:30:00+00'::timestamptz
\set slow_period_start '2025-01-17 10:00:00+00'::timestamptz
\set slow_period_end '2025-01-17 10:30:00+00'::timestamptz

SELECT
    'FAST PERIOD' AS period,
    wait_event_type,
    wait_event,
    avg_waiters
FROM flight_recorder.wait_summary(
    :'fast_period_start',
    :'fast_period_end'
)
UNION ALL
SELECT
    'SLOW PERIOD' AS period,
    wait_event_type,
    wait_event,
    avg_waiters
FROM flight_recorder.wait_summary(
    :'slow_period_start',
    :'slow_period_end'
)
ORDER BY period, avg_waiters DESC;
```

### Root Cause Checklist

- [ ] **Plan regression?** - Stats out of date, run `ANALYZE` on affected tables
- [ ] **Missing index?** - Check `shared_blks_read` is high → sequential scan
- [ ] **Lock contention?** - Check `recent_locks_current()` for blockers
- [ ] **Temp file spills?** - Check `temp_blks_written` > 0 → increase `work_mem`
- [ ] **I/O bottleneck?** - Check wait events for `DataFileRead` → slow storage
- [ ] **Checkpoint interference?** - Check `compare()` for checkpoint during slow period
- [ ] **Parallel query disabled?** - Check if `max_parallel_workers` is configured

---

## 4. High CPU Usage

**Scenario**: Database CPU is pegged at 100%. What queries are burning CPU?

### Investigation Queries

```sql
-- Which queries are consuming the most CPU?
-- (High execution time + no disk I/O = CPU-bound)
SELECT
    query_preview,
    calls,
    total_exec_time / 1000 AS total_cpu_seconds,
    mean_exec_time AS avg_ms,
    shared_blks_hit,
    shared_blks_read,
    CASE
        WHEN (shared_blks_hit + shared_blks_read) > 0
        THEN round(100.0 * shared_blks_hit / (shared_blks_hit + shared_blks_read), 1)
        ELSE NULL
    END AS cache_hit_pct
FROM flight_recorder.statement_snapshots ss
JOIN flight_recorder.snapshots s ON s.id = ss.snapshot_id
WHERE s.captured_at > now() - interval '1 hour'
    AND blk_read_time = 0  -- No disk I/O wait time → CPU-bound
ORDER BY total_exec_time DESC
LIMIT 20;

-- Check for CPU-related wait events (minimal - most CPU work doesn't show waits)
SELECT * FROM flight_recorder.recent_waits_current()
WHERE wait_event_type IS NULL  -- NULL means active on CPU
ORDER BY count DESC;

-- Are there too many active connections doing work?
SELECT count(*) AS active_connections
FROM flight_recorder.recent_activity_current()
WHERE state = 'active' AND wait_event IS NULL;
```

### Common CPU Burnout Causes

| Cause | How to Identify | Fix |
|-------|----------------|-----|
| **Inefficient query** | High `calls` + high `total_exec_time` + good cache hit | Optimize query (add index, rewrite) |
| **Sequential scans** | High `shared_blks_hit` or `shared_blks_read` | Add appropriate index |
| **Too many workers** | Many active connections with no wait events | Reduce connection pool size |
| **Complex JSON/text processing** | Query has heavy `jsonb` operations or regex | Optimize logic, consider computed columns |
| **Aggregate-heavy queries** | Queries with `SUM`, `COUNT`, `GROUP BY` on millions of rows | Add materialized views, pre-aggregate data |

---

## 5. Lock Contention / Blocked Queries

**Scenario**: Queries are getting blocked by locks. Who's blocking whom?

### Investigation Queries

```sql
-- Current lock conflicts
SELECT
    blocked_duration,
    blocked_pid,
    blocked_user,
    blocked_query_preview,
    blocking_pid,
    blocking_user,
    blocking_query_preview,
    lock_type
FROM flight_recorder.recent_locks_current()
ORDER BY blocked_duration DESC;

-- Historical lock patterns (who blocks whom most often?)
SELECT
    blocked_user,
    blocking_user,
    lock_type,
    occurrence_count,
    max_duration,
    avg_duration,
    sample_query
FROM flight_recorder.lock_aggregates
WHERE start_time > now() - interval '24 hours'
ORDER BY max_duration DESC
LIMIT 30;

-- Find the "serial blocker" (one session blocking many)
WITH blocker_stats AS (
    SELECT
        blocking_pid,
        blocking_user,
        blocking_query_preview,
        count(DISTINCT blocked_pid) AS num_victims,
        max(blocked_duration) AS max_victim_wait
    FROM flight_recorder.lock_samples_archive
    WHERE captured_at > now() - interval '1 hour'
    GROUP BY blocking_pid, blocking_user, blocking_query_preview
)
SELECT * FROM blocker_stats
ORDER BY num_victims DESC, max_victim_wait DESC;
```

### Lock Troubleshooting Decision Tree

1. **Is there one blocker with many victims?**
   - Yes → Check what the blocker is doing. If idle in transaction → kill it.
   - `SELECT pg_terminate_backend(<blocking_pid>);`

2. **Are locks on a specific table?**
   - Look up the table: `SELECT oid::regclass FROM pg_class WHERE oid = <locked_relation_oid>;`
   - Consider table-level optimizations (partitioning, index tuning)

3. **Lock type is `transactionid`?**
   - Two transactions trying to update the same row
   - Application logic issue: reduce row-level contention

4. **Lock type is `relation` with `AccessExclusiveLock`?**
   - DDL operation (ALTER TABLE, CREATE INDEX, etc.) blocking everything
   - Schedule DDL during maintenance windows

5. **Lock type is `tuple`?**
   - Row-level lock conflicts (UPDATE/DELETE contention)
   - Review application transaction boundaries, use SELECT FOR UPDATE SKIP LOCKED

---

## 6. Connection Exhaustion

**Scenario**: "Too many connections" errors or approaching `max_connections`.

### Investigation Queries

```sql
-- Connection utilization over time
SELECT
    captured_at,
    connections_active,
    connections_total,
    connections_max,
    round(100.0 * connections_total / connections_max, 1) AS utilization_pct
FROM flight_recorder.snapshots
WHERE captured_at > now() - interval '4 hours'
ORDER BY captured_at;

-- Who's hogging connections? (Current state)
SELECT
    usename,
    application_name,
    state,
    count(*) AS conn_count
FROM flight_recorder.recent_activity_current()
GROUP BY usename, application_name, state
ORDER BY conn_count DESC;

-- Historical: which apps created the most connections?
SELECT
    application_name,
    usename,
    backend_type,
    count(*) AS sample_count
FROM flight_recorder.activity_samples_archive
WHERE captured_at > now() - interval '24 hours'
GROUP BY application_name, usename, backend_type
ORDER BY sample_count DESC
LIMIT 20;

-- Are connections idle?
SELECT
    state,
    count(*) AS connection_count,
    round(100.0 * count(*) / sum(count(*)) OVER (), 1) AS pct
FROM flight_recorder.recent_activity_current()
GROUP BY state
ORDER BY connection_count DESC;
```

### Diagnosis & Fixes

| Symptom | Root Cause | Fix |
|---------|------------|-----|
| Many `idle` connections | App not closing connections | Enable connection pooling (pgBouncer) |
| Many `idle in transaction` | App not committing/rolling back | Fix application code, set `idle_in_transaction_session_timeout` |
| Connections spike at certain times | Cron jobs or scheduled tasks | Stagger job execution, use queue |
| One app consuming most connections | No connection pooling | Implement connection pooling at app level |
| Approaching `max_connections` | Under-provisioned | Increase `max_connections` (carefully - uses RAM per connection) |

---

## 7. Disk I/O Problems

**Scenario**: Queries are slow due to disk I/O. Investigate storage bottlenecks.

### Investigation Queries

```sql
-- Check for I/O wait events
SELECT
    wait_event,
    total_waiters,
    avg_waiters,
    max_waiters,
    pct_of_samples
FROM flight_recorder.wait_summary(
    now() - interval '1 hour',
    now()
)
WHERE wait_event_type = 'IO'
ORDER BY total_waiters DESC;

-- Which queries are doing the most disk reads?
SELECT
    query_preview,
    calls,
    shared_blks_read,
    blk_read_time / 1000 AS read_time_seconds,
    CASE
        WHEN shared_blks_read > 0
        THEN round(blk_read_time / shared_blks_read, 2)
        ELSE 0
    END AS ms_per_block
FROM flight_recorder.statement_snapshots ss
JOIN flight_recorder.snapshots s ON s.id = ss.snapshot_id
WHERE s.captured_at > now() - interval '1 hour'
    AND shared_blks_read > 0
ORDER BY blk_read_time DESC
LIMIT 20;

-- Are checkpoints causing I/O spikes?
SELECT
    captured_at,
    ckpt_write_time / 1000 AS ckpt_write_seconds,
    ckpt_sync_time / 1000 AS ckpt_sync_seconds,
    ckpt_buffers,
    ckpt_timed,
    ckpt_requested
FROM flight_recorder.snapshots
WHERE captured_at > now() - interval '4 hours'
    AND (ckpt_write_time > 0 OR ckpt_sync_time > 0)
ORDER BY captured_at;

-- Check buffer cache hit ratio
SELECT
    captured_at,
    blks_hit,
    blks_read,
    round(100.0 * blks_hit / NULLIF(blks_hit + blks_read, 0), 1) AS cache_hit_pct
FROM flight_recorder.snapshots
WHERE captured_at > now() - interval '4 hours'
ORDER BY captured_at;
```

### I/O Bottleneck Solutions

| Problem | Indicator | Solution |
|---------|-----------|----------|
| **Sequential scans on large tables** | High `shared_blks_read` on specific queries | Add indexes, enable parallel seq scan |
| **Poor cache hit ratio** | `cache_hit_pct` < 90% | Increase `shared_buffers` (up to 25% RAM) |
| **Slow storage** | High `ms_per_block` (>1ms) | Upgrade to faster disks (SSD/NVMe) |
| **Checkpoint I/O storms** | High `ckpt_write_time` or `ckpt_sync_time` | Increase `checkpoint_timeout`, tune `checkpoint_completion_target` |
| **Forced checkpoints** | `ckpt_requested` increasing | Increase `max_wal_size` |

---

## 8. Checkpoint Storms

**Scenario**: Performance degrades periodically - could be checkpoint interference.

### Investigation Queries

```sql
-- Checkpoint timeline and impact
SELECT
    checkpoint_time,
    ckpt_timed,
    ckpt_requested,
    ckpt_write_time / 1000 AS write_seconds,
    ckpt_sync_time / 1000 AS sync_seconds,
    ckpt_buffers,
    bgw_buffers_backend,
    bgw_buffers_backend_fsync
FROM flight_recorder.snapshots
WHERE captured_at > now() - interval '24 hours'
ORDER BY checkpoint_time;

-- Detect forced checkpoints (BAD - means WAL limit exceeded)
SELECT
    checkpoint_time,
    ckpt_requested - LAG(ckpt_requested) OVER (ORDER BY captured_at) AS forced_ckpts,
    wal_bytes / (1024*1024*1024) AS wal_gb
FROM flight_recorder.snapshots
WHERE captured_at > now() - interval '24 hours'
    AND checkpoint_time IS NOT NULL
ORDER BY checkpoint_time;

-- Check for anomalies related to checkpoints
SELECT * FROM flight_recorder.anomaly_report(
    now() - interval '24 hours',
    now()
)
WHERE anomaly_type IN ('CHECKPOINT_DURING_WINDOW', 'FORCED_CHECKPOINT', 'BACKEND_FSYNC');
```

### Checkpoint Tuning Guide

**Symptoms of Checkpoint Problems:**

- Performance dips every `checkpoint_timeout` interval (default 5 min)
- `FORCED_CHECKPOINT` anomalies
- High `bgw_buffers_backend_fsync` (backends forced to sync)

**Fixes:**

```sql
-- Increase checkpoint spacing (reduce frequency)
ALTER SYSTEM SET checkpoint_timeout = '15min';  -- from default 5min

-- Allow more WAL before forcing checkpoint
ALTER SYSTEM SET max_wal_size = '4GB';  -- from default 1GB

-- Spread checkpoint I/O over longer period
ALTER SYSTEM SET checkpoint_completion_target = 0.9;  -- from default 0.9 (already good)

-- Apply changes
SELECT pg_reload_conf();
```

---

## 9. Memory Pressure / work_mem Issues

**Scenario**: Queries spilling to disk due to insufficient `work_mem`.

### Investigation Queries

```sql
-- Detect temp file spills (BAD - means work_mem exhausted)
SELECT
    captured_at,
    temp_files,
    temp_bytes / (1024*1024) AS temp_mb
FROM flight_recorder.snapshots
WHERE captured_at > now() - interval '4 hours'
    AND temp_files > 0
ORDER BY temp_bytes DESC;

-- Which queries are spilling to temp files?
SELECT
    query_preview,
    calls,
    temp_blks_written,
    temp_blks_written * 8192 / (1024*1024) AS temp_mb_written,
    mean_exec_time AS avg_ms
FROM flight_recorder.statement_snapshots ss
JOIN flight_recorder.snapshots s ON s.id = ss.snapshot_id
WHERE s.captured_at > now() - interval '1 hour'
    AND temp_blks_written > 0
ORDER BY temp_blks_written DESC;

-- Check for temp spill anomalies
SELECT * FROM flight_recorder.anomaly_report(
    now() - interval '1 hour',
    now()
)
WHERE anomaly_type = 'TEMP_FILE_SPILLS';
```

### work_mem Tuning Strategy

**Problem**: Sorts, hashes, and aggregates spilling to disk (slow).

**Solution**: Increase `work_mem`, but carefully (per-query memory allocation).

```sql
-- Check current setting
SHOW work_mem;

-- For specific queries (in the app):
SET work_mem = '256MB';  -- Just for this session
-- Run expensive query here

-- Globally (use with caution - this is per sort/hash operation!):
ALTER SYSTEM SET work_mem = '64MB';  -- From default 4MB
SELECT pg_reload_conf();
```

**⚠️ Warning**: Setting `work_mem` too high can cause OOM kills!

- Formula: `work_mem * max_connections * max_parallel_workers` should be < available RAM
- Better: Set per-query in application code for known expensive queries

---

## General Tips for AI Assistants

When helping diagnose issues:

1. **Start with the incident time window** - Get exact timestamps from customer
2. **Run `anomaly_report()` first** - It auto-detects common problems
3. **Use `summary_report()` for context** - Gives high-level overview
4. **Compare before vs during** - Use `compare()` and `statement_compare()`
5. **Drill into specifics** - Based on anomaly type, query deeper
6. **Look for correlations** - Did checkpoint + lock + slow query happen together?
7. **Explain in plain English** - Customers don't speak PostgreSQL
8. **Provide actionable next steps** - Not just "there's a problem" but "here's how to fix it"

---

## Quick Reference: Function Cheat Sheet

| Function | Purpose | When to Use |
|----------|---------|-------------|
| `recent_activity_current()` | Live activity right now | Real-time triage |
| `recent_waits_current()` | Current wait events | What's blocking right now? |
| `recent_locks_current()` | Current lock conflicts | Who's blocking whom? |
| `compare(start, end)` | System metrics delta | Compare two time periods |
| `statement_compare(start, end)` | Query performance delta | Which queries regressed? |
| `wait_summary(start, end)` | Wait event breakdown | What were sessions waiting on? |
| `anomaly_report(start, end)` | Auto-detect problems | Quick problem scan |
| `summary_report(start, end)` | Comprehensive overview | High-level incident summary |
| `activity_samples_archive` | Raw activity samples | Forensic deep-dive |
| `lock_samples_archive` | Raw lock samples | Detailed lock investigation |

---

**Last Updated**: 2026-01-18
**Maintainer**: Flight Recorder Team
**Feedback**: Report issues or suggest improvements in GitHub Issues
