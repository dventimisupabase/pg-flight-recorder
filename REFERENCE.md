# pg-flight-recorder Reference

Technical reference for pg-flight-recorder, a PostgreSQL monitoring extension.

## Table of Contents

**Part I: Fundamentals**

- [Overview](#overview)
- [Core Concepts](#core-concepts)

**Part II: Installation & Configuration**

- [Installation](#installation)
- [Configuration Profiles](#configuration-profiles)
- [Advanced Configuration](#advanced-configuration)

**Part III: Usage**

- [Functions Reference](#functions-reference)
- [Views Reference](#views-reference)
- [Common Workflows](#common-workflows)

**Part IV: Safety & Operations**

- [Safety Mechanisms](#safety-mechanisms)
- [Catalog Lock Behavior](#catalog-lock-behavior)

**Part V: Capacity Planning**

- [Capacity Planning](#capacity-planning)

**Part VI: Reference Material**

- [Troubleshooting Guide](#troubleshooting-guide)
- [Anomaly Detection Reference](#anomaly-detection-reference)
- [Testing and Benchmarking](#testing-and-benchmarking)
- [Project Structure](#project-structure)

---

## Part I: Fundamentals

## Overview

### Purpose and Scope

pg-flight-recorder continuously samples PostgreSQL system state, storing data in a four-tier architecture optimized for minimal overhead and flexible retention. Use it to diagnose performance issues, track capacity trends, and understand database behavior over time.

### Requirements

- PostgreSQL 15, 16, or 17
- `pg_cron` extension (1.4.1+ recommended)
- Superuser privileges for installation
- Optional: `pg_stat_statements` for query analysis

### How It Works

Flight Recorder uses pg_cron to run five collection jobs:

| Job | Frequency | Purpose |
|-----|-----------|---------|
| **Sample** | Adaptive (see [Sample Intervals](#sample-intervals)) | Captures wait events, active sessions, locks to ring buffer |
| **Flush** | Every 5 minutes | Moves ring buffer data to durable aggregates |
| **Archive** | Every 15 minutes | Preserves raw samples for forensic analysis |
| **Snapshot** | Every 5 minutes | Records cumulative system stats (WAL, checkpoints, I/O) |
| **Cleanup** | Daily at 3 AM | Removes data beyond retention period |

Analysis functions compare snapshots or aggregate samples to diagnose performance issues.

## Core Concepts

### Four-Tier Data Architecture

Flight Recorder organizes data into four tiers optimized for different access patterns:

```
┌─────────────────────────────────────────────────────────────────────────┐
│ TIER 1: Ring Buffers (UNLOGGED)                                         │
│   High-frequency sampling, 6-10 hour retention, auto-overwrites         │
├─────────────────────────────────────────────────────────────────────────┤
│ TIER 1.5: Raw Archives (LOGGED)                                         │
│   Periodic raw sample snapshots, 7-day retention, forensic detail       │
├─────────────────────────────────────────────────────────────────────────┤
│ TIER 2: Aggregates (LOGGED)                                             │
│   Summarized data, 7-day retention, pattern analysis                    │
├─────────────────────────────────────────────────────────────────────────┤
│ TIER 3: Snapshots (LOGGED)                                              │
│   Cumulative stats, 30-day retention, trend analysis                    │
└─────────────────────────────────────────────────────────────────────────┘
```

**TIER 1: Ring Buffers**

Low-overhead, high-frequency sampling using a fixed 120-slot circular buffer.

| Table | Rows | Purpose |
|-------|------|---------|
| `samples_ring` | 120 | Master slot tracker |
| `wait_samples_ring` | 12,000 | Wait events (120 slots × 100 rows) |
| `activity_samples_ring` | 3,000 | Active sessions (120 slots × 25 rows) |
| `lock_samples_ring` | 12,000 | Lock contention (120 slots × 100 rows) |

Characteristics:

- **UNLOGGED tables** eliminate WAL overhead (data lost on crash—acceptable for telemetry)
- **Pre-populated rows** with UPDATE-only pattern achieve 100% HOT updates (zero dead tuples)
- **Modular arithmetic** (`epoch / interval % 120`) auto-overwrites old slots
- Query via: `recent_waits`, `recent_activity`, `recent_locks` views

**TIER 1.5: Raw Archives**

Periodic preservation of raw samples for forensic analysis beyond ring buffer retention.

| Table | Purpose |
|-------|---------|
| `activity_samples_archive` | PIDs, queries, session details |
| `lock_samples_archive` | Complete blocking chains |
| `wait_samples_archive` | Wait patterns with counts |

Preserves details that aggregates lose: specific PIDs, exact timestamps, complete blocking chains. Query directly for forensic analysis.

**TIER 2: Aggregates**

Durable summaries of ring buffer data for medium-term analysis.

| Table | Purpose |
|-------|---------|
| `wait_event_aggregates` | Wait patterns over 5-minute windows |
| `lock_aggregates` | Lock contention patterns |
| `query_aggregates` | Query execution patterns |

Query via: `wait_summary(start, end)` function.

**TIER 3: Snapshots**

Point-in-time cumulative statistics for long-term trends.

| Table | Purpose |
|-------|---------|
| `snapshots` | pg_stat_bgwriter, pg_stat_database, WAL, temp files, I/O |
| `replication_snapshots` | pg_stat_replication, replication slots |
| `statement_snapshots` | pg_stat_statements top queries |
| `table_snapshots` | Per-table activity (seq scans, index scans, writes, bloat) |
| `index_snapshots` | Per-index usage and size |
| `config_snapshots` | PostgreSQL configuration parameters |

Query via: `compare(start, end)`, `statement_compare(start, end)`, `deltas` view, `table_compare(start, end)`, `index_efficiency(start, end)`, `config_at(timestamp)`.

### Ring Buffer Mechanism

The ring buffer uses modular arithmetic for slot rotation:

```
slot_id = (epoch_seconds / sample_interval_seconds) % 120
```

With 120 slots:

- At 180s intervals: 120 × 180s = 6 hours retention
- At 300s intervals: 120 × 300s = 10 hours retention

**Why UNLOGGED tables?** Eliminates WAL overhead. Telemetry data lost on crash is acceptable—the system recovers and resumes collection.

**Why UPDATE-only pattern?** Child tables use `UPDATE ... SET col = NULL` to clear slots, then `INSERT ... ON CONFLICT DO UPDATE`. This achieves 100% HOT (Heap-Only Tuple) updates, eliminating dead tuples and autovacuum pressure.

**Storage optimization:**

- Master table: `fillfactor=70` (30% free space for HOT)
- Child tables: `fillfactor=90` (10% free space for HOT)

### Collection Modes

Modes control collection intensity. See [Sample Intervals](#sample-intervals) for timing details.

| Mode | Locks | Activity | Use Case |
|------|-------|----------|----------|
| `normal` | Yes | Full (25 rows) | Default, recommended |
| `emergency` | No | Limited | System stressed |

```sql
-- Check current mode
SELECT * FROM flight_recorder.get_mode();

-- Change mode
SELECT flight_recorder.set_mode('emergency');
```

The system can auto-switch modes based on load (see [Adaptive Mode](#adaptive-mode)).

### Glossary

| Term | Definition |
|------|------------|
| **HOT updates** | Heap-Only Tuple updates that modify row data without updating indexes, reducing I/O and bloat |
| **wait_event** | PostgreSQL's classification of what a backend is waiting for (Lock, IO, CPU, etc.) |
| **pg_cron** | PostgreSQL extension that schedules and executes jobs within the database |
| **AccessShareLock** | Lightest lock level, acquired by SELECT statements; blocks only ACCESS EXCLUSIVE |
| **fillfactor** | Table storage parameter controlling how full pages are packed; free space enables HOT updates |
| **UNLOGGED** | Tables that skip WAL writes for performance; contents lost on crash |
| **Ring buffer** | Fixed-size circular data structure that automatically overwrites oldest entries |

---

## Part II: Installation & Configuration

## Installation

### Basic Installation

```bash
psql -f install.sql
```

This creates the `flight_recorder` schema with all tables, functions, and views. Collection starts automatically via pg_cron jobs.

### Pre-flight Validation

Run before installation to check prerequisites:

```sql
SELECT * FROM flight_recorder.preflight_check();
```

Returns GO/NO-GO status with actionable recommendations.

### Enabling and Disabling

```sql
-- Start collection (schedules pg_cron jobs)
SELECT flight_recorder.enable();

-- Stop all collection
SELECT flight_recorder.disable();
```

### Uninstallation

```sql
SELECT flight_recorder.disable();
DROP SCHEMA flight_recorder CASCADE;
```

Or use the uninstall script:

```bash
psql -f uninstall.sql
```

## Configuration Profiles

Profiles provide pre-configured settings for common use cases. Start here instead of tuning individual parameters.

### Profile Comparison

| Profile | Interval | Overhead | Collectors | Safety | Retention | Archive |
|---------|----------|----------|------------|--------|-----------|---------|
| `default` | 180s | 0.013% | All | Balanced | 30d/7d | 15min/7d |
| `production_safe` | 300s | 0.008% | Wait/activity | Aggressive | 30d/7d | 30min/14d |
| `development` | 180s | 0.013% | All | Balanced | 7d/3d | 15min/3d |
| `troubleshooting` | 60s | 0.04% | All + top 50 | Lenient | 7d/3d | 5min/7d |
| `minimal_overhead` | 300s | 0.008% | Wait/activity | Very aggressive | 7d/3d | Disabled |
| `high_ddl` | 180s | 0.013% | All | DDL-optimized | 30d/7d | 15min/7d |

### Profile Commands

```sql
-- List available profiles
SELECT * FROM flight_recorder.list_profiles();

-- Preview changes before applying
SELECT * FROM flight_recorder.explain_profile('production_safe')
WHERE will_change = true;

-- Apply a profile
SELECT * FROM flight_recorder.apply_profile('production_safe');

-- Check which profile matches current config
SELECT * FROM flight_recorder.get_current_profile();
```

### Choosing a Profile

**`default`** — General-purpose monitoring. Start here.

**`production_safe`** — Production with strict SLAs. 40% less overhead, aggressive safety thresholds, locks disabled.

**`development`** — Staging/dev environments. Always collects (no adaptive sampling skip), shorter retention.

**`troubleshooting`** — Active incidents. High-frequency collection (60s), lenient safety thresholds. **Temporary use only**—switch back after incident.

**`minimal_overhead`** — Resource-constrained systems, replicas. Minimum footprint, archives disabled.

**`high_ddl`** — Multi-tenant SaaS, frequent schema changes. Pre-checks for DDL locks, fast lock timeout.

### Combining Profiles with Overrides

```sql
-- Apply profile as base
SELECT flight_recorder.apply_profile('production_safe');

-- Override specific settings
UPDATE flight_recorder.config SET value = '450' WHERE key = 'sample_interval_seconds';

-- Verify result
SELECT * FROM flight_recorder.get_current_profile();
```

## Advanced Configuration

All settings stored in `flight_recorder.config`:

```sql
SELECT * FROM flight_recorder.config;
```

### Configuration Reference

| Key | Default | Purpose |
|-----|---------|---------|
| `sample_interval_seconds` | 180 | Ring buffer sample frequency |
| `statements_interval_minutes` | 15 | pg_stat_statements collection interval |
| `statements_top_n` | 20 | Number of top queries to capture |
| `snapshot_based_collection` | true | Use temp table snapshot (reduces catalog locks) |
| `adaptive_sampling` | true | Skip collection when system idle |
| `adaptive_sampling_idle_threshold` | 5 | Skip if < N active connections |
| `circuit_breaker_threshold_ms` | 1000 | Max collection duration before skip |
| `circuit_breaker_enabled` | true | Enable circuit breaker |
| `auto_mode_enabled` | true | Auto-adjust collection mode |
| `auto_mode_connections_threshold` | 60 | % connections to trigger emergency mode |
| `section_timeout_ms` | 250 | Per-section query timeout |
| `lock_timeout_ms` | 100 | Max wait for catalog locks |
| `skip_locks_threshold` | 50 | Skip lock collection if > N blocked |
| `skip_activity_conn_threshold` | 100 | Skip activity if > N active connections |
| `load_shedding_enabled` | true | Skip during high connection load |
| `load_shedding_active_pct` | 70 | Skip if active connections > N% of max |
| `load_throttle_enabled` | true | Skip during I/O/transaction pressure |
| `load_throttle_xact_threshold` | 1000 | Skip if transactions > N/sec |
| `load_throttle_blk_threshold` | 10000 | Skip if block I/O > N/sec |
| `schema_size_warning_mb` | 5000 | Log warning at this schema size |
| `schema_size_critical_mb` | 10000 | Auto-disable at this schema size |

### Retention Settings

Single authoritative reference for all retention periods:

| Setting | Default | Tier | Purpose |
|---------|---------|------|---------|
| `aggregate_retention_days` | 7 | TIER 2 | Aggregated summaries |
| `archive_retention_days` | 7 | TIER 1.5 | Raw sample archives |
| `retention_snapshots_days` | 30 | TIER 3 | System stat snapshots |
| `retention_statements_days` | 30 | TIER 3 | Query snapshots |
| `retention_collection_stats_days` | 30 | Internal | Collection performance stats |

Ring buffer (TIER 1) self-cleans via slot overwrite—no retention setting needed.

### Sample Intervals

Single authoritative reference for sample timing:

| Mode | Interval | Slots | Retention | Collections/Day |
|------|----------|-------|-----------|-----------------|
| `normal` | 180s | 120 | 6 hours | 480 |
| `emergency` | 300s | 120 | 10 hours | 288 |

Formula: `retention = slots × interval` (120 × 180s = 21,600s = 6 hours)

### Archive Settings

| Setting | Default | Purpose |
|---------|---------|---------|
| `archive_samples_enabled` | true | Enable periodic raw sample archival |
| `archive_sample_frequency_minutes` | 15 | How often to archive |
| `archive_activity_samples` | true | Archive activity samples |
| `archive_lock_samples` | true | Archive lock samples |
| `archive_wait_samples` | true | Archive wait event samples |

### Capacity Planning Settings

| Setting | Default | Purpose |
|---------|---------|---------|
| `capacity_planning_enabled` | true | Enable capacity metrics collection |
| `capacity_thresholds_warning_pct` | 60 | Warning threshold |
| `capacity_thresholds_critical_pct` | 80 | Critical threshold |
| `capacity_forecast_window_days` | 90 | Forecast window for projections |
| `collect_database_size` | true | Collect db_size_bytes |
| `collect_connection_metrics` | true | Collect connection metrics |

### Table & Index Tracking Settings

| Setting | Default | Purpose |
|---------|---------|---------|
| `table_stats_enabled` | true | Enable per-table statistics collection |
| `table_stats_top_n` | 50 | Number of hottest tables to track per snapshot |
| `index_stats_enabled` | true | Enable per-index usage tracking |

Table tracking captures seq scans, index scans, tuple activity, bloat indicators, and vacuum/analyze counts for the top N most active tables.

Index tracking captures scan counts, tuple reads/fetches, and index sizes for detecting unused or inefficient indexes.

### Configuration Snapshot Settings

| Setting | Default | Purpose |
|---------|---------|---------|
| `config_snapshots_enabled` | true | Enable PostgreSQL config tracking |

Configuration snapshots capture ~50 relevant PostgreSQL parameters (memory, connections, parallelism, WAL, autovacuum, etc.) to provide context during incident analysis and detect configuration drift.

### Threshold Tuning

Safety thresholds control when collection skips or degrades:

| Threshold | Default | Trigger |
|-----------|---------|---------|
| `circuit_breaker_threshold_ms` | 1000 | Collection took too long |
| `load_shedding_active_pct` | 70 | Active connections exceed % of max |
| `load_throttle_xact_threshold` | 1000 | Transactions/sec exceeded |
| `load_throttle_blk_threshold` | 10000 | Block I/O/sec exceeded |
| `auto_mode_connections_threshold` | 60 | % to trigger emergency mode |

Adjust for your environment:

```sql
-- More conservative (earlier skipping)
UPDATE flight_recorder.config SET value = '50' WHERE key = 'load_shedding_active_pct';

-- More aggressive (later skipping)
UPDATE flight_recorder.config SET value = '85' WHERE key = 'load_shedding_active_pct';
```

---

## Part III: Usage

## Functions Reference

### Analysis Functions

| Function | Purpose |
|----------|---------|
| `compare(start, end)` | Compare system stats between two timestamps |
| `wait_summary(start, end)` | Aggregate wait events over time period |
| `activity_at(timestamp)` | What was happening at a specific moment |
| `anomaly_report(start, end)` | Auto-detect 6 issue types |
| `summary_report(start, end)` | Comprehensive diagnostic report |
| `statement_compare(start, end)` | Compare query performance (requires pg_stat_statements) |
| `capacity_summary(time_window)` | Analyze resource utilization and headroom |
| `table_compare(start, end)` | Compare table activity (seq scans, writes, bloat) |
| `table_hotspots(start, end)` | Detect table issues (seq scan storms, bloat, low HOT ratio) |
| `index_efficiency(start, end)` | Analyze index usage and selectivity |
| `unused_indexes(lookback)` | Find indexes with zero or low usage |
| `config_at(timestamp)` | Show PostgreSQL configuration at a point in time |
| `config_changes(start, end)` | Detect configuration parameter changes |
| `config_health_check()` | Analyze current config for common issues |

### Control Functions

| Function | Purpose |
|----------|---------|
| `enable()` | Start collection (schedules pg_cron jobs) |
| `disable()` | Stop all collection |
| `set_mode('normal'/'emergency')` | Adjust collection intensity |
| `get_mode()` | Show current mode and settings |
| `cleanup(interval)` | Delete old data (default: 7 days) |
| `validate_config()` | Validate configuration settings |

### Health & Monitoring Functions

| Function | Purpose |
|----------|---------|
| `preflight_check()` | Pre-installation validation |
| `quarterly_review()` | 90-day health check |
| `health_check()` | Component status overview |
| `ring_buffer_health()` | Ring buffer XID age, dead tuples, HOT updates |
| `performance_report(interval)` | Flight recorder's own performance |
| `check_alerts(interval)` | Active alerts (if enabled) |
| `config_recommendations()` | Optimization suggestions |
| `export_json(start, end)` | AI-friendly data export |

### Internal Functions (pg_cron scheduled)

| Function | Purpose |
|----------|---------|
| `snapshot()` | Collect system stats snapshot |
| `sample()` | Collect ring buffer sample |
| `flush_ring_to_aggregates()` | Flush ring buffer to durable aggregates |
| `archive_ring_samples()` | Archive raw samples for forensic analysis |
| `cleanup_aggregates()` | Clean old aggregate and archive data |

## Views Reference

| View | Purpose |
|------|---------|
| `recent_waits` | Wait events (10-hour window, covers all modes) |
| `recent_activity` | Active sessions (10-hour window) |
| `recent_locks` | Lock contention (10-hour window) |
| `recent_replication` | Replication lag (2 hours, from snapshots) |
| `deltas` | Snapshot-over-snapshot changes |
| `capacity_dashboard` | Resource utilization and headroom |

**Dynamic retention functions** return data matching current mode's retention:

- `recent_waits_current()`
- `recent_activity_current()`
- `recent_locks_current()`

## Common Workflows

### Daily Monitoring

```sql
-- Quick health check
SELECT * FROM flight_recorder.health_check();

-- Capacity overview
SELECT * FROM flight_recorder.capacity_dashboard;

-- Recent activity summary
SELECT backend_type, wait_event_type, count(*)
FROM flight_recorder.recent_waits
GROUP BY 1, 2
ORDER BY 3 DESC;
```

### Incident Response

**Step 1:** Switch to troubleshooting mode for detailed data:

```sql
SELECT flight_recorder.apply_profile('troubleshooting');
```

**Step 2:** Collect data for 10-15 minutes during the incident.

**Step 3:** Analyze:

```sql
-- What happened in the last hour?
SELECT * FROM flight_recorder.anomaly_report(
    now() - interval '1 hour',
    now()
);

-- Detailed wait analysis
SELECT * FROM flight_recorder.wait_summary(
    now() - interval '1 hour',
    now()
);

-- What was happening at a specific moment?
SELECT * FROM flight_recorder.activity_at('2024-01-15 14:30:00');
```

**Step 4:** Switch back to normal collection:

```sql
SELECT flight_recorder.apply_profile('default');
```

### Performance Analysis

```sql
-- Compare two time periods
SELECT * FROM flight_recorder.compare(
    '2024-01-15 10:00',
    '2024-01-15 11:00'
);

-- Query performance changes (requires pg_stat_statements)
SELECT * FROM flight_recorder.statement_compare(
    '2024-01-15 10:00',
    '2024-01-15 11:00'
)
WHERE mean_exec_time_delta_ms > 100
ORDER BY mean_exec_time_delta_ms DESC;

-- Recent snapshot deltas
SELECT * FROM flight_recorder.deltas
ORDER BY captured_at DESC
LIMIT 10;
```

### Table Hotspot Analysis

Identify which tables are under pressure during an incident:

```sql
-- Compare table activity over a time window
SELECT * FROM flight_recorder.table_compare(
    '2024-01-15 10:00',
    '2024-01-15 11:00'
)
ORDER BY total_activity DESC
LIMIT 10;

-- Auto-detect table issues (seq scan storms, bloat, low HOT ratio)
SELECT * FROM flight_recorder.table_hotspots(
    '2024-01-15 10:00',
    '2024-01-15 11:00'
);
```

### Index Usage Analysis

Find unused or inefficient indexes:

```sql
-- Indexes never used in last 7 days (candidates for removal)
SELECT * FROM flight_recorder.unused_indexes('7 days');

-- Index efficiency during a time window
SELECT * FROM flight_recorder.index_efficiency(
    '2024-01-15 10:00',
    '2024-01-15 11:00'
)
ORDER BY idx_scan_delta DESC;
```

### Configuration Analysis

Understand configuration context during incidents:

```sql
-- What was the configuration at incident time?
SELECT * FROM flight_recorder.config_at('2024-01-15 14:30:00');

-- Detect configuration changes over time
SELECT * FROM flight_recorder.config_changes(
    '2024-01-14 00:00',
    '2024-01-15 00:00'
);

-- Check current configuration for common issues
SELECT * FROM flight_recorder.config_health_check();
```

### Quarterly Review

Run every 90 days:

```sql
SELECT * FROM flight_recorder.quarterly_review();
```

Returns health status with recommendations for each component.

---

## Part IV: Safety & Operations

## Safety Mechanisms

Flight Recorder includes multiple protections to minimize observer effect.

### Observer Effect Overview

**Measured overhead** (PostgreSQL 17.6, typical workload):

| Metric | Value |
|--------|-------|
| Median collection time | 23ms |
| P95 | 31ms |
| P99 | 86ms |
| Sustained CPU at 180s intervals | 0.013% |

Overhead is roughly constant regardless of workload—the question is whether your system has ~25ms of headroom every 3 minutes.

**Validated environments:**

- Development laptops (M-series, Intel)
- Supabase Micro (t4g.nano, 2 core ARM, 1GB RAM): 32ms median, 0.018% CPU

Run `./benchmark/measure_absolute.sh` to measure in your environment.

### Load Protection

**Load Shedding** — Skips collection when connections are high:

| Condition | Action |
|-----------|--------|
| Active connections > 70% of max_connections | Skip collection |
| Configurable via `load_shedding_active_pct` | |

**Load Throttling** — Skips during sustained heavy workload:

| Condition | Action |
|-----------|--------|
| Transactions > 1,000/sec | Skip collection |
| Block I/O > 10,000/sec | Skip collection |
| Uses pg_stat_database rates | |

Both enabled by default. Disable if needed:

```sql
UPDATE flight_recorder.config SET value = 'false' WHERE key = 'load_shedding_enabled';
UPDATE flight_recorder.config SET value = 'false' WHERE key = 'load_throttle_enabled';
```

**pg_stat_statements Protection** — Skips when hash table utilization > 80% to prevent statement evictions.

### Circuit Breaker

Automatic protection when collections run slow:

| Setting | Value |
|---------|-------|
| Threshold | 1000ms (configurable) |
| Window | 15-minute moving average |
| Action | Skip next collection |
| Recovery | Auto-resume when system recovers |

```sql
-- View recent collection performance
SELECT collection_type, started_at, duration_ms, success, skipped
FROM flight_recorder.collection_stats
ORDER BY started_at DESC LIMIT 10;

-- Adjust threshold
UPDATE flight_recorder.config SET value = '2000' WHERE key = 'circuit_breaker_threshold_ms';
```

### Adaptive Mode

Automatically adjusts collection intensity:

| Transition | Trigger |
|------------|---------|
| Normal → Emergency | Connections reach 60% of max, or circuit breaker trips 3× in 10 minutes |
| Emergency → Normal | 10 minutes without trips and load drops below threshold |

Enabled by default. Disable:

```sql
UPDATE flight_recorder.config SET value = 'false' WHERE key = 'auto_mode_enabled';
```

### Storage Management

**Schema size limits:**

| Size | Action |
|------|--------|
| < 5GB | Normal operation |
| 5-8GB | Proactive cleanup (5-day retention) |
| 8-10GB | Warning state |
| > 10GB | Aggressive cleanup (3-day), then disable if still over |
| < 8GB (when disabled) | Auto-re-enable |

The 2GB hysteresis (disable at 10GB, re-enable at 8GB) prevents flapping.

```sql
-- Check current storage status
SELECT * FROM flight_recorder._check_schema_size();

-- Adjust thresholds
UPDATE flight_recorder.config SET value = '6000' WHERE key = 'schema_size_warning_mb';
UPDATE flight_recorder.config SET value = '12000' WHERE key = 'schema_size_critical_mb';
```

### Job Health Monitoring

Detects when pg_cron jobs are deleted, disabled, or broken.

**Required jobs:**

- `flight_recorder_sample` — Ring buffer sampling
- `flight_recorder_snapshot` — System stats
- `flight_recorder_flush` — Ring buffer → aggregates
- `flight_recorder_cleanup` — Old data removal

```sql
-- Check job health
SELECT * FROM flight_recorder.health_check()
WHERE component = 'pg_cron Jobs';

-- View jobs directly
SELECT jobid, jobname, schedule, active
FROM cron.job
WHERE jobname LIKE 'flight_recorder%';

-- Recreate missing jobs
SELECT flight_recorder.enable();
```

### Job Deduplication

Prevents job queue buildup during slow collections or outages. Each collection checks for already-running jobs and skips if found.

```sql
-- View deduplication skips
SELECT started_at, collection_type, skipped_reason
FROM flight_recorder.collection_stats
WHERE skipped = true AND skipped_reason LIKE '%Job deduplication%'
ORDER BY started_at DESC;
```

### Graceful Degradation

Each collection section is wrapped in exception handlers:

- Wait events fail → activity samples still collected
- Lock detection fails → progress tracking continues
- Partial data is better than no data during incidents

## Catalog Lock Behavior

Flight Recorder acquires AccessShareLock on system catalogs during collection. This is the lightest lock level and generally harmless.

### System Views Accessed

| View | Acquired By | Frequency |
|------|-------------|-----------|
| `pg_stat_activity` | sample(), snapshot() | Sample: adaptive, Snapshot: 5min |
| `pg_stat_replication` | snapshot() | Every 5min |
| `pg_locks` | sample() | Adaptive (if locks enabled) |
| `pg_stat_statements` | snapshot() | Every 15min (if enabled) |

### Lock Timeout Behavior

Default `lock_timeout` = 100ms:

- If catalog locked by DDL > 100ms: collection fails with timeout error
- If collection starts before DDL: DDL waits up to 100ms behind flight recorder
- Circuit breaker trips after 3 lock timeout failures in 15 minutes

### Catalog Lock Minimization

**Snapshot-based collection** (enabled by default): Creates temp table copy of `pg_stat_activity` once per sample. All sections query the temp table instead of hitting the catalog 3 times. Reduces catalog locks from 3 to 1 per sample (67% reduction).

**OID storage**: Stores relation OIDs instead of names in lock_samples_ring, avoiding pg_class joins during collection. Name resolution happens at query time.

### High-DDL Environments

Validated: DDL blocking is negligible with snapshot-based collection.

**Test results** (Supabase Micro, 202 DDL operations):

- 0% blocking rate
- 14 operations concurrent with collection—no delays

If you experience DDL issues (frequent `lock_timeout` errors, circuit breaker trips):

```sql
-- Reduce lock timeout
UPDATE flight_recorder.config SET value = '50' WHERE key = 'lock_timeout_ms';

-- Use emergency mode during high-DDL periods
SELECT flight_recorder.set_mode('emergency');

-- Or apply high_ddl profile
SELECT flight_recorder.apply_profile('high_ddl');
```

---

## Part V: Capacity Planning

## Capacity Planning

Flight Recorder tracks resource utilization to answer: "Do I have the right amount of resources?"

### Overview

Capacity planning uses data already collected (every 5 minutes) with no additional overhead. It provides:

- Resource utilization across 6 dimensions
- Headroom assessment for right-sizing
- Trend analysis for growth patterns
- Traffic light status (healthy/warning/critical)
- Actionable recommendations

### Metrics Tracked

| Metric | Source | Purpose |
|--------|--------|---------|
| `xact_commit` | pg_stat_database | Committed transactions |
| `xact_rollback` | pg_stat_database | Rolled back transactions |
| `blks_read` | pg_stat_database | Blocks read from disk |
| `blks_hit` | pg_stat_database | Blocks found in cache |
| `connections_active` | pg_stat_activity | Non-idle connections |
| `connections_total` | pg_stat_activity | All connections |
| `connections_max` | max_connections | Configured limit |
| `db_size_bytes` | pg_class.relpages | Database size (statistical estimate) |

Storage overhead: ~40 bytes per snapshot = 11.5 KB/day.

### Using capacity_summary()

Analyzes resource utilization over a time window:

```sql
-- Last 24 hours (default)
SELECT * FROM flight_recorder.capacity_summary();

-- Last 7 days
SELECT * FROM flight_recorder.capacity_summary(interval '7 days');

-- Last hour
SELECT * FROM flight_recorder.capacity_summary(interval '1 hour');
```

**Output columns:**

| Column | Purpose |
|--------|---------|
| `metric` | Resource dimension |
| `current_usage` | Human-readable current usage |
| `provisioned_capacity` | Configured capacity |
| `utilization_pct` | Percentage of capacity used |
| `headroom_pct` | Available capacity remaining |
| `status` | healthy / warning / critical / insufficient_data |
| `recommendation` | Actionable advice |

**Metrics analyzed:**

| Metric | Warning | Critical | Issue |
|--------|---------|----------|-------|
| `connections` | ≥60% | ≥80% | Connection exhaustion |
| `memory_shared_buffers` | >1k writes | >10k writes | Backends bypassing bgwriter |
| `memory_work_mem` | >100 MB spilled | >1 GB spilled | Queries exceeding work_mem |
| `io_buffer_cache` | <95% hit | <90% hit | Working set exceeds shared_buffers |
| `storage_growth` | Growth detected | — | Informational |
| `transaction_rate` | Trend-based | — | Throughput changes |

### Using capacity_dashboard

At-a-glance view for monitoring:

```sql
SELECT * FROM flight_recorder.capacity_dashboard;
```

| Column | Purpose |
|--------|---------|
| `last_updated` | Most recent snapshot |
| `overall_status` | Worst status across all dimensions |
| `critical_issues` | Array of warnings |
| `connections_status` | Connection status |
| `connections_utilization_pct` | Connection % |
| `memory_status` | Overall memory status |
| `memory_pressure_score` | 0-100 composite score |
| `io_status` | I/O and cache status |
| `storage_growth_mb_per_day` | Growth rate |

Memory pressure score: `(shared_buffers_utilization × 0.6) + (work_mem_utilization × 0.4)`

### Interpreting Results

**Status progression:**

```
healthy (< 60%) → warning (60-80%) → critical (> 80%)
```

**When to act:**

- **healthy** — Monitor periodically
- **warning** — Plan capacity increases within 30-60 days
- **critical** — Immediate action needed

**Common patterns:**

| Pattern | Likely Cause | Action |
|---------|--------------|--------|
| High connection + low memory | Many idle connections | Connection pooling |
| Low connection + high memory | Memory-intensive queries | Optimize queries, increase work_mem |
| High I/O + low cache hit | Working set > shared_buffers | Increase shared_buffers |
| Steady storage growth | Normal growth | Plan storage expansion |
| Sudden growth + rising TPS | Feature launch, spike | Review retention policies |

### Practical Examples

**Sizing a new production instance:**

```sql
-- Run in staging with similar load for 7 days
SELECT metric, utilization_pct, status, recommendation
FROM flight_recorder.capacity_summary(interval '7 days')
ORDER BY utilization_pct DESC NULLS LAST;

-- Provision production with 2× headroom for critical resources
```

**Storage growth forecast:**

```sql
SELECT metric, current_usage, recommendation
FROM flight_recorder.capacity_summary(interval '30 days')
WHERE metric = 'storage_growth';
```

**Pre-incident detection:**

```sql
-- Monitor daily
SELECT overall_status, critical_issues, memory_pressure_score
FROM flight_recorder.capacity_dashboard;
```

**Post-incident right-sizing:**

```sql
-- Analyze peak usage during incident
SELECT * FROM flight_recorder.capacity_summary(interval '2 hours')
WHERE status IN ('warning', 'critical');
```

**Cost optimization (find over-provisioned resources):**

```sql
SELECT metric, utilization_pct, headroom_pct
FROM flight_recorder.capacity_summary(interval '30 days')
WHERE utilization_pct < 30
ORDER BY utilization_pct;
```

---

## Part VI: Reference Material

## Troubleshooting Guide

### Common Issues

**"insufficient_data" status in capacity_summary:**

Cause: < 2 snapshots in time window.

```sql
-- Wait 10 minutes, or use longer window
SELECT * FROM flight_recorder.capacity_summary(interval '24 hours');

-- Verify snapshots are being collected
SELECT count(*) FROM flight_recorder.snapshots
WHERE captured_at > now() - interval '1 hour';
-- Should return 12+ (collected every 5 minutes)

-- Check collection status
SELECT * FROM flight_recorder.health_check()
WHERE component = 'Collection Status';
```

**NULL values in capacity columns:**

Cause: Historical snapshots before capacity planning was enabled.

Impact: Gracefully handled. May show "insufficient_data" for older time windows.

Solution: Wait for new data (7+ days for meaningful trends).

**Capacity metrics not collected:**

```sql
-- Verify enabled
SELECT * FROM flight_recorder.config WHERE key = 'capacity_planning_enabled';

-- Check for errors
SELECT * FROM flight_recorder.collection_stats
WHERE success = false AND started_at > now() - interval '1 hour'
ORDER BY started_at DESC;

-- Test manually
SELECT flight_recorder.snapshot();

-- Verify columns populated
SELECT xact_commit, connections_active, db_size_bytes
FROM flight_recorder.snapshots
ORDER BY captured_at DESC LIMIT 1;
```

**Collection timing out:**

```sql
-- Check recent performance
SELECT collection_type, avg(duration_ms), max(duration_ms)
FROM flight_recorder.collection_stats
WHERE started_at > now() - interval '1 hour'
GROUP BY collection_type;

-- If consistently slow, switch to emergency mode
SELECT flight_recorder.set_mode('emergency');

-- Or increase timeout
UPDATE flight_recorder.config SET value = '2000' WHERE key = 'circuit_breaker_threshold_ms';
```

**Lock timeout errors:**

```sql
-- Check lock failures
SELECT collection_type, count(*) as failures, max(started_at)
FROM flight_recorder.collection_stats
WHERE error_message LIKE '%lock_timeout%'
  AND started_at > now() - interval '1 hour'
GROUP BY collection_type;

-- Reduce lock timeout
UPDATE flight_recorder.config SET value = '50' WHERE key = 'lock_timeout_ms';
```

**Load shedding skipping too often:**

```sql
-- Check skip reasons
SELECT skipped_reason, count(*)
FROM flight_recorder.collection_stats
WHERE skipped = true AND started_at > now() - interval '24 hours'
GROUP BY skipped_reason
ORDER BY count(*) DESC;

-- Adjust threshold
UPDATE flight_recorder.config SET value = '80' WHERE key = 'load_shedding_active_pct';
```

### Diagnostic Patterns

**Lock contention (batch slower than expected):**

```sql
SELECT * FROM flight_recorder.recent_locks
WHERE captured_at BETWEEN '...' AND '...';

SELECT * FROM flight_recorder.wait_summary('...', '...')
WHERE wait_event_type = 'Lock';
```

**Buffer pressure (backends writing directly to disk):**

```sql
SELECT * FROM flight_recorder.compare('...', '...');
-- Look for: bgw_buffers_backend_delta > 0
```

**Checkpoint issues (I/O spikes, slow commits):**

```sql
SELECT * FROM flight_recorder.compare('...', '...');
-- Look for: checkpoint_occurred = true, high ckpt_write_time_ms
```

**work_mem exhaustion (slow sorts/joins):**

```sql
SELECT * FROM flight_recorder.compare('...', '...');
-- Look for: temp_files_delta > 0, large temp_bytes_delta
```

### Error Messages

| Error | Cause | Resolution |
|-------|-------|------------|
| `lock_timeout` | Catalog locked by DDL | Reduce lock_timeout_ms or wait |
| `statement_timeout` | Collection taking too long | Increase circuit_breaker_threshold_ms |
| `Job deduplication` | Previous job still running | Normal during slow periods |
| `Load shedding` | High connection count | Normal, adjust threshold if needed |
| `Schema size critical` | > 10GB stored | Reduce retention, check cleanup job |

## Anomaly Detection Reference

`anomaly_report()` auto-detects these issue types:

| Type | Meaning | Investigation |
|------|---------|---------------|
| `CHECKPOINT_DURING_WINDOW` | Checkpoint occurred (I/O spike) | Check checkpoint_completion_target |
| `FORCED_CHECKPOINT` | WAL exceeded max_wal_size | Increase max_wal_size or checkpoint frequency |
| `BUFFER_PRESSURE` | Backends writing directly to disk | Increase shared_buffers |
| `BACKEND_FSYNC` | Backends doing fsync (bgwriter overwhelmed) | Tune bgwriter settings |
| `TEMP_FILE_SPILLS` | Queries spilling to disk | Increase work_mem or optimize queries |
| `LOCK_CONTENTION` | Sessions blocked on locks | Review blocking queries, add indexes |

```sql
SELECT * FROM flight_recorder.anomaly_report(
    '2024-01-15 10:00',
    '2024-01-15 11:00'
);
```

## Testing and Benchmarking

### Running Tests

Run tests locally with Docker (supports PostgreSQL 15, 16, 17):

```bash
# Test on PostgreSQL 16 (default)
./test.sh

# Test on specific version
./test.sh 15
./test.sh 17

# Test all versions sequentially
./test.sh all

# Test all versions in parallel (fastest)
./test.sh parallel
```

Tests are split into 6 files in `tests/` for per-file timing with `pg_prove --timer`:

| File | Tests | Coverage |
|------|-------|----------|
| `01_foundation.sql` | 54 | Installation, functions, core |
| `02_ring_buffer_analysis.sql` | 25 | Ring buffer, analysis, config |
| `03_safety_features.sql` | 70 | Kill switch, P0-P4 safety |
| `04_boundary_critical.sql` | 104 | Boundary tests, critical functions |
| `05_error_version.sql` | 100 | Error handling, PG15/16/17 specifics |
| `06_load_archive_capacity.sql` | 89 | Load shedding, archive, capacity |

Or against your own PostgreSQL instance:

```bash
psql -f install.sql
psql -c "CREATE EXTENSION pgtap;"
pg_prove --timer -U postgres -d postgres tests/*.sql
```

VACUUM warnings during tests are expected (tests run in transactions).

### Benchmarking Approach

Flight Recorder's overhead is roughly constant regardless of workload. The cost of `SELECT * FROM pg_stat_activity` doesn't scale with your TPS—it's an absolute cost.

**The right question:** Does your system have ~25ms of headroom every 180 seconds?

**Measure absolute costs:**

```bash
cd benchmark
./measure_absolute.sh 100  # 100 iterations
```

This measures:

- CPU time per collection (mean, p50, p95, p99)
- I/O operations per collection
- Sustained CPU % at different intervals

**Reference measurements:**

| Environment | Median | P95 | Sustained CPU |
|-------------|--------|-----|---------------|
| MacBook Pro (M-series) | 23ms | 31ms | 0.013% |
| Supabase Micro (t4g.nano) | 32ms | 46ms | 0.018% |

### Measuring Overhead

**Decision framework:**

| Collection Time | Recommendation |
|-----------------|----------------|
| < 100ms | Safe everywhere |
| 100-200ms | Safe on 2+ vCPU, test on 1 vCPU |
| > 200ms | Investigate (database size? config?) |

**Tiny systems (1 vCPU, < 2GB RAM):** Test in staging for 24 hours before production.

**Always-on production:** Start with `production_safe` profile, monitor, upgrade if comfortable.

**Measure in your environment:**

```sql
-- Check recent collection performance
SELECT * FROM flight_recorder.performance_report('1 day');

-- View collection stats
SELECT collection_type,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY duration_ms) as p50,
       percentile_cont(0.95) WITHIN GROUP (ORDER BY duration_ms) as p95
FROM flight_recorder.collection_stats
WHERE started_at > now() - interval '1 day'
GROUP BY collection_type;
```

## Project Structure

```
pg-flight-recorder/
├── install.sql                  # Installation script
├── uninstall.sql                # Uninstall script
├── flight_recorder_test.sql     # Original monolithic test file (kept as backup)
├── tests/                       # Split test files for per-file timing
│   ├── 01_foundation.sql
│   ├── 02_ring_buffer_analysis.sql
│   ├── 03_safety_features.sql
│   ├── 04_boundary_critical.sql
│   ├── 05_error_version.sql
│   └── 06_load_archive_capacity.sql
├── docker-compose.yml           # PostgreSQL + pg_cron for testing
├── docker-compose.parallel.yml  # Parallel testing across PG 15, 16, 17
├── test.sh                      # Test runner (supports parallel mode)
├── README.md                    # Quick start guide
├── REFERENCE.md                 # This file
├── FEATURE_DESIGNS.md           # Technical designs for new features
└── benchmark/
    ├── measure_absolute.sh      # Overhead measurement
    └── measure_ddl_impact.sh    # DDL interaction testing
```
