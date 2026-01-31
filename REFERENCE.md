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
- [Performance Forecasting](#performance-forecasting)

**Part VI: Reference Material**

- [Troubleshooting Guide](#troubleshooting-guide)
- [Anomaly Detection Reference](#anomaly-detection-reference)
- [Canary Queries](#canary-queries)
- [Query Storm Detection](#query-storm-detection)
- [Performance Regression Detection](#performance-regression-detection)
- [Visual Timeline](#visual-timeline)
- [Testing and Benchmarking](#testing-and-benchmarking)
- [Code Browser](#code-browser)

---

## Part I: Fundamentals

## Overview

### Purpose and Scope

pg-flight-recorder continuously samples PostgreSQL system state using two independent collection systems: sampled activity (wait events, sessions, locks) and cumulative snapshots (WAL, checkpoints, I/O). Use it to diagnose performance issues, track capacity trends, and understand database behavior over time.

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

### Data Architecture

Flight Recorder uses two independent collection systems that answer different questions:

- **Sampled Activity**: "What's happening right now?" (wait events, sessions, locks)
- **Snapshots**: "What is/was the system state?" (counters, config, query stats)

```
┌─────────────────────────────────────────────────────────────────────────┐
│ SAMPLED ACTIVITY SYSTEM                                                 │
│   Collects: wait events, active sessions, locks                         │
│   Job: sample() every 3 minutes                                         │
│                                                                         │
│   ┌─────────────────────────────────────────────────────────────────┐   │
│   │ Ring Buffers (hot storage)                                      │   │
│   │   UNLOGGED, 6-10 hour retention, auto-overwrites                │   │
│   └──────────────────────┬──────────────────────────────────────────┘   │
│                          │                                              │
│            ┌─────────────┴─────────────┐                                │
│            ▼                           ▼                                │
│   ┌─────────────────────┐     ┌─────────────────────┐                   │
│   │ Raw Archives        │     │ Aggregates          │                   │
│   │   Detail preserved  │     │   Summarized        │                   │
│   │   7-day retention   │     │   7-day retention   │                   │
│   │   Forensic analysis │     │   Pattern analysis  │                   │
│   └─────────────────────┘     └─────────────────────┘                   │
├─────────────────────────────────────────────────────────────────────────┤
│ SNAPSHOT SYSTEM                                                         │
│   Job: snapshot() every 5 minutes, 30-day retention                     │
│                                                                         │
│   ┌───────────────────┐ ┌───────────────────┐ ┌───────────────────┐     │
│   │ Cumulative        │ │ Point-in-time     │ │ Top-N Rankings    │     │
│   │ Counters          │ │ State             │ │                   │     │
│   │                   │ │                   │ │                   │     │
│   │ WAL, checkpoints, │ │ Config params,    │ │ Top queries by    │     │
│   │ I/O, table/index  │ │ replication lag   │ │ time, calls       │     │
│   │ stats             │ │                   │ │                   │     │
│   └───────────────────┘ └───────────────────┘ └───────────────────┘     │
└─────────────────────────────────────────────────────────────────────────┘
```

---

### Sampled Activity System

Samples current database state every 3 minutes, capturing three categories of observational data: wait events, active sessions, and locks. Data lands in ring buffers (hot storage), then flows to two parallel outputs for longer-term retention.

**Ring Buffers (hot storage)**

Low-overhead, high-frequency sampling using a configurable circular buffer (default 120 slots, range 72-2880).

| Table | Default Rows | Purpose |
|-------|--------------|---------|
| `samples_ring` | slots | Master slot tracker |
| `wait_samples_ring` | slots × 100 | Wait events |
| `activity_samples_ring` | slots × 25 | Active sessions |
| `lock_samples_ring` | slots × 100 | Lock contention |

Characteristics:

- **UNLOGGED tables** eliminate WAL overhead (data lost on crash—acceptable for telemetry)
- **Pre-populated rows** with UPDATE-only pattern achieve high HOT update ratios (no index bloat)
- **Autovacuum configurable** via `configure_ring_autovacuum()`; can be disabled since fixed-size tables have bounded bloat
- **Modular arithmetic** (`epoch / interval % ring_buffer_slots`) auto-overwrites old slots
- **Configurable size** via `ring_buffer_slots` config and `rebuild_ring_buffers()` function
- Query via: `recent_waits`, `recent_activity`, `recent_locks` views

**Raw Archives (cold storage, detail preserved)**

Periodic preservation of ring buffer samples for forensic analysis. Captured every 15 minutes.

| Table | Purpose |
|-------|---------|
| `wait_samples_archive` | Wait patterns with counts |
| `activity_samples_archive` | PIDs, queries, session details, session/transaction age |
| `lock_samples_archive` | Complete blocking chains |

Preserves details that aggregates lose: specific PIDs, exact timestamps, complete blocking chains. Query directly for forensic analysis ("what exactly was happening at 14:32:17?").

**Session and Transaction Age Tracking**

Activity samples include `backend_start` and `xact_start` timestamps from `pg_stat_activity`, enabling:

- **Session age** (`session_age`): How long a connection has been open
- **Transaction age** (`xact_age`): How long the current transaction has been running

Use cases:

- Identify long-running transactions causing bloat or replication lag
- Detect connection leaks (very old sessions)
- Find idle-in-transaction sessions holding locks

**Aggregates (cold storage, summarized)**

Durable summaries of ring buffer data. Flushed every 5 minutes.

| Table | Purpose |
|-------|---------|
| `wait_event_aggregates` | Wait patterns over 5-minute windows |
| `activity_aggregates` | Activity/query execution patterns |
| `lock_aggregates` | Lock contention patterns |

Compresses data for pattern analysis ("Lock:transactionid wait occurred 47 times this hour"). Query via `wait_summary(start, end)` function.

**Data flow:**

| Category | Ring Buffer | → Archive | → Aggregate |
|----------|-------------|-----------|-------------|
| Wait events | `wait_samples_ring` | `wait_samples_archive` | `wait_event_aggregates` |
| Activity | `activity_samples_ring` | `activity_samples_archive` | `activity_aggregates` |
| Locks | `lock_samples_ring` | `lock_samples_archive` | `lock_aggregates` |

Note: Archives and aggregates both derive directly from ring buffers—they are parallel outputs, not sequential stages.

---

### Snapshot System

Captures periodic snapshots of system state every 5 minutes. Unlike sampled activity (which captures "what's happening now"), snapshots capture "what is/was the system state." The snapshot system collects three types of data:

**Cumulative Counters** — Monotonically increasing values from `pg_stat_*` views. Compare two snapshots to compute deltas (e.g., "500 checkpoints occurred between 10:00 and 11:00").

| Table | Purpose |
|-------|---------|
| `snapshots` | pg_stat_bgwriter, pg_stat_database, WAL, temp files, I/O, XID age, archiver status |
| `table_snapshots` | Per-table activity (seq scans, index scans, writes, bloat, XID age) |
| `index_snapshots` | Per-index usage and size |

Query via: `compare(start, end)`, `deltas` view, `table_compare(start, end)`, `index_efficiency(start, end)`.

**Point-in-time State** — Non-cumulative values that represent current state at capture time.

| Table | Purpose |
|-------|---------|
| `config_snapshots` | PostgreSQL configuration parameters |
| `replication_snapshots` | pg_stat_replication, replication slots, lag |
| `vacuum_progress_snapshots` | pg_stat_progress_vacuum for long-running vacuums |

Query via: `config_at(timestamp)`, `config_changes(start, end)`.

**Vacuum Progress Monitoring**

Captures vacuum progress from `pg_stat_progress_vacuum` at each snapshot, tracking:

- Vacuum phase (scanning heap, vacuuming indexes, etc.)
- Blocks scanned and vacuumed (with percentage calculations)
- Dead tuple counts
- Index vacuum iterations

Query via: `recent_vacuum_progress` view. Useful for monitoring long-running vacuums during incidents.

**WAL Archiver Status**

When `archive_mode` is enabled, snapshots capture archiver metrics from `pg_stat_archiver`:

| Column | Purpose |
|--------|---------|
| `archived_count` | Total WAL files archived |
| `last_archived_wal` | Name of last archived WAL |
| `last_archived_time` | When last archive completed |
| `failed_count` | Total archive failures |
| `last_failed_wal` | Name of last failed WAL |
| `last_failed_time` | When last failure occurred |
| `archiver_stats_reset` | When archiver stats were reset |

Query via: `archiver_status` view for delta calculations between snapshots. Null when `archive_mode = off`.

**Database Conflicts (Standby Servers)**

On standby servers, snapshots capture query cancellation metrics from `pg_stat_database_conflicts`:

| Column | Purpose |
|--------|---------|
| `confl_tablespace` | Queries cancelled due to dropped tablespaces |
| `confl_lock` | Queries cancelled due to lock timeouts |
| `confl_snapshot` | Queries cancelled due to old snapshots |
| `confl_bufferpin` | Queries cancelled due to pinned buffers |
| `confl_deadlock` | Queries cancelled due to deadlocks |
| `confl_active_logicalslot` | Queries cancelled due to logical replication slots (PG16+) |

These columns are NULL on primary servers (conflicts only occur on standbys during recovery). Use to diagnose replica query cancellations.

**Top-N Rankings** — Periodic capture of top queries by various metrics.

| Table | Purpose |
|-------|---------|
| `statement_snapshots` | pg_stat_statements top queries by time, calls |

Query via: `statement_compare(start, end)`.

### Ring Buffer Mechanism

The ring buffer uses modular arithmetic for slot rotation:

```
slot_id = (epoch_seconds / sample_interval_seconds) % ring_buffer_slots
```

The slot count is configurable (default 120, range 72-2880):

```
retention_hours = (ring_buffer_slots × sample_interval_seconds) / 3600
```

Example configurations:

| Slots | Interval | Retention | Memory |
|-------|----------|-----------|--------|
| 120 | 180s | 6 hours | ~15 MB |
| 360 | 60s | 6 hours | ~45 MB |
| 720 | 30s | 6 hours | ~90 MB |

**Why UNLOGGED tables?** Eliminates WAL overhead. Telemetry data lost on crash is acceptable—the system recovers and resumes collection.

**Why UPDATE-only pattern?** Child tables use `UPDATE ... SET col = NULL` to clear slots, then `INSERT ... ON CONFLICT DO UPDATE`. With proper fillfactor, most updates are HOT (Heap-Only Tuple) - they create new tuple versions on the same page without updating indexes. HOT updates still create tuple chains within pages, but these are collapsed by page pruning during subsequent UPSERTs or by autovacuum.

**Storage optimization:**

- Master table: `fillfactor=70` (30% free space for HOT)
- Child tables: `fillfactor=90` (10% free space for HOT)

**Autovacuum configuration:**

Ring buffer tables use PostgreSQL's default autovacuum behavior. Since they're fixed-size, pre-allocated, and UNLOGGED, bloat is bounded regardless of autovacuum settings. You can disable autovacuum to minimize observer effect if desired:

```sql
SELECT flight_recorder.configure_ring_autovacuum(false); -- Disable autovacuum
SELECT flight_recorder.configure_ring_autovacuum(true);  -- Re-enable (default)
```

### Ring Buffer Optimization

Ring buffer size can be configured for different monitoring scenarios. Use optimization profiles for common configurations:

```sql
-- View available optimization profiles
SELECT * FROM flight_recorder.get_optimization_profiles();

-- Apply a profile (updates config, warns if rebuild needed)
SELECT * FROM flight_recorder.apply_optimization_profile('fine_grained');

-- Rebuild ring buffers to new size (clears ring buffer data)
SELECT flight_recorder.rebuild_ring_buffers();

-- Validate current ring buffer configuration
SELECT * FROM flight_recorder.validate_ring_configuration();
```

**Optimization Profiles:**

| Profile | Slots | Interval | Retention | Use Case |
|---------|-------|----------|-----------|----------|
| `standard` | 120 | 180s | 6h | Default, balanced |
| `fine_grained` | 360 | 60s | 6h | 1-min granularity |
| `ultra_fine` | 720 | 30s | 6h | 30-sec granularity |
| `low_overhead` | 72 | 300s | 6h | Minimal footprint |
| `high_retention` | 240 | 180s | 12h | Extended retention |
| `forensic` | 1440 | 15s | 6h | Temporary debugging |

**Ring Buffer Configuration:**

| Setting | Default | Range | Purpose |
|---------|---------|-------|---------|
| `ring_buffer_slots` | 120 | 72-2880 | Number of ring buffer slots |

**Validation Checks:**

The `validate_ring_configuration()` function checks:

- **ring_buffer_retention**: WARN if <4h, ERROR if <2h
- **batching_efficiency**: WARN if <3:1 or >15:1 ratio
- **cpu_overhead**: WARN if >0.1%
- **memory_usage**: WARN if >200MB

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

### Upgrading

To upgrade an existing installation while preserving telemetry data:

**Step 1: Check current version**

```sql
SELECT value FROM flight_recorder.config WHERE key = 'schema_version';
```

**Step 2: Run migrations**

```bash
psql -f migrations/upgrade.sql
```

This automatically detects your version and applies needed migrations.

**Step 3: Reinstall functions**

```bash
psql -f install.sql
```

This is safe — it preserves all data and updates functions/views to the latest version.

### Version History

| Version | Changes |
|---------|---------|
| 2.13 | Visual Timeline: `_sparkline()`, `_bar()`, `timeline()`, `sparkline_metrics()` functions for ASCII-based metric visualization |
| 2.12 | Performance regression detection: `query_regressions` table, `detect_regressions()`, `regression_status()`, `regression_dashboard` view |
| 2.11 | Query storm severity levels and correlation data, anti-flapping protection for auto-resolution |
| 2.10 | Query storm detection: `query_storms` table, `detect_query_storms()`, `storm_status()`, `storm_dashboard` view |
| 2.9 | Canary queries: `canaries` and `canary_results` tables, `run_canaries()`, `canary_status()` functions |
| 2.8 | OID exhaustion detection: `max_catalog_oid` and `large_object_count` columns, `OID_EXHAUSTION_RISK` anomaly type, rate calculation functions (`oid_consumption_rate`, `time_to_oid_exhaustion`) |
| 2.7 | Autovacuum observer enhancements: `n_mod_since_analyze` column, configurable sampling modes (`top_n`/`all`/`threshold`), rate calculation functions (`dead_tuple_growth_rate`, `modification_rate`, `hot_update_ratio`, `time_to_budget_exhaustion`) |
| 2.6 | New anomaly detections (idle-in-transaction, dead tuple accumulation, vacuum starvation, connection leak, replication lag velocity), database conflict columns (`pg_stat_database_conflicts`), `recent_idle_in_transaction` view |
| 2.5 | Activity session/transaction age (`backend_start`, `xact_start`), vacuum progress monitoring (`pg_stat_progress_vacuum`), WAL archiver status (`pg_stat_archiver`) |
| 2.4 | Client IP address tracking (`client_addr` in activity sampling) |
| 2.3 | XID wraparound metrics (`datfrozenxid_age`, `relfrozenxid_age`) |
| 2.2 | Configurable ring buffer slots (72-2880 range) |
| 2.1 | I/O read timing columns from pg_stat_io |
| 2.0 | Initial versioned release |

See `migrations/README.md` for detailed migration documentation.

## Configuration Profiles

Profiles provide pre-configured settings for common use cases. Each profile sets **77 parameters** with values tuned for its specific use case. Start here instead of tuning individual parameters.

### Profile Comparison

| Profile | Interval | Overhead | Collectors | Safety | Retention | Archive |
|---------|----------|----------|------------|--------|-----------|---------|
| `default` | 180s | 0.013% | All | Balanced | 30d/7d | 15min/7d |
| `production_safe` | 300s | 0.008% | Wait/activity | Aggressive | 30d/7d | 30min/14d |
| `development` | 180s | 0.013% | All | Balanced | 7d/3d | 15min/3d |
| `troubleshooting` | 60s | 0.04% | All + top 100 | Lenient | 7d/7d | 5min/7d |
| `minimal_overhead` | 300s | 0.008% | Wait/activity | Very aggressive | 7d/3d | Disabled |
| `high_ddl` | 180s | 0.013% | All | DDL-optimized | 30d/7d | 15min/7d |

### Key Profile Settings

#### Timeouts & Memory

| Profile | section_timeout | statement_timeout | work_mem |
|---------|-----------------|-------------------|----------|
| `default` | 250ms | 1000ms | 2MB |
| `production_safe` | 200ms | 800ms | 1MB |
| `development` | 250ms | 1000ms | 2MB |
| `troubleshooting` | 500ms | 2000ms | 4MB |
| `minimal_overhead` | 100ms | 500ms | 1MB |
| `high_ddl` | 200ms | 800ms | 2MB |

#### Load Thresholds

| Profile | skip_locks | skip_activity | throttle_xact | throttle_blk |
|---------|------------|---------------|---------------|--------------|
| `default` | 50 | 100 | 1000/s | 10000/s |
| `production_safe` | 30 | 50 | 500/s | 5000/s |
| `development` | 50 | 100 | 1000/s | 10000/s |
| `troubleshooting` | 100 | 200 | 2000/s | 20000/s |
| `minimal_overhead` | 20 | 30 | 300/s | 3000/s |
| `high_ddl` | 30 | 100 | 1000/s | 10000/s |

#### Statement Collection

| Profile | interval | min_calls | top_n |
|---------|----------|-----------|-------|
| `default` | 15min | 1 | 20 |
| `production_safe` | 30min | 5 | 20 |
| `development` | 15min | 1 | 20 |
| `troubleshooting` | 5min | 1 | 50 |
| `minimal_overhead` | 30min | 10 | 20 |
| `high_ddl` | 15min | 1 | 20 |

#### Advanced Features

| Profile | Canary | Storm | Regression | Forecast | Jitter | Auto-mode |
|---------|--------|-------|------------|----------|--------|-----------|
| `default` | off | off | off | on | on | 60% |
| `production_safe` | off | off | off | on | on | 50% |
| `development` | **on** | **on** | **on** | on+alerts | on | 60% |
| `troubleshooting` | **on** | **on** | **on** | on+alerts | off | disabled |
| `minimal_overhead` | off | off | off | off | off | 40% |
| `high_ddl` | off | off | off | on | on | 60% |

#### Troubleshooting-Specific Tuning

The `troubleshooting` profile includes more sensitive detection settings:

| Setting | Default | Troubleshooting |
|---------|---------|-----------------|
| `storm_threshold_multiplier` | 3.0x | 2.0x |
| `storm_baseline_days` | 7 | 3 |
| `storm_lookback_interval` | 1 hour | 30 minutes |
| `storm_min_duration_minutes` | 5 | 2 |
| `regression_threshold_pct` | 50% | 25% |
| `regression_baseline_days` | 7 | 3 |
| `regression_lookback_interval` | 1 hour | 30 minutes |
| `regression_min_duration_minutes` | 30 | 10 |
| `forecast_lookback_days` | 7 | 3 |
| `forecast_window_days` | 7 | 3 |

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

**`default`** — General-purpose monitoring. Balanced settings for most workloads. Start here.

**`production_safe`** — Production with strict SLAs. 40% less overhead through aggressive skipping, faster timeouts, lower memory usage. Locks and progress tracking disabled. Emergency mode triggers earlier (50% connections).

**`development`** — Staging/dev environments. Always collects (no adaptive sampling skip), shorter retention. **Enables canary queries, storm detection, regression detection, and forecast alerts** for full feature testing.

**`troubleshooting`** — Active incidents. High-frequency collection (60s), lenient safety thresholds, more memory for complex queries. Disables jitter for consistent timing and auto-mode to prevent emergency throttling. **More sensitive detection thresholds** for storms and regressions. **Temporary use only**—switch back after incident.

**`minimal_overhead`** — Resource-constrained systems, replicas. Minimum footprint with very aggressive skipping, fastest timeouts, archives disabled, forecasting disabled. Emergency mode triggers very early (40% connections).

**`high_ddl`** — Multi-tenant SaaS, frequent schema changes. Pre-checks for DDL locks, fast lock timeout, aggressive lock skipping to avoid blocking on DDL operations.

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

#### Core Settings

| Key | Default | Purpose |
|-----|---------|---------|
| `schema_version` | 2.16 | Current schema version (read-only) |
| `mode` | normal | Operating mode: `normal`, `emergency`, or `disabled` |
| `sample_interval_seconds` | 180 | Ring buffer sample frequency |
| `ring_buffer_slots` | 120 | Number of ring buffer slots (72-2880) |

#### Statement Collection

| Key | Default | Purpose |
|-----|---------|---------|
| `statements_enabled` | auto | Enable pg_stat_statements: `true`, `false`, or `auto` |
| `statements_interval_minutes` | 15 | pg_stat_statements collection interval |
| `statements_top_n` | 20 | Number of top queries to capture |
| `statements_min_calls` | 1 | Minimum call count to include query |
| `enable_locks` | true | Collect lock contention data |
| `enable_progress` | true | Collect pg_stat_progress_* data |

#### Safety & Timeouts

| Key | Default | Purpose |
|-----|---------|---------|
| `snapshot_based_collection` | true | Use temp table snapshot (reduces catalog locks) |
| `lock_timeout_strategy` | fail_fast | Lock strategy: `fail_fast` or `skip_if_locked` |
| `check_ddl_before_collection` | true | Check for DDL locks before collecting |
| `check_replica_lag` | true | Skip collection if replica lag is high |
| `replica_lag_threshold` | 10 seconds | Max replica lag before skipping |
| `check_checkpoint_backup` | true | Check for checkpoint/backup activity |
| `check_pss_conflicts` | true | Check for pg_stat_statements conflicts |
| `statement_timeout_ms` | 1000 | Statement timeout for collection queries |
| `section_timeout_ms` | 250 | Per-section query timeout |
| `lock_timeout_ms` | 100 | Max wait for catalog locks |
| `work_mem_kb` | 2048 | work_mem for collection queries |

#### Circuit Breaker

| Key | Default | Purpose |
|-----|---------|---------|
| `circuit_breaker_enabled` | true | Enable circuit breaker |
| `circuit_breaker_threshold_ms` | 1000 | Max collection duration before skip |
| `circuit_breaker_window_minutes` | 15 | Window for tracking circuit breaker trips |

#### Load Protection

| Key | Default | Purpose |
|-----|---------|---------|
| `adaptive_sampling` | true | Skip collection when system idle |
| `adaptive_sampling_idle_threshold` | 5 | Skip if < N active connections |
| `load_shedding_enabled` | true | Skip during high connection load |
| `load_shedding_active_pct` | 70 | Skip if active connections > N% of max |
| `load_throttle_enabled` | true | Skip during I/O/transaction pressure |
| `load_throttle_xact_threshold` | 1000 | Skip if transactions > N/sec |
| `load_throttle_blk_threshold` | 10000 | Skip if block I/O > N/sec |
| `skip_locks_threshold` | 50 | Skip lock collection if > N blocked |
| `skip_activity_conn_threshold` | 100 | Skip activity if > N active connections |

#### Auto Mode

| Key | Default | Purpose |
|-----|---------|---------|
| `auto_mode_enabled` | true | Auto-adjust collection mode |
| `auto_mode_connections_threshold` | 60 | % connections to trigger emergency mode |
| `auto_mode_trips_threshold` | 1 | Circuit breaker trips to trigger emergency |

#### Collection Jitter

| Key | Default | Purpose |
|-----|---------|---------|
| `collection_jitter_enabled` | true | Add random delay to prevent thundering herd |
| `collection_jitter_max_seconds` | 10 | Maximum jitter delay in seconds |

#### Schema Size Limits

| Key | Default | Purpose |
|-----|---------|---------|
| `schema_size_check_enabled` | true | Enable schema size checking |
| `schema_size_warning_mb` | 5000 | Log warning at this schema size |
| `schema_size_critical_mb` | 10000 | Auto-disable at this schema size |
| `schema_size_use_percentage` | true | Use percentage-based limits |
| `schema_size_percentage` | 5.0 | Max schema size as % of database |
| `schema_size_min_mb` | 1000 | Minimum threshold for percentage mode |
| `schema_size_max_mb` | 10000 | Maximum threshold for percentage mode |

#### Health & Monitoring

| Key | Default | Purpose |
|-----|---------|---------|
| `self_monitoring_enabled` | true | Track Flight Recorder's own performance |
| `health_check_enabled` | true | Enable health check function |

#### Alerts

| Key | Default | Purpose |
|-----|---------|---------|
| `alert_enabled` | false | Enable pg_notify alerts |
| `alert_circuit_breaker_count` | 5 | Alert after N circuit breaker trips |
| `alert_schema_size_mb` | 8000 | Alert when schema exceeds this size |

### Retention Settings

Single authoritative reference for all retention periods:

| Setting | Default | Storage | Purpose |
|---------|---------|---------|---------|
| `retention_samples_days` | 7 | Samples | Raw ring buffer samples |
| `aggregate_retention_days` | 7 | Aggregates | Aggregated summaries |
| `archive_retention_days` | 7 | Raw Archives | Raw sample archives |
| `retention_snapshots_days` | 30 | Snapshots | System stat snapshots |
| `retention_statements_days` | 30 | Snapshots | Query snapshots |
| `retention_collection_stats_days` | 30 | Internal | Collection performance stats |
| `retention_canary_days` | 7 | Canary | Canary query results |
| `retention_storms_days` | 30 | Storms | Query storm history |
| `retention_regressions_days` | 30 | Regressions | Performance regression history |
| `snapshot_retention_days_extended` | 90 | Extended | Extended snapshot retention for capacity planning |

Ring buffers self-clean via slot overwrite—no retention setting needed. Size is configurable via `ring_buffer_slots` (see [Ring Buffer Optimization](#ring-buffer-optimization)).

### Sample Intervals

Single authoritative reference for sample timing:

| Mode | Default Interval | Default Slots | Default Retention | Collections/Day |
|------|------------------|---------------|-------------------|-----------------|
| `normal` | 180s | 120 | 6 hours | 480 |
| `emergency` | 300s | 120 | 10 hours | 288 |

Formula: `retention = ring_buffer_slots × sample_interval_seconds`

Both `ring_buffer_slots` and `sample_interval_seconds` are configurable. Use optimization profiles for common configurations (see [Ring Buffer Optimization](#ring-buffer-optimization)).

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
| `table_stats_top_n` | 50 | Number of hottest tables to track per snapshot (for `top_n` mode) |
| `table_stats_mode` | top_n | Collection mode: `top_n` (limit to N), `all` (every table), `threshold` (activity filter) |
| `table_stats_activity_threshold` | 0 | Minimum activity score for `threshold` mode |
| `index_stats_enabled` | true | Enable per-index usage tracking |

Table tracking captures seq scans, index scans, tuple activity, bloat indicators, vacuum/analyze counts, and modifications since last analyze for tracked tables.

Index tracking captures scan counts, tuple reads/fetches, and index sizes for detecting unused or inefficient indexes.

### Configuration Snapshot Settings

| Setting | Default | Purpose |
|---------|---------|---------|
| `config_snapshots_enabled` | true | Enable PostgreSQL config tracking |
| `db_role_config_snapshots_enabled` | true | Enable database/role config override tracking |

Configuration snapshots capture ~50 relevant PostgreSQL parameters (memory, connections, parallelism, WAL, autovacuum, etc.) to provide context during incident analysis and detect configuration drift. Database/role config snapshots track `ALTER DATABASE ... SET` and `ALTER ROLE ... SET` overrides.

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
| `db_role_config_at(timestamp)` | Show database/role configuration overrides at a point in time |
| `db_role_config_changes(start, end)` | Detect database/role configuration changes |
| `db_role_config_summary()` | Overview of database/role configuration overrides |

### Autovacuum Observer Functions

| Function | Purpose |
|----------|---------|
| `dead_tuple_growth_rate(relid, window)` | Calculate dead tuple accumulation rate (tuples/second) |
| `modification_rate(relid, window)` | Calculate row modification rate (modifications/second) |
| `hot_update_ratio(relid)` | Calculate HOT (Heap-Only Tuple) update percentage |
| `time_to_budget_exhaustion(relid, budget)` | Estimate time until dead tuple budget exceeded |

These functions support autovacuum monitoring and control systems by providing rate calculations based on historical snapshots. They return NULL when insufficient data exists (< 2 snapshots within window) or for non-existent table OIDs.

**Example usage:**

```sql
-- Dead tuple growth rate over last hour
SELECT flight_recorder.dead_tuple_growth_rate(
    'my_table'::regclass::oid,
    '1 hour'::interval
);

-- Time until 10,000 dead tuple budget exhausted
SELECT flight_recorder.time_to_budget_exhaustion(
    'my_table'::regclass::oid,
    10000
);

-- HOT update efficiency
SELECT relname,
       flight_recorder.hot_update_ratio(relid) as hot_pct
FROM pg_stat_user_tables
WHERE n_tup_upd > 0
ORDER BY hot_pct NULLS LAST;
```

### Control Functions

| Function | Purpose |
|----------|---------|
| `enable()` | Start collection (schedules pg_cron jobs) |
| `disable()` | Stop all collection |
| `set_mode('normal'/'emergency')` | Adjust collection intensity |
| `get_mode()` | Show current mode and settings |
| `cleanup(interval)` | Delete old data (default: 7 days) |
| `validate_config()` | Validate configuration settings |
| `configure_ring_autovacuum(enabled)` | Toggle autovacuum on ring buffer tables (default: true) |
| `validate_ring_configuration()` | Validate ring buffer config (retention, batching, overhead) |
| `get_optimization_profiles()` | List available ring buffer optimization profiles |
| `apply_optimization_profile(name)` | Apply a ring buffer optimization profile |
| `rebuild_ring_buffers(slots)` | Resize ring buffers (clears ring buffer data) |

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
| `report(start, end)` | Human/AI-readable diagnostic report |
| `report(interval)` | Same as above, for interval ending now |

### Internal Functions (pg_cron scheduled)

| Function | Purpose |
|----------|---------|
| `snapshot()` | Collect system stats snapshot |
| `sample()` | Collect ring buffer sample |
| `flush_ring_to_aggregates()` | Flush ring buffer to durable aggregates |
| `archive_ring_samples()` | Archive raw samples for forensic analysis |
| `cleanup_aggregates()` | Clean old aggregate and archive data |

### report() Output Structure

The `report(interval)` and `report(start, end)` functions return a diagnostic report readable by both humans and AI systems.

**Report sections:**

| Section | Source | Content |
|---------|--------|---------|
| Header | Generated | Version, timestamp, time range |
| Anomalies | `anomaly_report()` | Auto-detected issues (checkpoints, buffer pressure, etc.) |
| Wait Event Summary | `wait_summary()` | Aggregated wait events by type |
| Snapshots | Snapshots table | System stats: WAL, checkpoints, bgwriter |
| Table Hotspots | `table_hotspots()` | Table issues: seq scan storms, bloat, low HOT ratio |
| Index Efficiency | `index_efficiency()` | Index usage analysis: scans, selectivity, size |
| Statement Performance | `statement_compare()` | Query performance changes (requires pg_stat_statements) |
| Lock Contention | `lock_samples_archive` | Lock blocking events |
| Long-Running Transactions | `activity_samples_archive` | Transactions running >5 minutes with session/xact age |
| Vacuum Progress | `vacuum_progress_snapshots` | Vacuum phases, completion percentages, dead tuples |
| WAL Archiver Status | Snapshots table | Archive counts and failures during window |
| Configuration Changes | `config_changes()` | PostgreSQL parameter changes during window |
| Role Configuration Changes | `db_role_config_changes()` | Database/role override changes during window |

**Example usage:**

```sql
-- Report for the last hour
SELECT flight_recorder.report('1 hour');

-- Report for specific time window
SELECT flight_recorder.report(
    '2024-01-15 14:00:00'::timestamptz,
    '2024-01-15 15:00:00'::timestamptz
);

-- Save to file
\o /tmp/incident_report.md
SELECT flight_recorder.report('1 hour');
\o
```

## Views Reference

| View | Purpose |
|------|---------|
| `recent_waits` | Wait events (10-hour window, covers all modes) |
| `recent_activity` | Active sessions with session/transaction age (10-hour window) |
| `recent_locks` | Lock contention (10-hour window) |
| `recent_idle_in_transaction` | Sessions idle in transaction, ordered by duration (10-hour window) |
| `recent_replication` | Replication lag (2 hours, from snapshots) |
| `recent_vacuum_progress` | Vacuum progress with % scanned/vacuumed (2 hours) |
| `archiver_status` | WAL archiver metrics with delta calculations (24 hours) |
| `deltas` | Snapshot-over-snapshot changes |
| `capacity_dashboard` | Resource utilization and headroom |

### I/O Timing Metrics

The `deltas` view and `compare()` function include I/O timing columns from `pg_stat_io` (PostgreSQL 16+). These track storage latency by backend type:

| Column | Source | Purpose |
|--------|--------|---------|
| `io_ckpt_reads_delta` | checkpointer | Block reads by checkpointer |
| `io_ckpt_read_time_ms` | checkpointer | Read latency (ms) |
| `io_ckpt_writes_delta` | checkpointer | Block writes by checkpointer |
| `io_ckpt_write_time_ms` | checkpointer | Write latency (ms) |
| `io_autovacuum_reads_delta` | autovacuum | Block reads by autovacuum |
| `io_autovacuum_read_time_ms` | autovacuum | Read latency (ms) |
| `io_autovacuum_writes_delta` | autovacuum | Block writes by autovacuum |
| `io_autovacuum_write_time_ms` | autovacuum | Write latency (ms) |
| `io_client_reads_delta` | client backends | Block reads by queries |
| `io_client_read_time_ms` | client backends | Read latency (ms) |
| `io_client_writes_delta` | client backends | Block writes by queries |
| `io_client_write_time_ms` | client backends | Write latency (ms) |
| `io_bgwriter_reads_delta` | background writer | Block reads by bgwriter |
| `io_bgwriter_read_time_ms` | background writer | Read latency (ms) |
| `io_bgwriter_writes_delta` | background writer | Block writes by bgwriter |
| `io_bgwriter_write_time_ms` | background writer | Write latency (ms) |

**Note:** Timing columns require `track_io_timing = on` in PostgreSQL. Without it, `*_time_ms` values are zero.

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

### Database/Role Configuration Analysis

Track configuration overrides set via `ALTER DATABASE ... SET` or `ALTER ROLE ... SET`. These overrides are often overlooked during incident analysis but can significantly impact performance.

```sql
-- What database/role config overrides existed at incident time?
SELECT * FROM flight_recorder.db_role_config_at('2024-01-15 14:30:00');

-- Filter by specific database or role
SELECT * FROM flight_recorder.db_role_config_at(
    '2024-01-15 14:30:00',
    p_database := 'mydb'
);

-- Detect database/role config changes over time
SELECT * FROM flight_recorder.db_role_config_changes(
    '2024-01-14 00:00',
    '2024-01-15 00:00'
);

-- Get an overview of all database/role config overrides
SELECT * FROM flight_recorder.db_role_config_summary();
```

**Why this matters:**

- `ALTER DATABASE mydb SET work_mem = '256MB'` overrides global settings
- `ALTER ROLE analyst SET statement_timeout = '30s'` affects specific users
- These overrides don't appear in `pg_settings` unless you're connected as that role to that database

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
| `datfrozenxid_age` | pg_database | Database XID age (wraparound risk) |
| `relfrozenxid_age` | pg_class | Per-table XID age (in table_snapshots) |

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

## Performance Forecasting

Flight Recorder predicts resource depletion using linear regression on historical data. This enables proactive capacity planning by answering: "When will I run out?"

### Overview

Forecasting extends existing capacity planning:

| Feature | Question Answered | Focus |
|---------|-------------------|-------|
| `capacity_summary()` | "What's my current utilization?" | Present state, headroom % |
| `capacity_dashboard` | "What's my overall status?" | At-a-glance with growth rate |
| `forecast()` | "When will I run out?" | Future depletion time |
| `forecast_summary()` | "What resources need attention soon?" | Prioritized by urgency |

**Key benefits:**

- Uses existing snapshot data (no new collection overhead)
- Predicts when resources will be exhausted
- Provides confidence scores (R²) for predictions
- Supports pg_notify alerts for critical forecasts

### Using forecast()

Forecast a single metric:

```sql
-- Forecast database size growth
SELECT * FROM flight_recorder.forecast('db_size');

-- Custom lookback and forecast windows
SELECT * FROM flight_recorder.forecast('db_size', '14 days', '30 days');

-- Forecast connections
SELECT * FROM flight_recorder.forecast('connections', '7 days', '7 days');
```

**Supported metrics:**

| Metric | Aliases | Depletion Tracked |
|--------|---------|-------------------|
| `db_size` | `storage` | Yes (configurable disk capacity) |
| `connections` | - | Yes (max_connections) |
| `wal_bytes` | `wal` | No (informational) |
| `xact_commit` | `transactions` | No (informational) |
| `temp_bytes` | `temp` | No (informational) |

**Output columns:**

| Column | Purpose |
|--------|---------|
| `metric` | Metric name |
| `current_value` | Current raw value |
| `current_display` | Human-readable current value |
| `forecast_value` | Predicted value at end of forecast window |
| `forecast_display` | Human-readable forecast value |
| `rate_per_day` | Growth rate (units per day) |
| `rate_display` | Human-readable growth rate |
| `confidence` | R² coefficient (0-1, higher is better) |
| `depleted_at` | Predicted depletion timestamp (if applicable) |
| `time_to_depletion` | Time until depletion (if applicable) |

### Using forecast_summary()

Multi-metric forecast dashboard:

```sql
-- All metrics with default windows (7 days lookback, 7 days forecast)
SELECT * FROM flight_recorder.forecast_summary();

-- Custom windows
SELECT * FROM flight_recorder.forecast_summary('14 days', '30 days');
```

**Output columns:**

| Column | Purpose |
|--------|---------|
| `metric` | Metric name |
| `current` | Current value (display format) |
| `forecast` | Forecast value (display format) |
| `rate` | Growth rate per day |
| `confidence` | R² coefficient (0-1) |
| `depleted_at` | Predicted depletion timestamp |
| `status` | Status classification |
| `recommendation` | Actionable advice |

**Status values:**

| Status | Condition |
|--------|-----------|
| `critical` | Depletion within 24 hours |
| `warning` | Depletion within 7 days |
| `attention` | Depletion within 30 days |
| `healthy` | No depletion predicted or >30 days |
| `insufficient_data` | Not enough snapshots for forecast |
| `flat` | No significant trend detected |

### Config Settings

| Setting | Default | Description |
|---------|---------|-------------|
| `forecast_enabled` | `true` | Enable/disable forecasting |
| `forecast_lookback_days` | `7` | Default lookback window |
| `forecast_window_days` | `7` | Default forecast window |
| `forecast_alert_enabled` | `false` | Enable pg_notify alerts |
| `forecast_alert_threshold` | `3 days` | Alert when depletion is within this window |
| `forecast_notify_channel` | `flight_recorder_forecasts` | pg_notify channel name |
| `forecast_disk_capacity_gb` | `100` | Assumed disk capacity for db_size forecasts |
| `forecast_min_samples` | `10` | Minimum snapshots required for forecast |
| `forecast_min_confidence` | `0.5` | Minimum R² for alerts |

### Setting Up Alerts

Enable forecast alerts to receive pg_notify messages when resources are predicted to deplete soon:

```sql
-- Enable alerts
UPDATE flight_recorder.config
SET value = 'true'
WHERE key = 'forecast_alert_enabled';

-- Configure disk capacity (if different from default)
UPDATE flight_recorder.config
SET value = '500'  -- 500 GB
WHERE key = 'forecast_disk_capacity_gb';

-- Schedule alert checks via pg_cron (every 4 hours)
SELECT cron.schedule(
    'forecast-alerts',
    '0 */4 * * *',
    'SELECT flight_recorder.check_forecast_alerts()'
);
```

**Listen for alerts:**

```sql
-- In a separate session
LISTEN flight_recorder_forecasts;

-- Alerts are JSON payloads:
-- {"type":"forecast_alert","metric":"db_size","current_value":"45 GB","depleted_at":"2026-02-15 10:30:00","confidence":0.92,"status":"warning","timestamp":"2026-01-30 14:00:00"}
```

### Interpreting Results

**High confidence (R² > 0.8):**

The trend is clear and predictions are reliable. Take action based on status.

**Medium confidence (R² 0.5-0.8):**

Some variability in data. Predictions are directionally correct but timing may vary.

**Low confidence (R² < 0.5):**

Data is noisy or non-linear. Consider using longer lookback windows or investigating anomalies.

### Practical Examples

**Weekly capacity review:**

```sql
SELECT metric, current, forecast, rate, status, recommendation
FROM flight_recorder.forecast_summary('14 days', '30 days')
ORDER BY
    CASE status
        WHEN 'critical' THEN 1
        WHEN 'warning' THEN 2
        WHEN 'attention' THEN 3
        ELSE 4
    END;
```

**Storage planning:**

```sql
-- Check when disk will be full
SELECT
    current_display AS "Current Size",
    rate_display AS "Growth Rate",
    depleted_at AS "Full At",
    time_to_depletion AS "Time Left",
    confidence AS "Confidence"
FROM flight_recorder.forecast('db_size', '30 days', '90 days');
```

**Connection pool sizing:**

```sql
SELECT
    current_display AS "Connections",
    rate_display AS "Trend",
    CASE
        WHEN depleted_at IS NOT NULL THEN
            format('Will hit limit at %s', depleted_at)
        ELSE 'No limit reached in forecast window'
    END AS "Prediction"
FROM flight_recorder.forecast('connections', '7 days', '30 days');
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
| `XID_WRAPAROUND_RISK` | Database XID age approaching limit | Run VACUUM FREEZE, tune autovacuum |
| `TABLE_XID_WRAPAROUND_RISK` | Table XID age approaching limit | Run VACUUM FREEZE on specific table |
| `OID_EXHAUSTION_RISK` | OID counter approaching 4.3 billion limit | Review lo_create() usage, pg_dump/pg_restore to reset |
| `IDLE_IN_TRANSACTION` | Session idle in transaction >5 min | Terminate stale sessions, blocks vacuum |
| `DEAD_TUPLE_ACCUMULATION` | Table has >10% dead tuples | Run VACUUM, check autovacuum settings |
| `VACUUM_STARVATION` | Dead tuples growing, no vacuum in 24h | Tune autovacuum thresholds |
| `CONNECTION_LEAK` | Session open >7 days | Investigate leak, use connection pooling |
| `REPLICATION_LAG_GROWING` | Replica lag trending upward | Check replica capacity, network, queries |

### XID Wraparound Detection

Transaction ID (XID) wraparound is a critical PostgreSQL failure mode. Flight Recorder tracks XID ages and alerts when approaching dangerous thresholds.

**Thresholds** (based on `autovacuum_freeze_max_age` setting, default 200M):

| Severity | Threshold | Meaning |
|----------|-----------|---------|
| `high` | > 50% of freeze_max_age | Proactive warning |
| `critical` | > 80% of freeze_max_age | Urgent action needed |

**Per-table awareness:** Each table can have its own `autovacuum_freeze_max_age` storage parameter. The detection respects per-table settings when configured via `ALTER TABLE ... SET (autovacuum_freeze_max_age = ...)`.

**Metrics collected:**

- `datfrozenxid_age` — Database-level XID age (in `snapshots` table)
- `relfrozenxid_age` — Per-table XID age (in `table_snapshots` table)

**Example output:**

```sql
SELECT * FROM flight_recorder.anomaly_report(now() - interval '1 hour', now());
```

| anomaly_type | severity | description | metric_value | threshold |
|--------------|----------|-------------|--------------|-----------|
| XID_WRAPAROUND_RISK | high | Database approaching transaction ID wraparound | XID age: 120,000,000 (60% of autovacuum_freeze_max_age) | datfrozenxid_age > 100,000,000 (50% of 200,000,000) |
| TABLE_XID_WRAPAROUND_RISK | critical | Table public.large_table approaching XID wraparound | XID age: 170,000,000 (85% of autovacuum_freeze_max_age=200,000,000) | relfrozenxid_age > 100,000,000 (50% of 200,000,000) |

**Related PostgreSQL parameters:**

| Parameter | Default | Purpose |
|-----------|---------|---------|
| `autovacuum_freeze_max_age` | 200M | Forces anti-wraparound autovacuum |
| `vacuum_freeze_table_age` | 150M | Triggers aggressive freeze during normal vacuum |
| `vacuum_freeze_min_age` | 50M | Minimum age before tuples can be frozen |

```sql
SELECT * FROM flight_recorder.anomaly_report(
    '2024-01-15 10:00',
    '2024-01-15 11:00'
);
```

### OID Exhaustion Detection

Object Identifiers (OIDs) are 32-bit unsigned integers (max ~4.3 billion) used internally by PostgreSQL for system catalog entries and large objects. Unlike XIDs, OIDs are not recycled—they simply exhaust. Recovery requires `pg_dump`/`pg_restore` to reset the counter.

**What consumes OIDs:**

- Large objects created via `lo_create()`
- System catalog entries (tables, indexes, functions, etc.)
- Toast tables and indexes

**Thresholds** (based on 4,294,967,295 max):

| Severity | Threshold | Meaning |
|----------|-----------|---------|
| `high` | > 75% (~3.22 billion) | Proactive warning |
| `critical` | > 90% (~3.87 billion) | Urgent action needed |

**Metrics collected:**

- `max_catalog_oid` — Highest OID in pg_class (approximates counter position)
- `large_object_count` — Count from pg_largeobject_metadata

**Rate calculation functions:**

```sql
-- OID consumption rate over last hour (OIDs/second)
SELECT flight_recorder.oid_consumption_rate('1 hour');

-- Estimated time until OID exhaustion
SELECT flight_recorder.time_to_oid_exhaustion();
```

**Example output:**

| anomaly_type | severity | description | metric_value | threshold |
|--------------|----------|-------------|--------------|-----------|
| OID_EXHAUSTION_RISK | high | Database approaching OID exhaustion | Max catalog OID: 3,500,000,000 (81.5% of 4.3 billion), Large objects: 150,000 | max_catalog_oid > 3,221,225,471 (75% of 4,294,967,295) |

**Remediation:**

1. Identify and clean up unused large objects: `SELECT lo_unlink(oid) FROM pg_largeobject_metadata WHERE ...`
2. Review application logic that creates large objects
3. If OIDs are near exhaustion, plan for `pg_dump`/`pg_restore` to reset the counter

### Idle-in-Transaction Detection

Detects sessions that have been idle in a transaction for too long. These sessions block autovacuum, hold locks, and can cause replication lag on replicas.

**Thresholds:**

| Severity | Threshold |
|----------|-----------|
| `medium` | 5-15 minutes idle in transaction |
| `high` | 15-60 minutes idle in transaction |
| `critical` | >60 minutes idle in transaction |

**Quick visibility:**

```sql
SELECT * FROM flight_recorder.recent_idle_in_transaction;
```

### Dead Tuple Accumulation Detection

Detects tables with high dead tuple ratios that indicate bloat risk.

**Thresholds:**

| Condition | Severity |
|-----------|----------|
| >10% dead tuples AND >10,000 dead rows | `medium` |
| >30% dead tuples | `high` |

### Vacuum Starvation Detection

Detects tables where dead tuples are accumulating but autovacuum hasn't run recently.

**Conditions for alert:**

- Dead tuples grew by >1,000 since last snapshot
- No autovacuum in past 24 hours

**Severity:** Always `high`

### Connection Leak Detection

Detects sessions that have been open for an unusually long time, which may indicate connection leaks.

**Thresholds:**

| Severity | Threshold |
|----------|-----------|
| `medium` | Session open 7-30 days |
| `high` | Session open >30 days |

### Replication Lag Velocity Detection

Detects when replica lag is trending upward (lag is growing over time).

**Conditions for alert:**

- Lag grew by >60 seconds during the time window
- Current lag is >30 seconds
- At least 3 samples in the window

**Thresholds:**

| Severity | Current Lag |
|----------|-------------|
| `medium` | 30-60 seconds |
| `high` | 60-300 seconds |
| `critical` | >300 seconds |

## Canary Queries

Canary queries are synthetic workloads that run periodically to detect silent performance degradation. Unlike passive monitoring that waits for problems to appear in real queries, canary queries proactively test database responsiveness with known, lightweight operations.

### Overview

A "canary query" is a simple, well-understood query that establishes a performance baseline. When canary performance degrades, it indicates systemic issues (I/O problems, CPU contention, memory pressure) before user-facing queries are impacted.

**Use cases:**

- Detect silent degradation before users report slowness
- Establish performance baselines for capacity planning
- Validate database health after maintenance or changes
- Identify infrastructure issues (storage latency, network problems)

### Built-in Canary Queries

Flight Recorder includes four pre-defined canary queries targeting common database operations:

| Name | Description | Query |
|------|-------------|-------|
| `index_lookup` | B-tree index lookup on pg_class | `SELECT oid FROM pg_class WHERE relname = 'pg_class' LIMIT 1` |
| `small_agg` | Count aggregation on pg_stat_activity | `SELECT count(*) FROM pg_stat_activity` |
| `seq_scan_baseline` | Sequential scan count on pg_namespace | `SELECT count(*) FROM pg_namespace` |
| `simple_join` | Join pg_namespace to pg_class | `SELECT count(*) FROM pg_namespace n JOIN pg_class c ON c.relnamespace = n.oid WHERE n.nspname = 'pg_catalog'` |

These queries use only system catalogs and are safe to run frequently.

### Status Levels

Canary status compares current performance (p50 over last hour) to baseline (p50 over last 7 days, excluding last day):

| Status | Condition | Description |
|--------|-----------|-------------|
| `OK` | Current < baseline × warning threshold | Performance within normal range |
| `DEGRADED` | Current >= baseline × warning threshold | Performance degraded, warrants monitoring |
| `CRITICAL` | Current >= baseline × critical threshold | Severe degradation, immediate attention needed |
| `INSUFFICIENT_DATA` | Not enough samples | Need more data to establish baseline |

Default thresholds: warning = 1.5x (50% slower), critical = 2.0x (100% slower). Thresholds are configurable per canary.

### Enabling Canary Monitoring

Canary monitoring is opt-in. Enable it with:

```sql
SELECT flight_recorder.enable_canaries();
```

This schedules automatic execution every 15 minutes via pg_cron. To disable:

```sql
SELECT flight_recorder.disable_canaries();
```

### Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `canary_enabled` | `false` | Master enable/disable |
| `canary_interval_minutes` | `15` | How often to run canary queries |
| `canary_capture_plans` | `false` | Store EXPLAIN output with results |
| `retention_canary_days` | `7` | How long to keep canary results |

Adjust configuration:

```sql
-- Run canaries more frequently
UPDATE flight_recorder.config
SET value = '5'
WHERE key = 'canary_interval_minutes';

-- Enable plan capture for debugging
UPDATE flight_recorder.config
SET value = 'true'
WHERE key = 'canary_capture_plans';

-- Keep results longer
UPDATE flight_recorder.config
SET value = '30'
WHERE key = 'retention_canary_days';
```

### Manual Execution

Run canaries without enabling automatic scheduling:

```sql
SELECT * FROM flight_recorder.run_canaries();
```

Output:

| canary_name | duration_ms | success | error_message |
|-------------|-------------|---------|---------------|
| index_lookup | 0.42 | true | |
| small_agg | 1.23 | true | |
| seq_scan_baseline | 0.31 | true | |
| simple_join | 2.15 | true | |

### Monitoring Canary Health

**Status function** (detailed list):

```sql
SELECT * FROM flight_recorder.canary_status();
```

Output:

| canary_name | description | baseline_ms | current_ms | change_pct | status | last_executed | last_error |
|-------------|-------------|-------------|------------|------------|--------|---------------|------------|
| index_lookup | B-tree index lookup on pg_class | 0.45 | 0.42 | -6.7 | OK | 2024-01-15 10:30:00 | |
| small_agg | Count aggregation on pg_stat_activity | 1.20 | 2.40 | 100.0 | CRITICAL | 2024-01-15 10:30:00 | |

### Adding Custom Canaries

Add your own canary queries for application-specific monitoring:

```sql
-- Add a canary for your most critical table
INSERT INTO flight_recorder.canaries (name, description, query_text, threshold_warning, threshold_critical)
VALUES (
    'orders_lookup',
    'Primary key lookup on orders table',
    'SELECT id FROM orders WHERE id = 1 LIMIT 1',
    1.5,  -- Alert at 50% slower
    2.0   -- Critical at 100% slower
);

-- Add a canary with expected baseline timing
INSERT INTO flight_recorder.canaries (name, description, query_text, expected_time_ms)
VALUES (
    'inventory_count',
    'Count active inventory items',
    'SELECT count(*) FROM inventory WHERE status = ''active''',
    5.0  -- Expected to run in ~5ms
);
```

Disable a canary without deleting it:

```sql
UPDATE flight_recorder.canaries
SET enabled = false
WHERE name = 'orders_lookup';
```

### Function Reference

| Function | Description |
|----------|-------------|
| `enable_canaries()` | Enable and schedule automatic canary execution |
| `disable_canaries()` | Disable and unschedule canary execution |
| `run_canaries()` | Execute all enabled canaries immediately |
| `canary_status()` | Get current status comparing performance to baseline |

### Table Reference

| Object | Type | Description |
|--------|------|-------------|
| `canaries` | Table | Canary query definitions |
| `canary_results` | Table | Execution history with timing and optional plans |

### Quick Start

```sql
-- 1. Enable canary monitoring
SELECT flight_recorder.enable_canaries();

-- 2. Wait for some executions, then check status
SELECT * FROM flight_recorder.canary_status();

-- 3. Add a custom canary for your application
INSERT INTO flight_recorder.canaries (name, description, query_text)
VALUES ('my_canary', 'Check critical table', 'SELECT 1 FROM my_table LIMIT 1');
```

## Query Storm Detection

Query storm detection identifies when queries spike beyond baseline thresholds. Storms are classified by type and can be monitored, resolved manually, or auto-resolved when counts normalize.

### Overview

A "query storm" is a sudden spike in query execution frequency compared to historical baseline. Common causes include:

- **Retry storms**: Application retry logic causing exponential request growth
- **Cache misses**: Cold cache or invalidation causing repeated database hits
- **Traffic spikes**: Legitimate load increases beyond normal patterns

### Storm Types

| Type | Classification Criteria | Common Causes |
|------|------------------------|---------------|
| `RETRY_STORM` | Query contains RETRY or FOR UPDATE keywords | Application retry loops, lock contention |
| `CACHE_MISS` | Execution count > 10x baseline | Cache invalidation, cold start, missing indexes |
| `SPIKE` | Execution count > threshold multiplier (default 3x) | Traffic spike, batch job, query plan regression |
| `NORMAL` | Within normal range | No action needed |

### Severity Levels

Storms are classified by severity based on how far the query count exceeds the baseline:

| Severity | Multiplier Range | Description |
|----------|-----------------|-------------|
| `LOW` | <= 5.0x | Minor spike, may be normal traffic variation |
| `MEDIUM` | 5.0x - 10.0x | Significant spike, warrants monitoring |
| `HIGH` | 10.0x - 50.0x | Major spike, likely requires investigation |
| `CRITICAL` | > 50.0x or RETRY_STORM | Severe spike, immediate attention needed |

Severity thresholds are configurable (see Configuration below).

### Enabling Storm Detection

Storm detection is opt-in. Enable it with:

```sql
SELECT flight_recorder.enable_storm_detection();
```

This schedules automatic detection every 15 minutes via pg_cron. To disable:

```sql
SELECT flight_recorder.disable_storm_detection();
```

### Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `storm_detection_enabled` | `false` | Master enable/disable |
| `storm_threshold_multiplier` | `3.0` | Spike detection threshold (recent/baseline ratio) |
| `storm_lookback_interval` | `1 hour` | Recent window for comparison |
| `storm_baseline_days` | `7` | Historical baseline period |
| `storm_detection_interval_minutes` | `15` | Auto-detection frequency |
| `storm_min_duration_minutes` | `5` | Minimum storm age before auto-resolution |
| `storm_notify_enabled` | `true` | Send pg_notify alerts |
| `storm_notify_channel` | `flight_recorder_storms` | Notification channel name |
| `storm_severity_low_max` | `5.0` | Maximum multiplier for LOW severity |
| `storm_severity_medium_max` | `10.0` | Maximum multiplier for MEDIUM severity |
| `storm_severity_high_max` | `50.0` | Maximum multiplier for HIGH severity |
| `retention_storms_days` | `30` | How long to keep storm history |

Adjust configuration:

```sql
-- Increase sensitivity (detect smaller spikes)
UPDATE flight_recorder.config
SET value = '2.0'
WHERE key = 'storm_threshold_multiplier';

-- Longer baseline for more stable detection
UPDATE flight_recorder.config
SET value = '14'
WHERE key = 'storm_baseline_days';

-- Adjust severity thresholds for more sensitive alerting
UPDATE flight_recorder.config
SET value = '3.0'
WHERE key = 'storm_severity_low_max';
```

### Manual Detection

Detect storms without enabling automatic detection:

```sql
-- Detect with default settings
SELECT * FROM flight_recorder.detect_query_storms();

-- Custom lookback and threshold
SELECT * FROM flight_recorder.detect_query_storms('30 minutes'::interval, 2.0);
```

Output:

| queryid | query_fingerprint | storm_type | severity | recent_count | baseline_count | multiplier |
|---------|-------------------|------------|----------|--------------|----------------|------------|
| 789012 | UPDATE inventory SET... FOR UPDATE | RETRY_STORM | CRITICAL | 8500 | 200 | 42.50 |
| 123456 | SELECT * FROM orders WHERE... | SPIKE | MEDIUM | 15000 | 2500 | 6.00 |

### Monitoring Storms

**Dashboard view** (at-a-glance summary):

```sql
SELECT * FROM flight_recorder.storm_dashboard;
```

Returns: active storm counts by type and severity, resolution rate, average resolution time, storm-prone queries, overall status, and recommendations.

**Status function** (detailed list):

```sql
-- Active and recent storms
SELECT * FROM flight_recorder.storm_status();

-- Storms in last 4 hours
SELECT * FROM flight_recorder.storm_status('4 hours');
```

**In reports:**

```sql
SELECT flight_recorder.report('1 hour');
```

The report includes a Query Storms section when storm detection is enabled.

### Resolving Storms

**Manual resolution:**

```sql
-- Resolve single storm with notes
SELECT flight_recorder.resolve_storm(123, 'Fixed by adding index on orders.customer_id');

-- Resolve all storms for a specific query
SELECT flight_recorder.resolve_storms_by_queryid(456789, 'Query optimized, deployed v2.1');

-- Bulk resolve after incident review
SELECT flight_recorder.resolve_all_storms('Incident #42 reviewed and closed');

-- Reopen if resolved incorrectly
SELECT flight_recorder.reopen_storm(123);
```

**Auto-resolution:**

When `auto_detect_storms()` runs (via pg_cron), it automatically resolves storms whose query counts have returned to normal. Auto-resolution includes anti-flapping protection:

- Storms must be active for at least `storm_min_duration_minutes` (default: 5) before auto-resolution
- Resolution note: "Auto-resolved: query counts returned to normal"

### Correlation Data

When a storm is detected, pg-flight-recorder captures correlated metrics to help identify root causes. This data is stored in the `correlation` JSONB column of `query_storms`:

```sql
SELECT severity, correlation FROM flight_recorder.storm_status() WHERE status = 'ACTIVE';
```

**Correlation structure:**

```json
{
  "checkpoint": {
    "active": true,
    "ckpt_write_time_ms": 1234,
    "ckpt_sync_time_ms": 567,
    "ckpt_buffers": 8192
  },
  "locks": {
    "blocked_count": 5,
    "max_duration_seconds": 12.5,
    "lock_types": ["RowExclusiveLock", "AccessShareLock"]
  },
  "waits": {
    "top_events": [
      {"event": "LWLock:buffer_content", "count": 150},
      {"event": "IO:DataFileRead", "count": 89}
    ],
    "total_waiters": 239
  },
  "io": {
    "temp_bytes_delta": 104857600,
    "blks_read_delta": 5000,
    "connections_active": 45,
    "connections_total": 100
  }
}
```

**Correlation sections:**

| Section | Source | Indicators |
|---------|--------|------------|
| `checkpoint` | `snapshots` | Active checkpoint can cause I/O contention |
| `locks` | `lock_aggregates` | Lock contention may indicate retry storms |
| `waits` | `wait_event_aggregates` | Wait events show resource bottlenecks |
| `io` | `snapshots` | Temp file usage, block reads, connection pressure |

Empty sections are omitted when no relevant data is found.

### Real-Time Alerts

Storm detection sends pg_notify alerts when storms are detected or resolved:

```sql
-- Listen for alerts (in psql or application)
LISTEN flight_recorder_storms;
```

**Notification payload (JSON):**

```json
{
  "action": "detected",
  "storm_id": 123,
  "queryid": 456789,
  "storm_type": "SPIKE",
  "severity": "MEDIUM",
  "timestamp": "2024-01-15T10:30:00Z",
  "recent_count": 15000,
  "baseline_count": 2500,
  "multiplier": 6.0
}
```

```json
{
  "action": "resolved",
  "storm_id": 123,
  "queryid": 456789,
  "storm_type": "SPIKE",
  "severity": "MEDIUM",
  "timestamp": "2024-01-15T11:00:00Z",
  "resolution_notes": "Auto-resolved: query counts returned to normal"
}
```

Disable notifications:

```sql
UPDATE flight_recorder.config
SET value = 'false'
WHERE key = 'storm_notify_enabled';
```

### Functions Reference

| Function | Purpose |
|----------|---------|
| `enable_storm_detection()` | Enable and schedule automatic detection |
| `disable_storm_detection()` | Disable and unschedule detection |
| `detect_query_storms(interval, numeric)` | Manual storm detection (returns severity) |
| `auto_detect_storms()` | Detect new storms, auto-resolve normalized (called by pg_cron) |
| `storm_status(interval)` | List active and recent storms (includes severity and correlation) |
| `resolve_storm(bigint, text)` | Resolve single storm by ID |
| `resolve_storms_by_queryid(bigint, text)` | Resolve all storms for a queryid |
| `resolve_all_storms(text)` | Bulk resolve all active storms |
| `reopen_storm(bigint)` | Reopen a previously resolved storm |
| `_compute_storm_correlation(interval)` | Internal: gather correlated metrics for storm context |

### Tables and Views

| Object | Type | Purpose |
|--------|------|---------|
| `query_storms` | Table | Storm detection history |
| `storm_dashboard` | View | At-a-glance monitoring summary |

### Example Workflow

```sql
-- 1. Enable storm detection
SELECT flight_recorder.enable_storm_detection();

-- 2. Check dashboard periodically
SELECT * FROM flight_recorder.storm_dashboard;

-- 3. Investigate active storms
SELECT * FROM flight_recorder.storm_status() WHERE status = 'ACTIVE';

-- 4. Get query details (requires pg_stat_statements)
SELECT query, calls, mean_exec_time
FROM pg_stat_statements
WHERE queryid = 456789;

-- 5. Resolve after fixing
SELECT flight_recorder.resolve_storm(123, 'Added missing index');

-- 6. Monitor via pg_notify in your application
-- Application code: LISTEN flight_recorder_storms
```

## Performance Regression Detection

Performance regression detection identifies queries whose execution time has significantly increased compared to historical baselines. Regressions are classified by severity and can be monitored, resolved manually, or auto-resolved when performance normalizes.

### Overview

A "performance regression" is a significant slowdown in query execution time compared to historical baseline. Unlike storm detection (which tracks execution frequency), regression detection focuses on **execution time** changes. Common causes include:

- **Plan changes**: Query planner choosing a worse execution plan
- **Statistics staleness**: Out-of-date table statistics leading to suboptimal plans
- **Data growth**: Table size increases affecting sequential scans
- **Cache misses**: Data falling out of shared buffers
- **Resource contention**: Competing workloads affecting I/O

### Severity Levels

Regressions are classified by severity based on percentage change from baseline:

| Severity | Change % | Description |
|----------|----------|-------------|
| `LOW` | 50% - 200% | Minor slowdown, may be normal variation |
| `MEDIUM` | 200% - 500% | Noticeable degradation, warrants monitoring |
| `HIGH` | 500% - 1000% | Significant regression, investigate |
| `CRITICAL` | > 1000% | Severe regression, immediate attention needed |

Severity thresholds are configurable (see Configuration below).

### Enabling Regression Detection

Regression detection is opt-in. Enable it with:

```sql
SELECT flight_recorder.enable_regression_detection();
```

This schedules automatic detection every 60 minutes via pg_cron. To disable:

```sql
SELECT flight_recorder.disable_regression_detection();
```

### Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `regression_detection_enabled` | `false` | Master enable/disable |
| `regression_threshold_pct` | `50.0` | Minimum % increase to detect |
| `regression_lookback_interval` | `1 hour` | Recent window for comparison |
| `regression_baseline_days` | `7` | Historical baseline period |
| `regression_detection_interval_minutes` | `60` | Auto-detection frequency |
| `regression_min_duration_minutes` | `30` | Minimum age before auto-resolution |
| `regression_notify_enabled` | `true` | Send pg_notify alerts |
| `regression_notify_channel` | `flight_recorder_regressions` | Notification channel name |
| `regression_severity_low_max` | `200.0` | Maximum % for LOW severity |
| `regression_severity_medium_max` | `500.0` | Maximum % for MEDIUM severity |
| `regression_severity_high_max` | `1000.0` | Maximum % for HIGH severity |
| `retention_regressions_days` | `30` | How long to keep regression history |

Adjust configuration:

```sql
-- Increase sensitivity (detect smaller slowdowns)
UPDATE flight_recorder.config
SET value = '25.0'
WHERE key = 'regression_threshold_pct';

-- Check more frequently
UPDATE flight_recorder.config
SET value = '30'
WHERE key = 'regression_detection_interval_minutes';

-- Adjust severity thresholds
UPDATE flight_recorder.config
SET value = '300.0'
WHERE key = 'regression_severity_medium_max';
```

### Manual Detection

Detect regressions without enabling automatic detection:

```sql
-- Detect with default settings
SELECT * FROM flight_recorder.detect_regressions();

-- Custom lookback and threshold
SELECT * FROM flight_recorder.detect_regressions('2 hours'::interval, 100.0);
```

Output:

| queryid | query_fingerprint | severity | baseline_avg_ms | current_avg_ms | change_pct | probable_causes |
|---------|-------------------|----------|-----------------|----------------|------------|-----------------|
| 789012 | SELECT * FROM orders WHERE... | CRITICAL | 5.23 | 85.67 | 1538.24 | {Statistics may be out of date} |
| 123456 | UPDATE inventory SET... | MEDIUM | 12.50 | 45.00 | 260.00 | {Low cache hit ratio} |

### Monitoring Regressions

**Dashboard view** (at-a-glance summary):

```sql
SELECT * FROM flight_recorder.regression_dashboard;
```

Returns: active regression counts by severity, resolution rate, average resolution time, regression-prone queries, overall status, and recommendations.

**Status function** (detailed list):

```sql
-- Active and recent regressions
SELECT * FROM flight_recorder.regression_status();

-- Regressions in last 4 hours
SELECT * FROM flight_recorder.regression_status('4 hours');
```

### Resolving Regressions

**Manual resolution:**

```sql
-- Resolve single regression with notes
SELECT flight_recorder.resolve_regression(123, 'Fixed by running ANALYZE on orders table');

-- Resolve all regressions for a specific query
SELECT flight_recorder.resolve_regressions_by_queryid(456789, 'Query optimized, deployed v2.1');

-- Bulk resolve after incident review
SELECT flight_recorder.resolve_all_regressions('Incident #42 reviewed and closed');

-- Reopen if resolved incorrectly
SELECT flight_recorder.reopen_regression(123);
```

**Auto-resolution:**

When `auto_detect_regressions()` runs (via pg_cron), it automatically resolves regressions whose query execution times have returned to normal. Auto-resolution includes anti-flapping protection:

- Regressions must be active for at least `regression_min_duration_minutes` (default: 30) before auto-resolution
- Resolution note: "Auto-resolved: performance returned to normal"

### Probable Causes

When a regression is detected, pg-flight-recorder analyzes the query to suggest probable causes:

- **Temp file spills**: Query is spilling to disk
- **Low cache hit ratio**: Data not in shared buffers
- **Recent checkpoint**: I/O contention from checkpoint activity
- **Statistics staleness**: Tables may need ANALYZE

### Real-Time Alerts

Regression detection sends pg_notify alerts when regressions are detected or resolved:

```sql
-- Listen for alerts (in psql or application)
LISTEN flight_recorder_regressions;
```

**Notification payload (JSON):**

```json
{
  "action": "detected",
  "regression_id": 123,
  "queryid": 456789,
  "severity": "HIGH",
  "timestamp": "2024-01-15T10:30:00Z",
  "baseline_avg_ms": 12.5,
  "current_avg_ms": 87.3,
  "change_pct": 598.4
}
```

```json
{
  "action": "resolved",
  "regression_id": 123,
  "queryid": 456789,
  "severity": "HIGH",
  "timestamp": "2024-01-15T11:00:00Z",
  "resolution_notes": "Auto-resolved: performance returned to normal"
}
```

Disable notifications:

```sql
UPDATE flight_recorder.config
SET value = 'false'
WHERE key = 'regression_notify_enabled';
```

### Functions Reference

| Function | Purpose |
|----------|---------|
| `enable_regression_detection()` | Enable and schedule automatic detection |
| `disable_regression_detection()` | Disable and unschedule detection |
| `detect_regressions(interval, numeric)` | Manual regression detection |
| `auto_detect_regressions()` | Detect new regressions, auto-resolve normalized (called by pg_cron) |
| `regression_status(interval)` | List active and recent regressions |
| `resolve_regression(bigint, text)` | Resolve single regression by ID |
| `resolve_regressions_by_queryid(bigint, text)` | Resolve all regressions for a queryid |
| `resolve_all_regressions(text)` | Bulk resolve all active regressions |
| `reopen_regression(bigint)` | Reopen a previously resolved regression |
| `_diagnose_regression_causes(bigint)` | Internal: analyze query for probable causes |

### Tables and Views

| Object | Type | Purpose |
|--------|------|---------|
| `query_regressions` | Table | Regression detection history |
| `regression_dashboard` | View | At-a-glance monitoring summary |

### Example Workflow

```sql
-- 1. Enable regression detection
SELECT flight_recorder.enable_regression_detection();

-- 2. Check dashboard periodically
SELECT * FROM flight_recorder.regression_dashboard;

-- 3. Investigate active regressions
SELECT * FROM flight_recorder.regression_status() WHERE status = 'ACTIVE';

-- 4. Get query details and run EXPLAIN
SELECT query, mean_exec_time, calls
FROM pg_stat_statements
WHERE queryid = 456789;

EXPLAIN ANALYZE SELECT * FROM orders WHERE customer_id = 123;

-- 5. Fix the issue (e.g., run ANALYZE, add index)
ANALYZE orders;

-- 6. Resolve after fixing
SELECT flight_recorder.resolve_regression(123, 'Ran ANALYZE on orders table');

-- 7. Monitor via pg_notify in your application
-- Application code: LISTEN flight_recorder_regressions
```

## Time-Travel Debugging

Time-travel debugging enables forensic analysis of "what happened at exactly 10:23:47?" by interpolating between samples and identifying events with exact timestamps.

### Overview

Flight Recorder samples every few minutes. When a customer reports "my query hung at exactly 10:23:47", there's no data for that specific second. Time-travel debugging bridges this gap by:

1. Finding surrounding samples (before/after the timestamp)
2. Interpolating system metrics between samples
3. Identifying events with exact timestamps that anchor the timeline
4. Providing confidence levels based on data proximity
5. Generating actionable recommendations

### Functions Reference

| Function | Purpose |
|----------|---------|
| `_interpolate_metric(before, time_before, after, time_after, target)` | Linear interpolation helper |
| `what_happened_at(timestamp, context_window)` | Forensic analysis at any timestamp |
| `incident_timeline(start_time, end_time)` | Unified event timeline for incidents |

### Using what_happened_at()

The main function for point-in-time analysis:

```sql
-- What was happening at a specific moment?
SELECT * FROM flight_recorder.what_happened_at('2024-01-15 10:23:47');
```

Returns:

| Column | Description |
|--------|-------------|
| `requested_time` | The timestamp you queried |
| `sample_before` / `sample_after` | Surrounding sample timestamps |
| `snapshot_before` / `snapshot_after` | Surrounding snapshot timestamps |
| `est_connections_active` | Interpolated active connections |
| `est_connections_total` | Interpolated total connections |
| `est_xact_rate` | Estimated transactions per second |
| `est_blks_hit_ratio` | Buffer cache hit ratio percentage |
| `events` | JSONB array of exact-timestamp events |
| `sessions_active` | Active sessions from nearest sample |
| `long_running_queries` | Count of queries > 60 seconds |
| `longest_query_secs` | Duration of longest running query |
| `lock_contention_detected` | Whether locks were blocking sessions |
| `blocked_sessions` | Count of blocked sessions |
| `top_wait_events` | JSONB array of top wait events |
| `confidence` | high, medium, low, or very_low |
| `confidence_score` | Numeric confidence (0-1) |
| `data_quality_notes` | Array of data quality observations |
| `recommendations` | Array of actionable suggestions |

#### Custom Context Window

By default, the function looks for events within ±5 minutes of the target timestamp:

```sql
-- Wider context window (10 minutes each direction)
SELECT * FROM flight_recorder.what_happened_at(
    '2024-01-15 10:23:47',
    '10 minutes'::interval
);
```

### Using incident_timeline()

Reconstructs a chronological timeline for incident analysis:

```sql
-- What happened during the incident window?
SELECT * FROM flight_recorder.incident_timeline(
    '2024-01-15 10:00:00',
    '2024-01-15 11:00:00'
);
```

Returns events ordered chronologically:

| Column | Description |
|--------|-------------|
| `event_time` | When the event occurred |
| `event_type` | Type of event (see below) |
| `description` | Human-readable description |
| `details` | JSONB with event-specific details |

#### Event Types

| Event Type | Source | Description |
|------------|--------|-------------|
| `checkpoint` | snapshots | Checkpoint completed |
| `wal_archived` | snapshots | WAL file archived |
| `archive_failed` | snapshots | WAL archive failure |
| `query_started` | activity_samples | Query execution began |
| `transaction_started` | activity_samples | Transaction began |
| `connection_opened` | activity_samples | New connection established |
| `lock_contention` | lock_samples | Session blocked by another |
| `wait_spike` | wait_aggregates | Significant wait event spike |
| `snapshot` | snapshots | System state captured |

### Confidence Scoring

Confidence indicates how reliable the interpolated data is:

| Gap Between Samples | Score | Level |
|---------------------|-------|-------|
| < 60 seconds | 0.9+ | high |
| 60-300 seconds | 0.7-0.9 | medium |
| 300-600 seconds | 0.5-0.7 | low |
| > 600 seconds | < 0.5 | very_low |

**Bonuses:**

- Exact-timestamp event in window: +0.1
- Target close to actual sample (<30s): +0.05

### Data Sources for Exact Timestamps

| Source | Timestamp Column | What It Tells Us |
|--------|------------------|------------------|
| `snapshots` | `checkpoint_time` | When last checkpoint completed |
| `snapshots` | `last_archived_time` | When WAL was last archived |
| `snapshots` | `last_failed_time` | When archiving last failed |
| `activity_samples_*` | `query_start` | When each query began |
| `activity_samples_*` | `xact_start` | When each transaction began |
| `activity_samples_*` | `backend_start` | When each session connected |
| `activity_samples_*` | `state_change` | When state last changed |

### Example: Incident Investigation

```sql
-- 1. Start with what_happened_at for the reported time
SELECT * FROM flight_recorder.what_happened_at('2024-01-15 10:23:47');

-- 2. If lock contention detected, check the timeline
SELECT * FROM flight_recorder.incident_timeline(
    '2024-01-15 10:20:00',
    '2024-01-15 10:30:00'
)
WHERE event_type IN ('lock_contention', 'query_started');

-- 3. Use the recommendations array for next steps
SELECT unnest(recommendations) AS recommendation
FROM flight_recorder.what_happened_at('2024-01-15 10:23:47');
```

### Example Output

```sql
SELECT * FROM flight_recorder.what_happened_at('2024-01-15 10:23:47');
```

```
 requested_time      | 2024-01-15 10:23:47+00
 sample_before       | 2024-01-15 10:21:00+00
 sample_after        | 2024-01-15 10:24:00+00
 snapshot_before     | 2024-01-15 10:20:00+00
 snapshot_after      | 2024-01-15 10:25:00+00
 est_connections_active | 42
 est_xact_rate       | 156.3
 events              | [{"type":"checkpoint","time":"2024-01-15 10:23:15","offset_secs":-32}]
 sessions_active     | 38
 long_running_queries | 2
 lock_contention_detected | true
 blocked_sessions    | 3
 confidence          | high
 confidence_score    | 0.87
 data_quality_notes  | {"Checkpoint at 10:23:15 provides anchor","Sample gap is 180 seconds"}
 recommendations     | {"Review checkpoint impact","Investigate 3 blocked sessions"}
```

## Blast Radius Analysis

Blast radius analysis provides comprehensive impact assessment of database incidents. When something goes wrong, it answers: "What was the collateral damage?"

### Overview

| Function | Purpose |
|----------|---------|
| `blast_radius(start_time, end_time)` | Structured impact assessment with metrics |
| `blast_radius_report(start_time, end_time)` | Human-readable ASCII-formatted report |

### Impact Categories

Blast radius analysis examines multiple impact dimensions:

| Category | Metrics |
|----------|---------|
| Lock Impact | Blocked sessions, max/avg duration, lock types |
| Query Degradation | Queries slowed >50% vs baseline |
| Connection Impact | Before/during comparison, increase percentage |
| Application Impact | Affected apps grouped by blocked count |
| Wait Events | Top waits with percentage increase |
| Transaction Throughput | TPS before vs during |

### Severity Classification

Overall severity is the highest individual severity across all categories:

| Category | low | medium | high | critical |
|----------|-----|--------|------|----------|
| Blocked sessions | 1-5 | 6-20 | 21-50 | >50 |
| Max block duration | <10s | 10-60s | 1-5min | >5min |
| Connection increase | <25% | 25-50% | 50-100% | >100% |
| TPS decrease | <10% | 10-25% | 25-50% | >50% |
| Degraded queries | 1-3 | 4-10 | 11-25 | >25 |

### Basic Usage

```sql
-- Get structured blast radius analysis
SELECT * FROM flight_recorder.blast_radius(
    '2024-01-15 10:23:00',
    '2024-01-15 10:35:00'
);

-- Get formatted report for postmortem
SELECT flight_recorder.blast_radius_report(
    '2024-01-15 10:23:00',
    '2024-01-15 10:35:00'
);
```

### Return Columns

The `blast_radius()` function returns:

| Column | Type | Description |
|--------|------|-------------|
| incident_start | TIMESTAMPTZ | Start of analysis window |
| incident_end | TIMESTAMPTZ | End of analysis window |
| duration_seconds | NUMERIC | Incident duration |
| blocked_sessions_total | INTEGER | Total unique blocked sessions |
| blocked_sessions_max_concurrent | INTEGER | Max blocked at once |
| max_block_duration | INTERVAL | Longest single block |
| avg_block_duration | INTERVAL | Average block time |
| lock_types | JSONB | Lock type breakdown |
| degraded_queries_count | INTEGER | Queries slowed >50% |
| degraded_queries | JSONB | Details of degraded queries |
| connections_before | INTEGER | Baseline connections |
| connections_during_avg | INTEGER | Average during incident |
| connections_during_max | INTEGER | Peak connections |
| connection_increase_pct | NUMERIC | Connection growth |
| affected_applications | JSONB | Apps with blocked sessions |
| top_wait_events | JSONB | Top waits during incident |
| tps_before | NUMERIC | Baseline TPS |
| tps_during | NUMERIC | TPS during incident |
| tps_change_pct | NUMERIC | TPS change (negative = drop) |
| severity | TEXT | low/medium/high/critical |
| impact_summary | TEXT[] | Human-readable impact bullets |
| recommendations | TEXT[] | Actionable recommendations |

### Example Output

```sql
SELECT * FROM flight_recorder.blast_radius(
    '2024-01-15 10:23:00',
    '2024-01-15 10:35:00'
);
```

```
 incident_start          | 2024-01-15 10:23:00
 incident_end            | 2024-01-15 10:35:00
 duration_seconds        | 720
 blocked_sessions_total  | 47
 blocked_sessions_max    | 23
 max_block_duration      | 00:08:32
 avg_block_duration      | 00:02:45
 lock_types              | [{"type":"relation","count":42},{"type":"tuple","count":5}]
 degraded_queries_count  | 8
 degraded_queries        | [{"queryid":123,"query_preview":"SELECT...","baseline_ms":0.4,...}]
 connections_before      | 42
 connections_during_avg  | 87
 connections_during_max  | 98
 connection_increase_pct | 107
 affected_applications   | [{"app_name":"web-server","blocked_count":43}]
 top_wait_events         | [{"wait_type":"Lock","wait_event":"relation","total_count":156}]
 tps_before              | 1200
 tps_during              | 340
 tps_change_pct          | -72
 severity                | critical
 impact_summary          | {"47 sessions blocked (max 8m32s)","TPS dropped 72%"}
 recommendations         | {"Review blocking query","Consider lock_timeout"}
```

### Report Output

```sql
SELECT flight_recorder.blast_radius_report(
    '2024-01-15 10:23:00',
    '2024-01-15 10:35:00'
);
```

```
══════════════════════════════════════════════════════════════════════
                    BLAST RADIUS ANALYSIS REPORT
══════════════════════════════════════════════════════════════════════
Time Window: 2024-01-15 10:23:00 → 10:35:00 (12 minutes)
Severity: ██████████ CRITICAL

──────────────────────────────────────────────────────────────────────
LOCK IMPACT
──────────────────────────────────────────────────────────────────────
  Total blocked sessions:     47
  Max concurrent blocked:     23
  Longest block duration:     8m 32s
  Average block duration:     2m 45s

  Lock types:
    relation     ████████████████████ 42
    tuple        ███                   5

──────────────────────────────────────────────────────────────────────
AFFECTED APPLICATIONS
──────────────────────────────────────────────────────────────────────
  web-server       ████████████████████ 43 blocked
  api-service      ██████               12 blocked
  background-job   ██                    4 blocked

──────────────────────────────────────────────────────────────────────
QUERY DEGRADATION (8 queries slowed >50%)
──────────────────────────────────────────────────────────────────────
  SELECT * FROM users WHERE...     0.4ms → 1.8ms  (+350%)
  INSERT INTO posts...             0.2ms → 0.6ms  (+180%)

──────────────────────────────────────────────────────────────────────
RESOURCE IMPACT
──────────────────────────────────────────────────────────────────────
  Connections:  42 → 87 avg (98 max)  +107%
  Throughput:   1200 TPS → 340 TPS    -72%

──────────────────────────────────────────────────────────────────────
RECOMMENDATIONS
──────────────────────────────────────────────────────────────────────
  • Review the blocking query that held locks for 8+ minutes
  • Consider setting lock_timeout to prevent long waits
  • Investigate connection pool sizing (reached 98 connections)

══════════════════════════════════════════════════════════════════════
```

### Use Cases

1. **Incident Postmortem**: Generate comprehensive report for post-incident review
2. **Impact Assessment**: Quantify collateral damage during outages
3. **Capacity Planning**: Identify connection pool and throughput limits
4. **Application Mapping**: Discover which apps were affected by lock contention

## Visual Timeline

Visual timeline functions provide ASCII-based visualization of metrics, enabling quick pattern recognition in terminal-based reports.

### Overview

Visual timeline includes four functions:

| Function | Purpose |
|----------|---------|
| `_sparkline(numeric[])` | Compact Unicode sparkline from numeric array |
| `_bar(value, max)` | Horizontal progress bar |
| `timeline(metric, duration)` | Full ASCII chart with Y-axis and time labels |
| `sparkline_metrics(duration)` | Summary table with sparkline trends |

### Sparklines

Sparklines are compact inline charts using Unicode block characters (▁▂▃▄▅▆▇█):

```sql
-- Basic sparkline from array
SELECT flight_recorder._sparkline(ARRAY[1,2,4,8,4,2,1,2,4,8]);
-- Returns: ▁▂▃█▃▂▁▂▃█

-- With custom width (samples if array is larger)
SELECT flight_recorder._sparkline(ARRAY[1,2,3,4,5,6,7,8,9,10], 5);
-- Returns: ▁▃▄▆█
```

**Edge cases handled:**

- NULL array → empty string
- Empty array → empty string
- All-NULL values → empty string
- Constant values → middle-height bars (▄▄▄▄)
- Mixed NULL values → space character for NULLs

### Progress Bars

Horizontal progress bars show filled/empty portions:

```sql
-- 75% progress bar
SELECT flight_recorder._bar(75, 100, 20);
-- Returns: ███████████████░░░░░

-- 0% and 100%
SELECT flight_recorder._bar(0, 100, 10);   -- ░░░░░░░░░░
SELECT flight_recorder._bar(100, 100, 10); -- ██████████
```

### Timeline Charts

Full ASCII charts with Y-axis labels and time markers:

```sql
SELECT flight_recorder.timeline('connections', '2 hours');
```

**Example output:**

```
connections (last 2 hours)
    87 ┤          ╭────╮
    72 ┤    ╭─────╯    ╰──╮
    58 ┤╭───╯             ╰──
    43 ┼╯
       └───────┬───────┬───────
            14:00   15:00   16:00
```

**Supported metrics:**

| Alias | Column | Description |
|-------|--------|-------------|
| `connections` | `connections_active` | Active database connections |
| `connections_total` | `connections_total` | Total connections |
| `wal` | `wal_bytes` | WAL bytes generated |
| `temp` | `temp_bytes` | Temp file bytes |
| `commits` | `xact_commit` | Committed transactions |
| `rollbacks` | `xact_rollback` | Rolled back transactions |
| `blks_read` | `blks_read` | Blocks read from disk |
| `blks_hit` | `blks_hit` | Blocks found in cache |
| `db_size` | `db_size_bytes` | Database size |

**Parameters:**

```sql
flight_recorder.timeline(
    p_metric TEXT,             -- Metric name or alias
    p_duration INTERVAL DEFAULT '4 hours',
    p_width INTEGER DEFAULT 60,
    p_height INTEGER DEFAULT 10
)
```

### Sparkline Metrics Summary

Get an at-a-glance summary with sparkline trends for key metrics:

```sql
SELECT * FROM flight_recorder.sparkline_metrics('1 hour');
```

**Example output:**

```
     metric      │ current_value │     trend      │ min_value │ max_value
─────────────────┼───────────────┼────────────────┼───────────┼───────────
 connections_active │ 87          │ ▁▂▃▅▇█▆▄▃▂    │ 43        │ 87
 cache_hit_ratio │ 98.2%        │ █████████▇    │ 97.1%     │ 99.0%
 wal_bytes       │ 1.2 GB       │ ▃▃▃▄▅▇█▅▄▃    │ 0.8 GB    │ 1.5 GB
 temp_bytes      │ 0 B          │ ▁▁▁▁▁▁▁▁▁▁    │ 0 B       │ 0 B
 xact_commit     │ 15234        │ ▂▃▄▅▆▇█▇▆▅    │ 12000     │ 18000
 db_size_bytes   │ 2.5 GB       │ ▁▁▁▁▁▁▁▂▂▂    │ 2.4 GB    │ 2.5 GB
```

**Metrics included:**

- `connections_active` - Active connections
- `cache_hit_ratio` - Computed from blks_hit/(blks_hit+blks_read)
- `wal_bytes` - WAL generated
- `temp_bytes` - Temp file usage
- `xact_commit` - Committed transactions
- `db_size_bytes` - Database size

### Use Cases

**Quick health check:**

```sql
SELECT * FROM flight_recorder.sparkline_metrics('1 hour');
```

**Incident investigation:**

```sql
-- What did connections look like during the incident?
SELECT flight_recorder.timeline('connections', '4 hours');
```

**Capacity monitoring:**

```sql
-- Database growth trend
SELECT flight_recorder.timeline('db_size', '24 hours');
```

**Progress bars in reports:**

```sql
SELECT
    'Connections' AS resource,
    connections_active || '/' || connections_max AS usage,
    flight_recorder._bar(connections_active, connections_max) AS utilization
FROM flight_recorder.snapshots
ORDER BY captured_at DESC
LIMIT 1;
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

## Code Browser

An interactive HTML code browser is available at:

**<https://dventimisupabase.github.io/pg-flight-recorder/>**

Generated using GNU Global with Universal CTags, it provides:

- Symbol definitions and cross-references
- File browser with syntax highlighting
- Full-text search across the codebase

The code browser is automatically updated on every push to main.

### Local Code Navigation

For local development, use the `./tools/gg` wrapper:

```bash
# Set up the GTAGS database (choose one method)
./tools/setup-gtags              # requires global + universal-ctags
./tools/setup-gtags --download   # download pre-built from GitHub Actions
./tools/setup-gtags --docker     # use Docker (no local install needed)

# Navigate code
./tools/gg def snapshots         # find symbol definition
./tools/gg grep take_snapshot    # search file contents
./tools/gg ctx install.sql:100   # show context around line
```
