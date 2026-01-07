# pg-flight-recorder

**"What was happening in my database?"**

Server-side flight recorder for PostgreSQL. Runs automatically via pg_cron. Zero config.

## Install

**Supabase:**
```bash
supabase link --project-ref <your-ref>
supabase db push
```

**PostgreSQL 15+** (requires pg_cron):
```bash
psql -f install.sql
```

## Use

It runs automatically. Query when you need answers:

```sql
-- What's happening now?
SELECT * FROM flight_recorder.recent_activity;

-- What happened during this slow period?
SELECT * FROM flight_recorder.compare('2024-01-15 10:00', '2024-01-15 11:00');

-- What were queries waiting on?
SELECT * FROM flight_recorder.wait_summary('2024-01-15 10:00', '2024-01-15 11:00');

-- Auto-detect problems
SELECT * FROM flight_recorder.anomaly_report('2024-01-15 10:00', '2024-01-15 11:00');
```

## Emergency Controls

```sql
SELECT flight_recorder.set_mode('emergency');  -- Reduce overhead
SELECT flight_recorder.disable();              -- Stop completely
SELECT flight_recorder.enable();               -- Resume
```

## Safety

Built for production with minimal observer effect:

- **UNLOGGED tables** - No WAL overhead for telemetry data
- **Per-section timeouts** - Each query section limited to 250ms
- **O(n) algorithms** - Lock detection uses `pg_blocking_pids()`, not O(n^2) joins
- **Circuit breaker** - Auto-skips collection when system is stressed
- **Adaptive mode** - Automatically reduces overhead under load
- **Size limits** - Auto-disables if schema exceeds 10GB
- **Result limits** - Caps on rows collected (50 locks, 25 sessions, 50 statements)

## Observer Effect

pg-flight-recorder has measurable overhead. Exact cost depends on configuration:

| Config | Sample Interval | Timeout/Section | Worst-Case CPU | Notes |
|--------|-----------------|-----------------|----------------|-------|
| **Default** | 120s | 250ms | 0.8% | Recommended for production |
| **High Resolution** | 60s | 250ms | 1.7% | Configurable - higher temporal resolution |
| **Light Mode** | 120s | 250ms | 0.7% | Disables progress tracking |
| **Emergency Mode** | 120s | 250ms | 0.5% | Disables locks and progress tracking |

Additional considerations:

- **Catalog locks**: Every collection acquires AccessShareLock on system catalogs (configurable to 1 lock per sample)
- **Lock timeout**: 100ms - fails fast if catalogs are locked by DDL operations
- **Memory**: 2MB work_mem per collection (configurable)
- **Storage**: ~2-3 GB for 7 days retention (UNLOGGED, no WAL overhead)
- **pg_stat_statements**: 20 queries × 96 snapshots/day = 1,920 rows/day (87% reduction from older versions)

### Reducing Overhead

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

### Advanced Optimizations (Opt-In)

Additional features available via configuration:

```sql
-- Reduce catalog locks from 3 to 1 per sample (snapshot-based collection)
UPDATE flight_recorder.config SET value = 'true' WHERE key = 'snapshot_based_collection';

-- Skip collection when system idle (adaptive sampling)
UPDATE flight_recorder.config SET value = 'true' WHERE key = 'adaptive_sampling';

-- Adjust sample interval (60s = higher resolution, 180s = lower overhead)
UPDATE flight_recorder.config SET value = '60' WHERE key = 'sample_interval_seconds';
```

See [REFERENCE.md](REFERENCE.md) for detailed configuration options.

### Target Environments

- ✓ Production troubleshooting (enable during incidents)
- ✓ Staging/dev (always-on monitoring)
- ⚠ High-DDL workloads (frequent CREATE/DROP/ALTER may cause catalog lock contention)
- ✗ Resource-constrained databases (< 2 CPU cores, < 4GB RAM)

## More

See [REFERENCE.md](REFERENCE.md) for:
- All functions and views
- Configuration options
- Anomaly detection details
- Diagnostic patterns
- Testing instructions
