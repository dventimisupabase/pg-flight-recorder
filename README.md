# pg-flight-recorder

**"What was happening in my database?"**

Server-side flight recorder for PostgreSQL. Runs automatically via pg_cron. Zero config.

## Install

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

## Performance Impact

**Default overhead: ~0.5% CPU** (3-minute sampling)

Built for production with automatic safety controls (circuit breaker, adaptive mode, timeouts). See [REFERENCE.md](REFERENCE.md) for detailed performance characteristics and tuning options.

## Set and Forget

Safe for always-on monitoring. Before enabling:

```sql
-- Run preflight check (one-time setup validation)
SELECT * FROM flight_recorder.preflight_check();
```

After installation, run quarterly health checks:

```sql
-- Run every 3 months (takes ~1 second)
SELECT * FROM flight_recorder.quarterly_review();
```

These affordances make safety validation a no-brainer.

## Uninstall

```bash
psql -f uninstall.sql
```

## Testing

Run tests locally with Docker:

```bash
# Test on PostgreSQL 16 (default)
./test.sh

# Test on specific version
./test.sh 15   # PostgreSQL 15
./test.sh 17   # PostgreSQL 17
./test.sh 18   # PostgreSQL 18

# Test on all versions (15, 16, 17, 18)
./test.sh all
```

Or test against your own PostgreSQL 15+ instance with pg_cron and pgTAP installed:

```bash
psql -f install.sql
psql -c "CREATE EXTENSION pgtap;"
pg_prove -U postgres -d postgres tests/flight_recorder_test.sql
```

## Documentation

See [REFERENCE.md](REFERENCE.md) for complete documentation including:
- All analysis functions and views
- Performance tuning and configuration
- Safety mechanisms and troubleshooting
- Advanced features
