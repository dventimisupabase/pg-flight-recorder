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

## Performance Impact

**Default overhead: ~0.8% CPU**

Built for production with automatic safety controls (circuit breaker, adaptive mode, timeouts). See [REFERENCE.md](REFERENCE.md) for detailed performance characteristics and tuning options.

## Uninstall

```bash
psql -f uninstall.sql
```

## Documentation

See [REFERENCE.md](REFERENCE.md) for complete documentation including:
- All analysis functions and views
- Performance tuning and configuration
- Safety mechanisms and troubleshooting
- Advanced features
