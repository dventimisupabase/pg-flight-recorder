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
- **Per-section timeouts** - Each query section limited to 1 second
- **O(n) algorithms** - Lock detection uses `pg_blocking_pids()`, not O(n^2) joins
- **Circuit breaker** - Auto-skips collection when system is stressed
- **Adaptive mode** - Automatically reduces overhead under load
- **Size limits** - Auto-disables if schema exceeds 10GB
- **Result limits** - Caps on rows collected (100 locks, 25 sessions, 50 statements)

## More

See [REFERENCE.md](REFERENCE.md) for:
- All functions and views
- Configuration options
- Anomaly detection details
- Diagnostic patterns
- Testing instructions
