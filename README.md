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
SELECT flight_recorder.set_mode('light');      -- Sample every 3 min (same as normal)
SELECT flight_recorder.set_mode('emergency');  -- Sample every 5 min (40% less overhead)
SELECT flight_recorder.disable();              -- Stop completely (use this if overhead is a concern)
SELECT flight_recorder.enable();               -- Resume
```

Modes adjust sampling frequency:
- **Normal**: Every 3 minutes (default, recommended)
- **Light**: Every 3 minutes (same as normal)
- **Emergency**: Every 5 minutes (40% less overhead)

## Is It Safe?

**Yes.** Measured overhead: **~0.02% CPU** on production systems.

**Validated on:**
- MacBook Pro (M-series): 23ms per collection
- Supabase Micro (2 core, 1GB RAM): 32ms per collection
- **32ms every 3 minutes = negligible impact**

**DDL operations (ALTER TABLE, CREATE INDEX, etc.):**
- Tested 202 DDL operations on Supabase
- **0% blocking** - no DDL delays observed
- Safe for high-DDL workloads

**Safe for production:**
- ✓ Staging and development (always-on)
- ✓ Production troubleshooting (enable during incidents)
- ✓ Production always-on (test in staging first)

**One-command disable if needed:**
```sql
SELECT flight_recorder.disable();  -- Stop immediately
```

**Built-in protection:**
- Skips collection when system is under load
- Automatic circuit breaker if collections run slow
- Fast lock timeout (100ms) - fails fast, doesn't block

See [REFERENCE.md](REFERENCE.md) for detailed measurements and safety analysis.

## Health Checks

Optional but recommended:

```sql
-- Before you start (one time)
SELECT * FROM flight_recorder.preflight_check();

-- Every 3 months (takes 1 second)
SELECT * FROM flight_recorder.quarterly_review();
```

That's it. No maintenance required.

## Uninstall

```bash
psql -f uninstall.sql
```

## For Developers

Want to modify or contribute? Run the test suite:

```bash
./test.sh     # PostgreSQL 16
./test.sh all # All versions (15, 16, 17)
```

## Need More?

See [REFERENCE.md](REFERENCE.md) for full documentation.
