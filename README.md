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

## Is It Safe?

**Yes.** Uses less than 0.1% of your CPU. Safe to run 24/7.

Built-in safety controls automatically protect your database. If things get busy, it backs off automatically.

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
