# pg-flight-recorder

[![Test Suite](https://github.com/dventimisupabase/pg-flight-recorder/actions/workflows/test.yml/badge.svg)](https://github.com/dventimisupabase/pg-flight-recorder/actions/workflows/test.yml)
[![Lint](https://github.com/dventimisupabase/pg-flight-recorder/actions/workflows/lint.yml/badge.svg)](https://github.com/dventimisupabase/pg-flight-recorder/actions/workflows/lint.yml)

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

## Optional: Configuration

**Most users:** Just install. The defaults work.

**Production tuning:**

```sql
SELECT flight_recorder.apply_profile('production_safe');
```

**Troubleshooting an incident:**

```sql
SELECT flight_recorder.apply_profile('troubleshooting');
```

**Need to stop it:**

```sql
SELECT flight_recorder.disable();
```

See [REFERENCE.md](REFERENCE.md) for all profiles and settings.

## Is It Safe?

**Yes.** ~0.02% CPU overhead. 23-32ms per collection every 3 minutes.

**Validated on resource-constrained hardware:**

- Supabase Micro (2 core, 1GB): 0% DDL blocking across 202 operations
- Built-in protection: load shedding, circuit breakers, fast timeouts

Safe for staging (always-on), production troubleshooting, and production always-on (test in staging first).

## Health Checks

Optional but recommended:

```sql
SELECT * FROM flight_recorder.preflight_check();  -- Before you start
SELECT * FROM flight_recorder.quarterly_review(); -- Every 3 months
```

## Uninstall

```bash
psql -f uninstall.sql
```

## For Developers

Run the test suite:

```bash
./test.sh     # PostgreSQL 16
./test.sh all # All versions (15, 16, 17)
```

See [REFERENCE.md](REFERENCE.md) for full documentation.
