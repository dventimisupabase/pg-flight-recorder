# pg-flight-recorder

[![Test Suite](https://github.com/dventimisupabase/pg-flight-recorder/actions/workflows/test.yml/badge.svg)](https://github.com/dventimisupabase/pg-flight-recorder/actions/workflows/test.yml)
[![Lint](https://github.com/dventimisupabase/pg-flight-recorder/actions/workflows/lint.yml/badge.svg)](https://github.com/dventimisupabase/pg-flight-recorder/actions/workflows/lint.yml)

**"What was happening in my database?"**

Server-side flight recorder for PostgreSQL. Runs automatically via pg_cron. Zero config.

**Records:** Query activity, wait events, lock conflicts, connection stats, and performance metrics.

## Install

**PostgreSQL 15+** (requires pg_cron):

```bash
psql -f install.sql
```

## Use

It runs automatically. When you need answers:

```sql
SELECT flight_recorder.report('1 hour');
```

Paste the output into your AI assistant of choice, or read it yourself. The report includes anomalies, wait events, query performance, lock contention, configuration changes, and more.

## Is It Safe?

**Yes.** ~0.02% CPU overhead. 23-32ms per collection.

- Validated on resource-constrained hardware (Supabase Micro: 2 core, 1GB)
- Built-in protection: load shedding, circuit breakers, fast timeouts
- 0% DDL blocking across 202 operations in testing

Safe for staging (always-on), production troubleshooting, and production always-on (test in staging first).

## Uninstall

```bash
psql -f uninstall.sql
```

## More Info

See [REFERENCE.md](REFERENCE.md) for all functions, views, profiles, and configuration options.
