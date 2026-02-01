# pg-flight-recorder

Server-side flight recorder for PostgreSQL. Answers "what was happening in my database?"

## Install

```bash
psql -f install.sql
```

Requires PostgreSQL 15+ with pg_cron.

## Use

```sql
SELECT flight_recorder.report('1 hour');
```

That's it. It runs automatically. The report tells you what happened.

## Uninstall

```bash
psql -f uninstall.sql
```

## Reference

See [REFERENCE.md](REFERENCE.md) for configuration, functions, and details.
