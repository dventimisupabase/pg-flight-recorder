# pg-flight-recorder Analysis

You are helping the user analyze their PostgreSQL database using pg-flight-recorder.

## Quick Start

Run the comprehensive report:

```sql
SELECT flight_recorder.report('1 hour');
```

For longer time ranges:

```sql
SELECT flight_recorder.report('24 hours');
SELECT flight_recorder.report('7 days');
```

## Drilling Deeper

If the report reveals issues, use these functions:

| Function | Purpose |
|----------|---------|
| `anomaly_report(start, end)` | Detailed anomaly analysis |
| `table_hotspots(start, end)` | Table-level issues |
| `index_efficiency(start, end)` | Index usage analysis |
| `statement_compare(start, end)` | Query performance changes |
| `wait_summary(start, end)` | Wait event breakdown |

## Useful Views

| View | Purpose |
|------|---------|
| `deltas` | Snapshot-over-snapshot changes |
| `recent_waits` | Wait events (10-hour window) |
| `recent_activity` | Active sessions |
| `recent_locks` | Lock contention |
| `capacity_dashboard` | Resource utilization |

## Analysis Workflow

1. Start with `report()` for the time range of interest
2. Note any anomalies or concerning metrics
3. Drill down with specific functions as needed
4. Check views for current state
