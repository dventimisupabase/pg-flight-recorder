# Future Directions for pg-flight-recorder

Ideas for future development beyond the v2.x feature set.

---

## Feature Ideas

### Query Fingerprinting Improvements

Group similar queries more intelligently for better analysis:

- Normalize query parameters beyond `$1`/`$2` placeholders
- Detect query families (same structure, different literals)
- Track query evolution over time (how did this query change?)

### Alerting Integrations

Webhook support for external alerting systems:

- Slack notifications for storms, regressions, forecast alerts
- PagerDuty integration for critical severity events
- Generic webhook endpoint for custom integrations
- Email digest of daily/weekly performance summaries

### Data Retention Policies

Configurable auto-cleanup of historical data:

- Per-table retention periods (samples vs archives)
- Automatic archival to external storage
- Compression of old snapshots
- GDPR-compliant data purging

### Export/Import

Dump flight recorder data for offline analysis:

- Export time range to JSON/CSV for external tools
- Import historical data from backups
- Portable format for sharing incident data in postmortems
- Integration with observability platforms (Grafana, Datadog)

### Comparison Mode

Compare two time periods side-by-side:

```sql
SELECT * FROM flight_recorder.compare(
    '2025-01-15 10:00', '2025-01-15 11:00',  -- Period A
    '2025-01-14 10:00', '2025-01-14 11:00'   -- Period B
);
```

- Diff query performance between periods
- Highlight what changed (new queries, missing queries, degraded queries)
- Useful for "what changed after the deploy?"

### Interactive REPL Mode

A guided diagnostic session:

```sql
SELECT flight_recorder.diagnose();
-- Interactive prompts:
-- "When did the problem start?"
-- "What symptoms are you seeing?"
-- "Let me check for..."
```

- Walks through common diagnostic steps
- Asks clarifying questions
- Builds a diagnosis report

---

## Infrastructure Ideas

### GitHub Discussions

Enable community feedback and Q&A:

- Feature requests from users
- Troubleshooting help
- Share diagnostic patterns

### Issue Templates

Structured templates for bug reports and feature requests:

- Bug report: version, PostgreSQL version, steps to reproduce
- Feature request: use case, proposed solution

### Demo Environment

Quick-start playground for evaluation:

- Docker Compose with pre-loaded sample data
- Simulated incidents to explore
- Tutorial walkthroughs

### Performance Benchmarks

Automated overhead measurement:

- CI job that measures collection overhead
- Track overhead trends across versions
- Regression testing for performance

---

## Long-term Vision

### Fleet-Wide Analysis (from ADVANCED_FEATURE_IDEAS.md)

The one unimplemented feature from the original roadmap. Requires infrastructure beyond SQL:

- Central metrics aggregator service
- Anonymous, opt-in metric collection
- "Is this normal?" comparisons across similar databases
- Fleet-wide anomaly detection

This would require:

- Separate API service (Node.js/Python)
- TimescaleDB or similar for time-series storage
- Privacy-first design (no query text, no PII)
- Explicit opt-in per database

---

## Contributing

Have an idea? Open a GitHub issue or discussion to propose new features.

---

**Last Updated**: 2026-01-30
