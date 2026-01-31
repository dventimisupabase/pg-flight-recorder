# pg-flight-recorder Database Analysis

You are helping the user analyze their PostgreSQL database using pg-flight-recorder's **Top-Down methodology**.

## Step 1: Determine Data Source

The skill supports two modes:

### Mode A: Direct Database Connection
If an argument is NOT provided, check for database connectivity:
- Look for `PGHOST` and `PGDATABASE` environment variables
- Or `DATABASE_URL`

### Mode B: Pre-exported SQLite File (Offline Analysis)
If an argument IS provided (e.g., `/flight-recorder ~/customer/flight_recorder.db`), use that SQLite file directly. This supports the workflow where:
1. A customer exports their data: `psql -At -c "SELECT flight_recorder.export_sql()" | sqlite3 flight_recorder.db`
2. They send you the SQLite file
3. You analyze it without direct database access

**If a file path is provided, skip Steps 1-2 and go directly to Step 3.**

## Step 2: Export Data to SQLite (Direct Connection Only)

Export flight recorder data to a local SQLite file for analysis:

```bash
psql -At -c "SELECT flight_recorder.export_sql('7 days')" | sqlite3 $SCRATCHPAD/flight_recorder.db
```

Use the scratchpad directory for the SQLite file. Handle these potential errors:
- Connection refused: Ask user to verify database is running and credentials are correct
- Extension not installed: Suggest running `CREATE EXTENSION pg_flight_recorder` or check if the schema exists
- No data: The extension may be installed but not yet collecting data

If the export fails, ask if the user has an existing SQLite export they'd like to analyze instead.

## Step 3: Bootstrap Context from SQLite

Once you have a SQLite file (either exported or provided), read the methodology context:

**Important:** Use the provided file path if one was given as an argument, otherwise use `$SCRATCHPAD/flight_recorder.db`.

```bash
sqlite3 $DB_PATH "SELECT * FROM _guide ORDER BY step"
```

```bash
sqlite3 $DB_PATH "SELECT * FROM _tables"
```

```bash
sqlite3 $DB_PATH "SELECT * FROM _glossary"
```

```bash
sqlite3 $DB_PATH "SELECT * FROM _examples ORDER BY category, name"
```

These tables teach you:
- `_guide`: The 8-step Top-Down analysis methodology
- `_tables`: What data tables are available and what they contain
- `_glossary`: PostgreSQL terminology and concepts
- `_examples`: Ready-to-run query templates organized by tier

Also check the export metadata to understand what you're working with:

```bash
sqlite3 $DB_PATH "SELECT * FROM _export_metadata"
```

## Step 4: Ask the User's Objective

Present these options and ask what they're trying to accomplish:

1. **Troubleshooting** - "Something is slow" or "We had an incident"
2. **Health Check** - "Is my database healthy?"
3. **Capacity Planning** - "Will I run out of connections/disk/memory?"
4. **Forecasting** - "What trends should I watch?"
5. **General Exploration** - "What's interesting in my data?"

## Step 5: Execute Top-Down Analysis

Follow the methodology from `_guide`. The key principle is **progressive refinement**:

### Tier 1 - Quick Status (always start here)
Run the Tier 1 queries from `_examples`. These give a high-level view in seconds.

Present findings conversationally. Look for:
- Pre-detected anomalies (query storms, regressions)
- Obvious issues in high-level metrics
- Time periods that stand out

### Tier 2 - Drill Down (on demand)
If Tier 1 reveals something interesting, ask: "I found X. Want me to dig deeper?"

Only run Tier 2 queries when the user wants to investigate a specific finding.

### Tier 3+ - Deep Analysis (only when needed)
Progress to raw samples, correlation analysis, and configuration review only when earlier tiers point to something specific.

**Philosophy**: Record comprehensively, present hierarchically. Don't dump raw data—guide the user through discoveries.

## Step 6: Deliver Insights

After analysis, provide:

1. **Summary**: What did we find? (2-3 key points)
2. **Recommendations**: What should the user do? (specific, actionable)
3. **Next Steps**: What to monitor or investigate further?

## Query Execution

Run SQLite queries using:

```bash
sqlite3 -header -column $DB_PATH "YOUR QUERY HERE"
```

For complex queries, use `.mode markdown` for better formatting:

```bash
sqlite3 $DB_PATH <<'EOF'
.mode markdown
.headers on
YOUR QUERY HERE;
EOF
```

## Important Notes

- The SQLite export contains data based on the time range used during export
- All timestamps are in UTC
- The `_examples` table has ready-to-run queries—use them as templates
- Focus on actionable insights, not raw data dumps
- Ask before drilling down—let the user guide the investigation
- For offline analysis, remind the user that you can't access live data or run additional exports
