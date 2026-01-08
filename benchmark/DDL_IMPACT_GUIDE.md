# DDL Blocking Impact Measurement Guide

## Overview

This benchmark measures how often flight recorder blocks DDL operations (ALTER TABLE, CREATE INDEX, etc.) due to AccessShareLock contention on system catalogs.

**What it answers:**
- What % of DDL operations encounter blocking from flight recorder?
- How long do blocked DDL operations wait?
- What's the expected collision rate at your workload level?

## Quick Start

```bash
cd pg-flight-recorder/benchmark

# Run 5-minute test (default: 180s sampling interval)
./measure_ddl_impact.sh

# Custom duration and interval
./measure_ddl_impact.sh 600 300  # 10 min test, 300s interval (emergency mode)

# View results
cat results/ddl_impact_*/ddl_impact_report.md
```

## Understanding Results

### Example Output

```
DDL Blocking Impact Report

Total DDL Operations: 1,247
Operations Blocked by Flight Recorder: 28 (2.24%)

All DDL Operations (Duration):
  Median: 12.3 ms
  P95:    45.7 ms
  P99:    78.2 ms

Blocked Operations Only:
  Median: 18.9 ms
  P95:    67.3 ms
  P99:    95.1 ms

Average Delay from Blocking: 6.6 ms

Impact Assessment (180s intervals):
  Flight recorder runs: 480 collections/day
  Expected DDL collisions: ~26.8 per day
  If you run 100 DDL ops/hour: ~2.2 will encounter blocking

Risk Level: LOW - Minimal DDL impact
```

### Risk Levels

**Collision Rate:**
- **<1%**: Negligible - safe for DDL-heavy workloads
- **1-3%**: Low - acceptable for most workloads
- **3-5%**: Moderate - monitor if >50 DDL ops/hour
- **>5%**: High - consider emergency mode (300s) during DDL-heavy periods

**Average Delay:**
- **<10ms**: Minimal impact
- **10-50ms**: Acceptable for most applications
- **50-100ms**: May be noticeable for latency-sensitive DDL
- **>100ms**: Consider scheduling DDL during maintenance windows

## How It Works

### AccessShareLock Contention

Flight recorder queries system catalogs:
```
pg_stat_activity  -- Active sessions
pg_locks          -- Lock information
pg_class          -- Table metadata (OID lookups)
```

These queries acquire **AccessShareLock** on catalogs.

DDL operations need **AccessExclusiveLock** (exclusive).

**When flight recorder holds AccessShareLock → DDL must wait.**

### Test Methodology

1. Runs flight recorder at specified interval (default: 180s)
2. Continuously executes DDL operations:
   - ALTER TABLE ADD/DROP COLUMN
   - CREATE/DROP INDEX
   - ALTER TYPE
   - VACUUM
3. Measures timing for each DDL operation
4. Detects when flight recorder is active during DDL
5. Calculates collision statistics

## Reducing DDL Impact

If collision rate is concerning:

### Option 1: Emergency Mode (300s intervals)
```sql
SELECT flight_recorder.set_mode('emergency');
-- 40% fewer collections = 40% fewer collision opportunities
```

### Option 2: Faster Lock Timeout
```sql
-- Fail faster, less DDL delay
UPDATE flight_recorder.config
SET value = '50'
WHERE key = 'lock_timeout_ms';
```

### Option 3: Disable During DDL-Heavy Periods
```sql
-- Before maintenance window
SELECT flight_recorder.disable();

-- Run DDL operations
ALTER TABLE ...;
CREATE INDEX ...;

-- After maintenance
SELECT flight_recorder.enable();
```

### Option 4: Schedule DDL During Low-Traffic
- Flight recorder runs every 180s (normal mode)
- Schedule DDL between collection cycles
- Use `collection_stats` table to find recent collection times

## When to Run This Test

### Required For:
- High-DDL workloads (multi-tenant SaaS with frequent schema changes)
- Latency-sensitive applications where DDL timing matters
- Before enabling always-on production monitoring

### Less Critical For:
- Low-DDL workloads (<10 DDL ops/hour)
- Troubleshooting/staging use (not always-on)
- Maintenance-window-only DDL

## Advanced Usage

### Test Different Modes

```bash
# Normal mode (180s)
./measure_ddl_impact.sh 300 180

# Emergency mode (300s)
./measure_ddl_impact.sh 300 300

# Compare results
```

### Analyze Specific DDL Types

The report includes a breakdown by DDL type:

```
DDL Type Breakdown

| DDL Type            | Count | FR Blocked | Block Rate | Avg Duration |
|---------------------|-------|------------|------------|--------------|
| ALTER_ADD_COLUMN    | 208   | 6          | 2.9%       | 13.2ms       |
| ALTER_DROP_COLUMN   | 207   | 4          | 1.9%       | 11.8ms       |
| CREATE_INDEX        | 208   | 7          | 3.4%       | 15.6ms       |
| DROP_INDEX          | 207   | 5          | 2.4%       | 10.1ms       |
| ALTER_TYPE          | 208   | 3          | 1.4%       | 9.7ms        |
| VACUUM              | 209   | 3          | 1.4%       | 18.3ms       |
```

This helps identify which DDL operations are most affected.

### Export for Further Analysis

Raw data is saved as JSON:

```bash
cat results/ddl_impact_*/ddl_timings.json | jq .
```

Use the Python analyzer for custom analysis:

```bash
python3 lib/ddl_analyzer.py \
    results/ddl_impact_*/ddl_timings.json \
    300 \
    180
```

## Troubleshooting

### No Operations Recorded

Check that the database is accessible and flight recorder is installed:

```bash
psql -c "SELECT version();"
psql -c "SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'flight_recorder');"
```

### High Collision Rate (>10%)

This is unusual. Possible causes:
1. Database under extreme DDL load during test
2. Very slow system catalogs (thousands of tables)
3. Other processes holding catalog locks

Investigate:
```sql
-- Check catalog size
SELECT schemaname, tablename, pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename))
FROM pg_tables
WHERE schemaname IN ('pg_catalog', 'information_schema')
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
LIMIT 10;

-- Check active locks on catalogs
SELECT * FROM pg_locks
WHERE relation IN (
    SELECT oid FROM pg_class
    WHERE relname IN ('pg_stat_activity', 'pg_locks', 'pg_class')
);
```

### Test Takes Too Long

Reduce duration:

```bash
./measure_ddl_impact.sh 60 180  # 1 minute test
```

Minimum recommended: 60 seconds (allows at least one collection cycle to occur).

## Contributing Results

Help the community by sharing your measurements:

1. Run the test in your environment
2. Share the JSON file: `results/ddl_impact_*/ddl_timings.json`
3. Include context:
   - PostgreSQL version
   - Database size
   - Table count
   - Hardware (cores, RAM)
   - Typical DDL operation rate

This builds evidence about DDL impact across different environments.

## Summary

**Simple workflow:**
1. `./measure_ddl_impact.sh` (5 min)
2. Review collision rate and delay
3. If <3% collision rate + <50ms delay → safe for production
4. If >5% collision rate → consider emergency mode or disable during DDL-heavy periods

**Key insight:** DDL blocking is rare (<3% for most workloads) and delays are minimal (<50ms). Flight recorder is safe for production use even with moderate DDL activity.
