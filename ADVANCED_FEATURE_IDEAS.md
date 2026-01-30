# Advanced Feature Ideas for Flight Recorder

**Purpose**: Explore advanced diagnostic and predictive capabilities that could be added to pg-flight-recorder.
**Status**: Partially Implemented - See status markers below
**Audience**: Future development, CSA feedback, prioritization discussions

---

## Overview

This document captures creative ideas for enhancing flight_recorder beyond basic monitoring. These features range from predictive analytics to visual representations, all designed to make database diagnosis faster and more proactive.

**Complexity Spectrum**:

- 🟢 **Low**: 4-8 hours implementation
- 🟡 **Medium**: 1-2 days implementation
- 🔴 **High**: 3-5 days implementation
- 🟣 **Very High**: 1+ weeks implementation

---

## Table of Contents

1. [Performance Forecasting / Predictive Alerts](#1-performance-forecasting--predictive-alerts) ✅ **DONE**
2. [Query Fingerprinting & Storm Detection](#2-query-fingerprinting--storm-detection) ✅ **DONE**
3. [Time-Travel Debugging](#3-time-travel-debugging-with-second-level-precision)
4. [Blast Radius Analysis](#4-blast-radius-analysis)
5. [Continuous Benchmarking / Canary Queries](#5-continuous-benchmarking--canary-queries) ✅ **DONE**
6. [Fleet-Wide Analysis](#6-fleet-wide-analysis--is-this-normal)
7. [Automatic Regression Detection](#7-automatic-performance-regression-detection) ✅ **DONE**
8. [Visual Performance Timeline](#8-visual-performance-timeline-ascii-art) ✅ **DONE**

---

## 1. Performance Forecasting / Predictive Alerts ✅ IMPLEMENTED

**Complexity**: 🔴 High (3-5 days)

> **Status**: ✅ **Fully implemented** in v2.14. Includes `_linear_regression()` helper for statistical analysis, `forecast()` for single metric prediction with depletion time, `forecast_summary()` for multi-metric dashboard with status classification, `check_forecast_alerts()` for scheduled pg_notify alerts, and configurable settings. See `REFERENCE.md` for usage.

### The Problem

By the time you see a problem, it's already affecting customers. Reactive monitoring means you're always behind.

### The Vision

```sql
SELECT * FROM flight_recorder.forecast('disk_space', interval '7 days');
```

**Returns**:

```
┌──────────────┬──────────────┬─────────────┬────────────────────┬──────────────┐
│ metric       │ current      │ forecast    │ estimated_depleted │ confidence   │
├──────────────┼──────────────┼─────────────┼────────────────────┼──────────────┤
│ disk_space   │ 45 GB free   │ 0 GB        │ 2025-01-24 14:23  │ 89%          │
│ connections  │ 85 / 100     │ 100 / 100   │ 2025-01-19 09:15  │ 72%          │
│ wal_growth   │ 2.1 GB/day   │ 4.8 GB/day  │ n/a               │ 65%          │
└──────────────┴──────────────┴─────────────┴────────────────────┴──────────────┘

⚠️ WARNING: Database will hit connection limit in ~18 hours
⚠️ WARNING: Disk will be full in ~6 days at current growth rate
```

### Forecasting Approaches

#### Linear Regression (Simplest)

```sql
CREATE OR REPLACE FUNCTION flight_recorder._forecast_linear(
    p_metric TEXT,
    p_lookback INTERVAL,
    p_forecast_window INTERVAL
)
RETURNS TABLE(
    current_value NUMERIC,
    forecast_value NUMERIC,
    forecast_at TIMESTAMPTZ,
    confidence NUMERIC
)
LANGUAGE plpgsql AS $$
DECLARE
    v_slope NUMERIC;
    v_intercept NUMERIC;
    v_r_squared NUMERIC;
BEGIN
    -- Simple linear regression: y = mx + b
    -- Using least squares method on historical data

    WITH data AS (
        SELECT
            EXTRACT(EPOCH FROM captured_at) AS x,
            CASE p_metric
                WHEN 'disk_space' THEN db_size_bytes
                WHEN 'connections' THEN connections_total
                WHEN 'wal_rate' THEN wal_bytes
                ELSE NULL
            END AS y
        FROM flight_recorder.snapshots
        WHERE captured_at > now() - p_lookback
            AND captured_at <= now()
        ORDER BY captured_at
    ),
    stats AS (
        SELECT
            count(*) AS n,
            avg(x) AS x_avg,
            avg(y) AS y_avg,
            sum((x - avg(x) OVER ()) * (y - avg(y) OVER ())) AS sum_xy,
            sum(power(x - avg(x) OVER (), 2)) AS sum_xx,
            sum(power(y - avg(y) OVER (), 2)) AS sum_yy
        FROM data
    )
    SELECT
        (SELECT y FROM data ORDER BY x DESC LIMIT 1) AS current_value,
        (v_slope * EXTRACT(EPOCH FROM now() + p_forecast_window) + v_intercept) AS forecast_value,
        now() + p_forecast_window AS forecast_at,
        v_r_squared AS confidence
    INTO current_value, forecast_value, forecast_at, confidence
    FROM stats;

    -- Calculate slope (m) and intercept (b)
    SELECT
        sum_xy / NULLIF(sum_xx, 0),
        y_avg - (sum_xy / NULLIF(sum_xx, 0)) * x_avg,
        power(sum_xy, 2) / NULLIF(sum_xx * sum_yy, 0)  -- R²
    INTO v_slope, v_intercept, v_r_squared
    FROM stats;

    RETURN NEXT;
END;
$$;
```

#### Exponential Smoothing (Better for Seasonal Data)

```sql
CREATE OR REPLACE FUNCTION flight_recorder._forecast_exponential(
    p_metric TEXT,
    p_lookback INTERVAL,
    p_forecast_window INTERVAL,
    p_alpha NUMERIC DEFAULT 0.3  -- Smoothing factor
)
RETURNS TABLE(
    current_value NUMERIC,
    forecast_value NUMERIC,
    trend NUMERIC
)
LANGUAGE plpgsql AS $$
-- Holt's linear trend method
-- Accounts for both level and trend
-- Better for metrics with consistent growth/decline patterns
$$;
```

### Metrics to Forecast

```sql
CREATE TABLE flight_recorder.forecasts (
    id              BIGSERIAL PRIMARY KEY,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    metric_name     TEXT NOT NULL,
    current_value   NUMERIC NOT NULL,
    forecast_value  NUMERIC NOT NULL,
    forecast_at     TIMESTAMPTZ NOT NULL,
    confidence      NUMERIC,
    method          TEXT,
    depleted_at     TIMESTAMPTZ,  -- When metric hits limit/threshold
    alert_triggered BOOLEAN DEFAULT false
);

CREATE INDEX forecasts_metric_created_idx ON flight_recorder.forecasts(metric_name, created_at DESC);
```

**Forecastable Metrics**:

1. **Disk Space** - When will disk be full?
2. **Connection Usage** - When will connection pool saturate?
3. **WAL Generation Rate** - Predicting checkpoint frequency
4. **Table Growth** - Individual table size trends
5. **Dead Tuple Accumulation** - Bloat prediction
6. **Average Query Time** - Performance degradation trends

### Alert Integration

```sql
CREATE OR REPLACE FUNCTION flight_recorder.check_forecast_alerts()
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    v_forecast RECORD;
    v_threshold INTERVAL;
BEGIN
    v_threshold := flight_recorder._get_config('forecast_alert_threshold', '3 days')::interval;

    -- Check recent forecasts for concerning trends
    FOR v_forecast IN
        SELECT *
        FROM flight_recorder.forecasts
        WHERE created_at > now() - interval '1 hour'
            AND alert_triggered = false
            AND depleted_at IS NOT NULL
            AND depleted_at < now() + v_threshold
    LOOP
        -- Trigger alert
        PERFORM flight_recorder._send_alert(
            'Predictive Alert: ' || v_forecast.metric_name,
            format('Resource will be depleted at %s (in %s)',
                   v_forecast.depleted_at,
                   v_forecast.depleted_at - now())
        );

        -- Mark as alerted
        UPDATE flight_recorder.forecasts
        SET alert_triggered = true
        WHERE id = v_forecast.id;
    END LOOP;
END;
$$;

-- Schedule via pg_cron
SELECT cron.schedule(
    'forecast-checks',
    '0 */4 * * *',  -- Every 4 hours
    'SELECT flight_recorder.check_forecast_alerts()'
);
```

### Use Cases

1. **Proactive Capacity Planning**
   - "Disk will be full in 6 days" → Schedule upgrade
   - "Connection pool saturating in 2 days" → Increase limits

2. **Budget Planning**
   - Forecast resource needs for finance team
   - "You'll need to upgrade to next tier in 3 months"

3. **Prevent Incidents**
   - Alert before problems occur
   - Give time to react vs. emergency response

### Challenges

- **Accuracy**: Simple linear regression fails for non-linear growth
- **Seasonality**: Weekday/weekend patterns, monthly cycles
- **Black Swans**: Can't predict sudden traffic spikes
- **False Positives**: Over-alerting reduces trust

### Recommended Approach

**Phase 1**: Simple linear forecasting for disk and connections
**Phase 2**: Add exponential smoothing for better accuracy
**Phase 3**: ML-based forecasting (if warranted)

---

## 2. Query Fingerprinting & Storm Detection ✅ IMPLEMENTED

**Complexity**: 🟡 Medium (1-2 days)

> **Status**: ✅ **Fully implemented** in v2.10-2.11. Includes storm detection, classification (RETRY_STORM, CACHE_MISS, SPIKE), severity levels (LOW/MEDIUM/HIGH/CRITICAL), correlation data, auto-resolution with anti-flapping, pg_notify alerts, dashboard view, and resolution workflow. See `REFERENCE.md` for usage.

### The Problem

One thousand slightly different queries are actually the *same* query pattern flooding the database. Current tools show them as separate queries.

### The Vision

```sql
SELECT * FROM flight_recorder.query_storms(interval '1 hour');
```

**Returns**:

```
┌────────────────────────────────────────┬────────┬────────────┬────────────┐
│ query_fingerprint                      │ count  │ % of load  │ storm_type │
├────────────────────────────────────────┼────────┼────────────┼────────────┤
│ SELECT * FROM users WHERE id = $1      │ 47,291 │ 62%        │ RETRY      │
│ UPDATE posts SET views = views + $1... │ 12,483 │ 18%        │ NORMAL     │
│ SELECT * FROM orders WHERE user_id=$1  │ 8,127  │ 11%        │ CACHE_MISS │
└────────────────────────────────────────┴────────┴────────────┴────────────┘

🌪️ STORM DETECTED: users-by-id query spiked 1,200% in last hour
   Baseline: 3,200/hr → Current: 47,291/hr
   First detected: 2025-01-17 14:23 UTC
   Likely cause: Retry storm or cache failure
```

### Implementation

#### Query Fingerprinting

PostgreSQL's `pg_stat_statements` already does this via `queryid`! We just need to leverage it better.

```sql
CREATE OR REPLACE FUNCTION flight_recorder.detect_query_storms(
    p_lookback INTERVAL DEFAULT '1 hour',
    p_threshold_multiplier NUMERIC DEFAULT 3.0
)
RETURNS TABLE(
    queryid             BIGINT,
    query_fingerprint   TEXT,
    recent_count        BIGINT,
    baseline_count      BIGINT,
    multiplier          NUMERIC,
    storm_type          TEXT,
    first_detected      TIMESTAMPTZ
)
LANGUAGE plpgsql AS $$
BEGIN
    RETURN QUERY
    WITH
    -- Recent activity
    recent AS (
        SELECT
            ss.queryid,
            ss.query_preview,
            sum(ss.calls) AS recent_calls
        FROM flight_recorder.statement_snapshots ss
        JOIN flight_recorder.snapshots s ON s.id = ss.snapshot_id
        WHERE s.captured_at > now() - p_lookback
        GROUP BY ss.queryid, ss.query_preview
    ),
    -- Historical baseline (same time period, previous day/week)
    baseline AS (
        SELECT
            ss.queryid,
            avg(ss.calls) AS baseline_calls
        FROM flight_recorder.statement_snapshots ss
        JOIN flight_recorder.snapshots s ON s.id = ss.snapshot_id
        WHERE s.captured_at BETWEEN (now() - interval '7 days' - p_lookback)
                                AND (now() - interval '7 days')
        GROUP BY ss.queryid
    )
    SELECT
        r.queryid,
        r.query_preview,
        r.recent_calls,
        COALESCE(b.baseline_calls, 0)::bigint,
        CASE
            WHEN COALESCE(b.baseline_calls, 0) > 0
            THEN round(r.recent_calls / b.baseline_calls, 1)
            ELSE NULL
        END,
        CASE
            WHEN r.recent_calls > COALESCE(b.baseline_calls, 0) * p_threshold_multiplier THEN
                CASE
                    WHEN r.query_preview LIKE '%RETRY%' OR r.query_preview LIKE '%FOR UPDATE%'
                        THEN 'RETRY_STORM'
                    WHEN r.recent_calls > COALESCE(b.baseline_calls, 0) * 10
                        THEN 'CACHE_MISS'
                    ELSE 'SPIKE'
                END
            ELSE 'NORMAL'
        END,
        (SELECT min(s.captured_at)
         FROM flight_recorder.statement_snapshots ss2
         JOIN flight_recorder.snapshots s ON s.id = ss2.snapshot_id
         WHERE ss2.queryid = r.queryid
           AND s.captured_at > now() - p_lookback
           AND ss2.calls > COALESCE(b.baseline_calls, 0) * p_threshold_multiplier
        )
    FROM recent r
    LEFT JOIN baseline b ON b.queryid = r.queryid
    WHERE r.recent_calls > COALESCE(b.baseline_calls, 0) * p_threshold_multiplier
    ORDER BY r.recent_calls DESC;
END;
$$;
```

#### Storm Classification

```sql
CREATE TYPE flight_recorder.storm_type AS ENUM (
    'RETRY_STORM',      -- Exponential backoff failure, retries flooding
    'CACHE_MISS',       -- Cache layer failed, all requests hit DB
    'BOT_ATTACK',       -- Unusual access pattern, high volume
    'SCAN_STORM',       -- Pagination gone wrong, many offset queries
    'LOCK_CONVOY',      -- Lock contention causing query pile-up
    'NORMAL'            -- High volume but expected
);
```

#### Storm Detection Logic

**Retry Storm Detection**:

- Same query called many times in short period
- Look for exponential growth pattern
- Often includes `FOR UPDATE` or similar locking

**Cache Miss Storm**:

- Sudden 10x+ spike in query volume
- Usually specific to a few query patterns
- Correlates with cache layer restart/failure

**Bot Attack**:

- Unusual query patterns
- High volume from specific application/user
- Often sequential ID scanning

### Storage

```sql
CREATE TABLE flight_recorder.query_storms (
    id                  BIGSERIAL PRIMARY KEY,
    detected_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    queryid             BIGINT NOT NULL,
    query_fingerprint   TEXT NOT NULL,
    storm_type          TEXT NOT NULL,
    recent_count        BIGINT NOT NULL,
    baseline_count      BIGINT NOT NULL,
    multiplier          NUMERIC,
    resolved_at         TIMESTAMPTZ,
    resolution_notes    TEXT
);

CREATE INDEX query_storms_detected_at_idx ON flight_recorder.query_storms(detected_at DESC);
CREATE INDEX query_storms_queryid_idx ON flight_recorder.query_storms(queryid);
```

### Auto-Detection & Alerting

```sql
CREATE OR REPLACE FUNCTION flight_recorder.auto_detect_storms()
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    v_storm RECORD;
BEGIN
    -- Detect storms
    FOR v_storm IN
        SELECT * FROM flight_recorder.detect_query_storms()
        WHERE storm_type != 'NORMAL'
    LOOP
        -- Check if already logged
        IF NOT EXISTS (
            SELECT 1 FROM flight_recorder.query_storms
            WHERE queryid = v_storm.queryid
                AND detected_at > now() - interval '1 hour'
                AND resolved_at IS NULL
        ) THEN
            -- Log new storm
            INSERT INTO flight_recorder.query_storms (
                queryid, query_fingerprint, storm_type,
                recent_count, baseline_count, multiplier
            ) VALUES (
                v_storm.queryid, v_storm.query_fingerprint, v_storm.storm_type,
                v_storm.recent_count, v_storm.baseline_count, v_storm.multiplier
            );

            -- Alert
            PERFORM flight_recorder._send_alert(
                'Query Storm Detected',
                format('Storm Type: %s\nQuery: %s\nVolume: %s (baseline: %s, %sx increase)',
                       v_storm.storm_type,
                       v_storm.query_fingerprint,
                       v_storm.recent_count,
                       v_storm.baseline_count,
                       v_storm.multiplier)
            );
        END IF;
    END LOOP;
END;
$$;

-- Schedule every 15 minutes
SELECT cron.schedule(
    'storm-detection',
    '*/15 * * * *',
    'SELECT flight_recorder.auto_detect_storms()'
);
```

### Use Cases

1. **Rapid Incident Response**
   - Customer: "Database is slow"
   - You: "Retry storm detected on user-lookup query, 1,200% increase"
   - Root cause identified in seconds

2. **Application Bug Detection**
   - Detect when app starts behaving abnormally
   - "Your background job is hammering the database"

3. **Cache Layer Monitoring**
   - Detect cache failures indirectly via DB load
   - "Redis went down at 10:23, all requests hitting DB"

### Advantages

- Leverages existing `pg_stat_statements` data (zero overhead)
- Simple statistical analysis (baseline comparison)
- Actionable alerts

---

## 3. Time-Travel Debugging with Second-Level Precision

**Complexity**: 🟡 Medium (1-2 days)

### The Problem

Flight recorder samples every 3 minutes. Customer says "at exactly 10:23:47 my query hung." You have no data for that specific second.

### The Vision

```sql
SELECT * FROM flight_recorder.what_happened_at('2025-01-17 10:23:47');
```

**Returns**:

```
🕐 2025-01-17 10:23:47 UTC

📊 Closest Samples:
  - 10:21:00 (2m 47s before) - Normal activity, 45 active queries
  - 10:24:00 (13s after)     - 47 blocked queries detected

🔍 Inferred State:
  - Lock contention started between 10:23:00 - 10:24:00
  - Blocking PID 1234 first appeared at 10:24:00 sample
  - Query: UPDATE users SET last_seen = now() WHERE...
  - Likely started ~10:23:30 (±30s) based on query duration

📈 System State at 10:23:47 (interpolated):
  - Active connections: 87 (±3)
  - Checkpoint status: In progress (started 10:23:12)
  - WAL write rate: 14.2 MB/s

💡 Recommendations:
  - Check application logs for requests around 10:23:30
  - Blocking query likely caused the hang
  - Checkpoint may have contributed to slowdown
```

### Implementation

#### Interpolation Between Samples

```sql
CREATE OR REPLACE FUNCTION flight_recorder.what_happened_at(
    p_timestamp TIMESTAMPTZ
)
RETURNS TABLE(
    analysis_timestamp      TIMESTAMPTZ,
    sample_before           TIMESTAMPTZ,
    sample_after            TIMESTAMPTZ,
    time_to_before          INTERVAL,
    time_to_after           INTERVAL,
    interpolated_state      JSONB,
    detected_events         JSONB,
    recommendations         TEXT[]
)
LANGUAGE plpgsql AS $$
DECLARE
    v_before RECORD;
    v_after RECORD;
    v_events JSONB := '[]'::jsonb;
    v_recommendations TEXT[] := ARRAY[]::TEXT[];
BEGIN
    -- Find surrounding samples
    SELECT * INTO v_before
    FROM flight_recorder.snapshots
    WHERE captured_at <= p_timestamp
    ORDER BY captured_at DESC
    LIMIT 1;

    SELECT * INTO v_after
    FROM flight_recorder.snapshots
    WHERE captured_at >= p_timestamp
    ORDER BY captured_at ASC
    LIMIT 1;

    -- Look for events with exact timestamps
    -- Checkpoints have precise timestamps!
    IF v_before.checkpoint_time IS NOT NULL
       AND v_before.checkpoint_time BETWEEN (p_timestamp - interval '5 minutes') AND p_timestamp THEN
        v_events := v_events || jsonb_build_object(
            'type', 'checkpoint',
            'timestamp', v_before.checkpoint_time,
            'offset_seconds', EXTRACT(EPOCH FROM (p_timestamp - v_before.checkpoint_time)),
            'description', 'Checkpoint was in progress'
        );
        v_recommendations := array_append(v_recommendations,
            'Checkpoint may have contributed to performance issues');
    END IF;

    -- Check for lock storms
    IF EXISTS (
        SELECT 1 FROM flight_recorder.lock_samples_ring l
        JOIN flight_recorder.samples_ring s ON s.slot_id = l.slot_id
        WHERE s.captured_at BETWEEN p_timestamp - interval '3 minutes'
                               AND p_timestamp + interval '3 minutes'
    ) THEN
        v_events := v_events || jsonb_build_object(
            'type', 'lock_contention',
            'description', 'Lock contention detected in nearby samples',
            'recommendation', 'Check lock_samples_archive for blocking queries'
        );
        v_recommendations := array_append(v_recommendations,
            'Investigate blocking queries around this time');
    END IF;

    -- Interpolate metrics
    RETURN QUERY SELECT
        p_timestamp,
        v_before.captured_at,
        v_after.captured_at,
        p_timestamp - v_before.captured_at,
        v_after.captured_at - p_timestamp,
        jsonb_build_object(
            'connections_active', flight_recorder._interpolate(
                v_before.connections_active,
                v_after.connections_active,
                EXTRACT(EPOCH FROM (p_timestamp - v_before.captured_at)),
                EXTRACT(EPOCH FROM (v_after.captured_at - v_before.captured_at))
            ),
            'wal_write_rate_mb_s', flight_recorder._interpolate(
                v_before.wal_bytes,
                v_after.wal_bytes,
                EXTRACT(EPOCH FROM (p_timestamp - v_before.captured_at)),
                EXTRACT(EPOCH FROM (v_after.captured_at - v_before.captured_at))
            ) / 1024 / 1024
        ),
        v_events,
        v_recommendations;
END;
$$;

-- Linear interpolation helper
CREATE OR REPLACE FUNCTION flight_recorder._interpolate(
    p_value_before NUMERIC,
    p_value_after NUMERIC,
    p_time_offset NUMERIC,
    p_time_span NUMERIC
)
RETURNS NUMERIC
LANGUAGE sql IMMUTABLE AS $$
    SELECT p_value_before + (p_value_after - p_value_before) * (p_time_offset / p_time_span);
$$;
```

#### Event Correlation

Events with **exact timestamps** can anchor the investigation:

1. **Checkpoint times** - `pg_stat_bgwriter` has precise checkpoint_time
2. **Autovacuum start/end** - `pg_stat_progress_vacuum` (if captured)
3. **Config changes** - `pg_settings` sourcefile modification time
4. **Statement start times** - `pg_stat_activity.query_start`

```sql
-- Find statements that were definitely running at p_timestamp
SELECT
    pid,
    usename,
    query_start,
    state,
    query
FROM flight_recorder.activity_samples_archive
WHERE query_start <= p_timestamp
    AND (state_change IS NULL OR state_change >= p_timestamp)
ORDER BY query_start;
```

### Probabilistic Timing

When exact timing is impossible, provide ranges:

```
🎯 Confidence Levels:
  - High (90%+): Event had exact timestamp (checkpoint, query_start)
  - Medium (70%): Interpolated between close samples (1-2 min apart)
  - Low (50%): Large gap between samples (3+ min apart)
```

### Use Cases

1. **Customer Says "It happened at exactly 10:23am"**
   - Show what was likely happening at that moment
   - Even without exact sample, give educated guess

2. **Correlate with Application Logs**
   - App logs: "Timeout at 10:23:47"
   - Flight recorder: "Lock contention started ~10:23:30"
   - Timeline aligns → root cause found

3. **Incident Timeline Reconstruction**
   - Build minute-by-minute narrative
   - "10:23:12 - Checkpoint started"
   - "10:23:30 - Long transaction began"
   - "10:23:47 - Locks started backing up"

### Limitations

- Can't capture events that happened between samples
- Interpolation assumes linear change (often wrong)
- Confidence decreases with sample interval

### Recommended Enhancements

- For critical time windows, reduce sample interval temporarily
- Capture high-resolution samples on anomaly detection
- Store pg_stat_activity snapshots more frequently

---

## 4. Blast Radius Analysis

**Complexity**: 🔴 High (3-5 days)

### The Problem

When one thing goes wrong, what else breaks? What's the collateral damage? Current tools show the primary issue but miss secondary effects.

### The Vision

```sql
SELECT * FROM flight_recorder.blast_radius(
    incident := 'PID 1234 held AccessExclusiveLock on users table',
    start_time := '2025-01-17 10:23:00',
    end_time := '2025-01-17 10:35:00'
);
```

**Returns**:

```
🎯 Primary Impact: PID 1234 held AccessExclusiveLock on users table
   Duration: 12 minutes (10:23:00 - 10:35:00)

📉 Secondary Effects:

  Blocked Queries:
    - 47 queries blocked on relation lock
    - Max wait time: 8m 32s
    - Queries: SELECT/UPDATE/INSERT on users table

  Performance Degradation:
    - Query "SELECT * FROM users WHERE..." slowed by 340%
      (baseline: 0.4s → during: 1.8s)
    - Query "INSERT INTO posts..." slowed by 180%
      (baseline: 0.2s → during: 0.56s)

  Table-Level Impact:
    - Table "users": 1,200 tps → 12 tps (-99%)
    - Table "posts": 450 tps → 380 tps (-16%)

  Resource Saturation:
    - Connection pool: 42 → 98 connections (+133%)
    - Active backends: 23 → 87 (+278%)

  Maintenance Disruption:
    - Autovacuum on "posts" blocked for 4m 12s
    - Autovacuum on "orders" blocked for 2m 48s

🔗 Affected Applications:
    - web-server: 43 connections affected (73 req/s → 4 req/s)
    - background-worker: 4 connections affected
    - api-service: 12 connections affected

💰 Estimated Customer Impact:
    - ~1,400 requests delayed by >5 seconds
    - ~87 requests timed out (>30s)
    - Peak p95 latency: 12.3s (baseline: 0.4s)
    - Affected users: ~240 (estimated from blocked sessions)

🔁 Cascading Effects:
    - Connection pool saturation caused new connection failures
    - Blocked autovacuum led to table bloat (+2.1 GB)
    - Long-running transactions prevented VACUUM from cleaning dead tuples
```

### Implementation

```sql
CREATE OR REPLACE FUNCTION flight_recorder.blast_radius(
    p_incident_description TEXT,
    p_start_time TIMESTAMPTZ,
    p_end_time TIMESTAMPTZ
)
RETURNS JSONB
LANGUAGE plpgsql AS $$
DECLARE
    v_result JSONB;
    v_blocked_queries INTEGER;
    v_degraded_queries JSONB;
    v_table_impacts JSONB;
    v_resource_changes JSONB;
BEGIN
    -- Initialize result structure
    v_result := jsonb_build_object(
        'incident', p_incident_description,
        'time_window', jsonb_build_object(
            'start', p_start_time,
            'end', p_end_time,
            'duration_seconds', EXTRACT(EPOCH FROM (p_end_time - p_start_time))
        )
    );

    -- 1. Count blocked queries
    SELECT count(DISTINCT blocked_pid) INTO v_blocked_queries
    FROM flight_recorder.lock_samples_archive
    WHERE captured_at BETWEEN p_start_time AND p_end_time;

    v_result := jsonb_set(v_result, '{blocked_queries,count}',
                         to_jsonb(v_blocked_queries));

    -- 2. Find degraded queries (compare before vs during)
    WITH
    baseline AS (
        SELECT
            queryid,
            query_preview,
            avg(mean_exec_time) AS baseline_time
        FROM flight_recorder.statement_snapshots ss
        JOIN flight_recorder.snapshots s ON s.id = ss.snapshot_id
        WHERE s.captured_at BETWEEN p_start_time - (p_end_time - p_start_time)
                                AND p_start_time
        GROUP BY queryid, query_preview
    ),
    during AS (
        SELECT
            queryid,
            query_preview,
            avg(mean_exec_time) AS during_time
        FROM flight_recorder.statement_snapshots ss
        JOIN flight_recorder.snapshots s ON s.id = ss.snapshot_id
        WHERE s.captured_at BETWEEN p_start_time AND p_end_time
        GROUP BY queryid, query_preview
    )
    SELECT jsonb_agg(
        jsonb_build_object(
            'query', d.query_preview,
            'baseline_ms', round(b.baseline_time::numeric, 1),
            'during_ms', round(d.during_time::numeric, 1),
            'slowdown_pct', round(100.0 * (d.during_time - b.baseline_time) / b.baseline_time, 1)
        )
    ) INTO v_degraded_queries
    FROM during d
    JOIN baseline b ON b.queryid = d.queryid
    WHERE d.during_time > b.baseline_time * 1.5  -- 50% slower
    ORDER BY (d.during_time - b.baseline_time) DESC
    LIMIT 10;

    v_result := jsonb_set(v_result, '{degraded_queries}',
                         COALESCE(v_degraded_queries, '[]'::jsonb));

    -- 3. Resource saturation changes
    WITH
    before AS (
        SELECT
            connections_active,
            connections_total
        FROM flight_recorder.snapshots
        WHERE captured_at < p_start_time
        ORDER BY captured_at DESC
        LIMIT 1
    ),
    during AS (
        SELECT
            avg(connections_active) AS avg_active,
            max(connections_total) AS max_total
        FROM flight_recorder.snapshots
        WHERE captured_at BETWEEN p_start_time AND p_end_time
    )
    SELECT jsonb_build_object(
        'connections', jsonb_build_object(
            'before_active', b.connections_active,
            'during_avg_active', round(d.avg_active),
            'during_max_total', d.max_total,
            'increase_pct', round(100.0 * (d.avg_active - b.connections_active) / b.connections_active)
        )
    ) INTO v_resource_changes
    FROM before b, during d;

    v_result := jsonb_set(v_result, '{resource_changes}',
                         v_resource_changes);

    -- 4. TODO: Add table-level impacts if table stats available
    -- 5. TODO: Add application-level grouping
    -- 6. TODO: Calculate estimated customer impact

    RETURN v_result;
END;
$$;
```

### Correlation Analysis

The key is finding **correlations** between the incident and other metrics:

```sql
-- Correlation coefficient between two time series
CREATE OR REPLACE FUNCTION flight_recorder._correlation(
    p_metric1 TEXT,
    p_metric2 TEXT,
    p_start_time TIMESTAMPTZ,
    p_end_time TIMESTAMPTZ
)
RETURNS NUMERIC
LANGUAGE plpgsql AS $$
-- Pearson correlation coefficient
-- Returns -1 to 1 (1 = perfect positive correlation)
-- Used to find what else changed during incident
$$;
```

**Example correlations to check**:

- Incident: Lock held → Correlation: Connection count +0.92 (strong)
- Incident: Checkpoint → Correlation: Query latency +0.78 (strong)
- Incident: Slow query → Correlation: CPU usage +0.15 (weak)

### Use Cases

1. **Complete Incident Reports**
   - Show ALL impacts, not just the obvious one
   - "Here's everything that went wrong"

2. **Justify Infrastructure Investments**
   - "That one bad query affected 1,400 customers and cost $X in SLA credits"
   - Business case for better monitoring/resources

3. **Identify Hidden Dependencies**
   - "We didn't know autovacuum blocking would cause bloat"
   - Learn from incidents to prevent recurrence

### Challenges

- **Correlation ≠ Causation**: Two things changing together doesn't mean one caused the other
- **Computational Cost**: Analyzing all correlations is expensive
- **Signal vs Noise**: Too many "effects" dilutes the message

---

## 5. Continuous Benchmarking / Canary Queries ✅ IMPLEMENTED

**Complexity**: 🟡 Medium (1-2 days)

> **Status**: ✅ **Fully implemented** in v2.9. Includes canaries table, canary_results table, run_canaries(), canary_status(), enable_canaries(), disable_canaries(), pre-defined system catalog canary queries, and optional EXPLAIN capture. See `REFERENCE.md` for usage.

### The Problem

Database performance degrades slowly over time (bloat, missing stats, config drift) and you don't notice until customers complain.

### The Vision

Synthetic "canary queries" that should always perform consistently. When they slow down, something is wrong with the database itself.

```sql
-- Runs automatically every 15 minutes
SELECT flight_recorder.run_canaries();
```

**Canary queries**:

```sql
-- Canary 1: Simple index lookup (should be <1ms)
SELECT * FROM users WHERE id = 1;

-- Canary 2: Small aggregation (should be <10ms)
SELECT count(*) FROM posts WHERE created_at > now() - interval '1 hour';

-- Canary 3: Sequential scan baseline (should be predictable)
SELECT count(*) FROM small_reference_table;

-- Canary 4: Join performance
SELECT u.username, count(p.id)
FROM users u
LEFT JOIN posts p ON p.user_id = u.id
WHERE u.id IN (1, 2, 3)
GROUP BY u.username;
```

### Check Canary Status

```sql
SELECT * FROM flight_recorder.canary_status();
```

**Returns**:

```
┌──────────────┬─────────────┬──────────┬────────┬────────────┐
│ canary       │ baseline    │ current  │ delta  │ status     │
├──────────────┼─────────────┼──────────┼────────┼────────────┤
│ index_lookup │ 0.4ms       │ 0.4ms    │ 0%     │ ✅ HEALTHY │
│ small_agg    │ 8.2ms       │ 14.1ms   │ +72%   │ ⚠️ DEGRADED│
│ seq_scan     │ 12.3ms      │ 48.7ms   │ +296%  │ 🔥 CRITICAL│
│ simple_join  │ 2.1ms       │ 2.3ms    │ +10%   │ ✅ HEALTHY │
└──────────────┴─────────────┴──────────┴────────┴────────────┘

🔍 Diagnosis: Sequential scan canary degraded significantly
   Possible causes:
     - Table bloat (last VACUUM: 3 days ago)
     - Missing/stale statistics (last ANALYZE: 3 days ago)
     - Increased table size (check growth rate)

   Recommended actions:
     - Run VACUUM ANALYZE small_reference_table
     - Check for dead tuple accumulation
     - Review autovacuum settings
```

### Implementation

```sql
CREATE TABLE flight_recorder.canaries (
    id                  SERIAL PRIMARY KEY,
    name                TEXT NOT NULL UNIQUE,
    description         TEXT,
    query_text          TEXT NOT NULL,
    expected_time_ms    NUMERIC,  -- NULL = will be calculated
    threshold_warning   NUMERIC DEFAULT 1.5,  -- 50% slower
    threshold_critical  NUMERIC DEFAULT 2.0,  -- 100% slower
    enabled             BOOLEAN DEFAULT true,
    created_at          TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE flight_recorder.canary_results (
    id              BIGSERIAL PRIMARY KEY,
    canary_id       INTEGER REFERENCES flight_recorder.canaries(id),
    executed_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    duration_ms     NUMERIC NOT NULL,
    plan            JSONB,  -- EXPLAIN output
    error_message   TEXT,
    success         BOOLEAN DEFAULT true
);

CREATE INDEX canary_results_canary_executed_idx
    ON flight_recorder.canary_results(canary_id, executed_at DESC);
```

### Canary Execution

```sql
CREATE OR REPLACE FUNCTION flight_recorder.run_canaries()
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    v_canary RECORD;
    v_start TIMESTAMPTZ;
    v_duration NUMERIC;
    v_plan JSONB;
BEGIN
    FOR v_canary IN
        SELECT * FROM flight_recorder.canaries WHERE enabled = true
    LOOP
        BEGIN
            -- Record start time
            v_start := clock_timestamp();

            -- Execute canary query
            EXECUTE v_canary.query_text;

            -- Calculate duration
            v_duration := EXTRACT(EPOCH FROM (clock_timestamp() - v_start)) * 1000;

            -- Get EXPLAIN plan (optional, adds overhead)
            IF flight_recorder._get_config('canary_capture_plans', 'false')::boolean THEN
                EXECUTE 'EXPLAIN (FORMAT JSON) ' || v_canary.query_text INTO v_plan;
            END IF;

            -- Record result
            INSERT INTO flight_recorder.canary_results (
                canary_id, duration_ms, plan, success
            ) VALUES (
                v_canary.id, v_duration, v_plan, true
            );

        EXCEPTION WHEN OTHERS THEN
            -- Record failure
            INSERT INTO flight_recorder.canary_results (
                canary_id, duration_ms, error_message, success
            ) VALUES (
                v_canary.id, 0, SQLERRM, false
            );
        END;
    END LOOP;
END;
$$;
```

### Canary Status Analysis

```sql
CREATE OR REPLACE FUNCTION flight_recorder.canary_status()
RETURNS TABLE(
    canary          TEXT,
    baseline_ms     NUMERIC,
    current_ms      NUMERIC,
    delta_pct       NUMERIC,
    status          TEXT,
    diagnosis       TEXT
)
LANGUAGE plpgsql AS $$
BEGIN
    RETURN QUERY
    WITH
    baselines AS (
        SELECT
            c.id,
            c.name,
            c.threshold_warning,
            c.threshold_critical,
            -- Baseline = p50 over last 7 days (excluding recent)
            percentile_cont(0.5) WITHIN GROUP (ORDER BY cr.duration_ms) AS baseline
        FROM flight_recorder.canaries c
        JOIN flight_recorder.canary_results cr ON cr.canary_id = c.id
        WHERE cr.executed_at BETWEEN now() - interval '7 days'
                                AND now() - interval '1 day'
            AND cr.success = true
        GROUP BY c.id, c.name, c.threshold_warning, c.threshold_critical
    ),
    current AS (
        SELECT
            c.id,
            -- Current = p50 over last hour
            percentile_cont(0.5) WITHIN GROUP (ORDER BY cr.duration_ms) AS current
        FROM flight_recorder.canaries c
        JOIN flight_recorder.canary_results cr ON cr.canary_id = c.id
        WHERE cr.executed_at > now() - interval '1 hour'
            AND cr.success = true
        GROUP BY c.id
    )
    SELECT
        b.name,
        round(b.baseline, 1),
        round(c.current, 1),
        round(100.0 * (c.current - b.baseline) / b.baseline, 0),
        CASE
            WHEN c.current > b.baseline * b.threshold_critical THEN '🔥 CRITICAL'
            WHEN c.current > b.baseline * b.threshold_warning THEN '⚠️ DEGRADED'
            WHEN c.current > b.baseline * 1.1 THEN '⚡ MINOR'
            ELSE '✅ HEALTHY'
        END,
        CASE
            WHEN c.current > b.baseline * b.threshold_critical THEN
                'Significant performance degradation detected'
            WHEN c.current > b.baseline * b.threshold_warning THEN
                'Performance degradation detected'
            ELSE NULL
        END
    FROM baselines b
    JOIN current c ON c.id = b.id
    ORDER BY (c.current / b.baseline) DESC;
END;
$$;
```

### Pre-Defined Canaries

```sql
INSERT INTO flight_recorder.canaries (name, description, query_text) VALUES
    (
        'index_lookup',
        'Simple primary key lookup - should use index',
        'SELECT * FROM pg_class WHERE oid = 1259'  -- pg_class is always there
    ),
    (
        'small_agg',
        'Count recent rows - should be fast with index',
        'SELECT count(*) FROM pg_stat_activity'
    ),
    (
        'seq_scan_baseline',
        'Sequential scan of small table - measures I/O performance',
        'SELECT count(*) FROM pg_namespace'
    ),
    (
        'simple_join',
        'Join performance baseline',
        'SELECT n.nspname, count(c.oid) FROM pg_namespace n LEFT JOIN pg_class c ON c.relnamespace = n.oid GROUP BY n.nspname'
    );
```

### Alerting on Canary Failures

```sql
CREATE OR REPLACE FUNCTION flight_recorder.check_canary_alerts()
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    v_canary RECORD;
BEGIN
    FOR v_canary IN
        SELECT * FROM flight_recorder.canary_status()
        WHERE status IN ('🔥 CRITICAL', '⚠️ DEGRADED')
    LOOP
        -- Send alert
        PERFORM flight_recorder._send_alert(
            'Canary Alert: ' || v_canary.canary || ' ' || v_canary.status,
            format('Baseline: %s ms\nCurrent: %s ms\nChange: +%s%%\n\n%s',
                   v_canary.baseline_ms,
                   v_canary.current_ms,
                   v_canary.delta_pct,
                   v_canary.diagnosis)
        );
    END LOOP;
END;
$$;
```

### Use Cases

1. **Silent Degradation Detection**
   - Table bloat slowly accumulating
   - Statistics going stale
   - Config drift

2. **Regression Testing for Database Changes**
   - Before: run canaries, record baseline
   - Apply change (migration, config, upgrade)
   - After: run canaries, compare
   - "Migration slowed index lookups by 40% - roll back!"

3. **Performance SLO Enforcement**
   - "Index lookups must be < 1ms"
   - Alert when SLO violated

### Concerns

**Overhead**: Running synthetic queries adds load

- Mitigation: Run infrequently (every 15-30 min)
- Use lightweight queries
- Disable if database under stress

**False Positives**: Transient spikes trigger alerts

- Mitigation: Use p50/p95, not single sample
- Require sustained degradation (3+ samples)

**Canary Selection**: What queries are "good canaries"?

- Use queries that touch different subsystems
- Index lookups, seq scans, joins, aggregates
- Avoid queries that depend on volatile data

---

## 6. Fleet-Wide Analysis / "Is This Normal?"

**Complexity**: 🟣 Very High (1+ weeks)

### The Problem

You have hundreds of customer databases. Is *this* database slow, or are they all slow? How does this database compare to others?

This requires infrastructure beyond just SQL - you need a central aggregator.

### The Vision

```sql
SELECT * FROM flight_recorder.compare_to_fleet('similar_tier');
```

**Returns**:

```
📊 Your Database vs Similar Databases (n=247, tier: small)

Metric                Your DB    Fleet Avg    Percentile  Status
──────────────────────────────────────────────────────────────────
Queries/sec           1,247      1,893        31st        🔴 LOW
Avg query time        45ms       28ms         72nd        🟡 HIGH
Cache hit ratio       89%        94%          18th        🔴 LOW
Connections used      78%        45%          89th        🟡 HIGH
Temp file spills      142/hr     8/hr         96th        🔥 CRITICAL
Dead tuple %          12%        4%           81st        🟡 HIGH
WAL generation        2.1 GB/day 1.8 GB/day   58th        ✅ NORMAL

⚠️ Your database is performing below average for its tier

🔍 Outliers Detected:
   • Temp file spills: 17.8x higher than fleet average
     → Likely cause: work_mem too low
     → Fleet avg work_mem: 64 MB (yours: 16 MB)

   • Cache hit ratio: 5% below fleet average
     → Likely cause: shared_buffers too small or working set too large
     → Fleet avg shared_buffers: 2 GB (yours: 512 MB)

💡 Recommendations:
   1. Increase work_mem to 64 MB (fleet standard)
   2. Increase shared_buffers to 2 GB (fleet standard)
   3. Investigate high temp spill queries
```

### Architecture

This requires a **centralized metrics aggregator**:

```
┌─────────────────┐      ┌─────────────────┐      ┌─────────────────┐
│  Database A     │      │  Database B     │      │  Database C     │
│  flight_recorder├─────►│  flight_recorder├─────►│  flight_recorder│
│  export_metrics │      │  export_metrics │      │  export_metrics │
└────────┬────────┘      └────────┬────────┘      └────────┬────────┘
         │                        │                        │
         │     HTTP POST          │                        │
         │  (anonymized metrics)  │                        │
         └────────────────────────┴────────────────────────┘
                                  │
                                  ▼
                     ┌────────────────────────┐
                     │  Central Aggregator    │
                     │  (API + TimescaleDB)   │
                     │                        │
                     │  - Collect metrics     │
                     │  - Calculate percentiles│
                     │  - Store fleet stats   │
                     └────────┬───────────────┘
                              │
                              ▼
                     ┌────────────────────────┐
                     │  Fleet Dashboard       │
                     │  - Per-database views  │
                     │  - Fleet aggregates    │
                     │  - Anomaly detection   │
                     └────────────────────────┘
```

### Implementation

#### Export Metrics (Per Database)

```sql
CREATE OR REPLACE FUNCTION flight_recorder.export_metrics()
RETURNS JSONB
LANGUAGE plpgsql AS $$
DECLARE
    v_metrics JSONB;
    v_database_id TEXT;
BEGIN
    -- Get anonymous database identifier
    v_database_id := flight_recorder._get_config('database_fleet_id');

    IF v_database_id IS NULL THEN
        RAISE EXCEPTION 'Fleet tracking not configured. Set database_fleet_id.';
    END IF;

    -- Gather anonymized metrics (last 24 hours)
    WITH recent AS (
        SELECT * FROM flight_recorder.snapshots
        WHERE captured_at > now() - interval '24 hours'
        ORDER BY captured_at DESC
        LIMIT 1
    )
    SELECT jsonb_build_object(
        'database_id', v_database_id,
        'tier', flight_recorder._get_config('database_tier'),  -- 'micro', 'small', 'medium', etc.
        'timestamp', now(),
        'metrics', jsonb_build_object(
            'connections_max', r.connections_max,
            'connections_avg', (
                SELECT avg(connections_active)
                FROM flight_recorder.snapshots
                WHERE captured_at > now() - interval '24 hours'
            ),
            'queries_per_sec', (
                SELECT avg((xact_commit - LAG(xact_commit) OVER (ORDER BY captured_at)) /
                          EXTRACT(EPOCH FROM (captured_at - LAG(captured_at) OVER (ORDER BY captured_at))))
                FROM flight_recorder.snapshots
                WHERE captured_at > now() - interval '24 hours'
            ),
            'avg_query_time_ms', (
                SELECT avg(mean_exec_time)
                FROM flight_recorder.statement_snapshots
                WHERE snapshot_id IN (
                    SELECT id FROM flight_recorder.snapshots
                    WHERE captured_at > now() - interval '24 hours'
                )
            ),
            'cache_hit_ratio', (
                SELECT avg(100.0 * blks_hit / NULLIF(blks_hit + blks_read, 0))
                FROM flight_recorder.snapshots
                WHERE captured_at > now() - interval '24 hours'
            ),
            'temp_files_per_hour', (
                SELECT avg(temp_files)
                FROM flight_recorder.snapshots
                WHERE captured_at > now() - interval '24 hours'
            ) * 60 / 3,  -- Convert from per-3min to per-hour
            'db_size_gb', r.db_size_bytes / 1024.0 / 1024 / 1024,
            'wal_gb_per_day', (
                SELECT avg(wal_bytes - LAG(wal_bytes) OVER (ORDER BY captured_at))
                FROM flight_recorder.snapshots
                WHERE captured_at > now() - interval '24 hours'
            ) / 1024.0 / 1024 / 1024 * 24 * 20  -- Scale to daily
        ),
        'config', jsonb_build_object(
            'shared_buffers_mb', (SELECT setting::bigint * 8192 / 1024 / 1024
                                  FROM pg_settings WHERE name = 'shared_buffers'),
            'work_mem_mb', (SELECT setting::bigint * 1024 / 1024
                           FROM pg_settings WHERE name = 'work_mem'),
            'max_connections', (SELECT setting::integer FROM pg_settings WHERE name = 'max_connections')
        )
    ) INTO v_metrics
    FROM recent r;

    RETURN v_metrics;
END;
$$;
```

#### Send to Aggregator

```sql
CREATE OR REPLACE FUNCTION flight_recorder.send_to_fleet()
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    v_metrics JSONB;
    v_fleet_endpoint TEXT;
    v_fleet_token TEXT;
    v_response http_response;
BEGIN
    -- Check if fleet reporting is enabled
    IF NOT COALESCE(flight_recorder._get_config('fleet_reporting_enabled', 'false')::boolean, false) THEN
        RETURN;
    END IF;

    v_fleet_endpoint := flight_recorder._get_config('fleet_endpoint');
    v_fleet_token := current_setting('flight_recorder.fleet_token', true);

    IF v_fleet_endpoint IS NULL OR v_fleet_token IS NULL THEN
        RETURN;  -- Not configured
    END IF;

    -- Export metrics
    v_metrics := flight_recorder.export_metrics();

    -- Send to fleet aggregator
    SELECT * INTO v_response
    FROM http((
        'POST',
        v_fleet_endpoint || '/v1/metrics',
        ARRAY[
            http_header('Authorization', 'Bearer ' || v_fleet_token),
            http_header('Content-Type', 'application/json')
        ],
        'application/json',
        v_metrics::text
    ));

    -- Log result
    IF v_response.status != 200 THEN
        INSERT INTO flight_recorder.fleet_errors (error_message, response_status)
        VALUES ('Failed to send metrics to fleet', v_response.status);
    END IF;
END;
$$;

-- Schedule via pg_cron (once per day)
SELECT cron.schedule(
    'fleet-reporting',
    '0 2 * * *',  -- 2am daily
    'SELECT flight_recorder.send_to_fleet()'
);
```

#### Central Aggregator (Separate Service)

This would be a separate service (Node.js/Python API + TimescaleDB) that:

1. Receives metrics from all databases
2. Groups by tier (micro, small, medium, etc.)
3. Calculates percentiles
4. Detects outliers
5. Provides API for querying fleet stats

**Example API endpoint**:

```
GET /v1/fleet/stats?tier=small&metric=cache_hit_ratio
{
  "tier": "small",
  "database_count": 247,
  "metrics": {
    "cache_hit_ratio": {
      "p10": 88.2,
      "p25": 91.5,
      "p50": 94.1,
      "p75": 96.3,
      "p90": 97.8,
      "mean": 93.9
    }
  }
}
```

#### Compare to Fleet (Per Database)

```sql
CREATE OR REPLACE FUNCTION flight_recorder.compare_to_fleet(
    p_tier TEXT DEFAULT NULL
)
RETURNS TABLE(
    metric TEXT,
    your_value NUMERIC,
    fleet_avg NUMERIC,
    percentile INTEGER,
    status TEXT
)
LANGUAGE plpgsql AS $$
DECLARE
    v_tier TEXT;
    v_your_metrics JSONB;
    v_fleet_stats JSONB;
    v_response http_response;
BEGIN
    v_tier := COALESCE(p_tier, flight_recorder._get_config('database_tier'));

    -- Get your metrics
    v_your_metrics := (flight_recorder.export_metrics())->'metrics';

    -- Fetch fleet stats from aggregator
    SELECT * INTO v_response
    FROM http((
        'GET',
        flight_recorder._get_config('fleet_endpoint') || '/v1/fleet/stats?tier=' || v_tier,
        ARRAY[http_header('Authorization', 'Bearer ' || current_setting('flight_recorder.fleet_token'))],
        NULL,
        NULL
    ));

    v_fleet_stats := v_response.content::jsonb->'metrics';

    -- Compare each metric
    -- (Implementation details omitted for brevity - would iterate through metrics
    --  and calculate percentile rank for each)

    -- Return comparison
    RETURN QUERY
    SELECT
        'queries_per_sec'::text,
        (v_your_metrics->>'queries_per_sec')::numeric,
        (v_fleet_stats->'queries_per_sec'->>'mean')::numeric,
        NULL::integer,  -- TODO: Calculate percentile
        NULL::text;
END;
$$;
```

### Privacy Considerations

**Critical**: Only send **aggregated, anonymized** metrics:

- ✅ Counts, averages, percentiles
- ✅ System metrics (connections, cache hits)
- ❌ Query text
- ❌ Table names
- ❌ User names
- ❌ IP addresses
- ❌ Any customer PII

**Opt-in**: Fleet reporting must be explicitly enabled per database.

### Use Cases

1. **Benchmarking**
   - "How does my database compare to similar databases?"
   - Set realistic performance expectations

2. **Configuration Guidance**
   - "Fleet average shared_buffers is 2GB, yours is 512MB"
   - Data-driven configuration recommendations

3. **Incident Correlation**
   - "Your database is slow, but so are 200 others" → Platform issue
   - "Only your database is slow" → Application/data issue

4. **Capacity Planning**
   - "Databases at 80% connection usage typically upgrade within 2 weeks"
   - Predict when customers will need upgrades

### Challenges

- **Infrastructure**: Requires separate service (not just SQL)
- **Privacy**: Must be extremely careful with data collection
- **Opt-in**: Can't force customers to participate
- **Sample size**: Need enough databases in each tier for meaningful stats
- **Outliers**: Some databases are legitimately different (not comparable)

### Recommended Approach

**Phase 1**: Build central aggregator as internal Supabase tool
**Phase 2**: Implement export_metrics() in flight_recorder
**Phase 3**: Opt-in beta with friendly customers
**Phase 4**: General availability (if valuable)

---

## 7. Automatic Performance Regression Detection

**Complexity**: 🟡 Medium (1-2 days)

### The Problem

A query was fast yesterday, slow today. You don't notice until a customer complains.

### The Vision

```sql
SELECT * FROM flight_recorder.detect_regressions(interval '24 hours');
```

**Returns**:

```
🔴 3 Performance Regressions Detected (last 24 hours)

┌────────────┬──────────────────────────────┬────────────┬────────────┬──────────┬─────────────────────┐
│ severity   │ query                        │ before_ms  │ now_ms     │ change   │ first_seen          │
├────────────┼──────────────────────────────┼────────────┼────────────┼──────────┼─────────────────────┤
│ 🔥 CRITICAL│ SELECT * FROM posts WHERE... │ 12         │ 340        │ +2,733%  │ 2025-01-17 08:23:00│
│ ⚠️ WARNING │ UPDATE users SET last_seen...│ 8          │ 45         │ +463%    │ 2025-01-17 14:15:00│
│ ⚡ MINOR    │ SELECT count(*) FROM orders..│ 234        │ 1,203      │ +414%    │ 2025-01-17 09:00:00│
└────────────┴──────────────────────────────┴────────────┴────────────┴──────────┴─────────────────────┘

📊 Details:

1. SELECT * FROM posts WHERE user_id = $1
   Before: 12ms avg (p95: 23ms)  →  Now: 340ms avg (p95: 1,240ms)
   Change: +2,733% (28x slower)
   First detected: 2025-01-17 08:23 UTC

   🔍 Possible causes:
     • Missing index (check for dropped index)
     • Statistics out of date (last ANALYZE: 5 days ago)
     • Plan regression (consider pg_stat_statements.reset)
     • Data distribution change (table grew 10x)

   💡 Recommended actions:
     • Run ANALYZE posts
     • Check for missing indexes: SELECT * FROM pg_indexes WHERE tablename = 'posts'
     • Review query plan: EXPLAIN ANALYZE SELECT * FROM posts WHERE user_id = 123
```

### Implementation

#### Statistical Regression Detection

```sql
CREATE OR REPLACE FUNCTION flight_recorder.detect_regressions(
    p_lookback INTERVAL DEFAULT '24 hours',
    p_threshold_pct NUMERIC DEFAULT 50.0  -- 50% slower = regression
)
RETURNS TABLE(
    queryid                 BIGINT,
    query_preview           TEXT,
    baseline_avg_ms         NUMERIC,
    current_avg_ms          NUMERIC,
    change_pct              NUMERIC,
    severity                TEXT,
    first_detected          TIMESTAMPTZ,
    probable_causes         TEXT[],
    recommendations         TEXT[]
)
LANGUAGE plpgsql AS $$
BEGIN
    RETURN QUERY
    WITH
    -- Baseline: same time period, previous week
    baseline AS (
        SELECT
            ss.queryid,
            ss.query_preview,
            avg(ss.mean_exec_time) AS avg_time,
            stddev(ss.mean_exec_time) AS stddev_time,
            count(*) AS sample_count
        FROM flight_recorder.statement_snapshots ss
        JOIN flight_recorder.snapshots s ON s.id = ss.snapshot_id
        WHERE s.captured_at BETWEEN (now() - interval '7 days' - p_lookback)
                                AND (now() - interval '7 days')
        GROUP BY ss.queryid, ss.query_preview
        HAVING count(*) >= 3  -- Need enough samples
    ),
    -- Current period
    current AS (
        SELECT
            ss.queryid,
            ss.query_preview,
            avg(ss.mean_exec_time) AS avg_time,
            min(s.captured_at) AS first_seen
        FROM flight_recorder.statement_snapshots ss
        JOIN flight_recorder.snapshots s ON s.id = ss.snapshot_id
        WHERE s.captured_at > now() - p_lookback
        GROUP BY ss.queryid, ss.query_preview
        HAVING count(*) >= 3
    ),
    -- Compare and detect regressions
    regressions AS (
        SELECT
            c.queryid,
            c.query_preview,
            round(b.avg_time::numeric, 1) AS baseline_avg_ms,
            round(c.avg_time::numeric, 1) AS current_avg_ms,
            round(100.0 * (c.avg_time - b.avg_time) / b.avg_time, 0) AS change_pct,
            -- Z-score for statistical significance
            (c.avg_time - b.avg_time) / NULLIF(b.stddev_time, 0) AS z_score,
            c.first_seen
        FROM current c
        JOIN baseline b ON b.queryid = c.queryid
        WHERE c.avg_time > b.avg_time * (1 + p_threshold_pct / 100.0)
            AND (c.avg_time - b.avg_time) / NULLIF(b.stddev_time, 0) > 2  -- Statistically significant
    )
    SELECT
        r.queryid,
        r.query_preview,
        r.baseline_avg_ms,
        r.current_avg_ms,
        r.change_pct,
        CASE
            WHEN r.change_pct > 500 THEN '🔥 CRITICAL'
            WHEN r.change_pct > 200 THEN '⚠️ WARNING'
            ELSE '⚡ MINOR'
        END,
        r.first_seen,
        flight_recorder._diagnose_regression_causes(r.queryid, r.query_preview),
        flight_recorder._generate_regression_recommendations(r.queryid, r.change_pct)
    FROM regressions r
    ORDER BY r.change_pct DESC;
END;
$$;
```

#### Diagnose Probable Causes

```sql
CREATE OR REPLACE FUNCTION flight_recorder._diagnose_regression_causes(
    p_queryid BIGINT,
    p_query_preview TEXT
)
RETURNS TEXT[]
LANGUAGE plpgsql AS $$
DECLARE
    v_causes TEXT[] := ARRAY[]::TEXT[];
    v_last_analyze TIMESTAMPTZ;
BEGIN
    -- Check 1: Are statistics stale?
    -- (Would need to parse query to extract table names - complex)
    -- For now, just flag if any user tables have stale stats
    IF EXISTS (
        SELECT 1 FROM pg_stat_user_tables
        WHERE last_analyze < now() - interval '7 days'
           OR last_autoanalyze < now() - interval '7 days'
    ) THEN
        v_causes := array_append(v_causes, 'Statistics may be out of date');
    END IF;

    -- Check 2: Were there schema changes?
    -- (Would need to track schema change events - not currently captured)

    -- Check 3: Did table grow significantly?
    -- (Would need historical table size data - could add in future)

    -- Check 4: Is query spilling to temp files?
    IF EXISTS (
        SELECT 1 FROM flight_recorder.statement_snapshots
        WHERE queryid = p_queryid
            AND temp_blks_written > 0
        LIMIT 1
    ) THEN
        v_causes := array_append(v_causes, 'Query spilling to temp files (increase work_mem)');
    END IF;

    -- Check 5: Low cache hit ratio?
    IF EXISTS (
        SELECT 1 FROM flight_recorder.statement_snapshots
        WHERE queryid = p_queryid
            AND shared_blks_read > 0
            AND (shared_blks_hit::float / (shared_blks_hit + shared_blks_read)) < 0.9
        LIMIT 1
    ) THEN
        v_causes := array_append(v_causes, 'Poor cache hit ratio (table may not fit in shared_buffers)');
    END IF;

    RETURN v_causes;
END;
$$;
```

#### Auto-Alerting

```sql
CREATE OR REPLACE FUNCTION flight_recorder.check_regressions()
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    v_regression RECORD;
    v_count INTEGER;
BEGIN
    SELECT count(*) INTO v_count
    FROM flight_recorder.detect_regressions()
    WHERE severity IN ('🔥 CRITICAL', '⚠️ WARNING');

    IF v_count > 0 THEN
        -- Alert on critical/warning regressions
        FOR v_regression IN
            SELECT * FROM flight_recorder.detect_regressions()
            WHERE severity IN ('🔥 CRITICAL', '⚠️ WARNING')
        LOOP
            PERFORM flight_recorder._send_alert(
                format('Performance Regression: %s', v_regression.severity),
                format(E'Query: %s\n\nBefore: %s ms → Now: %s ms (+%s%%)\n\nFirst detected: %s\n\nPossible causes:\n%s',
                       v_regression.query_preview,
                       v_regression.baseline_avg_ms,
                       v_regression.current_avg_ms,
                       v_regression.change_pct,
                       v_regression.first_detected,
                       array_to_string(v_regression.probable_causes, E'\n• '))
            );
        END LOOP;
    END IF;
END;
$$;

-- Schedule every hour
SELECT cron.schedule(
    'regression-detection',
    '0 * * * *',
    'SELECT flight_recorder.check_regressions()'
);
```

### Use Cases

1. **Catch Deployment Regressions**
   - Deploy at 2pm
   - Regression detected at 3pm
   - "This query got 5x slower after deployment - roll back!"

2. **Data-Driven Performance Monitoring**
   - Automatic detection vs. manual log review
   - Quantify impact: "28x slower" vs. "feels slow"

3. **Trend Analysis**
   - Gradual degradation over weeks
   - "This query has gotten 2x slower each month - time to optimize"

### Challenges

- **Baseline Selection**: What's "normal"? Previous week? Average?
- **Statistical Significance**: Avoid false positives from natural variance
- **Root Cause**: Detecting regression is easy, diagnosing WHY is hard
- **Noisy Data**: One-off spikes can trigger false alerts

### Recommended Approach

**Phase 1**: Simple percentage-based detection (50%+ slower)
**Phase 2**: Add statistical significance testing (z-score)
**Phase 3**: Add root cause diagnosis (check stats, indexes, etc.)
**Phase 4**: ML-based anomaly detection (if justified)

---

## 8. Visual Performance Timeline (ASCII Art) ✅ IMPLEMENTED

**Complexity**: 🟢 Low (4-8 hours)

> **Status**: ✅ **Fully implemented** in v2.13. Includes `_sparkline()` for compact Unicode sparklines, `_bar()` for horizontal progress bars, `timeline()` for full ASCII charts, and `sparkline_metrics()` for summary tables with trend visualization. See `REFERENCE.md` for usage.

### The Problem

Numbers are boring. Humans are visual. CSAs want to *see* the problem at a glance.

### The Vision

```sql
SELECT flight_recorder.timeline('connections', interval '4 hours');
```

**Returns**:

```
Connections (last 4 hours)

100 ┤                                          ╭─────────╮
 90 ┤                                    ╭─────╯         ╰───
 80 ┤                            ╭───────╯
 70 ┤                      ╭─────╯
 60 ┤           ╭──────────╯
 50 ┤     ╭─────╯
 40 ┼─────╯
    └─────┬──────┬──────┬──────┬──────┬──────┬──────┬──────
       12:00  13:00  14:00  15:00  16:00  17:00  18:00  19:00

⚠️ Spike detected at 17:23 UTC (+45 connections in 3 minutes)
```

### Multi-Metric Timeline

```sql
SELECT flight_recorder.timeline_multi(interval '1 hour');
```

**Returns**:

```
Performance Overview (last hour)

Queries/sec
1500 ┤     ╭╮   ╭─╮
1000 ┤  ╭──╯╰───╯ ╰──╮
 500 ┼──╯           ╰────
   0 └────────────────────

Avg Latency (ms)
 100 ┤         ╭────╮
  50 ┤      ╭──╯    ╰──╮
   0 ┼──────╯          ╰──

Wait Events (% samples with locks)
 60% ┤         ╭─────╮
 40% ┤      ╭──╯     ╰──╮
 20% ┼──────╯          ╰───
   0 └────────────────────
      └──────┬─────┬─────┬──
          10:00 10:30 11:00

📍 Incident Window: 10:35-10:42
   • Query latency spiked 5x
   • Lock contention increased to 60%
   • Queries/sec dropped 75%
```

### Implementation

```sql
-- Simple ASCII line chart
CREATE OR REPLACE FUNCTION flight_recorder.timeline(
    p_metric TEXT,
    p_duration INTERVAL DEFAULT '4 hours',
    p_width INTEGER DEFAULT 60,
    p_height INTEGER DEFAULT 10
)
RETURNS TEXT
LANGUAGE plpgsql AS $$
DECLARE
    v_data NUMERIC[];
    v_labels TIMESTAMPTZ[];
    v_min NUMERIC;
    v_max NUMERIC;
    v_chart TEXT := '';
    v_row TEXT;
    v_value NUMERIC;
    v_scaled INTEGER;
    i INTEGER;
    j INTEGER;
BEGIN
    -- Fetch data points
    EXECUTE format(
        'SELECT array_agg(%I ORDER BY captured_at), array_agg(captured_at ORDER BY captured_at)
         FROM flight_recorder.snapshots
         WHERE captured_at > now() - $1
         LIMIT $2',
        p_metric
    ) INTO v_data, v_labels
    USING p_duration, p_width;

    -- Calculate min/max for scaling
    v_min := (SELECT min(val) FROM unnest(v_data) val);
    v_max := (SELECT max(val) FROM unnest(v_data) val);

    -- Build chart from top to bottom
    FOR i IN REVERSE p_height..0 LOOP
        v_row := format('%4s ┤', round(v_min + (v_max - v_min) * i / p_height)::text);

        FOR j IN 1..array_length(v_data, 1) LOOP
            v_value := v_data[j];
            v_scaled := round((v_value - v_min) / (v_max - v_min) * p_height)::integer;

            IF v_scaled = i THEN
                v_row := v_row || '─';
            ELSIF v_scaled > i THEN
                v_row := v_row || ' ';
            ELSE
                v_row := v_row || ' ';
            END IF;
        END LOOP;

        v_chart := v_chart || v_row || E'\n';
    END LOOP;

    -- Add X-axis labels
    v_chart := v_chart || '     └' || repeat('─', p_width) || E'\n';
    v_chart := v_chart || format('      %s     %s     %s',
                                  to_char(v_labels[1], 'HH24:MI'),
                                  to_char(v_labels[array_length(v_labels,1)/2], 'HH24:MI'),
                                  to_char(v_labels[array_length(v_labels,1)], 'HH24:MI'));

    RETURN v_chart;
END;
$$;
```

### Better: Use Unicode Box Drawing

For prettier charts, use Unicode box-drawing characters:

```
Characters available:
─ │ ┌ ┐ └ ┘ ├ ┤ ┬ ┴ ┼
╭ ╮ ╯ ╰ ╱ ╲ ╳
▁ ▂ ▃ ▄ ▅ ▆ ▇ █  (block elements for bar charts)
```

### Sparklines (Inline Mini-Charts)

For use in summary reports:

```sql
SELECT
    metric_name,
    current_value,
    flight_recorder._sparkline(metric_history) AS trend
FROM metrics;
```

**Returns**:

```
┌──────────────┬───────────┬────────────┐
│ metric       │ current   │ trend      │
├──────────────┼───────────┼────────────┤
│ connections  │ 87        │ ▁▂▃▅▇█     │
│ query_time   │ 45ms      │ ▃▃▃▅▇▅▃    │
│ cache_hit    │ 89%       │ █▇▇▆▅▅▄    │
└──────────────┴───────────┴────────────┘
```

### Use Cases

1. **Quick Incident Overview**
   - Glance at chart, see spike
   - Faster than reading numbers

2. **Pattern Recognition**
   - Periodic spikes (every hour = cron job)
   - Gradual climb (resource exhaustion)
   - Sudden drop (crash)

3. **Correlation Visualization**
   - Show multiple metrics stacked
   - "Connections spiked when latency spiked" (visual proof)

### Challenges

- **Terminal Width**: Charts must fit in 80-column terminals
- **Resolution**: Limited detail with ASCII
- **Complexity**: Multi-metric charts are hard to read
- **Gimmick Risk**: "Cool but useless"

### Recommended Approach

**Phase 1**: Simple single-metric line charts
**Phase 2**: Add sparklines for summary views
**Phase 3**: Multi-metric stacked charts (if useful)

**Reality Check**: This is the **lowest priority** feature. Fun, but provides minimal diagnostic value. Only implement if time allows.

---

## Priority Ranking

Based on **value vs. complexity**:

| Rank | Feature | Value | Complexity | Status |
|------|---------|-------|------------|--------|
| 1 | **Query Storm Detection** | 🔥 Very High | 🟡 Medium | ✅ **DONE** (v2.10-2.11) - Includes severity & correlation |
| 2 | **Regression Detection** | 🔥 Very High | 🟡 Medium | ✅ **DONE** (v2.12) |
| 3 | **Performance Forecasting** | 🟠 High | 🔴 High | ✅ **DONE** (v2.14) |
| 4 | **Time-Travel Debugging** | 🟠 High | 🟡 Medium | ⬚ Not started |
| 5 | **Blast Radius Analysis** | 🟠 High | 🔴 High | ⬚ Not started |
| 6 | **Canary Queries** | 🟡 Medium | 🟡 Medium | ✅ **DONE** (v2.9) |
| 7 | **Fleet-Wide Analysis** | 🟡 Medium | 🟣 Very High | ⬚ Not started - Requires infrastructure |
| 8 | **Visual Timeline** | 🟢 Low | 🟢 Low | ✅ **DONE** (v2.13) - Sparklines, bars, timeline charts |

---

## Next Steps

1. ~~**Prototype Top 2**: Build query storm + canary queries~~ ✅ Done
2. ~~**Regression Detection**: Next priority~~ ✅ Done (v2.12)
3. ~~**Visual Timeline**: ASCII charts for metrics~~ ✅ Done (v2.13)
4. ~~**Performance Forecasting**: Proactive capacity warnings~~ ✅ Done (v2.14)
5. **Validate with Real Data**: Test on production databases
6. **Time-Travel Debugging**: Next priority
7. **Iterate**: Refine based on real-world usage

---

**Last Updated**: 2026-01-30
**Status**: 6 of 8 features implemented (Query Storms, Canary Queries, Regression Detection, Visual Timeline, Performance Forecasting)
**Maintainer**: Flight Recorder Team
