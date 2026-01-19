# Flight Recorder Feature Designs

**Purpose**: Technical designs for new features that enhance diagnostic capabilities.
**Constraints**: SQL/PL/pgSQL only, minimal observer effect (Hippocratic Oath: do no harm).

---

## Feature 1: Table-Level Hotspot Tracking

### Problem Statement

When diagnosing incidents, we can see which **queries** are slow, but not which **tables** are under pressure. Knowing "table users got hammered with 10M sequential scans" is incredibly valuable.

### Data Source

`pg_stat_user_tables` provides perfect table-level metrics:

- `seq_scan`, `seq_tup_read` - Sequential scan activity
- `idx_scan`, `idx_tup_fetch` - Index scan activity
- `n_tup_ins`, `n_tup_upd`, `n_tup_del` - Write activity
- `n_live_tup`, `n_dead_tup` - Bloat indicators
- `vacuum_count`, `autovacuum_count` - Maintenance activity
- `analyze_count`, `autoanalyze_count` - Stats freshness

**Overhead**: Querying `pg_stat_user_tables` is very cheap - it's just reading shared memory stats.

### Storage Design

#### Option A: Snapshot-Based (Recommended)

Store table stats in a separate table, linked to the existing `snapshots` table:

```sql
CREATE TABLE IF NOT EXISTS flight_recorder.table_snapshots (
    snapshot_id         INTEGER REFERENCES flight_recorder.snapshots(id) ON DELETE CASCADE,
    schemaname          TEXT NOT NULL,
    relname             TEXT NOT NULL,
    relid               OID NOT NULL,
    seq_scan            BIGINT,
    seq_tup_read        BIGINT,
    idx_scan            BIGINT,
    idx_tup_fetch       BIGINT,
    n_tup_ins           BIGINT,
    n_tup_upd           BIGINT,
    n_tup_del           BIGINT,
    n_tup_hot_upd       BIGINT,
    n_live_tup          BIGINT,
    n_dead_tup          BIGINT,
    vacuum_count        BIGINT,
    autovacuum_count    BIGINT,
    analyze_count       BIGINT,
    autoanalyze_count   BIGINT,
    last_vacuum         TIMESTAMPTZ,
    last_autovacuum     TIMESTAMPTZ,
    last_analyze        TIMESTAMPTZ,
    last_autoanalyze    TIMESTAMPTZ,
    PRIMARY KEY (snapshot_id, relid)
);

CREATE INDEX IF NOT EXISTS table_snapshots_relid_idx
    ON flight_recorder.table_snapshots(relid);
```

**Retention**: Same as regular snapshots (default 30 days).

#### Collection Strategy

**Smart Sampling** to minimize overhead:

- Only collect top N hottest tables per snapshot (e.g., top 50 by total activity)
- Activity score = `seq_tup_read + idx_tup_fetch + n_tup_ins + n_tup_upd + n_tup_del`
- This avoids storing hundreds of rows for idle tables

**Collection Function** (add to existing `snapshot()` function):

```sql
CREATE OR REPLACE FUNCTION flight_recorder._collect_table_stats(p_snapshot_id INTEGER)
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    v_top_n INTEGER;
BEGIN
    v_top_n := COALESCE(
        flight_recorder._get_config('table_stats_top_n', '50')::integer,
        50
    );

    INSERT INTO flight_recorder.table_snapshots (
        snapshot_id, schemaname, relname, relid,
        seq_scan, seq_tup_read, idx_scan, idx_tup_fetch,
        n_tup_ins, n_tup_upd, n_tup_del, n_tup_hot_upd,
        n_live_tup, n_dead_tup,
        vacuum_count, autovacuum_count, analyze_count, autoanalyze_count,
        last_vacuum, last_autovacuum, last_analyze, last_autoanalyze
    )
    SELECT
        p_snapshot_id,
        schemaname,
        relname,
        relid,
        seq_scan,
        seq_tup_read,
        idx_scan,
        idx_tup_fetch,
        n_tup_ins,
        n_tup_upd,
        n_tup_del,
        n_tup_hot_upd,
        n_live_tup,
        n_dead_tup,
        vacuum_count,
        autovacuum_count,
        analyze_count,
        autoanalyze_count,
        last_vacuum,
        last_autovacuum,
        last_analyze,
        last_autoanalyze
    FROM pg_stat_user_tables
    ORDER BY (seq_tup_read + idx_tup_fetch + n_tup_ins + n_tup_upd + n_tup_del) DESC
    LIMIT v_top_n;
END;
$$;
```

### Analysis Functions

**1. Table Activity Comparison**

```sql
CREATE OR REPLACE FUNCTION flight_recorder.table_compare(
    p_start_time TIMESTAMPTZ,
    p_end_time TIMESTAMPTZ,
    p_limit INTEGER DEFAULT 25
)
RETURNS TABLE(
    schemaname              TEXT,
    relname                 TEXT,
    relid                   OID,
    seq_scan_delta          BIGINT,
    seq_tup_read_delta      BIGINT,
    idx_scan_delta          BIGINT,
    idx_tup_fetch_delta     BIGINT,
    n_tup_ins_delta         BIGINT,
    n_tup_upd_delta         BIGINT,
    n_tup_del_delta         BIGINT,
    n_tup_hot_upd_delta     BIGINT,
    dead_tup_pct            NUMERIC,
    vacuum_count_delta      BIGINT,
    autovacuum_count_delta  BIGINT,
    analyze_count_delta     BIGINT,
    autoanalyze_count_delta BIGINT,
    total_activity          BIGINT
)
LANGUAGE sql STABLE AS $$
    WITH
    start_snap AS (
        SELECT ts.*
        FROM flight_recorder.table_snapshots ts
        JOIN flight_recorder.snapshots s ON s.id = ts.snapshot_id
        WHERE s.captured_at <= p_start_time
        ORDER BY s.captured_at DESC
        LIMIT 1000
    ),
    end_snap AS (
        SELECT ts.*
        FROM flight_recorder.table_snapshots ts
        JOIN flight_recorder.snapshots s ON s.id = ts.snapshot_id
        WHERE s.captured_at >= p_end_time
        ORDER BY s.captured_at ASC
        LIMIT 1000
    ),
    matched AS (
        SELECT
            e.schemaname,
            e.relname,
            e.relid,
            e.seq_scan - COALESCE(s.seq_scan, 0) AS seq_scan_delta,
            e.seq_tup_read - COALESCE(s.seq_tup_read, 0) AS seq_tup_read_delta,
            e.idx_scan - COALESCE(s.idx_scan, 0) AS idx_scan_delta,
            e.idx_tup_fetch - COALESCE(s.idx_tup_fetch, 0) AS idx_tup_fetch_delta,
            e.n_tup_ins - COALESCE(s.n_tup_ins, 0) AS n_tup_ins_delta,
            e.n_tup_upd - COALESCE(s.n_tup_upd, 0) AS n_tup_upd_delta,
            e.n_tup_del - COALESCE(s.n_tup_del, 0) AS n_tup_del_delta,
            e.n_tup_hot_upd - COALESCE(s.n_tup_hot_upd, 0) AS n_tup_hot_upd_delta,
            e.n_live_tup,
            e.n_dead_tup,
            e.vacuum_count - COALESCE(s.vacuum_count, 0) AS vacuum_count_delta,
            e.autovacuum_count - COALESCE(s.autovacuum_count, 0) AS autovacuum_count_delta,
            e.analyze_count - COALESCE(s.analyze_count, 0) AS analyze_count_delta,
            e.autoanalyze_count - COALESCE(s.autoanalyze_count, 0) AS autoanalyze_count_delta
        FROM end_snap e
        LEFT JOIN start_snap s ON s.relid = e.relid
    )
    SELECT
        m.schemaname,
        m.relname,
        m.relid,
        m.seq_scan_delta,
        m.seq_tup_read_delta,
        m.idx_scan_delta,
        m.idx_tup_fetch_delta,
        m.n_tup_ins_delta,
        m.n_tup_upd_delta,
        m.n_tup_del_delta,
        m.n_tup_hot_upd_delta,
        CASE
            WHEN m.n_live_tup > 0
            THEN round(100.0 * m.n_dead_tup / (m.n_live_tup + m.n_dead_tup), 1)
            ELSE 0
        END AS dead_tup_pct,
        m.vacuum_count_delta,
        m.autovacuum_count_delta,
        m.analyze_count_delta,
        m.autoanalyze_count_delta,
        (m.seq_tup_read_delta + m.idx_tup_fetch_delta +
         m.n_tup_ins_delta + m.n_tup_upd_delta + m.n_tup_del_delta) AS total_activity
    FROM matched m
    WHERE (m.seq_tup_read_delta + m.idx_tup_fetch_delta +
           m.n_tup_ins_delta + m.n_tup_upd_delta + m.n_tup_del_delta) > 0
    ORDER BY total_activity DESC
    LIMIT p_limit
$$;
```

**2. Table Hotspot Summary**

```sql
CREATE OR REPLACE FUNCTION flight_recorder.table_hotspots(
    p_start_time TIMESTAMPTZ,
    p_end_time TIMESTAMPTZ
)
RETURNS TABLE(
    schemaname      TEXT,
    relname         TEXT,
    issue_type      TEXT,
    severity        TEXT,
    description     TEXT,
    recommendation  TEXT
)
LANGUAGE plpgsql STABLE AS $$
DECLARE
    v_table RECORD;
BEGIN
    FOR v_table IN
        SELECT * FROM flight_recorder.table_compare(p_start_time, p_end_time, 100)
    LOOP
        -- High sequential scan activity
        IF v_table.seq_scan_delta > 100 AND v_table.seq_tup_read_delta > 100000 THEN
            schemaname := v_table.schemaname;
            relname := v_table.relname;
            issue_type := 'SEQUENTIAL_SCAN_STORM';
            severity := CASE
                WHEN v_table.seq_tup_read_delta > 10000000 THEN 'high'
                WHEN v_table.seq_tup_read_delta > 1000000 THEN 'medium'
                ELSE 'low'
            END;
            description := format('%s sequential scans reading %s tuples',
                                 v_table.seq_scan_delta,
                                 v_table.seq_tup_read_delta);
            recommendation := 'Consider adding an index or reviewing query WHERE clauses';
            RETURN NEXT;
        END IF;

        -- High dead tuple percentage (bloat)
        IF v_table.dead_tup_pct > 20 THEN
            schemaname := v_table.schemaname;
            relname := v_table.relname;
            issue_type := 'TABLE_BLOAT';
            severity := CASE
                WHEN v_table.dead_tup_pct > 50 THEN 'high'
                WHEN v_table.dead_tup_pct > 30 THEN 'medium'
                ELSE 'low'
            END;
            description := format('%s%% dead tuples', round(v_table.dead_tup_pct));
            recommendation := 'Run VACUUM or check autovacuum settings';
            RETURN NEXT;
        END IF;

        -- Low HOT update ratio (inefficient updates)
        IF v_table.n_tup_upd_delta > 1000 THEN
            DECLARE
                v_hot_ratio NUMERIC;
            BEGIN
                v_hot_ratio := CASE
                    WHEN v_table.n_tup_upd_delta > 0
                    THEN 100.0 * v_table.n_tup_hot_upd_delta / v_table.n_tup_upd_delta
                    ELSE 100
                END;

                IF v_hot_ratio < 50 THEN
                    schemaname := v_table.schemaname;
                    relname := v_table.relname;
                    issue_type := 'LOW_HOT_UPDATE_RATIO';
                    severity := 'medium';
                    description := format('%s updates, only %s%% HOT',
                                         v_table.n_tup_upd_delta,
                                         round(v_hot_ratio, 1));
                    recommendation := 'Consider increasing fillfactor or reducing indexed columns';
                    RETURN NEXT;
                END IF;
            END;
        END IF;

        -- Frequent autovacuum (indicates high churn)
        IF v_table.autovacuum_count_delta > 5 THEN
            schemaname := v_table.schemaname;
            relname := v_table.relname;
            issue_type := 'HIGH_AUTOVACUUM_FREQUENCY';
            severity := 'low';
            description := format('%s autovacuums during period',
                                 v_table.autovacuum_count_delta);
            recommendation := 'High write activity detected; ensure autovacuum keeps up';
            RETURN NEXT;
        END IF;
    END LOOP;
END;
$$;
```

### Configuration

Add to `flight_recorder.config` table:

```sql
INSERT INTO flight_recorder.config (key, value) VALUES
    ('table_stats_enabled', 'true'),
    ('table_stats_top_n', '50'),
    ('table_stats_retention_days', '30')
ON CONFLICT (key) DO NOTHING;
```

### Impact Assessment

- **Storage**: ~50 rows × 15 columns × 8 bytes ≈ 6 KB per snapshot
  - At 3-min interval for 30 days: ~6 KB × 14,400 snapshots = **~86 MB**
- **CPU**: Single query to `pg_stat_user_tables` with ORDER BY and LIMIT
  - Estimated: **< 5ms** per collection
- **Overhead**: **Negligible** (0.003% of 3-minute collection window)

---

## Feature 2: Index Usage Tracking

### Problem Statement

Knowing which indexes are used (or **not** used) helps identify:

- Missing indexes causing seq scans
- Unused indexes wasting space and slowing writes
- Index bloat needing REINDEX

### Data Source

`pg_stat_user_indexes` provides index-level metrics:

- `idx_scan` - How many times index was scanned
- `idx_tup_read` - Tuples read from index
- `idx_tup_fetch` - Tuples fetched via index
- Plus `pg_class.relpages` for index size

**Overhead**: Querying `pg_stat_user_indexes` is cheap (shared memory).

### Storage Design

```sql
CREATE TABLE IF NOT EXISTS flight_recorder.index_snapshots (
    snapshot_id         INTEGER REFERENCES flight_recorder.snapshots(id) ON DELETE CASCADE,
    schemaname          TEXT NOT NULL,
    relname             TEXT NOT NULL,  -- Table name
    indexrelname        TEXT NOT NULL,  -- Index name
    relid               OID NOT NULL,   -- Table OID
    indexrelid          OID NOT NULL,   -- Index OID
    idx_scan            BIGINT,
    idx_tup_read        BIGINT,
    idx_tup_fetch       BIGINT,
    index_size_bytes    BIGINT,
    PRIMARY KEY (snapshot_id, indexrelid)
);

CREATE INDEX IF NOT EXISTS index_snapshots_indexrelid_idx
    ON flight_recorder.index_snapshots(indexrelid);

CREATE INDEX IF NOT EXISTS index_snapshots_relid_idx
    ON flight_recorder.index_snapshots(relid);
```

### Collection Strategy

**Smart Sampling**:

- Collect indexes for the top N hottest tables (from table stats)
- Or collect all indexes if total count is manageable

```sql
CREATE OR REPLACE FUNCTION flight_recorder._collect_index_stats(p_snapshot_id INTEGER)
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    v_enabled BOOLEAN;
BEGIN
    v_enabled := COALESCE(
        flight_recorder._get_config('index_stats_enabled', 'true')::boolean,
        true
    );

    IF NOT v_enabled THEN
        RETURN;
    END IF;

    INSERT INTO flight_recorder.index_snapshots (
        snapshot_id, schemaname, relname, indexrelname, relid, indexrelid,
        idx_scan, idx_tup_read, idx_tup_fetch, index_size_bytes
    )
    SELECT
        p_snapshot_id,
        i.schemaname,
        i.relname,
        i.indexrelname,
        i.relid,
        i.indexrelid,
        i.idx_scan,
        i.idx_tup_read,
        i.idx_tup_fetch,
        pg_relation_size(i.indexrelid) AS index_size_bytes
    FROM pg_stat_user_indexes i
    -- Optional: only collect for hot tables
    -- WHERE i.relid IN (SELECT relid FROM flight_recorder.table_snapshots WHERE snapshot_id = p_snapshot_id)
    ;
END;
$$;
```

### Analysis Functions

**1. Unused Index Detection**

```sql
CREATE OR REPLACE FUNCTION flight_recorder.unused_indexes(
    p_lookback_interval INTERVAL DEFAULT '7 days'
)
RETURNS TABLE(
    schemaname      TEXT,
    relname         TEXT,
    indexrelname    TEXT,
    index_size      TEXT,
    last_scan_count BIGINT,
    recommendation  TEXT
)
LANGUAGE sql STABLE AS $$
    WITH latest_snapshot AS (
        SELECT max(id) AS snapshot_id
        FROM flight_recorder.snapshots
        WHERE captured_at > now() - p_lookback_interval
    ),
    earliest_snapshot AS (
        SELECT min(id) AS snapshot_id
        FROM flight_recorder.snapshots
        WHERE captured_at > now() - p_lookback_interval
    ),
    index_usage AS (
        SELECT
            e.schemaname,
            e.relname,
            e.indexrelname,
            e.indexrelid,
            e.index_size_bytes,
            e.idx_scan - COALESCE(s.idx_scan, 0) AS scan_delta
        FROM flight_recorder.index_snapshots e
        CROSS JOIN latest_snapshot ls
        LEFT JOIN flight_recorder.index_snapshots s
            ON s.indexrelid = e.indexrelid
            AND s.snapshot_id = (SELECT snapshot_id FROM earliest_snapshot)
        WHERE e.snapshot_id = ls.snapshot_id
    )
    SELECT
        iu.schemaname,
        iu.relname,
        iu.indexrelname,
        flight_recorder._pretty_bytes(iu.index_size_bytes) AS index_size,
        iu.scan_delta AS last_scan_count,
        CASE
            WHEN iu.scan_delta = 0 THEN 'DROP INDEX (never used in ' || p_lookback_interval::text || ')'
            WHEN iu.scan_delta < 10 THEN 'Consider dropping (rarely used)'
            ELSE 'Keep (actively used)'
        END AS recommendation
    FROM index_usage iu
    WHERE iu.scan_delta < 100  -- Threshold for "rarely used"
        AND iu.indexrelname NOT LIKE '%_pkey'  -- Don't suggest dropping primary keys
    ORDER BY iu.index_size_bytes DESC
$$;
```

**2. Index Efficiency Analysis**

```sql
CREATE OR REPLACE FUNCTION flight_recorder.index_efficiency(
    p_start_time TIMESTAMPTZ,
    p_end_time TIMESTAMPTZ,
    p_limit INTEGER DEFAULT 25
)
RETURNS TABLE(
    schemaname          TEXT,
    relname             TEXT,
    indexrelname        TEXT,
    idx_scan_delta      BIGINT,
    idx_tup_read_delta  BIGINT,
    idx_tup_fetch_delta BIGINT,
    selectivity         NUMERIC,
    index_size          TEXT,
    scans_per_gb        NUMERIC
)
LANGUAGE sql STABLE AS $$
    WITH
    start_snap AS (
        SELECT i.*
        FROM flight_recorder.index_snapshots i
        JOIN flight_recorder.snapshots s ON s.id = i.snapshot_id
        WHERE s.captured_at <= p_start_time
        ORDER BY s.captured_at DESC
        LIMIT 1000
    ),
    end_snap AS (
        SELECT i.*
        FROM flight_recorder.index_snapshots i
        JOIN flight_recorder.snapshots s ON s.id = i.snapshot_id
        WHERE s.captured_at >= p_end_time
        ORDER BY s.captured_at ASC
        LIMIT 1000
    )
    SELECT
        e.schemaname,
        e.relname,
        e.indexrelname,
        e.idx_scan - COALESCE(s.idx_scan, 0) AS idx_scan_delta,
        e.idx_tup_read - COALESCE(s.idx_tup_read, 0) AS idx_tup_read_delta,
        e.idx_tup_fetch - COALESCE(s.idx_tup_fetch, 0) AS idx_tup_fetch_delta,
        CASE
            WHEN (e.idx_tup_read - COALESCE(s.idx_tup_read, 0)) > 0
            THEN round(100.0 * (e.idx_tup_fetch - COALESCE(s.idx_tup_fetch, 0)) /
                             (e.idx_tup_read - COALESCE(s.idx_tup_read, 0)), 1)
            ELSE NULL
        END AS selectivity,
        flight_recorder._pretty_bytes(e.index_size_bytes) AS index_size,
        CASE
            WHEN e.index_size_bytes > 0
            THEN round((e.idx_scan - COALESCE(s.idx_scan, 0)) /
                      (e.index_size_bytes / 1073741824.0::numeric), 2)
            ELSE NULL
        END AS scans_per_gb
    FROM end_snap e
    LEFT JOIN start_snap s ON s.indexrelid = e.indexrelid
    WHERE (e.idx_scan - COALESCE(s.idx_scan, 0)) > 0
    ORDER BY idx_scan_delta DESC
    LIMIT p_limit
$$;
```

### Configuration

```sql
INSERT INTO flight_recorder.config (key, value) VALUES
    ('index_stats_enabled', 'true'),
    ('index_stats_retention_days', '30')
ON CONFLICT (key) DO NOTHING;
```

### Impact Assessment

- **Storage**: Varies by schema size, but typically ~100-500 indexes
  - Estimate: 300 indexes × 10 columns × 8 bytes = 24 KB per snapshot
  - At 3-min interval for 30 days: ~24 KB × 14,400 = **~346 MB**
- **CPU**: Single query to `pg_stat_user_indexes` + size lookups
  - Estimated: **< 10ms** per collection (depends on index count)
- **Overhead**: **Negligible** (0.006% of 3-minute window)

---

## Feature 3: PostgreSQL Configuration Snapshots

### Problem Statement

When diagnosing issues, knowing the configuration context is critical:

- Was `work_mem` too low when queries spilled to disk?
- What was `max_connections` when we hit connection limits?
- Did someone change `shared_buffers` recently?

### Data Source

`pg_settings` view provides all configuration parameters with:

- `name` - Parameter name
- `setting` - Current value
- `unit` - Units (e.g., 'kB', 'ms', '8kB')
- `source` - Where the value came from (default, config file, ALTER SYSTEM, etc.)
- `sourcefile`, `sourceline` - Location in config file

**Overhead**: Querying `pg_settings` is very cheap (< 1ms).

### Storage Design

**Strategy**: Only capture **relevant** settings (not all 300+ parameters).

```sql
CREATE TABLE IF NOT EXISTS flight_recorder.config_snapshots (
    snapshot_id     INTEGER REFERENCES flight_recorder.snapshots(id) ON DELETE CASCADE,
    name            TEXT NOT NULL,
    setting         TEXT,
    unit            TEXT,
    source          TEXT,
    sourcefile      TEXT,
    PRIMARY KEY (snapshot_id, name)
);

CREATE INDEX IF NOT EXISTS config_snapshots_name_idx
    ON flight_recorder.config_snapshots(name);
```

### Collection Strategy

**Whitelist Approach**: Only collect settings that matter for diagnostics.

```sql
CREATE OR REPLACE FUNCTION flight_recorder._collect_config_snapshot(p_snapshot_id INTEGER)
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    v_enabled BOOLEAN;
    v_relevant_params TEXT[] := ARRAY[
        -- Memory
        'shared_buffers',
        'work_mem',
        'maintenance_work_mem',
        'effective_cache_size',
        'temp_buffers',
        -- Connections
        'max_connections',
        'superuser_reserved_connections',
        -- Query Planning
        'random_page_cost',
        'seq_page_cost',
        'effective_io_concurrency',
        'default_statistics_target',
        'enable_seqscan',
        'enable_indexscan',
        'enable_bitmapscan',
        'enable_hashjoin',
        'enable_mergejoin',
        'enable_nestloop',
        -- Parallelism
        'max_parallel_workers',
        'max_parallel_workers_per_gather',
        'max_worker_processes',
        'parallel_setup_cost',
        'parallel_tuple_cost',
        -- WAL
        'wal_level',
        'max_wal_size',
        'min_wal_size',
        'wal_buffers',
        'checkpoint_timeout',
        'checkpoint_completion_target',
        'checkpoint_warning',
        -- Autovacuum
        'autovacuum',
        'autovacuum_max_workers',
        'autovacuum_naptime',
        'autovacuum_vacuum_threshold',
        'autovacuum_vacuum_scale_factor',
        'autovacuum_analyze_threshold',
        'autovacuum_analyze_scale_factor',
        'autovacuum_vacuum_cost_delay',
        'autovacuum_vacuum_cost_limit',
        -- Logging
        'log_min_duration_statement',
        'log_lock_waits',
        'log_temp_files',
        'log_autovacuum_min_duration',
        -- Statement Behavior
        'statement_timeout',
        'lock_timeout',
        'idle_in_transaction_session_timeout',
        -- Resource Limits
        'temp_file_limit',
        'max_prepared_transactions',
        'max_locks_per_transaction',
        -- Extensions
        'shared_preload_libraries',
        'pg_stat_statements.track',
        'pg_stat_statements.max'
    ];
BEGIN
    v_enabled := COALESCE(
        flight_recorder._get_config('config_snapshots_enabled', 'true')::boolean,
        true
    );

    IF NOT v_enabled THEN
        RETURN;
    END IF;

    INSERT INTO flight_recorder.config_snapshots (
        snapshot_id, name, setting, unit, source, sourcefile
    )
    SELECT
        p_snapshot_id,
        name,
        setting,
        unit,
        source,
        sourcefile
    FROM pg_settings
    WHERE name = ANY(v_relevant_params);
END;
$$;
```

### Analysis Functions

**1. Configuration Changes Over Time**

```sql
CREATE OR REPLACE FUNCTION flight_recorder.config_changes(
    p_start_time TIMESTAMPTZ,
    p_end_time TIMESTAMPTZ
)
RETURNS TABLE(
    parameter_name  TEXT,
    old_value       TEXT,
    new_value       TEXT,
    old_source      TEXT,
    new_source      TEXT,
    changed_at      TIMESTAMPTZ
)
LANGUAGE sql STABLE AS $$
    WITH
    start_configs AS (
        SELECT cs.name, cs.setting, cs.unit, cs.source, s.captured_at
        FROM flight_recorder.config_snapshots cs
        JOIN flight_recorder.snapshots s ON s.id = cs.snapshot_id
        WHERE s.captured_at <= p_start_time
        ORDER BY s.captured_at DESC
        LIMIT 100
    ),
    end_configs AS (
        SELECT cs.name, cs.setting, cs.unit, cs.source, s.captured_at
        FROM flight_recorder.config_snapshots cs
        JOIN flight_recorder.snapshots s ON s.id = cs.snapshot_id
        WHERE s.captured_at >= p_end_time
        ORDER BY s.captured_at ASC
        LIMIT 100
    )
    SELECT
        COALESCE(e.name, s.name) AS parameter_name,
        s.setting || COALESCE(' ' || s.unit, '') AS old_value,
        e.setting || COALESCE(' ' || e.unit, '') AS new_value,
        s.source AS old_source,
        e.source AS new_source,
        e.captured_at AS changed_at
    FROM end_configs e
    FULL OUTER JOIN start_configs s ON s.name = e.name
    WHERE e.setting IS DISTINCT FROM s.setting
        OR e.source IS DISTINCT FROM s.source
    ORDER BY parameter_name
$$;
```

**2. Configuration Context for Incident**

```sql
CREATE OR REPLACE FUNCTION flight_recorder.config_at(
    p_timestamp TIMESTAMPTZ,
    p_category TEXT DEFAULT NULL  -- 'memory', 'connections', 'autovacuum', etc.
)
RETURNS TABLE(
    parameter_name  TEXT,
    value           TEXT,
    source          TEXT
)
LANGUAGE sql STABLE AS $$
    SELECT
        cs.name AS parameter_name,
        cs.setting || COALESCE(' ' || cs.unit, '') AS value,
        cs.source
    FROM flight_recorder.config_snapshots cs
    JOIN flight_recorder.snapshots s ON s.id = cs.snapshot_id
    WHERE s.captured_at <= p_timestamp
        AND (p_category IS NULL OR cs.name LIKE p_category || '%')
    ORDER BY s.captured_at DESC
    LIMIT 100
$$;
```

**3. Configuration Health Check**

```sql
CREATE OR REPLACE FUNCTION flight_recorder.config_health_check()
RETURNS TABLE(
    category        TEXT,
    parameter_name  TEXT,
    current_value   TEXT,
    issue           TEXT,
    recommendation  TEXT
)
LANGUAGE plpgsql STABLE AS $$
DECLARE
    v_shared_buffers BIGINT;
    v_work_mem BIGINT;
    v_max_connections INTEGER;
BEGIN
    -- Get current values
    SELECT setting::bigint * 8192 INTO v_shared_buffers
    FROM pg_settings WHERE name = 'shared_buffers';

    SELECT setting::bigint * 1024 INTO v_work_mem
    FROM pg_settings WHERE name = 'work_mem';

    SELECT setting::integer INTO v_max_connections
    FROM pg_settings WHERE name = 'max_connections';

    -- Check shared_buffers (should be 25% of RAM for dedicated DB server)
    IF v_shared_buffers < 134217728 THEN  -- < 128 MB
        category := 'memory';
        parameter_name := 'shared_buffers';
        current_value := flight_recorder._pretty_bytes(v_shared_buffers);
        issue := 'Very low shared_buffers';
        recommendation := 'Increase to at least 25% of available RAM';
        RETURN NEXT;
    END IF;

    -- Check work_mem (should be at least 16MB for analytical workloads)
    IF v_work_mem < 16777216 THEN  -- < 16 MB
        category := 'memory';
        parameter_name := 'work_mem';
        current_value := flight_recorder._pretty_bytes(v_work_mem);
        issue := 'Low work_mem may cause disk spills';
        recommendation := 'Consider increasing to 32-64MB, depending on workload';
        RETURN NEXT;
    END IF;

    -- Check max_connections (high values waste RAM)
    IF v_max_connections > 200 THEN
        category := 'connections';
        parameter_name := 'max_connections';
        current_value := v_max_connections::text;
        issue := 'High max_connections wastes memory';
        recommendation := 'Use connection pooling (pgBouncer) instead of high max_connections';
        RETURN NEXT;
    END IF;

    -- Check if statement timeout is set
    IF NOT EXISTS (
        SELECT 1 FROM pg_settings
        WHERE name = 'statement_timeout' AND setting != '0'
    ) THEN
        category := 'safety';
        parameter_name := 'statement_timeout';
        current_value := 'disabled';
        issue := 'No statement timeout protection';
        recommendation := 'Set statement_timeout to prevent runaway queries (e.g., 30s-5min)';
        RETURN NEXT;
    END IF;

    -- Add more checks as needed...
    RETURN;
END;
$$;
```

### Configuration

```sql
INSERT INTO flight_recorder.config (key, value) VALUES
    ('config_snapshots_enabled', 'true'),
    ('config_snapshots_retention_days', '90')  -- Keep longer for trend analysis
ON CONFLICT (key) DO NOTHING;
```

### Impact Assessment

- **Storage**: ~50 relevant parameters × 5 columns × 50 bytes ≈ 12.5 KB per snapshot
  - At 3-min interval for 90 days: ~12.5 KB × 43,200 = **~540 MB**
  - (But configs rarely change, so compression would be very effective)
- **CPU**: Single filtered query to `pg_settings`
  - Estimated: **< 1ms** per collection
- **Overhead**: **Negligible** (0.0006% of 3-minute window)

---

## Implementation Roadmap

### Phase 1: Table-Level Hotspots (Highest Value)

1. Add `table_snapshots` table
2. Add `_collect_table_stats()` function
3. Integrate into `snapshot()` function
4. Add analysis functions: `table_compare()`, `table_hotspots()`
5. Update diagnostic playbooks with table-level queries

**Estimated Effort**: 2-3 hours of implementation + testing

### Phase 2: Configuration Snapshots (High Value, Low Cost)

1. Add `config_snapshots` table
2. Add `_collect_config_snapshot()` function
3. Integrate into `snapshot()` function
4. Add analysis functions: `config_at()`, `config_changes()`, `config_health_check()`

**Estimated Effort**: 1-2 hours

### Phase 3: Index Usage Tracking (Good Value, Moderate Storage)

1. Add `index_snapshots` table
2. Add `_collect_index_stats()` function
3. Integrate into `snapshot()` function
4. Add analysis functions: `unused_indexes()`, `index_efficiency()`

**Estimated Effort**: 2-3 hours

---

## Testing Strategy

For each feature:

1. **Unit Test**: Verify collection functions work

   ```sql
   SELECT flight_recorder._collect_table_stats(1);
   SELECT * FROM flight_recorder.table_snapshots WHERE snapshot_id = 1;
   ```

2. **Performance Test**: Measure overhead

   ```sql
   \timing on
   SELECT flight_recorder._collect_table_stats(1);
   -- Should be < 10ms
   ```

3. **Integration Test**: Run full `snapshot()` cycle

   ```sql
   SELECT flight_recorder.snapshot();
   -- Verify all new tables populated
   ```

4. **Load Test**: Run on production-scale database
   - Monitor collection_stats for duration increases
   - Verify circuit breaker doesn't trip

---

## Rollback Strategy

Each feature can be independently disabled via configuration:

```sql
-- Disable a feature without uninstalling
SELECT flight_recorder._set_config('table_stats_enabled', 'false');
SELECT flight_recorder._set_config('index_stats_enabled', 'false');
SELECT flight_recorder._set_config('config_snapshots_enabled', 'false');
```

To fully remove a feature:

```sql
-- Drop tables (will cascade delete data)
DROP TABLE flight_recorder.table_snapshots CASCADE;
DROP TABLE flight_recorder.index_snapshots CASCADE;
DROP TABLE flight_recorder.config_snapshots CASCADE;

-- Remove collection functions
DROP FUNCTION flight_recorder._collect_table_stats(INTEGER);
DROP FUNCTION flight_recorder._collect_index_stats(INTEGER);
DROP FUNCTION flight_recorder._collect_config_snapshot(INTEGER);
```

---

**Last Updated**: 2026-01-18
**Status**: Design Phase - Ready for Implementation
**Estimated Total Storage Impact**: ~972 MB over 30 days (all features combined)
**Estimated Total CPU Impact**: < 16ms per 3-minute collection (0.009% overhead)
