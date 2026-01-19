# AI Integration Design: Self-Diagnosing Database

**Purpose**: Design document for integrating LLM APIs (Claude, GPT, etc.) directly into pg-flight-recorder for automated diagnostics.
**Status**: Design Phase - Not Yet Implemented
**Prerequisites**: `pg_http` or `pg_net` extension + API credentials

---

## The Core Idea

Instead of:

1. Query flight_recorder data in psql
2. Copy/paste into ChatGPT/Claude
3. Get diagnosis
4. Go back to psql for follow-up

Do this:

1. `SELECT flight_recorder.ai_diagnose('2025-01-17 10:00:00', '2025-01-17 11:00:00');`
2. Get diagnosis directly in psql
3. Done.

**The database diagnoses itself.**

---

## Level 1: Manual AI Diagnosis

### The Simplest Useful Thing

```sql
SELECT flight_recorder.ai_diagnose(
    '2025-01-17 10:00:00'::timestamptz,
    '2025-01-17 11:00:00'::timestamptz
);
```

**Returns**: Markdown-formatted diagnosis with root cause, fixes, and evidence.

### Example Output

```
## Root Cause: Forced Checkpoint + Lock Contention

Your database experienced a forced checkpoint at 10:23am that took
45 seconds to write buffers. During this time, 3 long-running UPDATE
queries on the `users` table blocked 47 other queries, with max wait
time of 2 minutes.

## Immediate Fix
1. Increase max_wal_size to 4GB (currently 1GB)
2. Review application code for transaction boundaries

## Evidence
- Checkpoint forced at 10:23:17 (ckpt_requested increased by 1)
- PID 1234 blocked 47 sessions with UPDATE query
- Query: UPDATE users SET last_seen = now() WHERE id IN (...)
- Wait event summary: 89% of samples showing Lock:relation
```

### Implementation

```sql
CREATE OR REPLACE FUNCTION flight_recorder.ai_diagnose(
    p_start_time TIMESTAMPTZ,
    p_end_time TIMESTAMPTZ,
    p_model TEXT DEFAULT 'claude-3-5-sonnet-20241022'
)
RETURNS TEXT
LANGUAGE plpgsql AS $$
DECLARE
    v_api_key TEXT;
    v_context JSONB;
    v_prompt TEXT;
    v_response JSONB;
    v_diagnosis TEXT;
BEGIN
    -- Get API key from secure config
    v_api_key := current_setting('flight_recorder.anthropic_api_key', true);
    IF v_api_key IS NULL THEN
        RAISE EXCEPTION 'Anthropic API key not configured. Set flight_recorder.anthropic_api_key';
    END IF;

    -- Check budget before proceeding
    IF NOT flight_recorder._check_ai_budget() THEN
        RAISE EXCEPTION 'Daily AI budget exceeded. Increase ai_daily_budget_usd or wait until tomorrow.';
    END IF;

    -- Gather diagnostic context
    v_context := flight_recorder._build_diagnostic_context(p_start_time, p_end_time);

    -- Build prompt
    v_prompt := flight_recorder._build_diagnostic_prompt(v_context, p_start_time, p_end_time);

    -- Call Claude API
    v_response := flight_recorder._call_anthropic_api(v_prompt, p_model);

    -- Extract diagnosis
    v_diagnosis := v_response->'content'->0->>'text';

    -- Log usage for auditing and cost tracking
    INSERT INTO flight_recorder.ai_diagnoses (
        created_at,
        time_window_start,
        time_window_end,
        model,
        context_size_bytes,
        diagnosis,
        tokens_used,
        cost_usd
    ) VALUES (
        now(),
        p_start_time,
        p_end_time,
        p_model,
        length(v_context::text),
        v_diagnosis,
        (v_response->'usage'->>'total_tokens')::integer,
        flight_recorder._calculate_cost(v_response->'usage', p_model)
    );

    RETURN v_diagnosis;
EXCEPTION
    WHEN OTHERS THEN
        -- Log error but provide fallback message
        INSERT INTO flight_recorder.ai_errors (
            occurred_at,
            error_message,
            context
        ) VALUES (now(), SQLERRM, jsonb_build_object('start', p_start_time, 'end', p_end_time));

        RETURN format(E'AI diagnosis unavailable (error: %s).\n\nUse manual diagnostic functions:\n- flight_recorder.anomaly_report(%L, %L)\n- flight_recorder.summary_report(%L, %L)',
                     SQLERRM, p_start_time, p_end_time, p_start_time, p_end_time);
END;
$$;
```

### Supporting Functions

```sql
-- Build context from flight recorder data
CREATE OR REPLACE FUNCTION flight_recorder._build_diagnostic_context(
    p_start_time TIMESTAMPTZ,
    p_end_time TIMESTAMPTZ
)
RETURNS JSONB
LANGUAGE plpgsql AS $$
DECLARE
    v_context JSONB;
BEGIN
    v_context := jsonb_build_object(
        'time_window', jsonb_build_object(
            'start', p_start_time,
            'end', p_end_time,
            'duration_minutes', round(EXTRACT(EPOCH FROM (p_end_time - p_start_time)) / 60)
        ),
        'anomalies', (
            SELECT jsonb_agg(
                jsonb_build_object(
                    'type', anomaly_type,
                    'severity', severity,
                    'description', description,
                    'metric_value', metric_value,
                    'recommendation', recommendation
                )
            )
            FROM flight_recorder.anomaly_report(p_start_time, p_end_time)
        ),
        'system_metrics', (
            SELECT row_to_json(c)::jsonb
            FROM flight_recorder.compare(p_start_time, p_end_time) c
        ),
        'wait_events', (
            SELECT jsonb_agg(
                jsonb_build_object(
                    'wait_event_type', wait_event_type,
                    'wait_event', wait_event,
                    'backend_type', backend_type,
                    'state', state,
                    'total_waiters', total_waiters,
                    'avg_waiters', avg_waiters,
                    'max_waiters', max_waiters,
                    'pct_of_samples', pct_of_samples
                )
            )
            FROM (
                SELECT * FROM flight_recorder.wait_summary(p_start_time, p_end_time)
                ORDER BY total_waiters DESC
                LIMIT 10
            ) w
        ),
        'slow_queries', (
            SELECT jsonb_agg(
                jsonb_build_object(
                    'query_preview', query_preview,
                    'calls_delta', calls_delta,
                    'total_time_delta_ms', total_exec_time_delta_ms,
                    'mean_time_increase_ms', mean_exec_time_end_ms - COALESCE(mean_exec_time_start_ms, 0),
                    'temp_blocks_written', temp_blks_written_delta,
                    'cache_hit_ratio_pct', hit_ratio_pct
                )
            )
            FROM (
                SELECT * FROM flight_recorder.statement_compare(p_start_time, p_end_time, 100, 10)
            ) s
        ),
        'lock_summary', (
            SELECT jsonb_build_object(
                'total_blocked_sessions', count(DISTINCT blocked_pid),
                'max_block_duration_seconds', EXTRACT(EPOCH FROM max(blocked_duration)),
                'top_blockers', jsonb_agg(
                    jsonb_build_object(
                        'blocking_pid', blocking_pid,
                        'blocking_query', blocked_query_preview,
                        'blocked_count', count(*)
                    )
                )
            )
            FROM flight_recorder.lock_samples_archive
            WHERE captured_at BETWEEN p_start_time AND p_end_time
            GROUP BY blocking_pid, blocked_query_preview
            ORDER BY count(*) DESC
            LIMIT 5
        )
    );

    RETURN v_context;
END;
$$;

-- Build the prompt for the LLM
CREATE OR REPLACE FUNCTION flight_recorder._build_diagnostic_prompt(
    p_context JSONB,
    p_start_time TIMESTAMPTZ,
    p_end_time TIMESTAMPTZ
)
RETURNS TEXT
LANGUAGE sql AS $$
    SELECT format(
        E'You are a PostgreSQL database performance expert analyzing diagnostic data from pg-flight-recorder.\n\n' ||
        E'## Time Window\n%s to %s (%s minutes)\n\n' ||
        E'## Diagnostic Data\n```json\n%s\n```\n\n' ||
        E'Provide a concise diagnosis with:\n' ||
        E'1. **Root Cause**: What caused the performance issue?\n' ||
        E'2. **Immediate Fix**: Actionable steps to resolve it\n' ||
        E'3. **Evidence**: Cite specific metrics from the data\n\n' ||
        E'Format your response in markdown. Be specific and reference actual values from the data.',
        p_start_time::text,
        p_end_time::text,
        (p_context->'time_window'->>'duration_minutes'),
        jsonb_pretty(p_context)
    )
$$;

-- Call Anthropic API via pg_http
CREATE OR REPLACE FUNCTION flight_recorder._call_anthropic_api(
    p_prompt TEXT,
    p_model TEXT DEFAULT 'claude-3-5-sonnet-20241022'
)
RETURNS JSONB
LANGUAGE plpgsql AS $$
DECLARE
    v_api_key TEXT;
    v_response http_response;
    v_result JSONB;
BEGIN
    v_api_key := current_setting('flight_recorder.anthropic_api_key');

    -- Make HTTP request
    SELECT * INTO v_response
    FROM http((
        'POST',
        'https://api.anthropic.com/v1/messages',
        ARRAY[
            http_header('x-api-key', v_api_key),
            http_header('anthropic-version', '2023-06-01'),
            http_header('content-type', 'application/json')
        ],
        'application/json',
        jsonb_build_object(
            'model', p_model,
            'max_tokens', 2000,
            'messages', jsonb_build_array(
                jsonb_build_object(
                    'role', 'user',
                    'content', p_prompt
                )
            )
        )::text
    ));

    -- Check response status
    IF v_response.status != 200 THEN
        RAISE EXCEPTION 'Anthropic API error (status %): %', v_response.status, v_response.content;
    END IF;

    v_result := v_response.content::jsonb;
    RETURN v_result;
END;
$$;

-- Calculate API cost based on token usage
CREATE OR REPLACE FUNCTION flight_recorder._calculate_cost(
    p_usage JSONB,
    p_model TEXT
)
RETURNS NUMERIC
LANGUAGE plpgsql AS $$
DECLARE
    v_input_tokens INTEGER;
    v_output_tokens INTEGER;
    v_cost NUMERIC;
BEGIN
    v_input_tokens := (p_usage->>'input_tokens')::integer;
    v_output_tokens := (p_usage->>'output_tokens')::integer;

    -- Pricing as of 2025 (subject to change)
    CASE p_model
        WHEN 'claude-3-5-sonnet-20241022' THEN
            v_cost := (v_input_tokens * 0.000003) + (v_output_tokens * 0.000015);
        WHEN 'claude-3-5-haiku-20241022' THEN
            v_cost := (v_input_tokens * 0.000001) + (v_output_tokens * 0.000005);
        ELSE
            v_cost := 0.0;  -- Unknown model, can't calculate
    END CASE;

    RETURN round(v_cost, 6);
END;
$$;
```

### Storage Tables

```sql
-- Log all AI diagnoses for auditing and learning
CREATE TABLE IF NOT EXISTS flight_recorder.ai_diagnoses (
    id                      BIGSERIAL PRIMARY KEY,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    time_window_start       TIMESTAMPTZ NOT NULL,
    time_window_end         TIMESTAMPTZ NOT NULL,
    model                   TEXT NOT NULL,
    context_size_bytes      INTEGER,
    diagnosis               TEXT NOT NULL,
    tokens_used             INTEGER,
    cost_usd                NUMERIC,
    auto_generated          BOOLEAN DEFAULT false,
    feedback_helpful        BOOLEAN,
    feedback_notes          TEXT
);

CREATE INDEX ai_diagnoses_created_at_idx ON flight_recorder.ai_diagnoses(created_at DESC);
CREATE INDEX ai_diagnoses_time_window_idx ON flight_recorder.ai_diagnoses(time_window_start, time_window_end);

-- Track API errors
CREATE TABLE IF NOT EXISTS flight_recorder.ai_errors (
    id              BIGSERIAL PRIMARY KEY,
    occurred_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    error_message   TEXT NOT NULL,
    context         JSONB
);

-- Track API usage for cost control
CREATE TABLE IF NOT EXISTS flight_recorder.ai_usage (
    id              BIGSERIAL PRIMARY KEY,
    called_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    function_name   TEXT NOT NULL,
    tokens_used     INTEGER,
    cost_usd        NUMERIC
);

CREATE INDEX ai_usage_called_at_idx ON flight_recorder.ai_usage(called_at DESC);
```

---

## Level 2: Automatic Self-Diagnosis

### Continuous Monitoring with Auto-Diagnosis

```sql
-- Scheduled via pg_cron every 15 minutes
SELECT cron.schedule(
    'auto-diagnose',
    '*/15 * * * *',
    'SELECT flight_recorder.auto_diagnose()'
);
```

### Implementation

```sql
CREATE OR REPLACE FUNCTION flight_recorder.auto_diagnose()
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    v_enabled BOOLEAN;
    v_anomaly_count INTEGER;
    v_diagnosis TEXT;
    v_lookback INTERVAL;
BEGIN
    -- Check if auto-diagnosis is enabled
    v_enabled := COALESCE(
        flight_recorder._get_config('auto_diagnosis_enabled', 'false')::boolean,
        false
    );

    IF NOT v_enabled THEN
        RETURN;
    END IF;

    -- Don't run if budget is exceeded
    IF NOT flight_recorder._check_ai_budget() THEN
        RETURN;
    END IF;

    v_lookback := flight_recorder._get_config('auto_diagnosis_lookback', '15 minutes')::interval;

    -- Check for anomalies
    SELECT count(*) INTO v_anomaly_count
    FROM flight_recorder.anomaly_report(
        now() - v_lookback,
        now()
    )
    WHERE severity IN ('high', 'medium');

    -- Only diagnose if there are anomalies
    IF v_anomaly_count > 0 THEN
        v_diagnosis := flight_recorder.ai_diagnose(
            now() - v_lookback,
            now()
        );

        -- Mark as auto-generated
        UPDATE flight_recorder.ai_diagnoses
        SET auto_generated = true
        WHERE id = (SELECT max(id) FROM flight_recorder.ai_diagnoses);

        -- Optional: Send alert
        IF flight_recorder._get_config('auto_diagnosis_alert', 'false')::boolean THEN
            PERFORM flight_recorder._send_alert(
                'Database Performance Alert',
                v_diagnosis
            );
        END IF;
    END IF;
END;
$$;
```

### Configuration

```sql
INSERT INTO flight_recorder.config (key, value) VALUES
    ('auto_diagnosis_enabled', 'false'),  -- Opt-in
    ('auto_diagnosis_lookback', '15 minutes'),
    ('auto_diagnosis_alert', 'false'),
    ('ai_daily_budget_usd', '10.00')
ON CONFLICT (key) DO NOTHING;
```

---

## Level 3: Conversational Database Agent

### Multi-Turn Investigation with Tool Use

The AI can iteratively drill down by calling flight_recorder functions.

```sql
SELECT flight_recorder.ai_chat('Why was the database slow between 8-9am today?');
```

**Behind the scenes**:

1. AI receives question + schema of available flight_recorder functions
2. AI calls `anomaly_report('08:00', '09:00')` → sees temp file spills
3. AI calls `statement_compare('08:00', '09:00')` → identifies specific query
4. AI calls `config_at('08:00')` → checks work_mem setting
5. AI synthesizes findings into diagnosis

### Implementation Approach

Use Claude's tool use / function calling:

```sql
CREATE OR REPLACE FUNCTION flight_recorder.ai_chat(p_question TEXT)
RETURNS TEXT
LANGUAGE plpgsql AS $$
DECLARE
    v_tools JSONB;
    v_messages JSONB[];
    v_response JSONB;
    v_tool_calls JSONB;
    v_final_response TEXT;
    v_iterations INTEGER := 0;
    v_max_iterations INTEGER := 5;
BEGIN
    -- Define available tools (flight_recorder functions)
    v_tools := jsonb_build_array(
        jsonb_build_object(
            'name', 'anomaly_report',
            'description', 'Detect anomalies in a time window',
            'input_schema', jsonb_build_object(
                'type', 'object',
                'properties', jsonb_build_object(
                    'start_time', jsonb_build_object('type', 'string', 'format', 'date-time'),
                    'end_time', jsonb_build_object('type', 'string', 'format', 'date-time')
                ),
                'required', jsonb_build_array('start_time', 'end_time')
            )
        ),
        jsonb_build_object(
            'name', 'statement_compare',
            'description', 'Compare query performance between two time periods',
            'input_schema', jsonb_build_object(
                'type', 'object',
                'properties', jsonb_build_object(
                    'start_time', jsonb_build_object('type', 'string'),
                    'end_time', jsonb_build_object('type', 'string'),
                    'limit', jsonb_build_object('type', 'integer', 'default', 25)
                ),
                'required', jsonb_build_array('start_time', 'end_time')
            )
        ),
        jsonb_build_object(
            'name', 'config_at',
            'description', 'Get PostgreSQL configuration at a specific time',
            'input_schema', jsonb_build_object(
                'type', 'object',
                'properties', jsonb_build_object(
                    'timestamp', jsonb_build_object('type', 'string')
                ),
                'required', jsonb_build_array('timestamp')
            )
        )
        -- Add more tools as needed
    );

    -- Initial message
    v_messages := ARRAY[jsonb_build_object('role', 'user', 'content', p_question)];

    -- Iterative tool use loop
    LOOP
        v_iterations := v_iterations + 1;
        IF v_iterations > v_max_iterations THEN
            RAISE EXCEPTION 'Max iterations exceeded in AI chat';
        END IF;

        -- Call API with tools
        v_response := flight_recorder._call_anthropic_api_with_tools(
            v_messages,
            v_tools
        );

        -- Check if AI wants to use tools
        IF v_response->'stop_reason' = '"tool_use"' THEN
            -- Execute tool calls and add results to messages
            v_tool_calls := v_response->'content';
            v_messages := array_append(v_messages, v_response->'content');

            -- Execute each tool and collect results
            FOR i IN 0..jsonb_array_length(v_tool_calls)-1 LOOP
                DECLARE
                    v_tool JSONB := v_tool_calls->i;
                    v_tool_name TEXT := v_tool->>'name';
                    v_tool_input JSONB := v_tool->'input';
                    v_tool_result TEXT;
                BEGIN
                    -- Execute the tool
                    v_tool_result := flight_recorder._execute_tool(v_tool_name, v_tool_input);

                    -- Add result to messages
                    v_messages := array_append(v_messages, jsonb_build_object(
                        'role', 'user',
                        'content', jsonb_build_array(
                            jsonb_build_object(
                                'type', 'tool_result',
                                'tool_use_id', v_tool->>'id',
                                'content', v_tool_result
                            )
                        )
                    ));
                END;
            END LOOP;
        ELSE
            -- AI is done, extract final response
            v_final_response := v_response->'content'->0->>'text';
            EXIT;
        END IF;
    END LOOP;

    RETURN v_final_response;
END;
$$;
```

---

## Level 4: Query-Specific Analysis

### Explain and Optimize Individual Queries

```sql
SELECT flight_recorder.ai_explain_query($$
    SELECT u.*, COUNT(o.*)
    FROM users u
    LEFT JOIN orders o ON o.user_id = u.id
    WHERE u.email LIKE '%@gmail.com'
    GROUP BY u.id
$$);
```

### Implementation

```sql
CREATE OR REPLACE FUNCTION flight_recorder.ai_explain_query(
    p_query TEXT,
    p_explain BOOLEAN DEFAULT true
)
RETURNS TEXT
LANGUAGE plpgsql AS $$
DECLARE
    v_query_stats JSONB;
    v_explain_plan TEXT;
    v_prompt TEXT;
    v_response JSONB;
BEGIN
    -- Get historical stats for this query from flight_recorder
    v_query_stats := (
        SELECT jsonb_build_object(
            'recent_executions', count(*),
            'avg_time_ms', avg(mean_exec_time),
            'max_time_ms', max(max_exec_time),
            'temp_files', sum(temp_blks_written),
            'cache_hit_ratio', avg(
                CASE
                    WHEN (shared_blks_hit + shared_blks_read) > 0
                    THEN 100.0 * shared_blks_hit / (shared_blks_hit + shared_blks_read)
                    ELSE NULL
                END
            )
        )
        FROM flight_recorder.statement_snapshots
        WHERE query_preview LIKE substring(p_query, 1, 100) || '%'
            AND captured_at > now() - interval '24 hours'
    );

    -- Optionally get EXPLAIN plan
    IF p_explain THEN
        BEGIN
            EXECUTE 'EXPLAIN (FORMAT JSON) ' || p_query INTO v_explain_plan;
        EXCEPTION WHEN OTHERS THEN
            v_explain_plan := 'Unable to generate EXPLAIN plan: ' || SQLERRM;
        END;
    END IF;

    -- Build prompt
    v_prompt := format(
        E'Analyze this PostgreSQL query and provide optimization recommendations.\n\n' ||
        E'## Query\n```sql\n%s\n```\n\n' ||
        E'## Historical Performance (last 24 hours)\n```json\n%s\n```\n\n' ||
        E'## EXPLAIN Plan\n```json\n%s\n```\n\n' ||
        E'Provide:\n' ||
        E'1. Performance issues identified\n' ||
        E'2. Specific optimization recommendations\n' ||
        E'3. Rewritten query if applicable',
        p_query,
        jsonb_pretty(v_query_stats),
        v_explain_plan
    );

    v_response := flight_recorder._call_anthropic_api(v_prompt);
    RETURN v_response->'content'->0->>'text';
END;
$$;
```

---

## Level 5: Self-Healing Database (CAUTION!)

### Automated Remediation with Safety Rails

```sql
SELECT flight_recorder.ai_diagnose_and_fix(
    '2025-01-17 10:00:00'::timestamptz,
    '2025-01-17 11:00:00'::timestamptz,
    auto_fix => true  -- DANGEROUS: requires explicit opt-in
);
```

### Safe Actions Whitelist

Only allow AI to perform **provably safe** actions:

```sql
CREATE TYPE flight_recorder.remediation_action AS ENUM (
    'none',                     -- No action needed
    'vacuum_table',             -- VACUUM ANALYZE specific table
    'terminate_idle',           -- Kill idle in transaction sessions
    'terminate_blocker',        -- Kill specific blocking session
    'refresh_stats',            -- ANALYZE specific table
    'reindex_concurrent'        -- REINDEX CONCURRENTLY (PG 12+)
);
```

### Implementation

```sql
CREATE OR REPLACE FUNCTION flight_recorder.ai_diagnose_and_fix(
    p_start_time TIMESTAMPTZ,
    p_end_time TIMESTAMPTZ,
    p_auto_fix BOOLEAN DEFAULT false
)
RETURNS TABLE(
    diagnosis TEXT,
    recommended_action TEXT,
    action_taken TEXT,
    action_result TEXT
)
LANGUAGE plpgsql AS $$
DECLARE
    v_diagnosis_json JSONB;
    v_action_type TEXT;
    v_action_target TEXT;
    v_result TEXT;
BEGIN
    -- Get structured diagnosis from AI
    v_diagnosis_json := flight_recorder._ai_diagnose_structured(
        p_start_time,
        p_end_time
    );

    diagnosis := v_diagnosis_json->>'diagnosis';
    recommended_action := v_diagnosis_json->>'recommended_action';
    v_action_type := v_diagnosis_json->>'action_type';
    v_action_target := v_diagnosis_json->>'action_target';

    -- Only perform action if auto_fix is enabled
    IF NOT p_auto_fix THEN
        action_taken := 'Auto-fix disabled';
        action_result := NULL;
        RETURN NEXT;
        RETURN;
    END IF;

    -- Execute safe actions based on type
    CASE v_action_type
        WHEN 'vacuum_table' THEN
            -- Verify target is a valid table
            IF NOT EXISTS (
                SELECT 1 FROM pg_tables
                WHERE schemaname || '.' || tablename = v_action_target
            ) THEN
                action_taken := 'REJECTED: Invalid table name';
                action_result := NULL;
            ELSE
                action_taken := 'VACUUM ANALYZE ' || v_action_target;
                EXECUTE 'VACUUM ANALYZE ' || v_action_target;
                action_result := 'SUCCESS';
            END IF;

        WHEN 'terminate_idle' THEN
            -- Kill sessions idle in transaction > threshold
            DECLARE
                v_killed_count INTEGER;
            BEGIN
                SELECT count(*) INTO v_killed_count
                FROM pg_stat_activity
                WHERE state = 'idle in transaction'
                    AND state_change < now() - interval '10 minutes'
                    AND pid != pg_backend_pid();

                PERFORM pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE state = 'idle in transaction'
                    AND state_change < now() - interval '10 minutes'
                    AND pid != pg_backend_pid();

                action_taken := format('Terminated %s idle sessions', v_killed_count);
                action_result := 'SUCCESS';
            END;

        WHEN 'terminate_blocker' THEN
            -- Kill specific blocking PID (requires validation)
            DECLARE
                v_pid INTEGER := v_action_target::integer;
            BEGIN
                -- Verify PID is actually blocking something
                IF EXISTS (
                    SELECT 1 FROM flight_recorder.recent_locks_current()
                    WHERE blocking_pid = v_pid
                ) THEN
                    PERFORM pg_terminate_backend(v_pid);
                    action_taken := 'Terminated blocking PID ' || v_pid;
                    action_result := 'SUCCESS';
                ELSE
                    action_taken := 'REJECTED: PID not currently blocking';
                    action_result := NULL;
                END IF;
            END;

        WHEN 'refresh_stats' THEN
            -- ANALYZE specific table
            action_taken := 'ANALYZE ' || v_action_target;
            EXECUTE 'ANALYZE ' || v_action_target;
            action_result := 'SUCCESS';

        ELSE
            action_taken := 'No automated action available';
            action_result := NULL;
    END CASE;

    -- Log remediation action
    INSERT INTO flight_recorder.ai_remediations (
        executed_at,
        time_window_start,
        time_window_end,
        diagnosis,
        action_type,
        action_target,
        action_taken,
        action_result
    ) VALUES (
        now(),
        p_start_time,
        p_end_time,
        diagnosis,
        v_action_type,
        v_action_target,
        action_taken,
        action_result
    );

    RETURN NEXT;
END;
$$;
```

### Remediation Audit Log

```sql
CREATE TABLE flight_recorder.ai_remediations (
    id                  BIGSERIAL PRIMARY KEY,
    executed_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    time_window_start   TIMESTAMPTZ NOT NULL,
    time_window_end     TIMESTAMPTZ NOT NULL,
    diagnosis           TEXT NOT NULL,
    action_type         TEXT NOT NULL,
    action_target       TEXT,
    action_taken        TEXT NOT NULL,
    action_result       TEXT,
    reverted_at         TIMESTAMPTZ,
    reverted_by         TEXT
);

-- Every remediation MUST be logged
CREATE INDEX ai_remediations_executed_at_idx ON flight_recorder.ai_remediations(executed_at DESC);
```

### Safety Checklist for Auto-Remediation

- [ ] **Never** modify schema (DROP, ALTER, CREATE)
- [ ] **Never** modify data (UPDATE, DELETE, INSERT)
- [ ] **Never** change configuration permanently (only session-level)
- [ ] **Always** log every action taken
- [ ] **Always** provide rollback/undo mechanism
- [ ] **Always** require explicit opt-in (`auto_fix => true`)
- [ ] **Validate** targets before executing (table exists, PID exists, etc.)
- [ ] **Rate limit** remediation actions (max 1 per hour per action type)
- [ ] **Alert** humans when remediation occurs
- [ ] **Test** in staging before production

---

## Level 6: Natural Language Interface

### Ask Questions in Plain English

```sql
SELECT flight_recorder.ai_ask('Show me the slowest queries from yesterday');
SELECT flight_recorder.ai_ask('Are there any unused indexes?');
SELECT flight_recorder.ai_ask('What tables are getting the most updates?');
SELECT flight_recorder.ai_ask('Did someone change max_connections recently?');
```

### Implementation

```sql
CREATE OR REPLACE FUNCTION flight_recorder.ai_ask(p_question TEXT)
RETURNS TEXT
LANGUAGE plpgsql AS $$
DECLARE
    v_function_mapping JSONB;
    v_prompt TEXT;
    v_response JSONB;
    v_sql TEXT;
    v_result TEXT;
BEGIN
    -- Map natural language patterns to flight_recorder functions
    v_function_mapping := jsonb_build_object(
        'patterns', jsonb_build_array(
            jsonb_build_object(
                'keywords', jsonb_build_array('slow', 'slowest', 'queries'),
                'function', 'statement_compare',
                'example', 'SELECT * FROM flight_recorder.statement_compare(now() - interval ''24 hours'', now(), 100, 25)'
            ),
            jsonb_build_object(
                'keywords', jsonb_build_array('unused', 'indexes'),
                'function', 'unused_indexes',
                'example', 'SELECT * FROM flight_recorder.unused_indexes(''7 days'')'
            ),
            jsonb_build_object(
                'keywords', jsonb_build_array('config', 'configuration', 'changed'),
                'function', 'config_changes',
                'example', 'SELECT * FROM flight_recorder.config_changes(now() - interval ''7 days'', now())'
            )
        )
    );

    -- Ask AI to translate to SQL
    v_prompt := format(
        E'Convert this natural language question to a SQL query using flight_recorder functions.\n\n' ||
        E'Question: %s\n\n' ||
        E'Available functions:\n%s\n\n' ||
        E'Return ONLY the SQL query, no explanation.',
        p_question,
        jsonb_pretty(v_function_mapping)
    );

    v_response := flight_recorder._call_anthropic_api(v_prompt, 'claude-3-5-haiku-20241022');  -- Use cheap model
    v_sql := v_response->'content'->0->>'text';

    -- Execute the generated SQL
    EXECUTE v_sql INTO v_result;

    RETURN v_result;
EXCEPTION
    WHEN OTHERS THEN
        RETURN 'Unable to answer question: ' || SQLERRM || E'\n\nTry:\n- flight_recorder.recent_activity_current()\n- flight_recorder.anomaly_report(start, end)';
END;
$$;
```

---

## Level 7: Customer-Facing Incident Reports

### Generate Polished Post-Mortems

```sql
SELECT flight_recorder.ai_incident_report(
    '2025-01-17 10:00:00'::timestamptz,
    '2025-01-17 11:00:00'::timestamptz,
    'external'  -- external = customer-facing, internal = technical
);
```

### Implementation

```sql
CREATE OR REPLACE FUNCTION flight_recorder.ai_incident_report(
    p_start_time TIMESTAMPTZ,
    p_end_time TIMESTAMPTZ,
    p_audience TEXT DEFAULT 'external'  -- 'external' or 'internal'
)
RETURNS TEXT
LANGUAGE plpgsql AS $$
DECLARE
    v_context JSONB;
    v_prompt TEXT;
    v_response JSONB;
BEGIN
    v_context := flight_recorder._build_diagnostic_context(p_start_time, p_end_time);

    IF p_audience = 'external' THEN
        v_prompt := format(
            E'Generate a customer-facing incident report based on this database performance data.\n\n' ||
            E'Time Window: %s to %s\n\n' ||
            E'Data:\n```json\n%s\n```\n\n' ||
            E'Format as markdown with sections:\n' ||
            E'# Incident Report: [Title]\n' ||
            E'- **Time**: \n' ||
            E'- **Duration**: \n' ||
            E'- **Impact**: \n\n' ||
            E'## What Happened\n[Customer-friendly explanation]\n\n' ||
            E'## Root Cause\n[Non-technical explanation]\n\n' ||
            E'## Resolution\n[What was done to fix it]\n\n' ||
            E'## Preventive Measures\n[What we''re doing to prevent recurrence]\n\n' ||
            E'Use plain language, avoid jargon, be reassuring but honest.',
            p_start_time, p_end_time, jsonb_pretty(v_context)
        );
    ELSE
        v_prompt := format(
            E'Generate a technical incident post-mortem based on this database performance data.\n\n' ||
            E'Include:\n' ||
            E'- Timeline of events\n' ||
            E'- Root cause analysis with evidence\n' ||
            E'- Impact metrics\n' ||
            E'- Remediation steps taken\n' ||
            E'- Action items to prevent recurrence\n\n' ||
            E'Data:\n```json\n%s\n```',
            jsonb_pretty(v_context)
        );
    END IF;

    v_response := flight_recorder._call_anthropic_api(v_prompt);
    RETURN v_response->'content'->0->>'text';
END;
$$;
```

---

## Practical Considerations

### 1. Cost Control

```sql
-- Budget checking function
CREATE OR REPLACE FUNCTION flight_recorder._check_ai_budget()
RETURNS BOOLEAN
LANGUAGE plpgsql AS $$
DECLARE
    v_daily_spend NUMERIC;
    v_budget_limit NUMERIC;
BEGIN
    SELECT COALESCE(sum(cost_usd), 0) INTO v_daily_spend
    FROM flight_recorder.ai_usage
    WHERE called_at > current_date;

    v_budget_limit := flight_recorder._get_config('ai_daily_budget_usd', '10.00')::numeric;

    RETURN v_daily_spend < v_budget_limit;
END;
$$;

-- Cost tracking
CREATE OR REPLACE FUNCTION flight_recorder._track_ai_usage(
    p_function_name TEXT,
    p_tokens_used INTEGER,
    p_cost_usd NUMERIC
)
RETURNS void
LANGUAGE sql AS $$
    INSERT INTO flight_recorder.ai_usage (function_name, tokens_used, cost_usd)
    VALUES (p_function_name, p_tokens_used, p_cost_usd);
$$;

-- Cost report
CREATE OR REPLACE FUNCTION flight_recorder.ai_cost_report(
    p_lookback INTERVAL DEFAULT '30 days'
)
RETURNS TABLE(
    date DATE,
    total_calls INTEGER,
    total_tokens INTEGER,
    total_cost_usd NUMERIC
)
LANGUAGE sql AS $$
    SELECT
        called_at::date,
        count(*),
        sum(tokens_used),
        sum(cost_usd)
    FROM flight_recorder.ai_usage
    WHERE called_at > now() - p_lookback
    GROUP BY called_at::date
    ORDER BY called_at::date DESC;
$$;
```

### 2. Security

```sql
-- Store API key securely
-- Method 1: ALTER SYSTEM (superuser only)
ALTER SYSTEM SET flight_recorder.anthropic_api_key = 'sk-ant-api03-...';
SELECT pg_reload_conf();

-- Method 2: Secrets table with RLS
CREATE TABLE IF NOT EXISTS flight_recorder.secrets (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

-- Lock down permissions
REVOKE ALL ON flight_recorder.secrets FROM PUBLIC;
GRANT SELECT ON flight_recorder.secrets TO flight_recorder_user;

-- Only allow specific keys to be read
CREATE POLICY secrets_read_policy ON flight_recorder.secrets
    FOR SELECT
    USING (key = 'anthropic_api_key');

-- Store the key
INSERT INTO flight_recorder.secrets (key, value)
VALUES ('anthropic_api_key', 'sk-ant-api03-...')
ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now();
```

### 3. Privacy & Data Sanitization

```sql
-- Sanitize query text before sending to API
CREATE OR REPLACE FUNCTION flight_recorder._sanitize_query(p_query TEXT)
RETURNS TEXT
LANGUAGE plpgsql AS $$
BEGIN
    -- Strip string literals
    p_query := regexp_replace(p_query, '''[^'']*''', '''<redacted>''', 'g');

    -- Strip numeric literals (but keep column names)
    p_query := regexp_replace(p_query, '\b\d+\b', '<N>', 'g');

    -- Strip email addresses
    p_query := regexp_replace(p_query, '\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', '<email>', 'gi');

    RETURN p_query;
END;
$$;

-- Sanitize full context before sending
CREATE OR REPLACE FUNCTION flight_recorder._sanitize_context(p_context JSONB)
RETURNS JSONB
LANGUAGE plpgsql AS $$
DECLARE
    v_sanitized JSONB;
BEGIN
    -- Deep copy and sanitize sensitive fields
    v_sanitized := p_context;

    -- Sanitize query previews
    IF v_sanitized ? 'slow_queries' THEN
        -- Apply sanitization to each query
        -- (Implementation details omitted for brevity)
    END IF;

    -- Remove potentially sensitive user info
    v_sanitized := jsonb_set(v_sanitized, '{lock_summary,top_blockers}',
                             jsonb_build_array(), true);

    RETURN v_sanitized;
END;
$$;
```

### 4. Reliability & Error Handling

```sql
-- Wrapper with retry logic
CREATE OR REPLACE FUNCTION flight_recorder._call_api_with_retry(
    p_prompt TEXT,
    p_max_retries INTEGER DEFAULT 3
)
RETURNS JSONB
LANGUAGE plpgsql AS $$
DECLARE
    v_attempt INTEGER := 0;
    v_response JSONB;
BEGIN
    LOOP
        v_attempt := v_attempt + 1;

        BEGIN
            v_response := flight_recorder._call_anthropic_api(p_prompt);
            RETURN v_response;
        EXCEPTION
            WHEN OTHERS THEN
                IF v_attempt >= p_max_retries THEN
                    RAISE;
                END IF;

                -- Exponential backoff
                PERFORM pg_sleep(power(2, v_attempt));
        END;
    END LOOP;
END;
$$;

-- Timeout protection
CREATE OR REPLACE FUNCTION flight_recorder.ai_diagnose_safe(
    p_start_time TIMESTAMPTZ,
    p_end_time TIMESTAMPTZ,
    p_timeout_seconds INTEGER DEFAULT 30
)
RETURNS TEXT
LANGUAGE plpgsql AS $$
DECLARE
    v_result TEXT;
BEGIN
    -- Set statement timeout for this function
    EXECUTE format('SET LOCAL statement_timeout = ''%ss''', p_timeout_seconds);

    v_result := flight_recorder.ai_diagnose(p_start_time, p_end_time);
    RETURN v_result;
EXCEPTION
    WHEN query_canceled THEN
        RETURN 'AI diagnosis timed out. Try manual diagnostic functions.';
    WHEN OTHERS THEN
        RETURN 'AI diagnosis failed: ' || SQLERRM;
END;
$$;
```

### 5. Rate Limiting

```sql
-- Rate limit AI calls
CREATE OR REPLACE FUNCTION flight_recorder._check_rate_limit(
    p_function_name TEXT,
    p_max_calls_per_hour INTEGER DEFAULT 10
)
RETURNS BOOLEAN
LANGUAGE plpgsql AS $$
DECLARE
    v_recent_calls INTEGER;
BEGIN
    SELECT count(*) INTO v_recent_calls
    FROM flight_recorder.ai_usage
    WHERE function_name = p_function_name
        AND called_at > now() - interval '1 hour';

    RETURN v_recent_calls < p_max_calls_per_hour;
END;
$$;
```

---

## Configuration Reference

```sql
INSERT INTO flight_recorder.config (key, value) VALUES
    -- AI Features
    ('ai_enabled', 'false'),
    ('ai_provider', 'anthropic'),  -- or 'openai'
    ('ai_model', 'claude-3-5-sonnet-20241022'),
    ('ai_daily_budget_usd', '10.00'),
    ('ai_max_calls_per_hour', '10'),
    ('ai_timeout_seconds', '30'),

    -- Auto-Diagnosis
    ('auto_diagnosis_enabled', 'false'),
    ('auto_diagnosis_lookback', '15 minutes'),
    ('auto_diagnosis_alert', 'false'),

    -- Auto-Remediation (DANGEROUS)
    ('auto_remediation_enabled', 'false'),
    ('auto_remediation_whitelist', 'vacuum_table,terminate_idle'),

    -- Privacy
    ('ai_sanitize_queries', 'true'),
    ('ai_redact_usernames', 'true')
ON CONFLICT (key) DO NOTHING;
```

---

## Implementation Roadmap

### Phase 1: Basic AI Diagnosis (MVP)

**Goal**: CSAs can call `ai_diagnose()` manually during incidents.

- [ ] Add `ai_diagnoses` table
- [ ] Implement `_build_diagnostic_context()`
- [ ] Implement `_call_anthropic_api()`
- [ ] Implement `ai_diagnose()`
- [ ] Add cost tracking
- [ ] Test with real incidents

**Estimated Effort**: 4-6 hours
**Value**: Immediate - reduces diagnosis time from hours to seconds

### Phase 2: Auto-Diagnosis

**Goal**: Database continuously self-monitors and flags issues.

- [ ] Implement `auto_diagnose()`
- [ ] Configure pg_cron job
- [ ] Add budget controls
- [ ] Test false positive rate

**Estimated Effort**: 2-3 hours
**Value**: High - proactive issue detection

### Phase 3: Incident Reports

**Goal**: Generate customer-facing post-mortems automatically.

- [ ] Implement `ai_incident_report()`
- [ ] Test report quality
- [ ] Create templates for different audiences

**Estimated Effort**: 2-3 hours
**Value**: High - saves hours of report writing

### Phase 4: Conversational Agent (Advanced)

**Goal**: Multi-turn investigation with tool use.

- [ ] Implement tool use framework
- [ ] Define tool schemas
- [ ] Implement `ai_chat()`
- [ ] Test iterative investigation

**Estimated Effort**: 8-12 hours
**Value**: Very High - replaces manual SQL investigation

### Phase 5: Auto-Remediation (CAUTION!)

**Goal**: Database can fix certain issues automatically.

- [ ] Define safe action whitelist
- [ ] Implement safety checks
- [ ] Implement `ai_diagnose_and_fix()`
- [ ] Add comprehensive audit logging
- [ ] **Extensive testing in staging**

**Estimated Effort**: 12-16 hours
**Value**: High - but **HIGH RISK**. Only after phases 1-4 proven reliable.

---

## Risk Assessment

### Low Risk (Safe to Implement)

- ✅ Manual `ai_diagnose()` calls
- ✅ Auto-diagnosis with logging only
- ✅ Incident report generation
- ✅ Query explanation

### Medium Risk (Needs Testing)

- ⚠️ Conversational agent (could generate bad SQL)
- ⚠️ Natural language interface (SQL injection risk)
- ⚠️ Cost control failures (budget exceeded)

### High Risk (Needs Extensive Validation)

- ❌ Auto-remediation (could make things worse)
- ❌ Unsanitized data sent to API (privacy leak)
- ❌ API key exposure (security breach)
- ❌ Uncontrolled API costs (financial impact)

---

## Testing Strategy

### 1. Unit Tests

```sql
-- Test context building
SELECT flight_recorder._build_diagnostic_context(
    now() - interval '1 hour',
    now()
);

-- Test sanitization
SELECT flight_recorder._sanitize_query(
    'SELECT * FROM users WHERE email = ''customer@example.com'' AND id = 123'
);
-- Should return: SELECT * FROM users WHERE email = '<redacted>' AND id = <N>
```

### 2. Integration Tests

```sql
-- Test full diagnosis (with mock API)
SELECT flight_recorder.ai_diagnose(
    now() - interval '1 hour',
    now()
);
```

### 3. Load Tests

```sql
-- Simulate high AI usage
DO $$
BEGIN
    FOR i IN 1..100 LOOP
        PERFORM flight_recorder.ai_diagnose(now() - interval '1 hour', now());
    END LOOP;
END $$;

-- Check budget enforcement kicked in
SELECT * FROM flight_recorder.ai_usage
WHERE called_at > now() - interval '1 hour';
```

### 4. Security Tests

```sql
-- Attempt to extract API key (should fail)
SELECT current_setting('flight_recorder.anthropic_api_key');
-- Should raise: ERROR: unrecognized configuration parameter

-- Attempt SQL injection via natural language
SELECT flight_recorder.ai_ask('Show me all users; DROP TABLE users;');
-- Should sanitize and reject
```

---

## Monitoring & Observability

### Dashboard Queries

```sql
-- AI usage summary
SELECT
    function_name,
    count(*) AS calls,
    sum(tokens_used) AS total_tokens,
    sum(cost_usd) AS total_cost,
    avg(cost_usd) AS avg_cost_per_call
FROM flight_recorder.ai_usage
WHERE called_at > now() - interval '30 days'
GROUP BY function_name;

-- Diagnosis quality (with human feedback)
SELECT
    auto_generated,
    count(*) AS total_diagnoses,
    count(*) FILTER (WHERE feedback_helpful = true) AS helpful,
    count(*) FILTER (WHERE feedback_helpful = false) AS not_helpful,
    round(100.0 * count(*) FILTER (WHERE feedback_helpful = true) / count(*), 1) AS helpful_pct
FROM flight_recorder.ai_diagnoses
WHERE created_at > now() - interval '30 days'
GROUP BY auto_generated;

-- Error rate
SELECT
    date_trunc('day', occurred_at) AS day,
    count(*) AS error_count
FROM flight_recorder.ai_errors
WHERE occurred_at > now() - interval '30 days'
GROUP BY day
ORDER BY day;
```

---

## Future Enhancements

### Multi-Model Support

Support OpenAI, Anthropic, local models:

```sql
CREATE OR REPLACE FUNCTION flight_recorder._call_llm_api(
    p_prompt TEXT,
    p_provider TEXT DEFAULT NULL
)
RETURNS JSONB AS $$
    -- Route to appropriate provider
    CASE COALESCE(p_provider, flight_recorder._get_config('ai_provider'))
        WHEN 'anthropic' THEN flight_recorder._call_anthropic_api(p_prompt)
        WHEN 'openai' THEN flight_recorder._call_openai_api(p_prompt)
        WHEN 'local' THEN flight_recorder._call_local_model(p_prompt)
        ELSE RAISE EXCEPTION 'Unknown AI provider'
    END
$$;
```

### Learning from Feedback

Train on past diagnoses:

```sql
-- CSA marks diagnosis as helpful/not helpful
UPDATE flight_recorder.ai_diagnoses
SET feedback_helpful = true,
    feedback_notes = 'Accurately identified lock contention'
WHERE id = 123;

-- Use feedback to improve prompts (human-in-loop)
SELECT
    diagnosis,
    feedback_notes
FROM flight_recorder.ai_diagnoses
WHERE feedback_helpful = false
ORDER BY created_at DESC
LIMIT 10;
```

### Integration with Alerts

Send AI diagnoses to Slack/PagerDuty:

```sql
CREATE OR REPLACE FUNCTION flight_recorder._send_alert(
    p_title TEXT,
    p_diagnosis TEXT
)
RETURNS void AS $$
    -- POST to webhook
    PERFORM http((
        'POST',
        current_setting('flight_recorder.slack_webhook_url'),
        ARRAY[http_header('content-type', 'application/json')],
        'application/json',
        jsonb_build_object(
            'text', p_title,
            'blocks', jsonb_build_array(
                jsonb_build_object('type', 'section', 'text', jsonb_build_object('type', 'mrkdwn', 'text', p_diagnosis))
            )
        )::text
    ));
$$;
```

---

## Conclusion

This design enables a truly **self-aware database** that can:

1. ✅ Diagnose its own performance issues
2. ✅ Explain problems in plain English
3. ✅ Generate customer-facing reports
4. ⚠️ (Optionally) Fix certain issues automatically

**Recommended starting point**: Level 1 + Level 2 + Level 7

- Manual diagnosis for CSAs
- Auto-diagnosis for continuous monitoring
- Incident report generation

**Total implementation time**: ~10-15 hours for MVP

**Key success factors**:

- Start simple (Level 1)
- Build confidence with testing
- Add guardrails at every step
- Monitor costs and quality
- Get CSA feedback early and often

---

**Last Updated**: 2026-01-18
**Status**: Design Complete - Ready for Prototyping
**Next Step**: Implement Level 1 (Manual AI Diagnosis) as proof-of-concept
