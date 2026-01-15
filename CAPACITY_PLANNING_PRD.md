# Product Requirements Document: Capacity Planning Enhancements

**Status:** Draft
**Version:** 1.0
**Date:** 2026-01-15
**Author:** Solutions Architecture Team

---

## Executive Summary

This PRD outlines enhancements to pg-flight-recorder to expand its capabilities from primarily forensic performance analysis to also serve as a comprehensive capacity planning tool. The goal is to enable solutions architects, DBAs, and SREs to make data-driven decisions about right-sizing PostgreSQL deployments.

**Core Value Proposition:** Enable customers to answer "Do I have the right amount of resources?" using the same low-overhead infrastructure that already answers "What went wrong?"

---

## Problem Statement

### Current State

pg-flight-recorder excels at forensic analysis:
- Automatically captures performance metrics with ~0.013% CPU overhead
- Four-tier data architecture (Ring Buffer → Raw Archives → Aggregates → Snapshots)
- Answers "What was happening during this time window?"
- 30-day retention for cumulative statistics (TIER 3)

### Gaps for Capacity Planning

Solutions architects need to right-size PostgreSQL deployments but currently face:

1. **Incomplete Resource Metrics**
   - No transaction rate trends (only used internally for throttling)
   - No block I/O rate trends (only used internally for throttling)
   - No database/table size growth tracking
   - No CPU utilization trends
   - No OS-level memory pressure indicators

2. **Limited Historical Context**
   - 30-day retention may miss quarterly/seasonal patterns
   - No built-in growth rate calculations or trend analysis
   - No capacity headroom projections

3. **Missing Right-Sizing Workflow**
   - No dedicated capacity planning views or reports
   - Analysts must manually construct queries across multiple tables
   - No comparison of resource usage vs. provisioned capacity
   - No actionable recommendations for scaling decisions

4. **CPU Visibility Gap**
   - Tool measures its own overhead but not overall system CPU
   - Cannot correlate CPU saturation with workload patterns
   - Cannot determine if performance issues are CPU-bound

### Who This Affects

- **Solutions Architects:** Need data to recommend instance sizes during migrations or scaling
- **DBAs:** Must justify infrastructure costs and predict future needs
- **SREs:** Need headroom visibility to prevent capacity-related incidents
- **FinOps Teams:** Require evidence to downsize over-provisioned databases

---

## Goals & Success Metrics

### Primary Goals

1. **Enable Right-Sizing Decisions**
   - Provide clear metrics showing if resources are under-provisioned, over-provisioned, or correctly sized
   - Target: 90% of capacity assessments can be completed using only pg-flight-recorder data

2. **Support Growth Planning**
   - Track resource consumption trends over time
   - Enable projection of future capacity needs
   - Target: Predict capacity needs 30-90 days in advance

3. **Maintain Core Performance**
   - Keep overhead below 0.02% sustained CPU (current: 0.013%)
   - No impact to forensic analysis capabilities
   - Target: <5% increase in storage overhead

### Success Metrics

**Adoption Metrics:**
- Number of deployments using capacity planning features (track via config flags)
- Queries to new capacity planning views/functions (track via pg_stat_user_functions)

**Business Impact:**
- Time saved: Reduce capacity assessment time from hours to minutes
- Cost optimization: Enable 10-30% cost reduction through right-sizing
- Availability: Reduce capacity-related incidents

**Technical Metrics:**
- Collection overhead remains <0.02% sustained CPU
- Storage overhead increase <5% (measured: MB/day)
- Query performance: Capacity planning queries <500ms p95

---

## User Personas & Use Cases

### Persona 1: Solutions Architect (Primary)

**Background:** Sarah recommends PostgreSQL configurations for 20+ customer migrations/year

**Use Cases:**
1. **Pre-Migration Sizing:** Analyze source database to size target infrastructure
2. **Right-Sizing Assessment:** Evaluate if existing deployment has correct resources
3. **Cost Optimization:** Identify over-provisioned resources for downsizing
4. **Growth Planning:** Project when current capacity will be exhausted

**Key Questions:**
- Is shared_buffers correctly sized for this workload?
- Do we need more CPU cores?
- How much connection pool headroom exists?
- What will resource needs be in 3 months?

### Persona 2: Database Administrator (Secondary)

**Background:** Mike manages 50+ PostgreSQL instances across dev/staging/production

**Use Cases:**
1. **Proactive Capacity Management:** Monitor trending toward limits
2. **Incident Prevention:** Identify capacity issues before outages
3. **Budget Planning:** Build evidence for infrastructure requests
4. **Performance Optimization:** Distinguish between tuning needs vs. capacity needs

**Key Questions:**
- Which databases are approaching connection limits?
- Are we spilling to disk due to work_mem constraints?
- Is I/O saturation causing slow queries?
- Which instances can be downsized?

### Persona 3: SRE/Platform Engineer (Secondary)

**Background:** Alex maintains database platform for 100+ microservices

**Use Cases:**
1. **Capacity Alerts:** Set thresholds for automated notifications
2. **Trend Analysis:** Identify gradual resource exhaustion
3. **Autoscaling Decisions:** Determine when horizontal/vertical scaling needed
4. **Cost Attribution:** Show per-database resource consumption

**Key Questions:**
- Are we trending toward OOM or connection exhaustion?
- Which services consume the most database resources?
- Should we scale up or scale out?
- What buffer exists before hitting limits?

---

## Functional Requirements

### FR-1: Enhanced Snapshot Metrics

**Priority:** P0 (Required for MVP)

Extend `flight_recorder.snapshots` table to capture additional metrics:

#### FR-1.1: Transaction Rate Metrics
- `xact_commit` (BIGINT) - Cumulative commits from pg_stat_database
- `xact_rollback` (BIGINT) - Cumulative rollbacks from pg_stat_database
- Enable delta calculation: transactions/second between snapshots

**Rationale:** Transaction rate is a key capacity indicator. Currently tracked internally (install.sql:1853-1865) but not stored.

#### FR-1.2: Block I/O Rate Metrics
- `blks_read` (BIGINT) - Cumulative blocks read from pg_stat_database
- `blks_hit` (BIGINT) - Cumulative blocks hit in cache from pg_stat_database
- Enable calculation: buffer cache hit ratio trends, I/O load trends

**Rationale:** I/O patterns indicate if storage/memory is correctly provisioned. Currently tracked but not stored.

#### FR-1.3: Connection Metrics
- `connections_active` (INTEGER) - Active connections at snapshot time
- `connections_total` (INTEGER) - Total connections at snapshot time
- `connections_max` (INTEGER) - max_connections setting at snapshot time

**Rationale:** Connection pool exhaustion is a common scaling trigger. Enable headroom trending.

#### FR-1.4: Database Size Metrics
- `db_size_bytes` (BIGINT) - Database size from pg_database_size()
- `largest_table_bytes` (BIGINT) - Largest table size (optional, may be expensive)

**Rationale:** Storage growth rate is essential for capacity planning. Missing from current implementation.

**Impact:**
- Storage increase: ~40 bytes/snapshot = 11.5 KB/day (0.5% increase)
- Collection time: +2-5ms for additional queries (within acceptable range)

---

### FR-2: Capacity Analysis Functions

**Priority:** P0 (Required for MVP)

#### FR-2.1: `capacity_summary(time_window INTERVAL)`

Returns current capacity status across all dimensions:

**Output Columns:**
- `metric` - Resource dimension (connections, memory, i/o, storage)
- `current_usage` - Current absolute value
- `provisioned_capacity` - Configured limit
- `utilization_pct` - Usage as percentage of capacity
- `headroom_pct` - Available capacity percentage
- `status` - 'healthy' | 'warning' | 'critical'
- `recommendation` - Human-readable action (e.g., "Increase shared_buffers by 50%")

**Logic:**
- `healthy`: utilization < 60%
- `warning`: utilization 60-80%
- `critical`: utilization > 80%

**Metrics Covered:**
- Connections (active/total vs. max_connections)
- Memory - shared_buffers (via bgw_buffers_backend trends)
- Memory - work_mem (via temp_bytes spills)
- I/O capacity (write times vs. write volume)
- Storage (database size vs. disk capacity, if available)
- Transaction rate (trend vs. historical peak)

**Example Usage:**
```sql
SELECT * FROM flight_recorder.capacity_summary(interval '24 hours');
```

#### FR-2.2: `capacity_trends(start_time, end_time, granularity)`

Returns resource utilization trends over time:

**Parameters:**
- `start_time` - TIMESTAMPTZ
- `end_time` - TIMESTAMPTZ
- `granularity` - INTERVAL (e.g., '1 hour', '1 day') for bucketing

**Output Columns:**
- `time_bucket` - Aggregation window timestamp
- `metric` - Resource dimension
- `avg_utilization_pct` - Average utilization in window
- `max_utilization_pct` - Peak utilization in window
- `min_utilization_pct` - Minimum utilization in window
- `growth_rate_pct_per_day` - Linear regression of growth

**Example Usage:**
```sql
-- Daily capacity trends for past 30 days
SELECT * FROM flight_recorder.capacity_trends(
    now() - interval '30 days',
    now(),
    interval '1 day'
)
WHERE metric = 'connections'
ORDER BY time_bucket;
```

#### FR-2.3: `capacity_forecast(metric, days_ahead, confidence_pct)`

Projects future capacity needs using linear regression:

**Parameters:**
- `metric` - Resource to forecast ('connections' | 'memory' | 'storage' | 'transactions')
- `days_ahead` - INTEGER (forecast horizon, 1-90 days)
- `confidence_pct` - NUMERIC (default 95, confidence interval)

**Output Columns:**
- `forecast_date` - TIMESTAMPTZ
- `metric` - Resource dimension
- `predicted_utilization_pct` - Projected utilization
- `confidence_lower_pct` - Lower bound of confidence interval
- `confidence_upper_pct` - Upper bound of confidence interval
- `days_until_critical` - Days until 80% utilization (NULL if not approaching)
- `recommendation` - Action to take (e.g., "Scale up in 45 days")

**Algorithm:**
- Use 30-day historical data for regression
- Calculate linear trend + standard error
- Flag if trend projects >80% utilization within forecast window

**Example Usage:**
```sql
-- Predict connection usage 60 days out
SELECT * FROM flight_recorder.capacity_forecast('connections', 60, 95);
```

---

### FR-3: Capacity Planning Views

**Priority:** P1 (High Value)

#### FR-3.1: `capacity_dashboard`

Materialized or regular view providing at-a-glance capacity status:

**Columns:**
- `last_updated` - Most recent snapshot timestamp
- `connections_status` / `connections_utilization_pct` / `connections_headroom`
- `memory_status` / `memory_pressure_score` (composite of buffers_backend + temp_spills)
- `io_status` / `io_saturation_pct`
- `storage_status` / `storage_utilization_pct` / `storage_growth_mb_per_day`
- `overall_status` - Worst status across all dimensions
- `critical_issues` - Array of TEXT warnings

**Status Values:**
- `healthy` (green) - All metrics <60%
- `warning` (yellow) - Any metric 60-80%
- `critical` (red) - Any metric >80%
- `insufficient_data` (gray) - <7 days of history

**Example Usage:**
```sql
SELECT * FROM flight_recorder.capacity_dashboard;
```

#### FR-3.2: `resource_growth_rates`

View showing day-over-day, week-over-week, month-over-month growth:

**Columns:**
- `metric` - Resource dimension
- `current_value` - Most recent measurement
- `daily_growth_pct` - Average daily growth rate (7-day window)
- `weekly_growth_pct` - Average weekly growth rate (30-day window)
- `monthly_growth_pct` - Month-over-month growth
- `linear_trend` - 'increasing' | 'stable' | 'decreasing'
- `doubling_time_days` - Days until metric doubles (if growing)

**Example Usage:**
```sql
SELECT * FROM flight_recorder.resource_growth_rates
WHERE linear_trend = 'increasing'
ORDER BY daily_growth_pct DESC;
```

---

### FR-4: Configuration Options

**Priority:** P1 (Flexibility)

Add new configuration keys to `flight_recorder.config`:

#### FR-4.1: Capacity Planning Settings
- `capacity_planning_enabled` (boolean, default: true)
- `capacity_thresholds_warning_pct` (integer, default: 60)
- `capacity_thresholds_critical_pct` (integer, default: 80)
- `capacity_forecast_window_days` (integer, default: 90)

#### FR-4.2: Extended Retention
- `snapshot_retention_days_extended` (integer, default: 90)
  - Separate retention tier for capacity planning vs. forensics
  - Keep only capacity-relevant columns in extended snapshots (reduce storage)

#### FR-4.3: Collection Control
- `collect_database_size` (boolean, default: true)
  - Toggle expensive pg_database_size() collection
- `collect_connection_metrics` (boolean, default: true)

**Migration Path:**
- New configurations added during upgrade
- Defaults maintain backward compatibility
- Existing installations continue working without changes

---

### FR-5: Capacity Planning Report

**Priority:** P2 (Nice to Have)

#### FR-5.1: `capacity_report(analysis_window INTERVAL)`

Comprehensive text report similar to existing `summary_report()`:

**Sections:**
1. **Executive Summary**
   - Overall capacity status (healthy/warning/critical)
   - Days until capacity issues (if trending toward limits)
   - Top recommendations (ranked by urgency)

2. **Resource Utilization**
   - Connections: current usage, peak usage, headroom
   - Memory: shared_buffers pressure, work_mem spills
   - I/O: read/write rates, saturation indicators
   - Storage: size, growth rate, projected exhaustion date

3. **Growth Trends**
   - Transaction rate: daily average, weekly trend
   - Connection count: growth pattern, peak times
   - Database size: growth rate MB/day, doubling time

4. **Optimization Opportunities**
   - Over-provisioned resources (potential cost savings)
   - Under-provisioned resources (performance risks)
   - Configuration recommendations

5. **Forecast**
   - 30/60/90-day capacity projections
   - Expected exhaustion dates for each resource
   - Recommended scaling timeline

**Example Usage:**
```sql
SELECT flight_recorder.capacity_report(interval '30 days');
```

**Output:** Multi-line TEXT formatted report (similar to anomaly_report)

---

## Non-Functional Requirements

### NFR-1: Performance

- Collection overhead increase: <5% (target: 2-3% from current 0.013%)
- New snapshot metrics add <5ms to snapshot() function (current: 23-32ms median)
- Capacity analysis functions: <500ms p95 execution time
- Views: <200ms p95 query time for typical usage

### NFR-2: Storage

- Snapshot table size increase: <10% (target: ~5%)
- Extended retention (90 days): ~3x storage for snapshots table only
- Optional: Implement snapshot column subsetting for extended retention
- Storage cleanup continues working automatically

### NFR-3: Compatibility

- PostgreSQL 15, 16, 17 support (existing requirement)
- Backward compatible: Existing queries/views continue working
- Graceful degradation: Capacity features return NULL if insufficient data
- Upgrade path: ALTER TABLE migrations in upgrade script

### NFR-4: Usability

- Capacity functions use consistent naming conventions
- Output includes human-readable recommendations (not just raw numbers)
- Status indicators use traffic light pattern (healthy/warning/critical)
- Documentation includes capacity planning cookbook/recipes

### NFR-5: Reliability

- Capacity planning failures do not affect forensic data collection
- Missing data handled gracefully (e.g., pg_stat_statements not installed)
- Forecasting requires minimum 7 days history, returns NULL otherwise
- Trend calculations handle edge cases (flat lines, sparse data)

---

## Technical Design Considerations

### Design Decision 1: Storage Strategy

**Option A: Extend Existing Snapshots Table (Recommended)**
- Add columns to `flight_recorder.snapshots`
- Pros: Simple implementation, leverages existing retention/cleanup
- Cons: Increases row size for all snapshots

**Option B: Separate Capacity Snapshots Table**
- Create `flight_recorder.capacity_snapshots` table
- Pros: Separates concerns, allows different retention
- Cons: Duplicate collection logic, join complexity

**Recommendation:** Option A (extend existing table)
- Simpler mental model
- Columns consume space only when populated (PostgreSQL NULL compression)
- 40 bytes/row is negligible (~0.5% increase)
- Can later move to separate table if needed

### Design Decision 2: Forecast Algorithm

**Option A: Simple Linear Regression (Recommended)**
- Calculate slope/intercept using least squares
- Pros: Fast, deterministic, explainable
- Cons: Assumes linear growth (may miss patterns)

**Option B: Moving Average**
- Use exponential weighted moving average
- Pros: Handles volatility better
- Cons: No confidence intervals, harder to project far ahead

**Option C: Time Series ML (Future)**
- Use PostGIS or external ML library
- Pros: Captures seasonality, non-linear patterns
- Cons: Complex dependency, harder to explain

**Recommendation:** Start with Option A (linear regression)
- Sufficient for 90% of use cases (growth trends are often linear)
- Can implement in pure SQL using window functions
- Future: Add Option C for advanced users via extension

### Design Decision 3: CPU Metrics Collection

**Challenge:** PostgreSQL has no built-in CPU metrics (unlike memory, I/O, WAL)

**Option A: No CPU Metrics (Current State)**
- Pros: No complexity
- Cons: Missing key capacity dimension

**Option B: OS-Level Collection (pg_proctab extension)**
- Query /proc/stat via pg_proctab or custom extension
- Pros: True CPU utilization
- Cons: Requires compilation, platform-specific (Linux only)

**Option C: Indirect CPU Indicators (Recommended)**
- Track query execution times, throughput degradation
- Create "CPU pressure score" based on:
  - Queries slowing down despite same I/O
  - Transaction rate declining
  - Wait events showing CPU contention
- Pros: Works cross-platform, no dependencies
- Cons: Indirect, less precise

**Recommendation:** Option C for MVP, Option B as optional enhancement
- Document that CPU is inferred, not measured directly
- Provide instructions for integrating external CPU monitoring
- Future: Make pg_proctab integration optional/pluggable

### Design Decision 4: Growth Rate Calculation

**Method:**
```sql
-- Linear regression slope using SQL
WITH data AS (
    SELECT
        captured_at,
        EXTRACT(EPOCH FROM captured_at - (SELECT min(captured_at) FROM snapshots)) / 86400 AS day_offset,
        connections_total AS y
    FROM flight_recorder.snapshots
    WHERE captured_at > now() - interval '30 days'
),
stats AS (
    SELECT
        count(*) AS n,
        sum(day_offset) AS sum_x,
        sum(y) AS sum_y,
        sum(day_offset * y) AS sum_xy,
        sum(day_offset * day_offset) AS sum_xx
    FROM data
)
SELECT
    (n * sum_xy - sum_x * sum_y) / NULLIF(n * sum_xx - sum_x * sum_x, 0) AS slope_per_day
FROM stats;
```

**Alternative:** Use PostgreSQL's regr_slope() aggregate function (simpler)

---

## Implementation Phases

### Phase 1: Foundation (MVP)
**Goal:** Capture extended metrics, enable basic capacity queries

**Tasks:**
1. Extend `snapshots` table with new columns (FR-1)
2. Modify `snapshot()` function to collect new metrics
3. Create `capacity_summary()` function (FR-2.1)
4. Create `capacity_dashboard` view (FR-3.1)
5. Add configuration options (FR-4)
6. Write comprehensive tests (pgTAP)
7. Update documentation (README, REFERENCE)

**Success Criteria:**
- All tests pass on PG15/16/17
- Collection overhead <0.02%
- Storage increase <10%
- capacity_summary() executes <500ms

**Timeline:** 2-3 weeks

---

### Phase 2: Trend Analysis
**Goal:** Enable historical trending and growth analysis

**Tasks:**
1. Create `capacity_trends()` function (FR-2.2)
2. Create `resource_growth_rates` view (FR-3.2)
3. Implement linear regression helpers (internal functions)
4. Add example queries to documentation
5. Create capacity planning cookbook guide

**Success Criteria:**
- Growth rate calculations accurate within 5%
- Trends handle sparse data gracefully
- Documentation includes 10+ real-world examples

**Timeline:** 1-2 weeks

---

### Phase 3: Forecasting
**Goal:** Predict future capacity needs

**Tasks:**
1. Create `capacity_forecast()` function (FR-2.3)
2. Implement confidence interval calculations
3. Add forecast validation tests
4. Create capacity planning dashboard examples
5. Document forecast limitations/assumptions

**Success Criteria:**
- Forecasts within 10% accuracy (validated against test data)
- Handles edge cases (insufficient data, flat trends)
- Clear documentation of confidence intervals

**Timeline:** 2 weeks

---

### Phase 4: Reporting & Polish
**Goal:** Comprehensive user-facing reports and tooling

**Tasks:**
1. Create `capacity_report()` function (FR-5.1)
2. Add extended retention configuration
3. Create migration guide for existing installations
4. Build example Grafana dashboard (optional)
5. Write capacity planning case studies

**Success Criteria:**
- Report provides actionable recommendations
- Users can complete capacity assessment in <10 minutes
- Documentation includes step-by-step workflows

**Timeline:** 1-2 weeks

---

### Phase 5: Advanced Features (Future)
**Scope:** Features beyond MVP

**Potential Additions:**
- Integration with pg_proctab for CPU metrics
- Seasonality detection in trends
- Anomaly detection for capacity (sudden spikes)
- Multi-database capacity aggregation (for clusters)
- Alerting integration (webhooks, email)
- Capacity planning API endpoint
- Visual capacity dashboard (web UI)

**Evaluation Criteria:**
- User feedback from Phases 1-4
- Feature request prioritization
- Resource availability

---

## Testing Strategy

### Unit Tests (pgTAP)

**New Test Coverage:**
- Snapshot collection with new metrics (150+ tests)
- Capacity calculation functions (50+ tests)
- Trend analysis with various data patterns (30+ tests)
- Forecast accuracy with synthetic data (20+ tests)
- Edge cases: NULL handling, sparse data, flat trends (40+ tests)
- Backward compatibility: Old queries still work (20+ tests)

**Target:** 95%+ code coverage for new functions

### Performance Tests

**Benchmarks:**
- Collection overhead: Measure snapshot() time before/after
- Storage overhead: Track snapshots table size growth
- Query performance: Time capacity functions with 30/60/90 days data
- Concurrent load: Ensure capacity queries don't block collections

**Acceptance Criteria:**
- Overhead increase <5%
- Storage increase <10%
- Query times meet NFR-1 targets

### Integration Tests

**Scenarios:**
- Fresh installation: Capacity features work immediately
- Upgrade from previous version: Migration successful
- Insufficient data: Functions return NULL gracefully
- Extended retention: Cleanup preserves capacity data
- PostgreSQL version differences: All versions behave consistently

### Validation Tests

**Forecast Accuracy:**
- Generate synthetic workload with known growth rate
- Collect data for 30 days
- Forecast next 30 days
- Compare predictions to actual (should be within 10%)

**Trend Detection:**
- Linear growth: Detect positive slope
- Flat usage: Detect stable trend
- Declining usage: Detect negative slope
- Seasonal patterns: Handle gracefully (may add seasonality support later)

---

## Documentation Updates

### README.md Updates

Add new section: **Capacity Planning**

```markdown
## Capacity Planning

pg-flight-recorder now supports capacity planning and right-sizing assessments:

### Quick Start

-- Check current capacity status
SELECT * FROM flight_recorder.capacity_dashboard;

-- Analyze trends over past 30 days
SELECT * FROM flight_recorder.capacity_summary(interval '30 days');

-- Forecast capacity needs
SELECT * FROM flight_recorder.capacity_forecast('connections', 60, 95);

### Common Questions

**Q: Do I need more connections?**
SELECT * FROM flight_recorder.capacity_summary(interval '7 days')
WHERE metric = 'connections';

**Q: Is my shared_buffers correctly sized?**
SELECT * FROM flight_recorder.capacity_summary(interval '30 days')
WHERE metric = 'shared_buffers';

**Q: When will I run out of storage?**
SELECT days_until_critical
FROM flight_recorder.capacity_forecast('storage', 90, 95);
```

### REFERENCE.md Updates

**New Sections:**
1. **Capacity Planning Functions** (detailed API reference)
2. **Capacity Planning Views** (schema documentation)
3. **Capacity Planning Configuration** (config keys)
4. **Capacity Planning Cookbook** (common recipes)
5. **Interpreting Capacity Results** (how to read outputs)

### New Document: CAPACITY_PLANNING_GUIDE.md

**Comprehensive guide including:**
1. Introduction to capacity planning
2. Right-sizing methodology
3. Step-by-step workflows for common scenarios
4. Case studies (e.g., pre-migration sizing, cost optimization)
5. Integration with external monitoring tools
6. Troubleshooting and FAQ
7. Best practices and recommendations

---

## Migration & Upgrade Path

### For Existing Installations

**Upgrade Script:** `upgrade_to_capacity_planning.sql`

```sql
-- Step 1: Add new columns to snapshots table
ALTER TABLE flight_recorder.snapshots
ADD COLUMN IF NOT EXISTS xact_commit BIGINT,
ADD COLUMN IF NOT EXISTS xact_rollback BIGINT,
ADD COLUMN IF NOT EXISTS blks_read BIGINT,
ADD COLUMN IF NOT EXISTS blks_hit BIGINT,
ADD COLUMN IF NOT EXISTS connections_active INTEGER,
ADD COLUMN IF NOT EXISTS connections_total INTEGER,
ADD COLUMN IF NOT EXISTS connections_max INTEGER,
ADD COLUMN IF NOT EXISTS db_size_bytes BIGINT;

-- Step 2: Add new configuration entries
INSERT INTO flight_recorder.config (key, value) VALUES
    ('capacity_planning_enabled', 'true'),
    ('capacity_thresholds_warning_pct', '60'),
    ('capacity_thresholds_critical_pct', '80'),
    ('capacity_forecast_window_days', '90'),
    ('snapshot_retention_days_extended', '90'),
    ('collect_database_size', 'true'),
    ('collect_connection_metrics', 'true')
ON CONFLICT (key) DO NOTHING;

-- Step 3: Recreate snapshot() function (new version)
-- (Include full function definition)

-- Step 4: Create new capacity planning functions
-- (Include all new functions)

-- Step 5: Create new capacity planning views
-- (Include all new views)
```

**Backward Compatibility:**
- Existing queries/views: Continue working unchanged
- New columns: Allow NULL for historical data
- Functions: Graceful degradation if data unavailable
- Configuration: Defaults preserve existing behavior

### Data Backfill Strategy

**Challenge:** Historical snapshots lack new metrics

**Options:**
1. **Accept NULL values** (Recommended)
   - Capacity features require 7+ days for trends
   - After 7 days, features fully functional
   - Historical forensic analysis unaffected

2. **Partial backfill**
   - Recalculate static values (db_size_bytes, connections_max)
   - Cannot backfill point-in-time values (active connections)
   - Limited benefit, adds complexity

**Recommendation:** Option 1 (accept NULL values)
- Simpler upgrade path
- Within 7 days, capacity planning fully operational
- Document in upgrade guide

---

## Success Criteria

### Phase 1 Success Criteria (MVP)

**Functional:**
- [ ] Capacity metrics collected every 5 minutes
- [ ] `capacity_summary()` returns accurate status for all dimensions
- [ ] `capacity_dashboard` view provides at-a-glance status
- [ ] All configuration options work correctly

**Non-Functional:**
- [ ] Collection overhead <0.02% sustained CPU
- [ ] Storage increase <10% for snapshots table
- [ ] 95%+ test coverage for new code
- [ ] Tests pass on PostgreSQL 15, 16, 17

**Documentation:**
- [ ] README updated with capacity planning section
- [ ] REFERENCE updated with full API documentation
- [ ] Example queries documented
- [ ] Upgrade guide published

**User Validation:**
- [ ] 3+ beta users complete capacity assessment successfully
- [ ] Feedback: "Significantly easier than manual analysis"
- [ ] Zero critical bugs in beta period

### Overall Project Success Criteria

**Adoption:**
- 30% of new installations enable capacity planning features (6 months post-launch)
- 10+ community contributions (GitHub issues/PRs) related to capacity planning

**User Impact:**
- Average capacity assessment time: <15 minutes (vs. 2-4 hours manual)
- User-reported cost savings: 10-30% through right-sizing
- Capacity-related incidents: Reduced through proactive monitoring

**Technical Quality:**
- Zero high-severity bugs in first 90 days
- Performance targets met in production
- Positive community feedback (GitHub stars, discussions)

---

## Open Questions & Risks

### Open Questions

1. **Q: Should we include per-table size tracking?**
   - **Risk:** pg_class queries can be expensive on large databases
   - **Mitigation:** Make optional, sample only top N tables
   - **Decision:** Defer to Phase 5, gather user feedback first

2. **Q: How to handle multi-database PostgreSQL instances?**
   - **Current:** Only tracks current database
   - **Option:** Add cross-database aggregation function
   - **Decision:** Document limitation, consider Phase 5 enhancement

3. **Q: Should forecasting account for seasonal patterns?**
   - **Risk:** Simple linear regression misses weekly/monthly cycles
   - **Mitigation:** Start simple, add seasonality detection in Phase 5
   - **Decision:** Document as known limitation

4. **Q: How to integrate with external monitoring (Datadog, Prometheus)?**
   - **Option:** Provide SQL queries users can wrap in exporters
   - **Option:** Build native integration endpoints
   - **Decision:** Start with SQL queries, consider integrations based on demand

### Risks & Mitigations

| Risk | Impact | Likelihood | Mitigation |
|------|--------|------------|------------|
| Performance regression | High | Low | Comprehensive benchmarking, feature flags |
| Storage explosion | Medium | Low | Configurable retention, cleanup automation |
| Forecast inaccuracy | Medium | Medium | Document assumptions, show confidence intervals |
| User adoption low | High | Medium | Beta program, clear documentation, example dashboards |
| Breaking changes | High | Low | Backward compatibility tests, staged rollout |
| CPU metric gap | Medium | High | Document limitation, provide indirect indicators |
| Complex upgrade path | Medium | Low | Automated migration script, thorough testing |

**Mitigation Strategies:**
- Feature flags allow disabling capacity planning if issues arise
- Beta program with 5-10 users before public release
- Performance regression tests in CI/CD
- Clear rollback procedures documented
- Community feedback loop via GitHub discussions

---

## Appendix

### A. Example Queries

#### Right-Sizing Assessment
```sql
-- Complete capacity health check
SELECT
    metric,
    utilization_pct,
    status,
    recommendation
FROM flight_recorder.capacity_summary(interval '30 days')
ORDER BY utilization_pct DESC;
```

#### Cost Optimization (Find Over-Provisioned Resources)
```sql
-- Find resources with <40% utilization (potential downsizing)
SELECT
    metric,
    current_usage,
    provisioned_capacity,
    utilization_pct,
    'Consider downsizing' AS opportunity
FROM flight_recorder.capacity_summary(interval '30 days')
WHERE utilization_pct < 40;
```

#### Growth Planning
```sql
-- Predict when connection pool will hit 80%
SELECT
    metric,
    days_until_critical,
    predicted_utilization_pct,
    recommendation
FROM flight_recorder.capacity_forecast('connections', 90, 95)
WHERE days_until_critical IS NOT NULL;
```

#### Historical Trend Analysis
```sql
-- Week-over-week transaction growth
SELECT
    time_bucket::date,
    avg_utilization_pct AS avg_tps_utilization,
    growth_rate_pct_per_day
FROM flight_recorder.capacity_trends(
    now() - interval '8 weeks',
    now(),
    interval '1 week'
)
WHERE metric = 'transactions'
ORDER BY time_bucket;
```

### B. Related Work & Inspiration

**Existing Tools:**
- AWS RDS Performance Insights (capacity recommendations)
- pganalyze (growth tracking)
- Datadog Database Monitoring (capacity alerts)
- Azure SQL Intelligent Insights (predictive analytics)

**Differentiation:**
- **Embedded:** No external dependencies, runs in-database
- **Low overhead:** <0.02% CPU (vs. 1-5% for some agents)
- **Free & open source:** No per-database licensing costs
- **Integrated:** Unified forensic + capacity planning

### C. Glossary

- **Right-Sizing:** Matching provisioned resources to actual workload needs
- **Headroom:** Unused capacity buffer (e.g., 30% headroom = 70% utilization)
- **Growth Rate:** Change in resource consumption over time (% per day/week/month)
- **Forecast Horizon:** Time period for capacity predictions (typically 30-90 days)
- **Confidence Interval:** Statistical range where true value likely falls (e.g., 95% CI)
- **Utilization:** Percentage of provisioned capacity currently in use
- **Capacity Exhaustion:** Point where resource reaches 100% utilization
- **Linear Regression:** Statistical method for estimating trend from historical data

---

## Approval & Sign-Off

**Stakeholders:**

- [ ] **Engineering Lead:** Approves technical approach and resource allocation
- [ ] **Product Owner:** Approves feature scope and priorities
- [ ] **Solutions Architecture:** Validates use cases and requirements
- [ ] **Documentation Lead:** Commits to documentation updates
- [ ] **QA Lead:** Approves testing strategy

**Next Steps:**
1. Review and approve PRD
2. Conduct technical design review
3. Estimate effort and create implementation tickets
4. Recruit beta users for Phase 1
5. Begin Phase 1 development

---

**Document History:**

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-01-15 | Solutions Architecture Team | Initial draft |
