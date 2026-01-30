# Changelog

All notable changes to pg-flight-recorder will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.16] - 2025-01-30

### Added

- **Blast Radius Analysis** - Comprehensive incident impact assessment
  - `blast_radius(start_time, end_time)` - Returns structured impact data including:
    - Lock impact (blocked sessions, duration, lock types)
    - Query degradation (queries slowed >50% vs baseline)
    - Connection impact (before/during comparison)
    - Application impact (affected apps by blocked count)
    - Wait event changes
    - Transaction throughput (TPS before vs during)
    - Severity classification (low/medium/high/critical)
    - Actionable recommendations
  - `blast_radius_report(start_time, end_time)` - ASCII-formatted report for postmortems

### Fixed

- `capacity_summary()` io_buffer_cache metric now properly bounds utilization_pct to 0-100 range

## [2.15] - 2025-01-29

### Added

- **Time-Travel Debugging** - Forensic analysis at any timestamp
  - `_interpolate_metric()` - Linear interpolation helper for estimating values between samples
  - `what_happened_at(timestamp)` - Reconstructs system state at any point in time
  - `incident_timeline(start_time, end_time)` - Unified event timeline from multiple sources

## [2.14] - 2025-01-28

### Added

- **Performance Forecasting** - Proactive capacity planning
  - `_linear_regression()` - Statistical analysis helper
  - `forecast(metric, horizon)` - Single metric prediction with depletion time
  - `forecast_summary()` - Multi-metric dashboard with status classification
  - `check_forecast_alerts()` - Scheduled pg_notify alerts for concerning trends

## [2.13] - 2025-01-27

### Added

- **Visual Performance Timeline** - ASCII-based metric visualization
  - `_sparkline(numeric[])` - Compact Unicode sparklines (▁▂▃▄▅▆▇█)
  - `_bar(value, max)` - Horizontal progress bars
  - `timeline(metric, duration)` - Full ASCII charts with Y-axis and time labels
  - `sparkline_metrics(duration)` - Summary table with sparkline trends

## [2.12] - 2025-01-26

### Added

- **Performance Regression Detection** - Automatic detection of query slowdowns
  - `detect_regressions(lookback, threshold)` - Find queries with significant performance degradation
  - `auto_detect_regressions()` - Scheduled detection with pg_notify alerts
  - `_diagnose_regression_causes()` - Root cause analysis helper
  - Severity classification (LOW/MEDIUM/HIGH/CRITICAL)
  - Anti-flapping protection for auto-resolution

## [2.11] - 2025-01-25

### Added

- Storm severity levels and correlation data
- `pg_notify` alerting for storm events
- Anti-flapping protection for storm auto-resolution

## [2.10] - 2025-01-24

### Added

- **Query Storm Detection** - Identify execution spikes and runaway queries
  - `detect_query_storms()` - Find queries with abnormal call frequency
  - `auto_detect_storms()` - Scheduled detection
  - `storm_dashboard` view for monitoring
  - Storm classification (RETRY_STORM, CACHE_MISS, SPIKE)
  - Resolution workflow functions

## [2.9] - 2025-01-23

### Added

- **Canary Queries** - Silent performance degradation detection
  - `canaries` table for defining synthetic benchmark queries
  - `canary_results` table for tracking execution times
  - `run_canaries()` - Execute all enabled canary queries
  - `canary_status()` - Compare current performance to baseline
  - Pre-defined system catalog canary queries
  - Optional EXPLAIN capture

## Earlier Versions

Versions prior to 2.9 were not formally tracked. The schema_version in the
config table indicates the installed version.
