#!/usr/bin/env python3
"""
DDL Blocking Impact Analyzer

Analyzes DDL operation timings to identify blocking patterns and flight recorder impact.
"""

import json
import sys
from datetime import datetime
from typing import Dict, List, Tuple
from dataclasses import dataclass
import statistics


@dataclass
class DDLOperation:
    """Represents a single DDL operation."""
    operation_id: int
    ddl_type: str
    start_time: str
    end_time: str
    duration_ms: float
    was_blocked: bool
    blocked_by: str = None
    lock_wait_ms: float = None

    @property
    def blocked_by_flight_recorder(self) -> bool:
        """Check if this operation was blocked by flight recorder."""
        if not self.was_blocked or not self.blocked_by:
            return False
        return 'flight_recorder' in self.blocked_by.lower()


class DDLAnalyzer:
    """Analyzes DDL operation data to measure flight recorder impact."""

    def __init__(self, operations: List[Dict]):
        self.operations = [DDLOperation(**op) for op in operations]
        self.total_count = len(self.operations)

    def percentile(self, values: List[float], p: float) -> float:
        """Calculate percentile of a list of values."""
        if not values:
            return 0.0
        return statistics.quantiles(sorted(values), n=100)[int(p) - 1] if len(values) > 1 else values[0]

    def duration_stats(self, operations: List[DDLOperation] = None) -> Dict:
        """Calculate duration statistics for operations."""
        ops = operations if operations is not None else self.operations
        if not ops:
            return {
                'count': 0,
                'min': 0, 'max': 0, 'mean': 0, 'median': 0,
                'p95': 0, 'p99': 0, 'stddev': 0
            }

        durations = [op.duration_ms for op in ops]
        return {
            'count': len(durations),
            'min': min(durations),
            'max': max(durations),
            'mean': statistics.mean(durations),
            'median': statistics.median(durations),
            'p95': self.percentile(durations, 95),
            'p99': self.percentile(durations, 99),
            'stddev': statistics.stdev(durations) if len(durations) > 1 else 0
        }

    def blocking_analysis(self) -> Dict:
        """Analyze blocking patterns."""
        blocked = [op for op in self.operations if op.was_blocked]
        fr_blocked = [op for op in self.operations if op.blocked_by_flight_recorder]

        return {
            'total_blocked': len(blocked),
            'total_blocked_pct': (len(blocked) / self.total_count * 100) if self.total_count > 0 else 0,
            'fr_blocked': len(fr_blocked),
            'fr_blocked_pct': (len(fr_blocked) / self.total_count * 100) if self.total_count > 0 else 0,
            'blocked_stats': self.duration_stats(blocked),
            'fr_blocked_stats': self.duration_stats(fr_blocked),
        }

    def ddl_type_breakdown(self) -> Dict[str, Dict]:
        """Break down statistics by DDL type."""
        breakdown = {}
        for op in self.operations:
            if op.ddl_type not in breakdown:
                breakdown[op.ddl_type] = {
                    'operations': [],
                    'blocked': [],
                    'fr_blocked': []
                }
            breakdown[op.ddl_type]['operations'].append(op)
            if op.was_blocked:
                breakdown[op.ddl_type]['blocked'].append(op)
            if op.blocked_by_flight_recorder:
                breakdown[op.ddl_type]['fr_blocked'].append(op)

        result = {}
        for ddl_type, data in breakdown.items():
            result[ddl_type] = {
                'count': len(data['operations']),
                'blocked_count': len(data['blocked']),
                'blocked_pct': (len(data['blocked']) / len(data['operations']) * 100) if data['operations'] else 0,
                'fr_blocked_count': len(data['fr_blocked']),
                'fr_blocked_pct': (len(data['fr_blocked']) / len(data['operations']) * 100) if data['operations'] else 0,
                'duration_stats': self.duration_stats(data['operations']),
                'blocked_duration_stats': self.duration_stats(data['blocked']) if data['blocked'] else None,
            }
        return result

    def collision_probability(self, interval_seconds: int = 180, operations_per_hour: int = 100) -> Dict:
        """Calculate collision probability at different rates."""
        fr_blocked_pct = (len([op for op in self.operations if op.blocked_by_flight_recorder]) /
                         self.total_count * 100) if self.total_count > 0 else 0

        # Collections per day at given interval
        collections_per_day = 86400 // interval_seconds

        # Expected collisions per day
        expected_collisions_per_day = collections_per_day * (fr_blocked_pct / 100) * (operations_per_hour * 24)

        return {
            'interval_seconds': interval_seconds,
            'collections_per_day': collections_per_day,
            'operations_per_hour': operations_per_hour,
            'collision_probability_pct': fr_blocked_pct,
            'expected_collisions_per_day': expected_collisions_per_day,
            'expected_collisions_per_hour': expected_collisions_per_day / 24,
        }

    def risk_assessment(self, interval_seconds: int = 180) -> Tuple[str, str]:
        """Assess risk level and provide recommendations."""
        fr_blocked_pct = (len([op for op in self.operations if op.blocked_by_flight_recorder]) /
                         self.total_count * 100) if self.total_count > 0 else 0

        fr_blocked = [op for op in self.operations if op.blocked_by_flight_recorder]
        avg_delay = statistics.mean([op.duration_ms for op in fr_blocked]) if fr_blocked else 0

        if fr_blocked_pct < 1:
            risk = "LOW"
            recommendation = "Safe for production use with high DDL workloads"
        elif fr_blocked_pct < 3:
            risk = "MODERATE"
            recommendation = "Safe for typical workloads. Monitor if >50 DDL ops/hour"
        elif fr_blocked_pct < 5:
            risk = "MODERATE-HIGH"
            recommendation = "Consider emergency mode (300s) during DDL-heavy periods"
        else:
            risk = "HIGH"
            recommendation = "Use emergency mode (300s) or schedule DDL during low-traffic windows"

        # Adjust based on average delay
        if avg_delay > 100:
            risk = risk + " (HIGH DELAY)"
            recommendation += f"\nAverage delay {avg_delay:.1f}ms is concerning for latency-sensitive applications"

        return risk, recommendation

    def generate_report(self, duration_seconds: int, interval_seconds: int = 180) -> str:
        """Generate a comprehensive report."""
        all_stats = self.duration_stats()
        blocking = self.blocking_analysis()
        type_breakdown = self.ddl_type_breakdown()
        collision = self.collision_probability(interval_seconds)
        risk, recommendation = self.risk_assessment(interval_seconds)

        report = f"""# DDL Blocking Impact Report

**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
**Test Duration:** {duration_seconds}s
**Flight Recorder Interval:** {interval_seconds}s

## Executive Summary

- **Total DDL Operations:** {self.total_count:,}
- **Operations Blocked (Any):** {blocking['total_blocked']:,} ({blocking['total_blocked_pct']:.2f}%)
- **Operations Blocked by Flight Recorder:** {blocking['fr_blocked']:,} ({blocking['fr_blocked_pct']:.2f}%)
- **Risk Level:** {risk}

## All DDL Operations

| Metric | Duration (ms) |
|--------|---------------|
| Count | {all_stats['count']:,} |
| Minimum | {all_stats['min']:.2f} |
| Mean | {all_stats['mean']:.2f} ± {all_stats['stddev']:.2f} |
| Median (P50) | {all_stats['median']:.2f} |
| P95 | {all_stats['p95']:.2f} |
| P99 | {all_stats['p99']:.2f} |
| Maximum | {all_stats['max']:.2f} |

"""

        if blocking['fr_blocked'] > 0:
            fr_stats = blocking['fr_blocked_stats']
            delay = fr_stats['mean'] - all_stats['mean']
            report += f"""## Operations Blocked by Flight Recorder

| Metric | Duration (ms) |
|--------|---------------|
| Count | {fr_stats['count']:,} |
| Mean | {fr_stats['mean']:.2f} ± {fr_stats['stddev']:.2f} |
| Median (P50) | {fr_stats['median']:.2f} |
| P95 | {fr_stats['p95']:.2f} |
| P99 | {fr_stats['p99']:.2f} |
| Maximum | {fr_stats['max']:.2f} |

**Average Delay from Flight Recorder:** {delay:.2f} ms

"""
        else:
            report += "## Operations Blocked by Flight Recorder\n\nNo operations were blocked by flight recorder.\n\n"

        report += f"""## Collision Probability Analysis

At **{interval_seconds}s intervals** (flight recorder runs {collision['collections_per_day']} times/day):

| Workload | Expected Collisions |
|----------|---------------------|
| 10 DDL ops/hour | ~{collision['collision_probability_pct'] * 10 * 24 / 100:.1f} per day |
| 50 DDL ops/hour | ~{collision['collision_probability_pct'] * 50 * 24 / 100:.1f} per day |
| 100 DDL ops/hour | ~{collision['collision_probability_pct'] * 100 * 24 / 100:.1f} per day |

**Measured collision rate:** {blocking['fr_blocked_pct']:.3f}%

This means for every 1,000 DDL operations, approximately {blocking['fr_blocked_pct'] * 10:.1f} will wait for flight recorder.

## DDL Type Breakdown

| DDL Type | Count | FR Blocked | Block Rate | Avg Duration | P95 Duration |
|----------|-------|------------|------------|--------------|--------------|
"""

        for ddl_type in sorted(type_breakdown.keys()):
            stats = type_breakdown[ddl_type]
            ds = stats['duration_stats']
            report += f"| {ddl_type} | {stats['count']:,} | {stats['fr_blocked_count']:,} | {stats['fr_blocked_pct']:.1f}% | {ds['mean']:.2f}ms | {ds['p95']:.2f}ms |\n"

        report += f"""
## Risk Assessment

**Risk Level:** {risk}

**Recommendation:**
{recommendation}

## Impact at Different Intervals

| Mode | Interval | Collections/Day | Expected Collisions* | Risk |
|------|----------|-----------------|---------------------|------|
| Normal | 180s | 480 | {blocking['fr_blocked_pct'] * 100 * 24 / 100:.1f}/day | {risk} |
| Light | 180s | 480 | {blocking['fr_blocked_pct'] * 100 * 24 / 100:.1f}/day | {risk} |
| Emergency | 300s | 288 | {blocking['fr_blocked_pct'] * 0.6 * 100 * 24 / 100:.1f}/day | Lower |

*Assuming 100 DDL operations per hour

## Methodology

This benchmark measures actual DDL blocking by:
1. Running flight recorder at {interval_seconds}s intervals
2. Continuously executing DDL operations (ALTER, CREATE INDEX, DROP, VACUUM, etc.)
3. Detecting when DDL operations wait for locks via pg_locks
4. Identifying if flight recorder is the blocking process

The collision rate represents the probability that a DDL operation will encounter
an AccessShareLock held by flight recorder's catalog queries (pg_stat_activity,
pg_locks, pg_class, etc.).
"""

        return report


def main():
    """Main entry point."""
    if len(sys.argv) < 2:
        print("Usage: ddl_analyzer.py <ddl_timings.json> [duration_seconds] [interval_seconds]")
        sys.exit(1)

    timings_file = sys.argv[1]
    duration = int(sys.argv[2]) if len(sys.argv) > 2 else 300
    interval = int(sys.argv[3]) if len(sys.argv) > 3 else 180

    # Load data
    with open(timings_file, 'r') as f:
        data = json.load(f)

    operations = data.get('ddl_operations', [])
    if not operations:
        print("ERROR: No DDL operations found in data file")
        sys.exit(1)

    # Analyze
    analyzer = DDLAnalyzer(operations)
    report = analyzer.generate_report(duration, interval)

    print(report)


if __name__ == '__main__':
    main()
