#!/usr/bin/env python3
"""
Statistical comparison of benchmark results.
Calculates impact of flight recorder on throughput and latency.
"""

import argparse
import json
import sys
from pathlib import Path
from typing import Dict, Any


def load_results(filepath: str) -> Dict[str, Any]:
    """Load JSON results file."""
    with open(filepath) as f:
        return json.load(f)


def calculate_impact(baseline: float, test: float) -> float:
    """Calculate percentage impact: ((test - baseline) / baseline) * 100"""
    if baseline == 0:
        return 0.0
    return ((test - baseline) / baseline) * 100.0


def format_impact(impact: float) -> str:
    """Format impact with + or - sign."""
    sign = "+" if impact >= 0 else ""
    return f"{sign}{impact:.2f}%"


def compare_throughput(baseline: Dict, test: Dict) -> Dict[str, Any]:
    """Compare throughput metrics."""
    baseline_tps = baseline['throughput']['tps']
    test_tps = test['throughput']['tps']
    impact = calculate_impact(baseline_tps, test_tps)

    return {
        'baseline_tps': baseline_tps,
        'test_tps': test_tps,
        'impact_pct': impact,
        'impact_formatted': format_impact(impact)
    }


def compare_latency(baseline: Dict, test: Dict) -> Dict[str, Any]:
    """Compare latency metrics across percentiles."""
    metrics = {}

    for metric in ['mean', 'p50', 'p95', 'p99', 'max']:
        baseline_val = baseline['latency_ms'][metric]
        test_val = test['latency_ms'][metric]
        impact = calculate_impact(baseline_val, test_val)

        metrics[metric] = {
            'baseline_ms': baseline_val,
            'test_ms': test_val,
            'impact_pct': impact,
            'impact_formatted': format_impact(impact)
        }

    return metrics


def compare_database_stats(baseline: Dict, test: Dict) -> Dict[str, Any]:
    """Compare database statistics (deltas)."""
    # Calculate deltas
    baseline_start = baseline['database_stats']['start']
    baseline_end = baseline['database_stats']['end']
    test_start = test['database_stats']['start']
    test_end = test['database_stats']['end']

    def delta(start: Dict, end: Dict, key: str) -> int:
        return end[key] - start[key]

    metrics = {}
    for key in ['xact_commit', 'xact_rollback', 'blks_read', 'blks_hit',
                'tup_returned', 'tup_fetched', 'tup_inserted', 'tup_updated']:
        baseline_delta = delta(baseline_start, baseline_end, key)
        test_delta = delta(test_start, test_end, key)
        impact = calculate_impact(baseline_delta, test_delta)

        metrics[key] = {
            'baseline': baseline_delta,
            'test': test_delta,
            'impact_pct': impact,
            'impact_formatted': format_impact(impact)
        }

    return metrics


def assess_impact(throughput_impact: float, latency_p95_impact: float) -> str:
    """Assess overall impact severity."""
    # Throughput degradation is bad (positive = slower)
    # Latency increase is bad (positive = slower)

    if abs(throughput_impact) < 2 and abs(latency_p95_impact) < 2:
        return "✓ NEGLIGIBLE (<2% impact)"
    elif abs(throughput_impact) < 5 and abs(latency_p95_impact) < 5:
        return "✓ LOW (<5% impact)"
    elif abs(throughput_impact) < 10 and abs(latency_p95_impact) < 10:
        return "⚠ MODERATE (<10% impact)"
    elif abs(throughput_impact) < 20 and abs(latency_p95_impact) < 20:
        return "⚠ HIGH (<20% impact)"
    else:
        return "✗ SEVERE (>20% impact)"


def generate_markdown_report(baseline: Dict, test: Dict, comparisons: Dict, output_path: str):
    """Generate markdown comparison report."""

    scenario = baseline['scenario']
    throughput = comparisons['throughput']
    latency = comparisons['latency']
    db_stats = comparisons['database_stats']

    # Overall assessment
    assessment = assess_impact(
        throughput['impact_pct'],
        latency['p95']['impact_pct']
    )

    with open(output_path, 'w') as f:
        f.write(f"# Benchmark Comparison: {scenario}\n\n")
        f.write(f"**Overall Impact:** {assessment}\n\n")

        f.write("## Test Configuration\n\n")
        f.write(f"- **Scenario**: {scenario}\n")
        f.write(f"- **Duration**: {baseline['duration_seconds']}s\n")
        f.write(f"- **Clients**: {baseline['clients']}\n")
        f.write(f"- **Baseline Run**: {baseline['start_time']} to {baseline['end_time']}\n")
        f.write(f"- **Test Run**: {test['start_time']} to {test['end_time']}\n")
        f.write("\n")

        f.write("## Throughput\n\n")
        f.write("| Metric | Baseline | With Flight Recorder | Impact |\n")
        f.write("|--------|----------|----------------------|--------|\n")
        f.write(f"| TPS | {throughput['baseline_tps']:.2f} | {throughput['test_tps']:.2f} | {throughput['impact_formatted']} |\n")
        f.write("\n")

        f.write("## Latency\n\n")
        f.write("| Percentile | Baseline (ms) | With Flight Recorder (ms) | Impact |\n")
        f.write("|------------|---------------|---------------------------|--------|\n")
        for metric_name in ['mean', 'p50', 'p95', 'p99', 'max']:
            metric = latency[metric_name]
            label = metric_name.upper() if metric_name != 'mean' else 'Mean'
            f.write(f"| {label} | {metric['baseline_ms']:.2f} | {metric['test_ms']:.2f} | {metric['impact_formatted']} |\n")
        f.write("\n")

        f.write("## Database Statistics\n\n")
        f.write("Delta over test duration:\n\n")
        f.write("| Metric | Baseline | With Flight Recorder | Impact |\n")
        f.write("|--------|----------|----------------------|--------|\n")

        stat_labels = {
            'xact_commit': 'Transactions (commit)',
            'xact_rollback': 'Transactions (rollback)',
            'blks_read': 'Blocks read (disk)',
            'blks_hit': 'Blocks hit (cache)',
            'tup_returned': 'Tuples returned',
            'tup_fetched': 'Tuples fetched',
            'tup_inserted': 'Tuples inserted',
            'tup_updated': 'Tuples updated'
        }

        for key, label in stat_labels.items():
            stat = db_stats[key]
            f.write(f"| {label} | {stat['baseline']:,} | {stat['test']:,} | {stat['impact_formatted']} |\n")
        f.write("\n")

        f.write("## Interpretation\n\n")

        # Throughput
        if throughput['impact_pct'] > 5:
            f.write(f"⚠ **Throughput degraded by {throughput['impact_formatted']}** - This is significant.\n\n")
        elif throughput['impact_pct'] > 2:
            f.write(f"⚠ **Throughput degraded by {throughput['impact_formatted']}** - Moderate impact.\n\n")
        else:
            f.write(f"✓ **Throughput impact {throughput['impact_formatted']}** - Negligible.\n\n")

        # Latency P95
        p95_impact = latency['p95']['impact_pct']
        if p95_impact > 5:
            f.write(f"⚠ **P95 latency increased by {latency['p95']['impact_formatted']}** - This is significant.\n\n")
        elif p95_impact > 2:
            f.write(f"⚠ **P95 latency increased by {latency['p95']['impact_formatted']}** - Moderate impact.\n\n")
        else:
            f.write(f"✓ **P95 latency impact {latency['p95']['impact_formatted']}** - Negligible.\n\n")

        # Overall
        if assessment.startswith("✓"):
            f.write("**Conclusion:** Flight recorder has acceptable overhead for this workload.\n\n")
        elif assessment.startswith("⚠ MODERATE"):
            f.write("**Conclusion:** Flight recorder has moderate overhead. Acceptable for troubleshooting, but monitor in production.\n\n")
        else:
            f.write("**Conclusion:** Flight recorder has significant overhead for this workload. Use with caution or switch to emergency mode.\n\n")

        f.write("---\n\n")
        f.write("*Generated by pg-flight-recorder benchmark framework*\n")


def main():
    parser = argparse.ArgumentParser(
        description='Compare benchmark results and calculate flight recorder impact'
    )
    parser.add_argument('--baseline', required=True, help='Baseline results JSON file')
    parser.add_argument('--test', required=True, help='Test results JSON file (with flight recorder)')
    parser.add_argument('--output', required=True, help='Output markdown file')

    args = parser.parse_args()

    # Load results
    try:
        baseline = load_results(args.baseline)
        test = load_results(args.test)
    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {e}", file=sys.stderr)
        sys.exit(1)

    # Validate scenarios match
    if baseline.get('scenario') != test.get('scenario'):
        print(f"Warning: Scenarios don't match: {baseline.get('scenario')} vs {test.get('scenario')}",
              file=sys.stderr)

    # Compare metrics
    comparisons = {
        'throughput': compare_throughput(baseline, test),
        'latency': compare_latency(baseline, test),
        'database_stats': compare_database_stats(baseline, test)
    }

    # Generate report
    generate_markdown_report(baseline, test, comparisons, args.output)

    print(f"Comparison report generated: {args.output}")

    # Print summary to console
    throughput = comparisons['throughput']
    latency = comparisons['latency']
    assessment = assess_impact(throughput['impact_pct'], latency['p95']['impact_pct'])

    print("\nSummary:")
    print(f"  Throughput impact: {throughput['impact_formatted']}")
    print(f"  Latency (p95) impact: {latency['p95']['impact_formatted']}")
    print(f"  Assessment: {assessment}")


if __name__ == '__main__':
    main()
