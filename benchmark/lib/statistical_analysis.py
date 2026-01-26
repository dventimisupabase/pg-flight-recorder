#!/usr/bin/env python3
"""
Statistical analysis for observer effect benchmarks.
Parses pgbench logs, computes statistics, and compares baseline vs enabled modes.
"""

import argparse
import glob
import json
import math
import os
import re
import statistics
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Any


@dataclass
class RunStats:
    """Statistics for a single benchmark run."""
    tps: float
    tps_stddev: float
    latency_mean: float
    latency_p50: float
    latency_p95: float
    latency_p99: float
    latency_max: float
    transactions: int
    duration: float


def parse_pgbench_summary(filepath: str) -> Optional[RunStats]:
    """Parse pgbench summary output file."""
    try:
        with open(filepath) as f:
            content = f.read()
    except FileNotFoundError:
        return None

    # Extract TPS (excluding connections establishing)
    tps_match = re.search(r'tps = ([\d.]+) \(without initial connection time\)', content)
    if not tps_match:
        tps_match = re.search(r'tps = ([\d.]+)', content)

    # Extract latency average
    lat_avg_match = re.search(r'latency average = ([\d.]+) ms', content)

    # Extract latency stddev
    lat_std_match = re.search(r'latency stddev = ([\d.]+) ms', content)

    # Extract number of transactions
    txn_match = re.search(r'number of transactions actually processed: (\d+)', content)

    if not all([tps_match, lat_avg_match]):
        return None

    tps = float(tps_match.group(1))
    latency_mean = float(lat_avg_match.group(1))
    tps_stddev = float(lat_std_match.group(1)) if lat_std_match else 0.0
    transactions = int(txn_match.group(1)) if txn_match else 0

    return RunStats(
        tps=tps,
        tps_stddev=tps_stddev,
        latency_mean=latency_mean,
        latency_p50=latency_mean,  # Will be updated from log if available
        latency_p95=latency_mean,
        latency_p99=latency_mean,
        latency_max=latency_mean,
        transactions=transactions,
        duration=0.0
    )


def parse_pgbench_log(filepath: str) -> List[float]:
    """Parse pgbench transaction log file for latencies."""
    latencies = []
    try:
        with open(filepath) as f:
            for line in f:
                parts = line.strip().split()
                if len(parts) >= 3:
                    try:
                        # Format: client_id transaction_no time usec_latency [script_no]
                        latency_us = int(parts[2])
                        latencies.append(latency_us / 1000.0)  # Convert to ms
                    except (ValueError, IndexError):
                        continue
    except FileNotFoundError:
        pass
    return latencies


def compute_percentiles(values: List[float]) -> Dict[str, float]:
    """Compute percentiles from a list of values."""
    if not values:
        return {'p50': 0, 'p95': 0, 'p99': 0, 'max': 0, 'mean': 0, 'stddev': 0}

    sorted_values = sorted(values)
    n = len(sorted_values)

    return {
        'mean': statistics.mean(values),
        'stddev': statistics.stdev(values) if n > 1 else 0,
        'p50': sorted_values[int(n * 0.50)],
        'p95': sorted_values[int(n * 0.95)],
        'p99': sorted_values[int(n * 0.99)],
        'max': sorted_values[-1]
    }


def compute_confidence_interval(values: List[float], confidence: float = 0.95) -> tuple:
    """Compute confidence interval for the mean."""
    if len(values) < 2:
        mean = values[0] if values else 0
        return (mean, mean, 0)

    n = len(values)
    mean = statistics.mean(values)
    stderr = statistics.stdev(values) / math.sqrt(n)

    # Use t-distribution critical value (approximation for n > 30)
    # For 95% CI, t ≈ 1.96 for large n
    t_critical = 1.96 if n > 30 else 2.0
    margin = t_critical * stderr

    return (mean - margin, mean + margin, margin)


def load_workload_results(results_dir: str, workload: str) -> Dict[str, Any]:
    """Load all results for a workload."""
    workload_dir = os.path.join(results_dir, workload)
    if not os.path.isdir(workload_dir):
        return {}

    baseline_runs = []
    enabled_runs = []
    baseline_latencies = []
    enabled_latencies = []

    # Find all summary files
    for summary_file in glob.glob(os.path.join(workload_dir, '*_summary.txt')):
        basename = os.path.basename(summary_file)
        mode = 'baseline' if basename.startswith('baseline_') else 'enabled'

        stats = parse_pgbench_summary(summary_file)
        if stats:
            if mode == 'baseline':
                baseline_runs.append(stats)
            else:
                enabled_runs.append(stats)

        # Find corresponding log file
        log_prefix = summary_file.replace('_summary.txt', '')
        for log_file in glob.glob(f"{log_prefix}.*"):
            if not log_file.endswith('_summary.txt'):
                latencies = parse_pgbench_log(log_file)
                if latencies:
                    if mode == 'baseline':
                        baseline_latencies.extend(latencies)
                    else:
                        enabled_latencies.extend(latencies)

    return {
        'baseline_runs': baseline_runs,
        'enabled_runs': enabled_runs,
        'baseline_latencies': baseline_latencies,
        'enabled_latencies': enabled_latencies
    }


def analyze_workload(data: Dict[str, Any]) -> Dict[str, Any]:
    """Analyze a single workload's results."""
    baseline_runs = data.get('baseline_runs', [])
    enabled_runs = data.get('enabled_runs', [])
    baseline_latencies = data.get('baseline_latencies', [])
    enabled_latencies = data.get('enabled_latencies', [])

    if not baseline_runs or not enabled_runs:
        return {'error': 'Insufficient data'}

    # TPS analysis
    baseline_tps = [r.tps for r in baseline_runs]
    enabled_tps = [r.tps for r in enabled_runs]

    baseline_tps_mean = statistics.mean(baseline_tps)
    enabled_tps_mean = statistics.mean(enabled_tps)
    tps_impact_pct = ((enabled_tps_mean - baseline_tps_mean) / baseline_tps_mean) * 100

    baseline_tps_ci = compute_confidence_interval(baseline_tps)
    enabled_tps_ci = compute_confidence_interval(enabled_tps)

    # Latency analysis
    baseline_lat_stats = compute_percentiles(baseline_latencies) if baseline_latencies else {}
    enabled_lat_stats = compute_percentiles(enabled_latencies) if enabled_latencies else {}

    # Compute latency impact
    latency_impact = {}
    for metric in ['mean', 'p50', 'p95', 'p99', 'max']:
        baseline_val = baseline_lat_stats.get(metric, 0)
        enabled_val = enabled_lat_stats.get(metric, 0)
        if baseline_val > 0:
            impact_pct = ((enabled_val - baseline_val) / baseline_val) * 100
            impact_ms = enabled_val - baseline_val
        else:
            impact_pct = 0
            impact_ms = 0

        latency_impact[metric] = {
            'baseline_ms': baseline_val,
            'enabled_ms': enabled_val,
            'impact_pct': impact_pct,
            'impact_ms': impact_ms
        }

    # Assessment
    assessment = assess_results(tps_impact_pct, latency_impact.get('p99', {}).get('impact_pct', 0),
                                latency_impact.get('p99', {}).get('impact_ms', 0))

    return {
        'tps': {
            'baseline': {
                'mean': baseline_tps_mean,
                'stddev': statistics.stdev(baseline_tps) if len(baseline_tps) > 1 else 0,
                'ci_low': baseline_tps_ci[0],
                'ci_high': baseline_tps_ci[1],
                'samples': len(baseline_tps)
            },
            'enabled': {
                'mean': enabled_tps_mean,
                'stddev': statistics.stdev(enabled_tps) if len(enabled_tps) > 1 else 0,
                'ci_low': enabled_tps_ci[0],
                'ci_high': enabled_tps_ci[1],
                'samples': len(enabled_tps)
            },
            'impact_pct': tps_impact_pct
        },
        'latency': latency_impact,
        'assessment': assessment
    }


def assess_results(tps_impact_pct: float, p99_impact_pct: float, p99_impact_ms: float) -> Dict[str, Any]:
    """Assess benchmark results against thresholds."""
    # TPS assessment (negative is degradation)
    tps_status = 'OK'
    if tps_impact_pct < -3:
        tps_status = 'CRITICAL'
    elif tps_impact_pct < -1:
        tps_status = 'WARNING'

    # P99 latency assessment (success if EITHER relative OR absolute is OK)
    # Warning: > 5% AND > 2ms, Critical: > 15% AND > 5ms
    p99_status = 'OK'
    if p99_impact_pct > 15 and p99_impact_ms > 5:
        p99_status = 'CRITICAL'
    elif p99_impact_pct > 5 and p99_impact_ms > 2:
        p99_status = 'WARNING'

    overall = 'PASS'
    if tps_status == 'CRITICAL' or p99_status == 'CRITICAL':
        overall = 'FAIL'
    elif tps_status == 'WARNING' or p99_status == 'WARNING':
        overall = 'WARNING'

    return {
        'tps_status': tps_status,
        'p99_status': p99_status,
        'overall': overall
    }


def generate_report(results: Dict[str, Any], output_path: str):
    """Generate JSON report."""
    with open(output_path, 'w') as f:
        json.dump(results, f, indent=2)


def print_summary(results: Dict[str, Any]):
    """Print human-readable summary."""
    print("\n" + "=" * 60)
    print("Observer Effect Benchmark Results")
    print("=" * 60)

    for workload, data in results.get('workloads', {}).items():
        if 'error' in data:
            print(f"\n{workload}: {data['error']}")
            continue

        print(f"\n{workload}:")
        print("-" * 40)

        tps = data.get('tps', {})
        baseline_tps = tps.get('baseline', {}).get('mean', 0)
        enabled_tps = tps.get('enabled', {}).get('mean', 0)
        tps_impact = tps.get('impact_pct', 0)

        print(f"  TPS: {baseline_tps:.1f} -> {enabled_tps:.1f} ({tps_impact:+.2f}%)")

        latency = data.get('latency', {})
        for metric in ['p50', 'p95', 'p99']:
            lat_data = latency.get(metric, {})
            baseline = lat_data.get('baseline_ms', 0)
            enabled = lat_data.get('enabled_ms', 0)
            impact_pct = lat_data.get('impact_pct', 0)
            impact_ms = lat_data.get('impact_ms', 0)
            print(f"  {metric}: {baseline:.2f}ms -> {enabled:.2f}ms ({impact_pct:+.2f}%, {impact_ms:+.2f}ms)")

        assessment = data.get('assessment', {})
        overall = assessment.get('overall', 'UNKNOWN')
        print(f"  Assessment: {overall}")

    print("\n" + "=" * 60)


def main():
    parser = argparse.ArgumentParser(
        description='Analyze observer effect benchmark results'
    )
    parser.add_argument('--results-dir', required=True,
                        help='Directory containing benchmark results')
    parser.add_argument('--output', required=True,
                        help='Output JSON file path')
    parser.add_argument('--quiet', action='store_true',
                        help='Suppress console output')

    args = parser.parse_args()

    results_dir = args.results_dir

    if not os.path.isdir(results_dir):
        print(f"Error: Results directory not found: {results_dir}", file=sys.stderr)
        sys.exit(1)

    # Find workloads
    workloads = {}
    for entry in os.listdir(results_dir):
        entry_path = os.path.join(results_dir, entry)
        if os.path.isdir(entry_path) and entry.startswith('oltp_'):
            data = load_workload_results(results_dir, entry)
            if data:
                workloads[entry] = analyze_workload(data)

    results = {
        'results_dir': results_dir,
        'workloads': workloads
    }

    # Generate report
    generate_report(results, args.output)

    if not args.quiet:
        print_summary(results)
        print(f"\nDetailed report: {args.output}")


if __name__ == '__main__':
    main()
