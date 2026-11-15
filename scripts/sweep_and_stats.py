"""
Repeated measurement sweep script — Week 1 deliverable.

Runs N rounds of measurements over all targets, then prints summary statistics:
  mean, median, p50, p95, p99 for each target × timing dimension.

Usage:
    cd "DE Project"
    python scripts/sweep_and_stats.py --probe-id india-01 --rounds 10 --output results/week1.jsonl
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import time
from collections import defaultdict
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from probe.netscope_probe.probe import measure_target, load_targets_from_yaml, load_targets_from_hosts
from schemas.measurement import Measurement


TIMING_FIELDS = ["dns_ms", "tcp_ms", "tls_ms", "ttfb_ms", "total_ms"]


def percentile(data: list[float], pct: float) -> float:
    """Return the p-th percentile of data (0–100)."""
    if not data:
        return float("nan")
    sorted_data = sorted(data)
    idx = (pct / 100) * (len(sorted_data) - 1)
    lo, hi = int(idx), min(int(idx) + 1, len(sorted_data) - 1)
    return sorted_data[lo] + (sorted_data[hi] - sorted_data[lo]) * (idx - lo)


def print_summary(all_measurements: list[Measurement]) -> None:
    # Group measurements by target_id
    by_target: dict[str, list[Measurement]] = defaultdict(list)
    for m in all_measurements:
        by_target[m.target_id].append(m)

    print("\n" + "=" * 80)
    print("NETSCOPE WEEK 1 — MEASUREMENT SUMMARY")
    print("=" * 80)

    for target_id, measurements in sorted(by_target.items()):
        successes = [m for m in measurements if m.is_success()]
        failures = [m for m in measurements if not m.is_success()]

        host = measurements[0].target if measurements else target_id
        print(f"\n📡  {host}  ({target_id})")
        print(f"    Rounds: {len(measurements)}  |  Success: {len(successes)}  |  Failures: {len(failures)}")

        if successes:
            avail = 100.0 * len(successes) / len(measurements)
            print(f"    Availability: {avail:.1f}%")

        for field in TIMING_FIELDS:
            vals = [getattr(m, field) for m in measurements if getattr(m, field) is not None]
            if not vals:
                print(f"    {field:<10} —")
                continue
            mean = statistics.mean(vals)
            median = statistics.median(vals)
            p95 = percentile(vals, 95)
            p99 = percentile(vals, 99)
            print(f"    {field:<10}  mean={mean:6.1f}ms  p50={median:6.1f}ms  "
                  f"p95={p95:6.1f}ms  p99={p99:6.1f}ms")

        # Error breakdown
        error_counts: dict[str, int] = defaultdict(int)
        for m in failures:
            error_counts[m.error_type or "UNKNOWN"] += 1
        if error_counts:
            print(f"    Errors: {dict(error_counts)}")

    print("\n" + "=" * 80)


def main() -> None:
    parser = argparse.ArgumentParser(description="NetScope sweep + statistics")
    parser.add_argument("--probe-id", default="india-01")
    parser.add_argument("--targets-file", default="targets/targets.yaml")
    parser.add_argument("--target", help="Single hostname")
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--interval", type=float, default=5.0,
                        help="Seconds between rounds")
    parser.add_argument("--output", default="results/week1_measurements.jsonl")
    parser.add_argument("--timeout", type=float, default=10.0)
    args = parser.parse_args()

    # Build target list
    if args.target:
        targets = load_targets_from_hosts([args.target])
    else:
        targets = load_targets_from_yaml(args.targets_file)
        # Skip controlled/local endpoint if not running
        targets = [t for t in targets if t.get("id") != "netscope-ctrl"]

    os.makedirs(os.path.dirname(args.output) if os.path.dirname(args.output) else ".", exist_ok=True)

    all_measurements: list[Measurement] = []

    print(f"Starting {args.rounds} rounds over {len(targets)} targets...")

    for round_num in range(1, args.rounds + 1):
        print(f"\n--- Round {round_num}/{args.rounds} ---")
        for t in targets:
            host = t.get("host", "")
            if not host:
                continue

            m = measure_target(
                probe_id=args.probe_id,
                target_id=t.get("id", host.replace(".", "-")),
                host=host,
                scheme=t.get("scheme", "https"),
                path=t.get("path", "/"),
                port=t.get("port"),
                timeout=args.timeout,
            )
            all_measurements.append(m)

            # Append to JSONL immediately (don't lose data on crash)
            with open(args.output, "a") as f:
                f.write(json.dumps(m.to_dict()) + "\n")

        if round_num < args.rounds:
            print(f"Sleeping {args.interval:.0f}s...")
            time.sleep(args.interval)

    print_summary(all_measurements)
    print(f"\nAll measurements saved to: {args.output}")


if __name__ == "__main__":
    main()
