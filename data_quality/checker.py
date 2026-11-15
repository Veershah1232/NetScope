"""
NetScope Data Quality Checker — M4 / Week 2

Validates a JSONL file of measurements and reports:
  - missing required fields
  - invalid timestamps
  - negative timings
  - duplicate measurement IDs
  - suspicious outliers (total_ms > 30 000 ms)

Usage:
    python data_quality/checker.py --input results/week1_measurements.jsonl
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from collections import defaultdict
from datetime import datetime

logger = logging.getLogger("netscope.quality")

REQUIRED_FIELDS = ["measurement_id", "probe_id", "target_id", "target", "timestamp"]
TIMING_FIELDS   = ["dns_ms", "tcp_ms", "tls_ms", "ttfb_ms", "total_ms"]
MAX_SANE_MS     = 30_000.0   # 30 seconds — anything beyond is suspicious


class QualityReport:
    def __init__(self, source: str):
        self.source = source
        self.total_records       = 0
        self.heartbeat_records   = 0
        self.valid_records       = 0
        self.issues: list[dict]  = []

        self.duplicate_ids: list[str]   = []
        self.seen_ids: set[str]         = set()

        # Per-probe and per-target counters
        self.probe_counts:  dict[str, int] = defaultdict(int)
        self.target_counts: dict[str, int] = defaultdict(int)
        self.error_counts:  dict[str, int] = defaultdict(int)

    def add_issue(self, measurement_id: str, probe_id: str, target: str,
                  issue_type: str, detail: str) -> None:
        self.issues.append({
            "measurement_id": measurement_id,
            "probe_id":       probe_id,
            "target":         target,
            "issue_type":     issue_type,
            "detail":         detail,
        })

    def print_summary(self) -> None:
        print("\n" + "=" * 70)
        print("NETSCOPE DATA QUALITY REPORT")
        print(f"Source: {self.source}")
        print("=" * 70)
        print(f"Total lines:        {self.total_records + self.heartbeat_records}")
        print(f"Heartbeats skipped: {self.heartbeat_records}")
        print(f"Measurement records:{self.total_records}")
        print(f"Valid records:      {self.valid_records}")
        print(f"Records with issues:{len(set(i['measurement_id'] for i in self.issues))}")
        print(f"Total issue events: {len(self.issues)}")

        if self.duplicate_ids:
            print(f"\n⚠  Duplicate measurement_ids: {len(self.duplicate_ids)}")
            for d in self.duplicate_ids[:5]:
                print(f"   {d}")
            if len(self.duplicate_ids) > 5:
                print(f"   ... and {len(self.duplicate_ids) - 5} more")

        if self.issues:
            # Group by issue type
            by_type: dict[str, int] = defaultdict(int)
            for issue in self.issues:
                by_type[issue["issue_type"]] += 1
            print("\nIssue breakdown:")
            for issue_type, count in sorted(by_type.items(), key=lambda x: -x[1]):
                print(f"   {issue_type:<35} {count:>5}")

        print("\nPer-probe record counts:")
        for probe, count in sorted(self.probe_counts.items()):
            print(f"   {probe:<20} {count:>6} records")

        print("\nTop targets by measurement count:")
        top_targets = sorted(self.target_counts.items(), key=lambda x: -x[1])[:10]
        for target, count in top_targets:
            print(f"   {target:<35} {count:>5}")

        if self.error_counts:
            print("\nError type distribution:")
            for err, count in sorted(self.error_counts.items(), key=lambda x: -x[1]):
                print(f"   {str(err):<40} {count:>5}")

        print("=" * 70)
        if not self.issues and not self.duplicate_ids:
            print("✅  All records passed quality checks.")
        else:
            print("⚠   Some records have quality issues (see above).")
        print()


def check_record(rec: dict, report: QualityReport) -> bool:
    """
    Validate a single measurement record.
    Returns True if valid, False if it has any issues.
    """
    mid    = rec.get("measurement_id", "<missing>")
    probe  = rec.get("probe_id", "<missing>")
    target = rec.get("target",   "<missing>")
    ok     = True

    # 1. Required fields
    for field in REQUIRED_FIELDS:
        if not rec.get(field):
            report.add_issue(mid, probe, target, "MISSING_FIELD",
                             f"Field '{field}' is missing or empty")
            ok = False

    # 2. Timestamp format
    ts = rec.get("timestamp", "")
    if ts:
        try:
            datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except ValueError:
            report.add_issue(mid, probe, target, "INVALID_TIMESTAMP",
                             f"Cannot parse timestamp: {ts!r}")
            ok = False

    # 3. Timing sanity
    for field in TIMING_FIELDS:
        val = rec.get(field)
        if val is None:
            continue   # None is acceptable (stage not reached)
        if not isinstance(val, (int, float)):
            report.add_issue(mid, probe, target, "INVALID_TIMING_TYPE",
                             f"{field}={val!r} is not numeric")
            ok = False
        elif val < 0:
            report.add_issue(mid, probe, target, "NEGATIVE_TIMING",
                             f"{field}={val} ms is negative")
            ok = False
        elif val > MAX_SANE_MS:
            report.add_issue(mid, probe, target, "OUTLIER_TIMING",
                             f"{field}={val:.0f} ms exceeds {MAX_SANE_MS:.0f} ms threshold")
            # Outlier is a warning, not a hard failure

    # 4. Duplicate detection
    if mid in report.seen_ids:
        report.duplicate_ids.append(mid)
        report.add_issue(mid, probe, target, "DUPLICATE_ID",
                         f"measurement_id already seen: {mid}")
        ok = False
    else:
        report.seen_ids.add(mid)

    # 5. HTTP status range (if present)
    status = rec.get("http_status")
    if status is not None:
        if not (100 <= status <= 599):
            report.add_issue(mid, probe, target, "INVALID_HTTP_STATUS",
                             f"http_status={status} is out of valid range 100-599")
            ok = False

    return ok


def check_jsonl_file(path: str) -> QualityReport:
    report = QualityReport(source=path)

    with open(path, "r") as f:
        for lineno, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue

            try:
                rec = json.loads(line)
            except json.JSONDecodeError as exc:
                report.issues.append({
                    "measurement_id": f"line:{lineno}",
                    "probe_id": "<parse_error>",
                    "target": "<parse_error>",
                    "issue_type": "JSON_PARSE_ERROR",
                    "detail": str(exc),
                })
                continue

            # Skip heartbeats
            if rec.get("_type") == "heartbeat":
                report.heartbeat_records += 1
                continue

            report.total_records += 1
            probe  = rec.get("probe_id", "unknown")
            target = rec.get("target_id", "unknown")
            report.probe_counts[probe]   += 1
            report.target_counts[target] += 1

            err = rec.get("error_type")
            if err:
                report.error_counts[err] += 1

            if check_record(rec, report):
                report.valid_records += 1

    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="NetScope Data Quality Checker")
    parser.add_argument("--input", required=True, help="Path to JSONL measurements file")
    parser.add_argument("--log-level", default="WARNING",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    if not os.path.isfile(args.input):
        print(f"ERROR: File not found: {args.input}", file=sys.stderr)
        sys.exit(1)

    report = check_jsonl_file(args.input)
    report.print_summary()

    # Exit with non-zero code if there are hard failures (not just outlier warnings)
    hard_issues = [i for i in report.issues if i["issue_type"] != "OUTLIER_TIMING"]
    if hard_issues or report.duplicate_ids:
        sys.exit(1)


if __name__ == "__main__":
    main()
