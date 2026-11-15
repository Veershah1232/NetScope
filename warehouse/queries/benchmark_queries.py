"""
ClickHouse Historical Analytics Queries & Benchmark Suite — M6 / TEST-009

Executes core analytical queries over ClickHouse:
1. Target latency ranking (Slowest targets by p95)
2. Protocol stage breakdown (DNS vs TCP vs TLS vs TTFB contribution)
3. Regional comparison (Average & p95 latency per probe/region)
4. Failure & availability rate ranking
5. Query latency benchmarks over 100k+ measurement datasets
"""

from __future__ import annotations

import logging
import random
import time
import uuid
from datetime import datetime, timezone, timedelta
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from warehouse.ingestion.clickhouse_client import ClickHouseClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("netscope.clickhouse_analytics")


# ---------------------------------------------------------------------------
# Core Analytical Queries
# ---------------------------------------------------------------------------

QUERY_SLOWEST_TARGETS = """
SELECT
    target,
    count() AS total_samples,
    round(avg(total_ms), 2) AS avg_latency_ms,
    round(quantile(0.50)(total_ms), 2) AS p50_ms,
    round(quantile(0.95)(total_ms), 2) AS p95_ms,
    round(quantile(0.99)(total_ms), 2) AS p99_ms
FROM netscope.measurements
WHERE error_type IS NULL AND timed_out = 0
GROUP BY target
ORDER BY p95_ms DESC
LIMIT 10;
"""

QUERY_PROTOCOL_BREAKDOWN = """
SELECT
    target,
    round(avg(dns_ms), 2) AS avg_dns_ms,
    round(avg(tcp_ms), 2) AS avg_tcp_ms,
    round(avg(tls_ms), 2) AS avg_tls_ms,
    round(avg(ttfb_ms), 2) AS avg_ttfb_ms,
    round(avg(total_ms), 2) AS avg_total_ms
FROM netscope.measurements
WHERE error_type IS NULL AND timed_out = 0
GROUP BY target
ORDER BY avg_total_ms DESC
LIMIT 10;
"""

QUERY_REGIONAL_LATENCY = """
SELECT
    probe_id,
    target,
    count() AS sample_count,
    round(avg(total_ms), 2) AS avg_latency_ms,
    round(quantile(0.95)(total_ms), 2) AS p95_ms,
    round(100.0 * countIf(error_type IS NULL AND timed_out = 0 AND http_status < 400) / count(), 2) AS availability_pct
FROM netscope.measurements
GROUP BY probe_id, target
ORDER BY probe_id, p95_ms DESC;
"""

QUERY_DAILY_STATS_MV = """
SELECT
    date,
    probe_id,
    target_id,
    sample_count,
    round(total_latency / sample_count, 2) AS avg_latency,
    errors
FROM netscope.target_daily_stats
ORDER BY sample_count DESC
LIMIT 10;
"""


def load_dataset(ch: ClickHouseClient, total_rows: int = 100_000) -> None:
    """Generate and bulk insert synthetic historical measurements for stress benchmarking."""
    logger.info("Generating and ingesting %d historical records into ClickHouse...", total_rows)

    probes = ["india-01", "sg-01", "eu-01"]
    targets = [
        ("github-com", "github.com"),
        ("google-com", "google.com"),
        ("cloudflare-com", "cloudflare.com"),
        ("wikipedia-org", "wikipedia.org"),
        ("amazon-com", "amazon.com"),
        ("flipkart-com", "www.flipkart.com"),
        ("irctc-co-in", "www.irctc.co.in"),
        ("iitb-ac-in", "www.iitb.ac.in"),
    ]

    base_time = datetime.now(timezone.utc) - timedelta(days=7)
    batch_size = 20_000

    start_insert = time.perf_counter()
    records_buffer = []

    for i in range(total_rows):
        probe = random.choice(probes)
        tid, thost = random.choice(targets)
        event_time = base_time + timedelta(seconds=i * 5 + random.randint(0, 10))

        # Baseline latencies depending on geography
        base_dns = random.uniform(5.0, 30.0)
        base_tcp = 20.0 if probe == "india-01" else random.uniform(70.0, 150.0)
        base_tls = random.uniform(25.0, 60.0)
        base_ttfb = random.uniform(50.0, 180.0)
        total_lat = base_dns + base_tcp + base_tls + base_ttfb + random.uniform(10.0, 50.0)

        # 2% error rate
        has_error = random.random() < 0.02
        status = 500 if has_error else 200
        error_type = "HTTP_500" if has_error else None

        records_buffer.append({
            "measurement_id": str(uuid.uuid4()),
            "probe_id": probe,
            "target_id": tid,
            "target": thost,
            "timestamp": event_time.isoformat(),
            "dns_ms": base_dns,
            "tcp_ms": base_tcp,
            "tls_ms": base_tls,
            "ttfb_ms": base_ttfb,
            "total_ms": total_lat,
            "http_status": status,
            "response_bytes": random.randint(500, 50000),
            "error_type": error_type,
            "timed_out": False,
        })

        if len(records_buffer) >= batch_size:
            ch.insert_measurements(records_buffer)
            records_buffer.clear()

    if records_buffer:
        ch.insert_measurements(records_buffer)

    elapsed = time.perf_counter() - start_insert
    rate = total_rows / elapsed
    logger.info("Ingestion complete: %d rows inserted in %.2fs (%.1f rows/sec)", total_rows, elapsed, rate)


def run_benchmarks(ch: ClickHouseClient) -> None:
    logger.info("=== RUNNING CLICKHOUSE HISTORICAL BENCHMARK SUITE ===")

    # Check total rows
    total_count = ch.query("SELECT count() FROM netscope.measurements")[0][0]
    logger.info("Total measurements in ClickHouse: %d rows", total_count)

    queries = [
        ("Slowest Targets by p95", QUERY_SLOWEST_TARGETS),
        ("Protocol Stage Latency Breakdown", QUERY_PROTOCOL_BREAKDOWN),
        ("Regional Comparison & Availability", QUERY_REGIONAL_LATENCY),
        ("Materialized View Pre-Aggregations", QUERY_DAILY_STATS_MV),
    ]

    for label, sql in queries:
        t0 = time.perf_counter()
        rows = ch.query(sql)
        query_time_ms = (time.perf_counter() - t0) * 1000

        print(f"\n📊 [QUERY BENCHMARK: {label}] (Execution Time: {query_time_ms:.2f} ms)")
        for row in rows[:5]:
            print(f"   {row}")
        if len(rows) > 5:
            print(f"   ... ({len(rows) - 5} more rows)")


def main() -> None:
    ch = ClickHouseClient(host="localhost", port=8123)

    # 1. Load 100K measurements into ClickHouse
    load_dataset(ch, total_rows=100_000)

    # 2. Run and benchmark historical queries
    run_benchmarks(ch)


if __name__ == "__main__":
    main()
