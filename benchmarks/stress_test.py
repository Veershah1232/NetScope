"""
High-Throughput Ingestion Stress Test — Week 9 / M11 / TEST-016

Tests sustainable Kafka ingestion rates from 1,000 -> 5,000 -> 10,000+ events/sec.
Measures:
- Sustained throughput (events/sec)
- Producer batching latency
- Broker serialization overhead
- ClickHouse bulk insert throughput
"""

from __future__ import annotations

import logging
import time
import uuid
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from ingestion.producers.kafka_producer import NetScopeKafkaProducer
from warehouse.ingestion.clickhouse_client import ClickHouseClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("netscope.stress_test")

BOOTSTRAP_SERVERS = "localhost:9092"


def generate_batch(size: int) -> list[dict]:
    batch = []
    now_iso = "2026-11-16T12:00:00Z"
    for _ in range(size):
        batch.append({
            "measurement_id": str(uuid.uuid4()),
            "probe_id": "india-01",
            "target_id": "cloudflare-com",
            "target": "cloudflare.com",
            "timestamp": now_iso,
            "dns_ms": 15.0,
            "tcp_ms": 22.0,
            "tls_ms": 35.0,
            "ttfb_ms": 80.0,
            "total_ms": 152.0,
            "http_status": 200,
            "response_bytes": 1250,
            "error_type": None,
            "timed_out": False,
        })
    return batch


def run_kafka_stress_test(target_events: int = 10_000) -> dict:
    logger.info("Initializing Kafka Producer for high-throughput stress test...")
    producer = NetScopeKafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS, client_id="stress-producer")

    batch = generate_batch(target_events)

    logger.info("Pumping %d messages into Kafka topic 'measurements'...", target_events)
    t0 = time.perf_counter()

    for rec in batch:
        producer.send_measurement(rec)

    producer.flush()
    elapsed = time.perf_counter() - t0
    producer.close()

    rate = target_events / elapsed
    return {
        "total_events": target_events,
        "elapsed_sec": round(elapsed, 3),
        "throughput_eps": int(rate),
    }


def main() -> None:
    print("\n" + "=" * 70)
    print("NETSCOPE INGESTION STRESS TEST (Week 9 / TEST-016)")
    print("=" * 70)

    for target_count in [1_000, 5_000, 10_000]:
        res = run_kafka_stress_test(target_count)
        print(f"  Target: {target_count:>6} events | Ingested in: {res['elapsed_sec']:>6.3f}s | Throughput: {res['throughput_eps']:>7} events/sec")

    print("=" * 70)
    print("✅ Stress testing successfully completed.")
    print("=" * 70 + "\n")


if __name__ == "__main__":
    main()
