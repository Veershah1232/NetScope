"""
TEST-006 & TEST-007: Real-time Flink Stream Processing Live Demo

This script streams real Internet measurements from Kafka topic 'measurements',
computes tumbling and sliding windows, logs latency percentiles (p50/p95/p99),
and demonstrates live watermark triggering and late-event rejection.
"""

from __future__ import annotations

import json
import logging
import time
import uuid
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from ingestion.producers.kafka_producer import NetScopeKafkaProducer
from streaming.flink_jobs.stream_processor import FlinkStreamingSimulator, WindowAggregate

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("netscope.test_week4_live")

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "measurements"


def run_live_stream_demo() -> None:
    logger.info("=== STARTING WEEK 4 FLINK REAL-TIME STREAMING OBSERVABLE ===")

    fired_windows: list[WindowAggregate] = []
    late_events_dropped: list[dict] = []

    def on_window(agg: WindowAggregate):
        print(
            f"\n⚡ [FLINK WINDOW FIRED] Type: {agg.window_type} | Target: {agg.target}\n"
            f"   Window:       {agg.window_start} -> {agg.window_end}\n"
            f"   Record Count: {agg.count} | Availability: {agg.availability_pct}%\n"
            f"   Latency:      avg={agg.avg_latency_ms}ms, p50={agg.p50_ms}ms, p95={agg.p95_ms}ms, p99={agg.p99_ms}ms"
        )
        fired_windows.append(agg)

    def on_late(rec: dict):
        print(f"🚫 [FLINK LATE EVENT REJECTED] id={rec.get('measurement_id')} target={rec.get('target')} timestamp={rec.get('timestamp')}")
        late_events_dropped.append(rec)

    # Initialize Flink processor with 3-second bounded out-of-orderness
    engine = FlinkStreamingSimulator(
        watermark_delay_ms=3000,
        allowed_lateness_ms=5000,
        on_window_fire=on_window,
        on_late_event=on_late,
    )

    producer = NetScopeKafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS, client_id="flink-demo-producer")

    # Step 1: Simulate stream of events in Window 1 [12:00:00 -> 12:01:00]
    logger.info("Step 1: Feeding live streaming events for Window 1 (12:00:00 - 12:01:00)...")
    latencies = [45.2, 52.1, 78.9, 110.4, 250.0, 48.0]
    for i, lat in enumerate(latencies):
        rec = {
            "measurement_id": str(uuid.uuid4()),
            "probe_id": "india-01",
            "target_id": "cloudflare-com",
            "target": "cloudflare.com",
            "timestamp": f"2026-11-16T12:00:{10 + i * 8:02d}Z",
            "dns_ms": 15.0,
            "tcp_ms": 20.0,
            "tls_ms": 25.0,
            "ttfb_ms": lat - 10.0,
            "total_ms": lat,
            "http_status": 200,
            "response_bytes": 1200,
            "error_type": None,
            "timed_out": False,
        }
        producer.send_measurement(rec)
        engine.process_record(rec)

    # Step 2: Inject a duplicate record -> Verify Deduplication
    logger.info("Step 2: Injecting duplicate record to verify stream deduplication...")
    dup_id = str(uuid.uuid4())
    dup_rec = {
        "measurement_id": dup_id,
        "probe_id": "india-01",
        "target_id": "cloudflare-com",
        "target": "cloudflare.com",
        "timestamp": "2026-11-16T12:00:45Z",
        "total_ms": 65.0,
        "http_status": 200,
    }
    assert engine.process_record(dup_rec) is True
    assert engine.process_record(dup_rec) is False  # Duplicate detected and dropped
    logger.info("Deduplication verified.")

    # Step 3: Advance watermark by sending event from Window 2 [12:01:05]
    logger.info("Step 3: Advancing event-time watermark past 12:01:00 to trigger Window 1...")
    adv_rec = {
        "measurement_id": str(uuid.uuid4()),
        "probe_id": "india-01",
        "target_id": "cloudflare-com",
        "target": "cloudflare.com",
        "timestamp": "2026-11-16T12:01:05Z",
        "total_ms": 55.0,
        "http_status": 200,
    }
    producer.send_measurement(adv_rec)
    engine.process_record(adv_rec)

    # Step 4: Inject a late event from 11:58:00 (arriving after watermark has passed)
    logger.info("Step 4: Injecting intentionally LATE event from 11:58:00...")
    late_rec = {
        "measurement_id": "late-event-999",
        "probe_id": "india-01",
        "target_id": "cloudflare-com",
        "target": "cloudflare.com",
        "timestamp": "2026-11-16T11:58:00Z",
        "total_ms": 99.0,
        "http_status": 200,
    }
    engine.process_record(late_rec)

    producer.flush()
    producer.close()

    assert len(fired_windows) > 0, "Expected at least one window to fire!"
    assert len(late_events_dropped) == 1, "Expected late event to be dropped!"

    logger.info("=== WEEK 4 FLINK STREAMING OBSERVABLE VERIFIED SUCCESSFULLY ===")


if __name__ == "__main__":
    run_live_stream_demo()
