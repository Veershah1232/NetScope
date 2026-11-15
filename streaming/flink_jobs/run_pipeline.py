"""
Kafka to Flink Streaming Pipeline Runner — M5

Streams live measurements directly from Kafka topic 'measurements', feeds them
into the FlinkStreamingSimulator, computes real-time 1-minute tumbling windows
and 5-minute sliding windows (p50/p95/p99/availability), and emits metrics.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from kafka import KafkaConsumer
from streaming.flink_jobs.stream_processor import FlinkStreamingSimulator, WindowAggregate

logger = logging.getLogger("netscope.flink_pipeline")


def on_window_emitted(agg: WindowAggregate) -> None:
    """Invoked when a window triggers and closes."""
    print(
        f"\n[STREAM WINDOW FIRED: {agg.window_type}]\n"
        f"  Range:        {agg.window_start} -> {agg.window_end}\n"
        f"  Probe/Target: [{agg.probe_id}] -> {agg.target} ({agg.target_id})\n"
        f"  Events:       {agg.count} records | Failures: {agg.failure_count} | Availability: {agg.availability_pct}%\n"
        f"  Latencies:    avg={agg.avg_latency_ms}ms | p50={agg.p50_ms}ms | p95={agg.p95_ms}ms | p99={agg.p99_ms}ms"
    )


def on_late_event_dropped(record: dict) -> None:
    logger.warning(
        "Late event side-output: id=%s target=%s timestamp=%s",
        record.get("measurement_id"),
        record.get("target"),
        record.get("timestamp"),
    )


def run_pipeline(
    bootstrap_servers: str = "localhost:9092",
    topic: str = "measurements",
    group_id: str = "flink-stream-processor-group",
    watermark_delay_ms: int = 5000,
) -> None:
    logger.info("Initializing Flink Stream Processor connecting to Kafka: %s", bootstrap_servers)

    engine = FlinkStreamingSimulator(
        watermark_delay_ms=watermark_delay_ms,
        allowed_lateness_ms=10000,
        on_window_fire=on_window_emitted,
        on_late_event=on_late_event_dropped,
    )

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers.split(","),
        group_id=group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    logger.info("Flink streaming processor listening for Kafka events on topic '%s'...", topic)

    running = True

    def stop_signal(sig, frame):
        nonlocal running
        logger.info("Stopping stream processor...")
        running = False

    signal.signal(signal.SIGINT, stop_signal)
    signal.signal(signal.SIGTERM, stop_signal)

    try:
        while running:
            records = consumer.poll(timeout_ms=1000)
            for topic_partition, messages in records.items():
                for message in messages:
                    engine.process_record(message.value)
    finally:
        consumer.close()
        logger.info("Pipeline stopped cleanly.")


def main() -> None:
    parser = argparse.ArgumentParser(description="NetScope Flink Stream Processor")
    parser.add_argument("--bootstrap-servers", default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--topic", default="measurements")
    parser.add_argument("--group-id", default="flink-stream-processor-group")
    parser.add_argument("--watermark-delay-ms", type=int, default=5000)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    run_pipeline(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        group_id=args.group_id,
        watermark_delay_ms=args.watermark_delay_ms,
    )


if __name__ == "__main__":
    main()
