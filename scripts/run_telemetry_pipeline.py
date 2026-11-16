"""
NetScope End-to-End Real-Time Pipeline & Web UI Runner

This unified service bridges the entire NetScope architecture:
1. Starts Prometheus Metrics Server on port 9102 (feeds Grafana on :3000).
2. Starts NetScope Interactive Web UI on port 8050 (http://localhost:8050).
3. Consumes real-time measurements from Kafka (produced by probe-india, probe-sg, probe-eu):
   - Inserts records into ClickHouse OLAP (netscope.measurements)
   - Writes micro-batches to MinIO S3 in Snappy Parquet Hive format
   - Updates Prometheus counters & gauges so Grafana charts light up in real time!
"""

from __future__ import annotations

import json
import logging
import os
import signal
import socketserver
import sys
import threading
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from kafka import KafkaConsumer
from warehouse.ingestion.clickhouse_client import ClickHouseClient
from probe.netscope_probe.writers.minio_writer import (
    _make_minio_client,
    ensure_bucket,
    _partition_key,
    write_partition_to_minio,
    BUCKET_DEFAULT,
)
from observability.metrics_exporter.exporter import (
    start_metrics_server,
    record_measurement_metric,
    record_heartbeat_metric,
)
from scripts.web_ui import NetScopeWebHandler

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("netscope.pipeline_runner")


def run_web_server(port: int = 8050):
    class ReusableTCPServer(socketserver.TCPServer):
        allow_reuse_address = True

    server = ReusableTCPServer(("0.0.0.0", port), NetScopeWebHandler)
    logger.info("Web UI listening at http://localhost:%d", port)
    server.serve_forever()


def run_kafka_consumer_pipeline():
    logger.info("Initializing Kafka ingestion pipeline...")

    # Connect to ClickHouse
    ch_client = None
    for attempt in range(5):
        try:
            ch_client = ClickHouseClient(host="localhost", port=8123)
            logger.info("Connected to ClickHouse OLAP on localhost:8123")
            break
        except Exception as e:
            logger.warning("ClickHouse connect retry %d/5: %s", attempt + 1, e)
            time.sleep(2.0)

    # Connect to MinIO
    minio_client = _make_minio_client("http://localhost:9000", "minioadmin", "minioadmin")
    try:
        ensure_bucket(minio_client, BUCKET_DEFAULT)
        logger.info("Connected to MinIO S3 bucket: %s", BUCKET_DEFAULT)
    except Exception as e:
        logger.warning("MinIO init warning: %s", e)

    # Connect Kafka consumer
    consumer = None
    for attempt in range(10):
        try:
            consumer = KafkaConsumer(
                "measurements",
                "probe_health",
                bootstrap_servers=["localhost:9092"],
                group_id="netscope-unified-pipeline",
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            )
            logger.info("Connected to Kafka broker on localhost:9092 (topics: measurements, probe_health)")
            break
        except Exception as e:
            logger.warning("Kafka connect retry %d/10: %s", attempt + 1, e)
            time.sleep(2.0)

    if consumer is None:
        logger.error("Could not connect to Kafka. Exiting consumer thread.")
        return

    buffer_ch: list[dict] = []
    buffer_minio: list[dict] = []
    last_flush = time.time()

    logger.info("Pipeline actively listening for probe streams...")

    while True:
        try:
            msg_batch = consumer.poll(timeout_ms=1000, max_records=50)
            now = time.time()

            for tp, records in msg_batch.items():
                for record in records:
                    topic = record.topic
                    val = record.value

                    if topic == "probe_health":
                        probe_id = val.get("probe_id", "unknown")
                        record_heartbeat_metric(probe_id=probe_id, healthy=True)

                    elif topic == "measurements":
                        probe_id = val.get("probe_id", "unknown")
                        target_id = val.get("target_id", "unknown")
                        http_status = val.get("http_status") or 0
                        dns_ms = val.get("dns_ms")
                        tcp_ms = val.get("tcp_ms")
                        tls_ms = val.get("tls_ms")
                        ttfb_ms = val.get("ttfb_ms")
                        total_ms = val.get("total_ms")

                        # 1. Update Prometheus metrics for Grafana
                        record_measurement_metric(
                            probe_id=probe_id,
                            target_id=target_id,
                            status_code=http_status,
                            dns_ms=dns_ms,
                            tcp_ms=tcp_ms,
                            tls_ms=tls_ms,
                            ttfb_ms=ttfb_ms,
                            total_ms=total_ms,
                        )
                        record_heartbeat_metric(probe_id=probe_id, healthy=True)

                        buffer_ch.append(val)
                        buffer_minio.append(val)

            # Flush to ClickHouse & MinIO if buffer size reached or time elapsed
            if len(buffer_ch) >= 15 or (now - last_flush > 5.0 and len(buffer_ch) > 0):
                # Flush to ClickHouse
                if ch_client and buffer_ch:
                    try:
                        inserted = ch_client.insert_measurements(buffer_ch)
                        logger.info("Persisted %d new measurements to ClickHouse OLAP", inserted)
                    except Exception as e:
                        logger.warning("ClickHouse insert warning: %s", e)
                buffer_ch.clear()

                # Flush to MinIO S3
                if buffer_minio:
                    try:
                        part_counter = getattr(run_kafka_consumer_pipeline, "_part_counter", 0) + 1
                        setattr(run_kafka_consumer_pipeline, "_part_counter", part_counter)

                        probes_group = {}
                        for r in buffer_minio:
                            pid = r.get("probe_id", "india-01")
                            probes_group.setdefault(pid, []).append(r)

                        for pid, items in probes_group.items():
                            ts_val = items[0].get("timestamp", datetime.now(timezone.utc).isoformat())
                            pkey = _partition_key(ts_val, pid)
                            write_partition_to_minio(
                                minio_client,
                                BUCKET_DEFAULT,
                                pkey,
                                part_counter,
                                items,
                            )
                        logger.info("Flushed %d measurements to MinIO S3 Parquet Lake", len(buffer_minio))
                    except Exception as e:
                        logger.warning("MinIO flush warning: %s", e)
                buffer_minio.clear()

                last_flush = now

        except Exception as exc:
            logger.error("Consumer loop exception: %s", exc)
            time.sleep(1.0)


def main():
    print("=" * 70)
    print("   NETSCOPE UNIFIED PIPELINE, METRICS EXPORTER & WEB UI")
    print("=" * 70)

    # 1. Start Prometheus Exporter on 9102
    try:
        start_metrics_server(port=9102)
        print("✅ [1/3] Prometheus Metrics Exporter: http://localhost:9102/metrics")
    except Exception as e:
        print(f"⚠️ [1/3] Metrics Exporter on 9102 already active or warning: {e}")

    # 2. Start Web UI in background thread
    t_web = threading.Thread(target=run_web_server, args=(8050,), daemon=True)
    t_web.start()
    print("✅ [2/3] Web UI Telemetry Explorer:  http://localhost:8050")

    # 3. Start Kafka -> ClickHouse & MinIO pipeline in background thread
    t_pipeline = threading.Thread(target=run_kafka_consumer_pipeline, daemon=True)
    t_pipeline.start()
    print("✅ [3/3] Kafka -> ClickHouse & MinIO Stream Pipeline: ACTIVE")

    print("\n🚀 All NetScope background streaming bridges are live!")
    print("   - Grafana (:3000) is now receiving live metrics from :9102")
    print("   - ClickHouse (:8123) is receiving live stream records from Kafka")
    print("   - MinIO (:9001) is receiving Parquet batches from Kafka")
    print("   - Web UI is running at http://localhost:8050\n")

    # Keep main thread alive
    try:
        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:
        print("\nStopping NetScope Unified Pipeline.")


if __name__ == "__main__":
    main()
