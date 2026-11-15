"""
NetScope Prometheus Metrics Exporter — M9 / FR_16

Collects real-time telemetry from across the entire NetScope stack:
1. Probe Telemetry:
   - netscope_measurements_total (Counter by probe_id, target, status)
   - netscope_probe_latency_ms (Histogram: DNS, TCP, TLS, TTFB, Total)
   - netscope_probe_heartbeat_timestamp (Gauge by probe_id)
2. Kafka & Ingestion Telemetry:
   - netscope_kafka_consumer_lag (Gauge by group_id, topic, partition)
   - netscope_kafka_messages_in_total (Counter by topic)
3. Flink Stream Processing:
   - netscope_flink_window_latency_ms (Gauge p50/p95/p99)
   - netscope_flink_late_events_total (Counter)
   - netscope_flink_dedup_drops_total (Counter)
4. Incident Engine:
   - netscope_active_incidents (Gauge by target, severity)
   - netscope_anomaly_score (Gauge by target)
"""

from __future__ import annotations

import logging
import time
from prometheus_client import (
    Counter,
    Gauge,
    Histogram,
    start_http_server,
)

logger = logging.getLogger("netscope.metrics_exporter")

# --- 1. Probes & Measurements ---
MEASUREMENTS_TOTAL = Counter(
    "netscope_measurements_total",
    "Total Internet measurements executed by probes",
    ["probe_id", "target_id", "status"],
)

PROBE_LATENCY_HIST = Histogram(
    "netscope_probe_latency_ms",
    "Measured latency in milliseconds across protocol stages",
    ["stage", "probe_id", "target_id"],
    buckets=[5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000],
)

PROBE_HEARTBEAT_TIME = Gauge(
    "netscope_probe_heartbeat_timestamp",
    "Timestamp of last received probe heartbeat",
    ["probe_id"],
)

PROBE_HEALTH_STATUS = Gauge(
    "netscope_probe_health_status",
    "Probe health status (1 = healthy, 0 = degraded/down)",
    ["probe_id"],
)

# --- 2. Kafka & Storage ---
KAFKA_CONSUMER_LAG = Gauge(
    "netscope_kafka_consumer_lag",
    "Unconsumed messages in Kafka partition",
    ["group_id", "topic", "partition"],
)

KAFKA_MESSAGES_IN_TOTAL = Counter(
    "netscope_kafka_messages_in_total",
    "Total messages ingested into Kafka topic",
    ["topic"],
)

# --- 3. Stream Engine (Flink) ---
FLINK_WINDOW_P95 = Gauge(
    "netscope_flink_window_p95_ms",
    "Current p95 latency computed over 1-minute tumbling window",
    ["target_id", "probe_id"],
)

FLINK_WINDOW_AVAILABILITY = Gauge(
    "netscope_flink_window_availability_pct",
    "Current availability % over 1-minute tumbling window",
    ["target_id"],
)

FLINK_LATE_EVENTS_TOTAL = Counter(
    "netscope_flink_late_events_total",
    "Total events rejected due to arriving past watermark allowed lateness",
)

FLINK_DEDUP_DROPS_TOTAL = Counter(
    "netscope_flink_dedup_drops_total",
    "Total duplicate measurement events dropped by stream deduplicator",
)

# --- 4. Incidents & Anomalies ---
ACTIVE_INCIDENTS = Gauge(
    "netscope_active_incidents",
    "Number of currently open active incidents",
    ["target_id", "severity"],
)

ANOMALY_SCORE = Gauge(
    "netscope_anomaly_score",
    "Calculated composite statistical anomaly score",
    ["target_id"],
)


def record_measurement_metric(
    probe_id: str,
    target_id: str,
    status_code: int,
    dns_ms: float | None = None,
    tcp_ms: float | None = None,
    tls_ms: float | None = None,
    ttfb_ms: float | None = None,
    total_ms: float | None = None,
) -> None:
    status_label = "success" if 200 <= status_code < 400 else "error"
    MEASUREMENTS_TOTAL.labels(probe_id=probe_id, target_id=target_id, status=status_label).inc()

    if dns_ms is not None:
        PROBE_LATENCY_HIST.labels(stage="dns", probe_id=probe_id, target_id=target_id).observe(dns_ms)
    if tcp_ms is not None:
        PROBE_LATENCY_HIST.labels(stage="tcp", probe_id=probe_id, target_id=target_id).observe(tcp_ms)
    if tls_ms is not None:
        PROBE_LATENCY_HIST.labels(stage="tls", probe_id=probe_id, target_id=target_id).observe(tls_ms)
    if ttfb_ms is not None:
        PROBE_LATENCY_HIST.labels(stage="ttfb", probe_id=probe_id, target_id=target_id).observe(ttfb_ms)
    if total_ms is not None:
        PROBE_LATENCY_HIST.labels(stage="total", probe_id=probe_id, target_id=target_id).observe(total_ms)


def record_heartbeat_metric(probe_id: str, healthy: bool = True) -> None:
    PROBE_HEARTBEAT_TIME.labels(probe_id=probe_id).set(time.time())
    PROBE_HEALTH_STATUS.labels(probe_id=probe_id).set(1 if healthy else 0)


def start_metrics_server(port: int = 9102) -> None:
    start_http_server(port)
    logger.info("Prometheus Metrics Server listening on http://0.0.0.0:%d/metrics", port)
