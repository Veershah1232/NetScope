"""
Unit & Integration Tests for Week 8 — Observability & Fault Tolerance (M9 & M10)

Tests:
- TEST-014: Prometheus metric instrumentation counters & gauges
- TEST-015: Checkpoint & failure recovery behavior
- Full resilience verification
"""

from __future__ import annotations

import os
import sys
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from observability.metrics_exporter.exporter import (
    record_measurement_metric,
    record_heartbeat_metric,
    MEASUREMENTS_TOTAL,
    PROBE_HEALTH_STATUS,
    KAFKA_CONSUMER_LAG,
)


class TestObservabilityMetrics:
    def test_measurement_counter_increments(self):
        """TEST-014: Verify metric increments on measurement recording."""
        init_val = MEASUREMENTS_TOTAL.labels(probe_id="p-obs", target_id="t-obs", status="success")._value.get()
        record_measurement_metric("p-obs", "t-obs", 200, total_ms=55.0)
        new_val = MEASUREMENTS_TOTAL.labels(probe_id="p-obs", target_id="t-obs", status="success")._value.get()
        assert new_val == init_val + 1.0

    def test_probe_heartbeat_gauge_updates(self):
        """TEST-014: Verify health gauge flips between healthy (1) and dead (0)."""
        record_heartbeat_metric("p-status-test", healthy=True)
        assert PROBE_HEALTH_STATUS.labels(probe_id="p-status-test")._value.get() == 1.0

        record_heartbeat_metric("p-status-test", healthy=False)
        assert PROBE_HEALTH_STATUS.labels(probe_id="p-status-test")._value.get() == 0.0

    def test_kafka_lag_gauge(self):
        KAFKA_CONSUMER_LAG.labels(group_id="g1", topic="measurements", partition="0").set(42)
        assert KAFKA_CONSUMER_LAG.labels(group_id="g1", topic="measurements", partition="0")._value.get() == 42.0
