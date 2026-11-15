"""
Unit & Integration Tests for Week 4 — Flink Real-Time Streaming (M5)

Tests:
- TEST-006: Tumbling 1-minute window calculation (count, avg, p50, p95, p99, availability)
- TEST-007: Event-time, Watermarks & Late-Event Handling policy
- TEST-008: Deduplication in the stream
"""

from __future__ import annotations

import json
import os
import sys
import uuid
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from streaming.flink_jobs.stream_processor import (
    FlinkStreamingSimulator,
    WindowAggregate,
    calculate_percentile,
    parse_iso_to_epoch_ms,
)


class TestFlinkPercentiles:
    def test_percentile_calculation(self):
        data = [10.0, 20.0, 30.0, 40.0, 50.0]
        assert calculate_percentile(data, 50.0) == 30.0
        assert calculate_percentile(data, 95.0) == 48.0
        assert calculate_percentile(data, 100.0) == 50.0


class TestFlinkStreamingEngine:
    def test_deduplication(self):
        """TEST-008: Verifies duplicate measurement IDs are cleanly deduplicated."""
        engine = FlinkStreamingSimulator(watermark_delay_ms=1000)
        mid = str(uuid.uuid4())

        event = {
            "measurement_id": mid,
            "probe_id": "india-01",
            "target_id": "github-com",
            "target": "github.com",
            "timestamp": "2026-11-16T10:00:05Z",
            "total_ms": 100.0,
            "http_status": 200,
        }

        # First record accepted
        assert engine.process_record(event) is True
        # Duplicate record dropped
        assert engine.process_record(event) is False

    def test_tumbling_window_metrics_and_watermark(self):
        """TEST-006: 1-minute window triggers when watermark passes window end."""
        fired_windows: list[WindowAggregate] = []

        engine = FlinkStreamingSimulator(
            watermark_delay_ms=2000,  # 2 second bounded out-of-orderness
            on_window_fire=lambda w: fired_windows.append(w),
        )

        # 3 events in window [10:00:00 -> 10:01:00]
        # Latencies: 100, 200, 300 -> avg=200, p50=200, p95=290
        engine.process_record({
            "measurement_id": str(uuid.uuid4()),
            "probe_id": "india-01",
            "target_id": "github-com",
            "target": "github.com",
            "timestamp": "2026-11-16T10:00:10Z",
            "total_ms": 100.0,
            "http_status": 200,
        })
        engine.process_record({
            "measurement_id": str(uuid.uuid4()),
            "probe_id": "india-01",
            "target_id": "github-com",
            "target": "github.com",
            "timestamp": "2026-11-16T10:00:25Z",
            "total_ms": 200.0,
            "http_status": 200,
        })
        engine.process_record({
            "measurement_id": str(uuid.uuid4()),
            "probe_id": "india-01",
            "target_id": "github-com",
            "target": "github.com",
            "timestamp": "2026-11-16T10:00:50Z",
            "total_ms": 300.0,
            "http_status": 200,
        })

        # Tumbling window [10:00:00 -> 10:01:00] shouldn't fire yet because watermark hasn't reached 10:01:00
        tumbling_early = [w for w in fired_windows if w.window_type == "tumbling_1m"]
        assert len(tumbling_early) == 0

        # Now send an event from 10:01:05 -> with delay 2000ms, watermark = 10:01:03 >= 10:01:00
        engine.process_record({
            "measurement_id": str(uuid.uuid4()),
            "probe_id": "india-01",
            "target_id": "github-com",
            "target": "github.com",
            "timestamp": "2026-11-16T10:01:05Z",
            "total_ms": 150.0,
            "http_status": 200,
        })

        # Tumbling window [10:00:00, 10:01:00] must now have fired!
        tumbling = [w for w in fired_windows if w.window_type == "tumbling_1m"]
        assert len(tumbling) == 1
        w = tumbling[0]
        assert w.count == 3
        assert w.avg_latency_ms == 200.0
        assert w.p50_ms == 200.0
        assert w.availability_pct == 100.0
        assert w.failure_count == 0

    def test_late_event_watermark_policy(self):
        """TEST-007: Late arrival handling. Events within allowed lateness vs past it."""
        late_events: list[dict] = []
        fired_windows: list[WindowAggregate] = []

        engine = FlinkStreamingSimulator(
            watermark_delay_ms=2000,
            allowed_lateness_ms=5000,
            on_window_fire=lambda w: fired_windows.append(w),
            on_late_event=lambda r: late_events.append(r),
        )

        # Event 1: 10:01:00
        engine.process_record({
            "measurement_id": "e1",
            "probe_id": "india-01",
            "target_id": "google-com",
            "target": "google.com",
            "timestamp": "2026-11-16T10:01:00Z",
            "total_ms": 50.0,
            "http_status": 200,
        })

        # Event 2 advances watermark far ahead: 10:03:00 (Watermark = 10:02:58)
        engine.process_record({
            "measurement_id": "e2",
            "probe_id": "india-01",
            "target_id": "google-com",
            "target": "google.com",
            "timestamp": "2026-11-16T10:03:00Z",
            "total_ms": 60.0,
            "http_status": 200,
        })

        # Event 3 is late: arrives with timestamp 10:00:30 (way behind 10:02:58 - 5s)
        late_record = {
            "measurement_id": "e3_late",
            "probe_id": "india-01",
            "target_id": "google-com",
            "target": "google.com",
            "timestamp": "2026-11-16T10:00:30Z",
            "total_ms": 70.0,
            "http_status": 200,
        }
        accepted = engine.process_record(late_record)

        # Must be rejected and routed to side-output
        assert accepted is False
        assert len(late_events) == 1
        assert late_events[0]["measurement_id"] == "e3_late"
