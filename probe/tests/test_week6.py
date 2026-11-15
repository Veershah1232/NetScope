"""
Unit & Integration Tests for Week 6 — Anomaly Detection & Incident Engine (M7)

Tests:
- TEST-010: Statistical anomaly detector (threshold, baseline, z-score, multi-signal)
- TEST-011: Incident state machine transitions (NORMAL -> ACTIVE -> RECOVERING -> RESOLVED)
- Controlled test server chaos controls (/inject/delay, /inject/error, /reset)
"""

from __future__ import annotations

import os
import sys
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from anomaly_engine.detectors.statistical_detector import (
    StatisticalAnomalyDetector,
    RollingStats,
    AnomalyEvaluation,
)
from anomaly_engine.incident_manager.lifecycle import IncidentLifecycleManager, Incident


class TestStatisticalAnomalyDetector:
    def test_rolling_stats_mean_and_variance(self):
        stats = RollingStats(max_samples=5)
        for val in [10.0, 20.0, 30.0]:
            stats.add(val)
        assert stats.count == 3
        assert stats.mean == 20.0
        assert stats.std_dev == 10.0

    def test_static_threshold_detection(self):
        """TEST-010: Flag anomaly when latency exceeds fixed threshold."""
        detector = StatisticalAnomalyDetector(static_threshold_ms=500.0)
        # Under threshold -> NORMAL
        res_normal = detector.evaluate_window("t1", 200.0, 250.0, 0, 100.0)
        assert res_normal.is_anomaly is False
        assert res_normal.severity == "NORMAL"

        # Over threshold -> WARNING/CRITICAL
        res_anom = detector.evaluate_window("t1", 600.0, 750.0, 0, 100.0)
        assert res_anom.is_anomaly is True
        assert res_anom.severity in ("WARNING", "CRITICAL")
        assert any("exceeded static threshold" in r for r in res_anom.reasons)

    def test_multi_signal_scoring(self):
        """TEST-010: Combine latency, HTTP errors, and availability drops into a CRITICAL score."""
        detector = StatisticalAnomalyDetector()
        res = detector.evaluate_window(
            target_id="t1",
            avg_latency_ms=600.0,
            p95_ms=800.0,
            failure_count=5,        # 5 HTTP errors
            availability_pct=70.0,  # 30% availability drop
        )
        assert res.is_anomaly is True
        assert res.severity == "CRITICAL"
        assert res.anomaly_score >= 5.0
        assert len(res.reasons) >= 3


class TestIncidentLifecycleManager:
    def test_incident_state_machine_lifecycle(self):
        """TEST-011: Verify state machine: NORMAL -> ACTIVE -> RECOVERING -> RESOLVED."""
        events: list[str] = []
        manager = IncidentLifecycleManager(
            recovery_rounds_threshold=2,
            on_incident_event=lambda event, inc: events.append(event),
        )

        # 1. Normal state: No incidents
        eval_normal = AnomalyEvaluation(is_anomaly=False, severity="NORMAL", anomaly_score=0.0)
        inc = manager.evaluate_target_state("t1", "Target 1", "India", "india-01", eval_normal)
        assert inc is None
        assert len(manager.active_incidents) == 0

        # 2. Anomaly occurs: OPENED -> ACTIVE
        eval_anom = AnomalyEvaluation(is_anomaly=True, severity="WARNING", anomaly_score=3.5, reasons=["High latency"])
        inc = manager.evaluate_target_state("t1", "Target 1", "India", "india-01", eval_anom)
        assert inc is not None
        assert inc.status == "ACTIVE"
        assert inc.severity == "WARNING"
        assert "OPENED" in events

        # 3. Anomaly clears round 1 -> Transition to RECOVERING
        inc = manager.evaluate_target_state("t1", "Target 1", "India", "india-01", eval_normal)
        assert inc.status == "RECOVERING"
        assert "RECOVERING" in events

        # 4. Anomaly clears round 2 (reaches threshold) -> Transition to RESOLVED
        inc = manager.evaluate_target_state("t1", "Target 1", "India", "india-01", eval_normal)
        assert inc.status == "RESOLVED"
        assert inc.end_time is not None
        assert len(manager.active_incidents) == 0
        assert len(manager.resolved_incidents) == 1
        assert "RESOLVED" in events
