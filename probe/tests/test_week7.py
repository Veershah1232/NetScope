"""
Unit & Integration Tests for Week 7 — Geographic & Traceroute Analysis (M8)

Tests:
- TEST-012: Regional vs Global degradation classification
- TEST-013: Traceroute parsing and hop latency collection
- Route signature hashing & route change detection
"""

from __future__ import annotations

import os
import sys
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from network_analysis.geographic.correlation_engine import (
    GeographicCorrelationEngine,
    RegionalObservation,
)
from network_analysis.traceroute.traceroute_engine import (
    Hop,
    RoutePath,
    compute_route_hash,
)


class TestGeographicCorrelationEngine:
    def test_likely_regional_degradation(self):
        """TEST-012: India is degraded, SG and EU are healthy -> LIKELY REGIONAL."""
        engine = GeographicCorrelationEngine()
        observations = [
            RegionalObservation("india-01", "India", avg_latency_ms=650.0, p95_ms=700.0, is_degraded=True),
            RegionalObservation("sg-01", "Singapore", avg_latency_ms=110.0, p95_ms=125.0, is_degraded=False),
            RegionalObservation("eu-01", "Europe", avg_latency_ms=120.0, p95_ms=130.0, is_degraded=False),
        ]

        diagnosis = engine.diagnose_target("github-com", "github.com", observations)
        assert diagnosis.classification == "LIKELY REGIONAL DEGRADATION"
        assert "India" in diagnosis.degraded_regions
        assert "Singapore" in diagnosis.healthy_regions
        assert "Europe" in diagnosis.healthy_regions

    def test_likely_global_outage(self):
        """TEST-012: All regions degraded -> LIKELY GLOBAL OUTAGE."""
        engine = GeographicCorrelationEngine()
        observations = [
            RegionalObservation("india-01", "India", avg_latency_ms=750.0, p95_ms=800.0, is_degraded=True),
            RegionalObservation("sg-01", "Singapore", avg_latency_ms=680.0, p95_ms=720.0, is_degraded=True),
            RegionalObservation("eu-01", "Europe", avg_latency_ms=720.0, p95_ms=760.0, is_degraded=True),
        ]

        diagnosis = engine.diagnose_target("github-com", "github.com", observations)
        assert diagnosis.classification == "LIKELY GLOBAL OUTAGE"
        assert len(diagnosis.degraded_regions) == 3
        assert len(diagnosis.healthy_regions) == 0

    def test_normal_performance(self):
        engine = GeographicCorrelationEngine()
        observations = [
            RegionalObservation("india-01", "India", avg_latency_ms=25.0, p95_ms=30.0, is_degraded=False),
            RegionalObservation("sg-01", "Singapore", avg_latency_ms=110.0, p95_ms=120.0, is_degraded=False),
        ]
        diagnosis = engine.diagnose_target("cloudflare-com", "cloudflare.com", observations)
        assert diagnosis.classification == "NORMAL"


class TestTracerouteEngine:
    def test_route_signature_hash_and_change_detection(self):
        """TEST-013: Verify route hash changes when path shifts."""
        path_a = [
            Hop(1, "192.168.1.1", 2.0),
            Hop(2, "10.0.0.1", 10.0),
            Hop(3, "172.16.0.1", 25.0),
            Hop(4, "1.1.1.1", 30.0),
        ]
        hash_a = compute_route_hash(path_a)
        assert len(hash_a) == 12

        # Route change at hop 3:
        path_b = [
            Hop(1, "192.168.1.1", 2.0),
            Hop(2, "10.0.0.1", 10.0),
            Hop(3, "104.28.1.1", 55.0),  # rerouted through transit provider
            Hop(4, "1.1.1.1", 60.0),
        ]
        hash_b = compute_route_hash(path_b)

        assert hash_a != hash_b, "Route hash must change when network path shifts!"

    def test_route_hash_handles_timeouts(self):
        """Timed-out hops (*) should not corrupt the route hash."""
        path_with_timeout = [
            Hop(1, "192.168.1.1", 2.0),
            Hop(2, "*", None),
            Hop(3, "1.1.1.1", 28.0),
        ]
        h = compute_route_hash(path_with_timeout)
        assert len(h) == 12
