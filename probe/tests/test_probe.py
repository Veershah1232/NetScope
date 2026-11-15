"""
Tests for NetScope Probe Engine — TEST-001, TEST-002

Run with:
    cd probe
    python -m pytest tests/ -v
"""

from __future__ import annotations

import json
import sys
import os
import time
import uuid
from unittest.mock import patch, MagicMock

import pytest

# Allow importing from project root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from schemas.measurement import Measurement, ProbeHeartbeat
from netscope_probe.probe import (
    measure_target,
    load_targets_from_hosts,
    emit_heartbeat,
    _fmt_ms,
)


# ---------------------------------------------------------------------------
# TEST-001: Schema and Measurement structure
# ---------------------------------------------------------------------------

class TestMeasurementSchema:
    """TEST-001 — Measurement schema correctness."""

    def test_measurement_has_required_fields(self):
        m = Measurement(probe_id="test-01", target_id="github-com", target="github.com")
        assert m.measurement_id  # uuid auto-generated
        assert m.probe_id == "test-01"
        assert m.target_id == "github-com"
        assert m.target == "github.com"
        assert m.timestamp  # auto-generated

    def test_measurement_id_is_valid_uuid(self):
        m = Measurement()
        parsed = uuid.UUID(m.measurement_id)  # raises if invalid
        assert str(parsed) == m.measurement_id

    def test_measurement_to_dict_contains_all_fields(self):
        m = Measurement(probe_id="p1", target_id="t1", target="example.com",
                        dns_ms=10.0, tcp_ms=20.0, tls_ms=30.0,
                        ttfb_ms=50.0, total_ms=80.0,
                        http_status=200, response_bytes=1234)
        d = m.to_dict()
        assert "measurement_id" in d
        assert "probe_id" in d
        assert "dns_ms" in d
        assert "http_status" in d
        assert d["dns_ms"] == 10.0

    def test_measurement_is_success_true(self):
        m = Measurement(http_status=200)
        assert m.is_success()

    def test_measurement_is_success_false_on_error(self):
        m = Measurement(http_status=200, error_type="TIMEOUT")
        assert not m.is_success()

    def test_measurement_is_success_false_on_5xx(self):
        m = Measurement(http_status=500)
        assert not m.is_success()

    def test_measurement_validate_missing_required_fields(self):
        m = Measurement()  # probe_id and target are empty
        errors = m.validate()
        assert any("probe_id" in e for e in errors)
        assert any("target_id" in e for e in errors)

    def test_measurement_validate_negative_timing(self):
        m = Measurement(probe_id="p1", target_id="t1", target="x.com",
                        dns_ms=-5.0)
        errors = m.validate()
        assert any("dns_ms" in e for e in errors)

    def test_measurement_validate_bad_timestamp(self):
        m = Measurement(probe_id="p1", target_id="t1", target="x.com",
                        timestamp="not-a-timestamp")
        errors = m.validate()
        assert any("timestamp" in e for e in errors)

    def test_measurement_valid_passes_validation(self):
        m = Measurement(probe_id="india-01", target_id="github-com",
                        target="github.com",
                        dns_ms=18.4, tcp_ms=32.1, tls_ms=47.2,
                        ttfb_ms=81.3, total_ms=104.8,
                        http_status=200, response_bytes=18291)
        errors = m.validate()
        assert errors == []


# ---------------------------------------------------------------------------
# TEST-001: Probe heartbeat schema
# ---------------------------------------------------------------------------

class TestHeartbeatSchema:
    def test_heartbeat_fields(self):
        hb = ProbeHeartbeat(probe_id="india-01", measurements_sent=100, failed_measurements=2)
        d = hb.to_dict()
        assert d["probe_id"] == "india-01"
        assert d["status"] == "healthy"
        assert d["version"] == "0.1.0"
        assert d["measurements_sent"] == 100
        assert d["failed_measurements"] == 2


# ---------------------------------------------------------------------------
# TEST-002: Timeout / retry behaviour (mocked network)
# ---------------------------------------------------------------------------

class TestTimeoutHandling:
    """TEST-002 — Timeout and error handling."""

    def test_dns_failure_sets_error_type(self):
        """When DNS fails, measurement should have error_type set and no TCP timing."""
        import socket
        with patch("socket.getaddrinfo", side_effect=socket.gaierror("NXDOMAIN")):
            m = measure_target(
                probe_id="test-01",
                target_id="nonexistent-xyz",
                host="nonexistent.xyz.invalid",
                scheme="https",
                timeout=2.0,
            )
        assert m.error_type is not None
        assert "DNS" in m.error_type
        assert m.tcp_ms is None
        assert m.tls_ms is None

    def test_tcp_refused_sets_error_type(self):
        """When TCP is refused, measurement should capture DNS time and TCP error."""
        import socket
        fake_addr = [(socket.AF_INET, None, None, None, ("127.0.0.1", 9))]
        with patch("socket.getaddrinfo", return_value=fake_addr):
            with patch("socket.socket") as mock_sock_cls:
                mock_sock = MagicMock()
                mock_sock.connect.side_effect = ConnectionRefusedError("refused")
                mock_sock_cls.return_value = mock_sock
                m = measure_target(
                    probe_id="test-01",
                    target_id="refused-host",
                    host="refused.local",
                    scheme="http",
                    timeout=2.0,
                )
        assert m.error_type == "TCP_REFUSED"
        assert m.dns_ms is not None  # DNS was "resolved"

    def test_timed_out_flag_set_on_connect_timeout(self):
        """A socket.timeout on connect must set timed_out=True."""
        import socket
        fake_addr = [(socket.AF_INET, None, None, None, ("127.0.0.1", 80))]
        with patch("socket.getaddrinfo", return_value=fake_addr):
            with patch("socket.socket") as mock_sock_cls:
                mock_sock = MagicMock()
                mock_sock.connect.side_effect = socket.timeout("timed out")
                mock_sock_cls.return_value = mock_sock
                m = measure_target(
                    probe_id="test-01",
                    target_id="slow-host",
                    host="slow.local",
                    scheme="http",
                    timeout=0.001,
                )
        assert m.timed_out is True
        assert "TIMEOUT" in m.error_type


# ---------------------------------------------------------------------------
# TEST-001 (integration): Live measurement — optional, skip if no network
# ---------------------------------------------------------------------------

@pytest.mark.integration
class TestLiveMeasurement:
    """
    Live measurements against real hosts.
    Skipped in CI or offline environments.
    Run with: pytest -m integration
    """

    def test_measure_cloudflare_dns(self):
        """Cloudflare 1.1.1.1 should respond quickly."""
        m = measure_target(
            probe_id="test-01",
            target_id="one-one-one-one",
            host="one.one.one.one",
            scheme="https",
            path="/",
            timeout=10.0,
        )
        assert m.dns_ms is not None, "DNS should succeed for 1.1.1.1"
        assert m.tcp_ms is not None, "TCP should succeed"
        assert m.http_status is not None, "Should get an HTTP response"
        errors = m.validate()
        assert errors == []

    def test_measure_github(self):
        m = measure_target(
            probe_id="test-01",
            target_id="github-com",
            host="github.com",
            scheme="https",
            path="/",
            timeout=15.0,
        )
        assert m.dns_ms is not None
        assert m.total_ms is not None
        assert m.total_ms > 0
