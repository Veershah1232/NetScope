"""
Unit tests for Week 3 Kafka Ingestion Layer — TEST-004 & TEST-005

Run with:
    cd probe
    python -m pytest tests/test_week3.py -v
"""

from __future__ import annotations

import json
import os
import sys
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from ingestion.producers.kafka_producer import NetScopeKafkaProducer
from schemas.measurement import Measurement, ProbeHeartbeat


class TestKafkaProducerMocked:
    """TEST-004: Kafka Producer serialization and topic dispatch."""

    @patch("kafka.KafkaProducer")
    def test_send_measurement_serializes_and_keys(self, mock_kafka):
        mock_instance = MagicMock()
        mock_kafka.return_value = mock_instance

        producer = NetScopeKafkaProducer(bootstrap_servers="localhost:9092")
        m = Measurement(
            probe_id="india-01",
            target_id="github-com",
            target="github.com",
            dns_ms=12.5,
            tcp_ms=25.0,
        )

        ok = producer.send_measurement(m.to_dict())
        assert ok is True

        # Check call arguments
        mock_instance.send.assert_called_once()
        topic, kwargs = mock_instance.send.call_args[0][0], mock_instance.send.call_args[1]
        assert topic == "measurements"
        assert kwargs["key"] == "india-01"
        assert kwargs["value"]["target_id"] == "github-com"

    @patch("kafka.KafkaProducer")
    def test_send_heartbeat_routes_to_probe_health(self, mock_kafka):
        mock_instance = MagicMock()
        mock_kafka.return_value = mock_instance

        producer = NetScopeKafkaProducer(bootstrap_servers="localhost:9092")
        hb = ProbeHeartbeat(probe_id="sg-01", measurements_sent=10, failed_measurements=0)

        ok = producer.send_heartbeat(hb.to_dict())
        assert ok is True

        topic, kwargs = mock_instance.send.call_args[0][0], mock_instance.send.call_args[1]
        assert topic == "probe_health"
        assert kwargs["key"] == "sg-01"
        assert kwargs["value"]["status"] == "healthy"

    @patch("kafka.KafkaProducer")
    def test_send_alert_routes_to_alerts_topic(self, mock_kafka):
        mock_instance = MagicMock()
        mock_kafka.return_value = mock_instance

        producer = NetScopeKafkaProducer(bootstrap_servers="localhost:9092")
        alert = {"target_id": "cloudflare-com", "severity": "HIGH", "score": 6.2}

        ok = producer.send_alert(alert)
        assert ok is True

        topic, kwargs = mock_instance.send.call_args[0][0], mock_instance.send.call_args[1]
        assert topic == "alerts"
        assert kwargs["key"] == "cloudflare-com"
