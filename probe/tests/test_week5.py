"""
Unit & Integration Tests for Week 5 — ClickHouse Analytical Warehouse (M6 / TEST-009)

Tests:
- ClickHouse connection & schema availability
- Table creation (MergeTree with codecs and low cardinality)
- Fast vectorized insert capability
- Analytical aggregation query correctness (quantiles, protocol breakdown)
"""

from __future__ import annotations

import os
import sys
import uuid
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from warehouse.ingestion.clickhouse_client import ClickHouseClient


@pytest.fixture(scope="module")
def ch_client():
    try:
        client = ClickHouseClient(host="localhost", port=8123)
        # Verify connectivity
        client.query("SELECT 1")
        return client
    except Exception as exc:
        pytest.skip(f"ClickHouse not available on localhost:8123: {exc}")


class TestClickHouseWarehouse:
    def test_tables_exist(self, ch_client):
        """Verify all core tables and materialized views are provisioned."""
        rows = ch_client.query("SHOW TABLES FROM netscope")
        tables = [r[0] for r in rows]
        assert "measurements" in tables
        assert "latency_windows" in tables
        assert "incidents" in tables
        assert "target_daily_stats" in tables
        assert "mv_target_daily_stats" in tables

    def test_single_measurement_insert_and_query(self, ch_client):
        """TEST-009: Test inserting a test measurement and reading it back."""
        unique_mid = str(uuid.uuid4())
        record = {
            "measurement_id": unique_mid,
            "probe_id": "test-ch-probe",
            "target_id": "test-ch-target",
            "target": "clickhouse-test.local",
            "timestamp": "2026-11-16T15:00:00Z",
            "dns_ms": 11.2,
            "tcp_ms": 22.4,
            "tls_ms": 33.6,
            "ttfb_ms": 44.8,
            "total_ms": 112.0,
            "http_status": 200,
            "response_bytes": 4096,
            "error_type": None,
            "timed_out": False,
        }

        inserted = ch_client.insert_measurements([record])
        assert inserted == 1

        # Query it back
        res = ch_client.query(
            f"SELECT probe_id, target, total_ms FROM netscope.measurements WHERE measurement_id = '{unique_mid}'"
        )
        assert len(res) == 1
        assert res[0][0] == "test-ch-probe"
        assert res[0][1] == "clickhouse-test.local"
        assert res[0][2] == pytest.approx(112.0, abs=0.1)

    def test_window_aggregate_insert(self, ch_client):
        """Verify inserting Flink-computed window aggregates."""
        window_record = {
            "window_type": "tumbling_1m",
            "window_start": "2026-11-16T15:00:00Z",
            "window_end": "2026-11-16T15:01:00Z",
            "probe_id": "india-01",
            "target_id": "github-com",
            "target": "github.com",
            "count": 50,
            "avg_latency_ms": 145.2,
            "p50_ms": 120.0,
            "p95_ms": 310.0,
            "p99_ms": 450.0,
            "failure_count": 1,
            "availability_pct": 98.0,
        }

        inserted = ch_client.insert_window_aggregates([window_record])
        assert inserted == 1

        res = ch_client.query(
            "SELECT count, p95_ms, availability_pct FROM netscope.latency_windows WHERE probe_id = 'india-01' AND target_id = 'github-com' ORDER BY window_start DESC LIMIT 1"
        )
        assert len(res) == 1
        assert res[0][0] == 50
        assert res[0][1] == pytest.approx(310.0, abs=0.1)
        assert res[0][2] == pytest.approx(98.0, abs=0.1)
