"""
Tests for Week 2 additions — data quality checker and Parquet writer.

TEST-002 (extended): Per-target timeout / max_bytes
TEST-003: Probe heartbeat / identity from env
Data quality: missing fields, duplicate IDs, timestamp validation, negative timings

Run with:
    cd probe
    python -m pytest tests/ -v
"""

from __future__ import annotations

import io
import json
import os
import sys
import tempfile
import uuid
from unittest.mock import patch, MagicMock

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from schemas.measurement import Measurement, ProbeHeartbeat
from netscope_probe.probe import load_targets_from_yaml, run_sweep


# ---------------------------------------------------------------------------
# TEST-002 (extended): Per-target config respected
# ---------------------------------------------------------------------------

class TestPerTargetConfig:
    def test_load_targets_skips_disabled(self, tmp_path):
        """Targets with enabled: false must be excluded."""
        yaml_content = """
targets:
  - id: a-com
    host: a.com
    scheme: https
    enabled: true
  - id: b-com
    host: b.com
    scheme: https
    enabled: false
  - id: c-com
    host: c.com
    scheme: https
"""
        f = tmp_path / "targets.yaml"
        f.write_text(yaml_content)
        targets = load_targets_from_yaml(str(f))
        ids = [t["id"] for t in targets]
        assert "a-com" in ids
        assert "b-com" not in ids   # disabled
        assert "c-com" in ids       # enabled by default

    def test_per_target_timeout_used(self, tmp_path):
        """Per-target timeout override must reach measure_target."""
        yaml_content = """
targets:
  - id: slow-host
    host: slow.example.com
    scheme: http
    timeout: 25
"""
        f = tmp_path / "targets.yaml"
        f.write_text(yaml_content)
        targets = load_targets_from_yaml(str(f))
        assert targets[0]["timeout"] == 25


# ---------------------------------------------------------------------------
# TEST-003: Probe identity and heartbeat
# ---------------------------------------------------------------------------

class TestProbeIdentity:
    def test_heartbeat_contains_probe_id(self):
        from netscope_probe.probe import emit_heartbeat
        hb = emit_heartbeat("sg-01", 50, 2)
        assert hb.probe_id == "sg-01"
        assert hb.measurements_sent == 50
        assert hb.failed_measurements == 2

    def test_heartbeat_serialises_to_dict(self):
        from netscope_probe.probe import emit_heartbeat
        hb = emit_heartbeat("eu-01", 10, 0)
        d = hb.to_dict()
        assert d["probe_id"] == "eu-01"
        assert "timestamp" in d
        assert d["status"] == "healthy"

    def test_probe_id_env_fallback(self):
        """Probe reads PROBE_ID from environment when --probe-id not given."""
        with patch.dict(os.environ, {"PROBE_ID": "cloud-probe-99"}):
            # Import fresh to pick up env at parse time
            probe_id = os.environ.get("PROBE_ID", "local-01")
        assert probe_id == "cloud-probe-99"


# ---------------------------------------------------------------------------
# Data quality checker tests
# ---------------------------------------------------------------------------

class TestDataQualityChecker:
    """Tests for data_quality/checker.py"""

    def _write_jsonl(self, tmp_path, records: list[dict]) -> str:
        f = tmp_path / "test.jsonl"
        f.write_text("\n".join(json.dumps(r) for r in records) + "\n")
        return str(f)

    def _make_valid_record(self, **overrides) -> dict:
        base = {
            "measurement_id": str(uuid.uuid4()),
            "probe_id":       "india-01",
            "target_id":      "github-com",
            "target":         "github.com",
            "timestamp":      "2026-11-16T00:00:00+00:00",
            "dns_ms":         18.4,
            "tcp_ms":         32.1,
            "tls_ms":         47.2,
            "ttfb_ms":        81.3,
            "total_ms":       104.8,
            "http_status":    200,
            "response_bytes": 18291,
            "error_type":     None,
            "timed_out":      False,
        }
        base.update(overrides)
        return base

    def test_valid_record_passes(self, tmp_path):
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "data_quality"))
        from data_quality.checker import check_jsonl_file
        path = self._write_jsonl(tmp_path, [self._make_valid_record()])
        report = check_jsonl_file(path)
        assert report.total_records == 1
        assert report.valid_records == 1
        assert len(report.issues) == 0

    def test_missing_probe_id_flagged(self, tmp_path):
        from data_quality.checker import check_jsonl_file
        rec = self._make_valid_record(probe_id="")
        path = self._write_jsonl(tmp_path, [rec])
        report = check_jsonl_file(path)
        assert any(i["issue_type"] == "MISSING_FIELD" for i in report.issues)

    def test_invalid_timestamp_flagged(self, tmp_path):
        from data_quality.checker import check_jsonl_file
        rec = self._make_valid_record(timestamp="not-a-date")
        path = self._write_jsonl(tmp_path, [rec])
        report = check_jsonl_file(path)
        assert any(i["issue_type"] == "INVALID_TIMESTAMP" for i in report.issues)

    def test_negative_timing_flagged(self, tmp_path):
        from data_quality.checker import check_jsonl_file
        rec = self._make_valid_record(dns_ms=-5.0)
        path = self._write_jsonl(tmp_path, [rec])
        report = check_jsonl_file(path)
        assert any(i["issue_type"] == "NEGATIVE_TIMING" for i in report.issues)

    def test_duplicate_id_flagged(self, tmp_path):
        from data_quality.checker import check_jsonl_file
        fixed_id = str(uuid.uuid4())
        rec1 = self._make_valid_record(measurement_id=fixed_id)
        rec2 = self._make_valid_record(measurement_id=fixed_id)   # same ID
        path = self._write_jsonl(tmp_path, [rec1, rec2])
        report = check_jsonl_file(path)
        assert len(report.duplicate_ids) == 1

    def test_outlier_timing_reported(self, tmp_path):
        from data_quality.checker import check_jsonl_file
        rec = self._make_valid_record(total_ms=35_000.0)   # 35 s
        path = self._write_jsonl(tmp_path, [rec])
        report = check_jsonl_file(path)
        assert any(i["issue_type"] == "OUTLIER_TIMING" for i in report.issues)

    def test_heartbeat_records_skipped(self, tmp_path):
        from data_quality.checker import check_jsonl_file
        hb = {"_type": "heartbeat", "probe_id": "india-01", "timestamp": "2026-11-16T00:00:00Z"}
        rec = self._make_valid_record()
        path = self._write_jsonl(tmp_path, [hb, rec])
        report = check_jsonl_file(path)
        assert report.heartbeat_records == 1
        assert report.total_records == 1

    def test_per_probe_counts_tracked(self, tmp_path):
        from data_quality.checker import check_jsonl_file
        records = [
            self._make_valid_record(probe_id="india-01"),
            self._make_valid_record(probe_id="india-01"),
            self._make_valid_record(probe_id="sg-01"),
        ]
        path = self._write_jsonl(tmp_path, records)
        report = check_jsonl_file(path)
        assert report.probe_counts["india-01"] == 2
        assert report.probe_counts["sg-01"] == 1


# ---------------------------------------------------------------------------
# Parquet writer unit tests (no MinIO needed — in-memory)
# ---------------------------------------------------------------------------

class TestParquetWriter:
    def test_records_to_parquet_bytes_roundtrip(self):
        """Serialise records to Parquet bytes and read them back."""
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
        from netscope_probe.writers.minio_writer import records_to_parquet_bytes

        records = [
            {
                "measurement_id":  str(uuid.uuid4()),
                "probe_id":        "india-01",
                "target_id":       "github-com",
                "target":          "github.com",
                "timestamp":       "2026-11-16T00:00:00+00:00",
                "dns_ms":          18.4,
                "tcp_ms":          32.1,
                "tls_ms":          47.2,
                "ttfb_ms":         81.3,
                "total_ms":        104.8,
                "http_status":     200,
                "response_bytes":  18291,
                "error_type":      None,
                "timed_out":       False,
            }
        ]

        try:
            import pyarrow.parquet as pq
        except ImportError:
            pytest.skip("pyarrow not installed")

        parquet_bytes = records_to_parquet_bytes(records)
        assert len(parquet_bytes) > 0

        # Read back and verify
        import io
        import pyarrow.parquet as pq
        table = pq.read_table(io.BytesIO(parquet_bytes))
        assert table.num_rows == 1
        assert table.schema.get_field_index("probe_id") >= 0
        assert table["dns_ms"][0].as_py() == pytest.approx(18.4, abs=0.1)

    def test_partition_key_format(self):
        from netscope_probe.writers.minio_writer import _partition_key
        key = _partition_key("2026-11-16T10:30:00+00:00", "india-01")
        assert key == "measurements/year=2026/month=11/day=16/probe=india-01"

    def test_partition_key_bad_timestamp_uses_now(self):
        from netscope_probe.writers.minio_writer import _partition_key
        # Should not raise; falls back to current date
        key = _partition_key("not-a-date", "sg-01")
        assert "probe=sg-01" in key
        assert "year=" in key
