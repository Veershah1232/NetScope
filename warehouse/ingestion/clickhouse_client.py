"""
ClickHouse Bulk Ingestion Engine — M6

Provides high-speed batch loading into ClickHouse:
1. Bulk insert raw measurements
2. Bulk insert window aggregates (from Flink)
3. Load historical measurements from JSONL or Parquet files

Uses clickhouse-connect for vectorized HTTP batch streaming.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from typing import Any, Iterable

import clickhouse_connect

logger = logging.getLogger("netscope.clickhouse_ingest")


class ClickHouseClient:
    def __init__(
        self,
        host: str = "localhost",
        port: int = 8123,
        username: str = "default",
        password: str = "",
        database: str = "netscope",
    ):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.client = clickhouse_connect.get_client(
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            database=self.database,
        )

    def insert_measurements(self, records: list[dict[str, Any]]) -> int:
        """Bulk insert rows into netscope.measurements."""
        if not records:
            return 0

        columns = [
            "timestamp",
            "measurement_id",
            "probe_id",
            "target_id",
            "target",
            "dns_ms",
            "tcp_ms",
            "tls_ms",
            "ttfb_ms",
            "total_ms",
            "http_status",
            "response_bytes",
            "error_type",
            "timed_out",
        ]

        data = []
        for r in records:
            # Parse timestamp to unix timestamp float or datetime
            ts = r.get("timestamp", "").replace("Z", "+00:00")
            data.append([
                ts,
                r.get("measurement_id"),
                r.get("probe_id", ""),
                r.get("target_id", ""),
                r.get("target", ""),
                r.get("dns_ms"),
                r.get("tcp_ms"),
                r.get("tls_ms"),
                r.get("ttfb_ms"),
                r.get("total_ms"),
                r.get("http_status"),
                r.get("response_bytes"),
                r.get("error_type"),
                1 if r.get("timed_out") else 0,
            ])

        self.client.insert("measurements", data, column_names=columns)
        return len(data)

    def insert_window_aggregates(self, windows: list[dict[str, Any]]) -> int:
        """Bulk insert aggregated windows into netscope.latency_windows."""
        if not windows:
            return 0

        columns = [
            "window_type",
            "window_start",
            "window_end",
            "probe_id",
            "target_id",
            "target",
            "count",
            "avg_latency_ms",
            "p50_ms",
            "p95_ms",
            "p99_ms",
            "failure_count",
            "availability_pct",
        ]

        data = []
        for w in windows:
            data.append([
                w.get("window_type", "tumbling_1m"),
                w.get("window_start", "").replace("Z", "+00:00"),
                w.get("window_end", "").replace("Z", "+00:00"),
                w.get("probe_id", ""),
                w.get("target_id", ""),
                w.get("target", ""),
                w.get("count", 0),
                w.get("avg_latency_ms", 0.0),
                w.get("p50_ms", 0.0),
                w.get("p95_ms", 0.0),
                w.get("p99_ms", 0.0),
                w.get("failure_count", 0),
                w.get("availability_pct", 100.0),
            ])

        self.client.insert("latency_windows", data, column_names=columns)
        return len(data)

    def query(self, query_sql: str) -> list[tuple]:
        result = self.client.query(query_sql)
        return result.result_rows

    def query_df(self, query_sql: str):
        return self.client.query_df(query_sql)
