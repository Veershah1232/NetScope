"""
MinIO / Parquet writer for NetScope — M4

Reads measurements from a JSONL file (or stdin) and writes them to MinIO
as Parquet files, partitioned by:

    measurements/year=YYYY/month=MM/day=DD/probe=<probe_id>/part-NNN.parquet

Requirements:
    pip install boto3 pyarrow

Environment variables (or .env file):
    MINIO_ENDPOINT   e.g. http://localhost:9000
    MINIO_ACCESS_KEY
    MINIO_SECRET_KEY
    MINIO_BUCKET     default: netscope-raw
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger("netscope.minio_writer")

BUCKET_DEFAULT = "netscope-raw"


# ---------------------------------------------------------------------------
# Schema: Parquet column types mirror the Measurement dataclass exactly
# ---------------------------------------------------------------------------

def _get_schema():
    """Return a PyArrow schema for the measurements table."""
    import pyarrow as pa
    return pa.schema([
        pa.field("measurement_id",  pa.string(),  nullable=False),
        pa.field("probe_id",        pa.string(),  nullable=False),
        pa.field("target_id",       pa.string(),  nullable=False),
        pa.field("target",          pa.string(),  nullable=False),
        pa.field("timestamp",       pa.string(),  nullable=False),   # ISO-8601 string
        pa.field("dns_ms",          pa.float32(), nullable=True),
        pa.field("tcp_ms",          pa.float32(), nullable=True),
        pa.field("tls_ms",          pa.float32(), nullable=True),
        pa.field("ttfb_ms",         pa.float32(), nullable=True),
        pa.field("total_ms",        pa.float32(), nullable=True),
        pa.field("http_status",     pa.int32(),   nullable=True),
        pa.field("response_bytes",  pa.int64(),   nullable=True),
        pa.field("error_type",      pa.string(),  nullable=True),
        pa.field("timed_out",       pa.bool_(),   nullable=False),
    ])


# ---------------------------------------------------------------------------
# MinIO client
# ---------------------------------------------------------------------------

def _make_minio_client(endpoint: str, access_key: str, secret_key: str):
    """Return a boto3 S3 client pointed at MinIO."""
    import boto3
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",   # MinIO doesn't care but boto3 needs something
    )


def ensure_bucket(client, bucket: str) -> None:
    """Create bucket if it doesn't exist."""
    try:
        client.head_bucket(Bucket=bucket)
    except Exception:
        client.create_bucket(Bucket=bucket)
        logger.info("Created bucket: %s", bucket)


# ---------------------------------------------------------------------------
# Partition key helpers
# ---------------------------------------------------------------------------

def _partition_key(ts_str: str, probe_id: str) -> str:
    """
    Build a Hive-style partition path from a timestamp and probe_id.

    Example:
        "2026-11-16T00:00:00Z", "india-01"
        → "measurements/year=2026/month=11/day=16/probe=india-01"
    """
    try:
        dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
    except ValueError:
        dt = datetime.now(timezone.utc)
        logger.warning("Could not parse timestamp '%s', using now()", ts_str)
    return (
        f"measurements/"
        f"year={dt.year:04d}/"
        f"month={dt.month:02d}/"
        f"day={dt.day:02d}/"
        f"probe={probe_id}"
    )


# ---------------------------------------------------------------------------
# Core write logic
# ---------------------------------------------------------------------------

def records_to_parquet_bytes(records: list[dict]) -> bytes:
    """Serialize a list of measurement dicts to Parquet bytes in memory."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    schema = _get_schema()
    columns: dict[str, list] = {field.name: [] for field in schema}

    for rec in records:
        for field in schema:
            val = rec.get(field.name)
            # Coerce types for safety
            if field.type == pa.bool_() and val is None:
                val = False
            columns[field.name].append(val)

    table = pa.table(columns, schema=schema)

    buf = io.BytesIO()
    pq.write_table(
        table,
        buf,
        compression="snappy",
        write_statistics=True,
    )
    return buf.getvalue()


def write_partition_to_minio(
    client,
    bucket: str,
    partition_path: str,
    part_index: int,
    records: list[dict],
) -> str:
    """Write one Parquet file for a partition. Returns the S3 key."""
    key = f"{partition_path}/part-{part_index:04d}.parquet"
    parquet_bytes = records_to_parquet_bytes(records)

    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=parquet_bytes,
        ContentType="application/octet-stream",
    )
    logger.info("Wrote %d records → s3://%s/%s (%d bytes)",
                len(records), bucket, key, len(parquet_bytes))
    return key


def flush_jsonl_to_minio(
    jsonl_path: str,
    endpoint: str,
    access_key: str,
    secret_key: str,
    bucket: str = BUCKET_DEFAULT,
    batch_size: int = 1000,
) -> int:
    """
    Read a JSONL file of measurements, group by partition, write to MinIO.
    Returns total records written.

    Partitioning strategy:
        Group records by (year, month, day, probe_id).
        Write each group as one Parquet file.
        Records are sorted by timestamp within each partition.
    """
    client = _make_minio_client(endpoint, access_key, secret_key)
    ensure_bucket(client, bucket)

    # Group records by partition key
    partitions: dict[str, list[dict]] = defaultdict(list)

    with open(jsonl_path, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError as exc:
                logger.warning("Skipping invalid JSON line: %s", exc)
                continue

            # Skip heartbeat records
            if rec.get("_type") == "heartbeat":
                continue

            probe_id = rec.get("probe_id", "unknown")
            ts       = rec.get("timestamp", "")
            key      = _partition_key(ts, probe_id)
            partitions[key].append(rec)

    total = 0
    for partition_path, records in partitions.items():
        # Sort by timestamp within partition
        records.sort(key=lambda r: r.get("timestamp", ""))
        # Write in batches (in case a partition has many records)
        for batch_idx, start in enumerate(range(0, len(records), batch_size)):
            batch = records[start:start + batch_size]
            write_partition_to_minio(client, bucket, partition_path, batch_idx, batch)
            total += len(batch)

    return total


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="NetScope → MinIO Parquet writer")
    parser.add_argument("--input",    required=True, help="Path to JSONL measurements file")
    parser.add_argument("--endpoint", default=os.environ.get("MINIO_ENDPOINT", "http://localhost:9000"))
    parser.add_argument("--access-key", default=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"))
    parser.add_argument("--secret-key", default=os.environ.get("MINIO_SECRET_KEY", "minioadmin"))
    parser.add_argument("--bucket",   default=os.environ.get("MINIO_BUCKET", BUCKET_DEFAULT))
    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    logger.info("Writing %s → s3://%s @ %s", args.input, args.bucket, args.endpoint)
    total = flush_jsonl_to_minio(
        jsonl_path=args.input,
        endpoint=args.endpoint,
        access_key=args.access_key,
        secret_key=args.secret_key,
        bucket=args.bucket,
    )
    logger.info("Done. Total records written: %d", total)


if __name__ == "__main__":
    main()
