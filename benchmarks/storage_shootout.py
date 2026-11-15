"""
Storage Format Shootout: JSON vs CSV vs Snappy Parquet — Week 9 / M11

Measures and compares on the exact same 50,000 Internet measurement records:
1. File size on disk (bytes) & Compression Ratio
2. Write speed / serialization latency (seconds & rows/sec)
3. Full scan read speed & deserialization latency (seconds & rows/sec)
4. Column-selective read speed (reading only 'total_ms' and 'target')
"""

from __future__ import annotations

import csv
import gzip
import io
import json
import logging
import os
import random
import time
import uuid
import sys

import pyarrow as pa
import pyarrow.parquet as pq

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("netscope.storage_benchmark")


def generate_sample_records(count: int = 50_000) -> list[dict]:
    probes = ["india-01", "sg-01", "eu-01", "us-01"]
    targets = [
        ("github-com", "github.com"),
        ("google-com", "google.com"),
        ("cloudflare-com", "cloudflare.com"),
        ("wikipedia-org", "wikipedia.org"),
        ("amazon-com", "amazon.com"),
        ("flipkart-com", "www.flipkart.com"),
        ("irctc-co-in", "www.irctc.co.in"),
        ("iitb-ac-in", "www.iitb.ac.in"),
    ]

    records = []
    for i in range(count):
        tid, host = random.choice(targets)
        records.append({
            "measurement_id": str(uuid.uuid4()),
            "probe_id": random.choice(probes),
            "target_id": tid,
            "target": host,
            "timestamp": "2026-11-16T12:00:00.000Z",
            "dns_ms": round(random.uniform(5.0, 35.0), 2),
            "tcp_ms": round(random.uniform(15.0, 120.0), 2),
            "tls_ms": round(random.uniform(20.0, 90.0), 2),
            "ttfb_ms": round(random.uniform(40.0, 200.0), 2),
            "total_ms": round(random.uniform(80.0, 450.0), 2),
            "http_status": 200 if random.random() > 0.02 else 500,
            "response_bytes": random.randint(500, 50000),
            "error_type": None,
            "timed_out": False,
        })
    return records


def run_storage_benchmark(records: list[dict], output_dir: str = "benchmarks/storage_results") -> dict:
    os.makedirs(output_dir, exist_ok=True)
    count = len(records)
    logger.info("Starting Storage Shootout on %d records...", count)

    results = {}

    # -----------------------------------------------------------------------
    # 1. JSON (JSONL uncompressed)
    # -----------------------------------------------------------------------
    json_path = os.path.join(output_dir, "measurements.jsonl")
    t0 = time.perf_counter()
    with open(json_path, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")
    json_write_sec = time.perf_counter() - t0
    json_size_bytes = os.path.getsize(json_path)

    t0 = time.perf_counter()
    json_read_count = 0
    with open(json_path, "r", encoding="utf-8") as f:
        for line in f:
            rec = json.loads(line)
            json_read_count += 1
    json_read_sec = time.perf_counter() - t0

    results["JSONL"] = {
        "size_mb": round(json_size_bytes / (1024 * 1024), 2),
        "write_sec": round(json_write_sec, 3),
        "write_rate": int(count / json_write_sec),
        "read_sec": round(json_read_sec, 3),
        "read_rate": int(count / json_read_sec),
    }

    # -----------------------------------------------------------------------
    # 2. CSV
    # -----------------------------------------------------------------------
    csv_path = os.path.join(output_dir, "measurements.csv")
    fieldnames = list(records[0].keys())
    t0 = time.perf_counter()
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)
    csv_write_sec = time.perf_counter() - t0
    csv_size_bytes = os.path.getsize(csv_path)

    t0 = time.perf_counter()
    csv_read_count = 0
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            csv_read_count += 1
    csv_read_sec = time.perf_counter() - t0

    results["CSV"] = {
        "size_mb": round(csv_size_bytes / (1024 * 1024), 2),
        "write_sec": round(csv_write_sec, 3),
        "write_rate": int(count / csv_write_sec),
        "read_sec": round(csv_read_sec, 3),
        "read_rate": int(count / csv_read_sec),
    }

    # -----------------------------------------------------------------------
    # 3. Apache Parquet (Snappy Compressed)
    # -----------------------------------------------------------------------
    parquet_path = os.path.join(output_dir, "measurements.parquet")
    columns = {k: [r[k] for r in records] for k in fieldnames}

    schema = pa.schema([
        ("measurement_id", pa.string()),
        ("probe_id", pa.string()),
        ("target_id", pa.string()),
        ("target", pa.string()),
        ("timestamp", pa.string()),
        ("dns_ms", pa.float32()),
        ("tcp_ms", pa.float32()),
        ("tls_ms", pa.float32()),
        ("ttfb_ms", pa.float32()),
        ("total_ms", pa.float32()),
        ("http_status", pa.int32()),
        ("response_bytes", pa.int64()),
        ("error_type", pa.string()),
        ("timed_out", pa.bool_()),
    ])

    t0 = time.perf_counter()
    table = pa.Table.from_pydict(columns, schema=schema)
    pq.write_table(table, parquet_path, compression="snappy")
    parquet_write_sec = time.perf_counter() - t0
    parquet_size_bytes = os.path.getsize(parquet_path)

    # Full read
    t0 = time.perf_counter()
    pq_table = pq.read_table(parquet_path)
    parquet_read_sec = time.perf_counter() - t0

    # Column-selective read (only target & total_ms)
    t0 = time.perf_counter()
    pq_cols = pq.read_table(parquet_path, columns=["target", "total_ms"])
    parquet_col_sec = time.perf_counter() - t0

    results["Parquet (Snappy)"] = {
        "size_mb": round(parquet_size_bytes / (1024 * 1024), 2),
        "write_sec": round(parquet_write_sec, 3),
        "write_rate": int(count / parquet_write_sec),
        "read_sec": round(parquet_read_sec, 3),
        "read_rate": int(count / parquet_read_sec),
        "col_selective_read_sec": round(parquet_col_sec, 4),
    }

    return results


def main() -> None:
    records = generate_sample_records(50_000)
    res = run_storage_benchmark(records)

    print("\n" + "=" * 80)
    print("NETSCOPE STORAGE BENCHMARK SHOOTOUT (50,000 Records)")
    print("=" * 80)
    print(f"{'Format':<18} | {'Disk Size':<10} | {'Write Rate':<15} | {'Read Rate':<15} | {'Read Time':<10}")
    print("-" * 80)
    for fmt, d in res.items():
        print(f"{fmt:<18} | {d['size_mb']:>6.2f} MB  | {d['write_rate']:>9} rows/s  | {d['read_rate']:>9} rows/s  | {d['read_sec']:>7.3f} s")
    print("=" * 80)
    p_size = res["Parquet (Snappy)"]["size_mb"]
    j_size = res["JSONL"]["size_mb"]
    ratio = round(j_size / p_size, 1)
    col_time = res["Parquet (Snappy)"]["col_selective_read_sec"]
    print(f"📊 Parquet is {ratio}x smaller than JSONL with Snappy compression.")
    print(f"⚡ Parquet column-selective read (2 columns) finished in: {col_time * 1000:.2f} ms")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    main()
