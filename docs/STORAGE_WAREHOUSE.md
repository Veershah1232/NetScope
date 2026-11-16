# Dual-Tier Storage & Analytical Warehouse

NetScope implements a dual-tier **Lakehouse architecture** combining an immutable, cost-effective raw S3 data lake with an ultra-fast columnar analytical warehouse:

```
                            RAW TELEMETRY EVENTS
                                     │
           ┌─────────────────────────┴─────────────────────────┐
           ▼                                                   ▼
   TIER 1: RAW OBJECT LAKE                             TIER 2: ANALYTICAL OLAP
   MinIO S3 Object Store                               ClickHouse Columnar Warehouse
   ├─ Format: Snappy Apache Parquet                    ├─ Engine: MergeTree & SummingMergeTree
   ├─ Partitioning: Hive (year/month/day/hour/probe)   ├─ Codecs: DoubleDelta, ZSTD, LowCardinality
   ├─ Purpose: Audit, Replay, Forensic Retrospective   ├─ Purpose: Sub-second p95/p99 & Drill-down
   └─ Metric: 6.3x smaller than JSONL                  └─ Metric: 27,277 rows/sec ingestion, 50ms queries
```

---

## 1. Tier 1: Raw Storage Lake (MinIO S3)

Implemented in `probe/netscope_probe/writers/minio_writer.py`.

### 1.1 Hive-Partitioned Directory Structure
Data files are uploaded to bucket `netscope-raw` under standard Hive partitioning syntax:
```
s3://netscope-raw/measurements/
  └── year=2026/
      └── month=11/
          └── day=16/
              └── hour=06/
                  └── probe_id=probe-01/
                      ├── part-0000.parquet
                      └── part-0001.parquet
```

#### Why Hive Partitioning?
- **Partition Pruning**: Query engines (Trino, DuckDB, Spark, Athena) skip scanning 99%+ of the data lake by inspecting partition directory keys during time-range or probe-specific queries.
- **Predictable File Sizing**: Micro-batches of 1,000 records prevent small-file fragmentation while ensuring snappy writes.

### 1.2 Storage Format Shootout Benchmark (50,000 Records)
Measured by `benchmarks/storage_shootout.py` on identical 50,000 telemetry measurements:

| Storage Format | Total File Size | Compression Ratio | Read Throughput | Selective Column Scan Time |
| :--- | :---: | :---: | :---: | :---: |
| **Parquet (Snappy)** | **2.67 MB** | **6.3x smaller (84.1% savings)** | **1,221,729 rows/sec** | **4.90 ms** |
| **CSV** | 6.85 MB | 2.4x smaller | 284,091 rows/sec | 176.32 ms |
| **JSONL (Newline JSON)** | 16.77 MB | 1.0x baseline | 84,746 rows/sec | 589.65 ms |

**Key Takeaways**:
- **Parquet achieved an 84.1% storage reduction** over raw JSONL.
- **Selective column reading was 120x faster** in Parquet (4.90 ms vs 589.65 ms) because columnar readers only seek and decode the requested columns (`total_latency_ms`), completely bypassing untouched strings.

---

## 2. Tier 2: Analytical Data Warehouse (ClickHouse)

ClickHouse serves as the low-latency analytical OLAP engine, defined in `warehouse/schemas/init.sql` and operated via `warehouse/ingestion/clickhouse_client.py`.

### 2.1 Table Schema & Specialized Column Codecs

```sql
CREATE TABLE IF NOT EXISTS netscope.measurements (
    measurement_id       UUID,
    probe_id             LowCardinality(String),
    target               LowCardinality(String),
    target_category      LowCardinality(String),
    timestamp            DateTime64(3) CODEC(DoubleDelta, LZ4),
    resolved_ip          String CODEC(ZSTD),
    dns_lookup_time_ms   Float32 CODEC(Gorilla, LZ4),
    tcp_connect_time_ms  Float32 CODEC(Gorilla, LZ4),
    tls_handshake_time_ms Float32 CODEC(Gorilla, LZ4),
    http_ttfb_ms         Float32 CODEC(Gorilla, LZ4),
    total_latency_ms     Float32 CODEC(Gorilla, LZ4),
    http_status_code     UInt16 CODEC(T64, LZ4),
    response_size_bytes  UInt32 CODEC(T64, LZ4),
    is_success           UInt8 CODEC(T64, LZ4),
    error_type           LowCardinality(String),
    error_message        String CODEC(ZSTD)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
PRIMARY KEY (target, probe_id, timestamp)
ORDER BY (target, probe_id, timestamp);
```

#### Codec Optimization Explanations:
1. **`LowCardinality(String)`**: Employs integer dictionary encoding for repetitive fields (`probe_id`, `target`, `error_type`). Replaces 20-byte strings with 1-byte integer tokens in memory and on disk.
2. **`DoubleDelta, LZ4`**: Applied to `timestamp`. Stores the delta of consecutive deltas ($\Delta(\Delta t)$). Since measurements arrive at regular intervals, the second derivative is near zero, compressing timestamps to bits.
3. **`Gorilla, LZ4`**: Applied to floating-point latency values (`Float32`). Uses XOR floating-point compression pioneered by Facebook Gorilla.
4. **`ZSTD`**: High-ratio dictionary-based compression for arbitrary text strings (`resolved_ip`, `error_message`).

### 2.2 Materialized View for Real-Time Aggregations

To avoid re-scanning millions of historical rows for daily trend dashboards, NetScope deploys a `SummingMergeTree` Materialized View:

```sql
CREATE MATERIALIZED VIEW IF NOT EXISTS netscope.target_daily_stats
ENGINE = SummingMergeTree()
PRIMARY KEY (target, date)
ORDER BY (target, date) AS
SELECT
    target,
    toDate(timestamp) AS date,
    count() AS total_measurements,
    sum(is_success) AS successful_measurements,
    sum(total_latency_ms) AS sum_latency_ms
FROM netscope.measurements
GROUP BY target, date;
```

When new rows are inserted into `measurements`, ClickHouse incrementally sums values directly into `target_daily_stats` with zero background query overhead.

### 2.3 ClickHouse 100,000 Row Benchmark Results
Measured by `warehouse/queries/benchmark_queries.py`:

| Operation | Dataset Size | Measured Latency / Throughput |
| :--- | :---: | :---: |
| **Vectorized Bulk Insert** | 100,000 rows | **3.67 seconds (27,277 rows/sec)** |
| **Slowest Targets by p95 Latency** | 100,000 rows | **71.03 ms** |
| **Protocol Stage Breakdown (DNS/TCP/TLS/TTFB)** | 100,000 rows | **67.09 ms** |
| **Regional Availability Aggregate** | 100,000 rows | **60.68 ms** |
| **Materialized View Pre-Aggregation (`target_daily_stats`)** | 100,000 rows | **50.27 ms** |
