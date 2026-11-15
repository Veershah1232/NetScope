# NETSCOPE — Distributed Internet Measurement & Analytics Platform

> **Status:** Weeks 1 – 5 Complete & Verified ✅ | **Active Phase:** Week 6  
> **Team:** 3 Members · **Duration:** 9 Weeks · **Stack:** Python · Kafka · Flink · MinIO · ClickHouse · Prometheus · Grafana

---

## 1. What is NetScope?

**NetScope** is a distributed Internet observatory and telemetry platform. It continuously measures protocol-level performance (DNS resolution, TCP connection, TLS handshake, and HTTP TTFB) from geographically distributed probes, streaming them through an enterprise-grade data engineering pipeline.

The platform provides:
- **Decoupled high-throughput ingestion** via Apache Kafka (KRaft mode).
- **Real-time stream processing** with event-time semantics, bounded out-of-orderness watermarks, and windowed statistical aggregations (p50/p95/p99) matching Apache Flink.
- **S3-compatible raw data lake** (MinIO) using Hive-partitioned Snappy Parquet files.
- **Analytical data warehouse** (ClickHouse) with columnar compression (`DoubleDelta`, `ZSTD`), `LowCardinality` dictionary encoding, and real-time Materialized Views.
- **Explainable statistical anomaly detection** and stateful incident lifecycle tracking.
- **End-to-end platform observability** with self-monitoring.

---

## 2. End-to-End Architecture

```
                         DISTRIBUTED INTERNET
                                   │
              ┌────────────────────┼────────────────────┐
              ▼                    ▼                    ▼
           Probe IN             Probe SG             Probe EU
          (india-01)            (sg-01)              (eu-01)
              │                    │                    │
              └────────────────────┼────────────────────┘
                                   │
                                   ▼
                         APACHE KAFKA (KRaft)
               ├─ measurements   (6 Partitions | Key: probe_id)
               ├─ probe_health   (3 Partitions | Key: probe_id)
               └─ alerts         (3 Partitions | Key: target_id)
                                   │
                   ┌───────────────┴───────────────┐
                   ▼                               ▼
       STREAM-TO-LAKE CONSUMER           APACHE FLINK ENGINE
       (Offset commit on write)          ├─ Event-time & watermarks
                   │                     ├─ Stream deduplication
                   ▼                     ├─ 1-min tumbling windows
          MINIO DATA LAKE                ├─ 5-min sliding windows
         (Snappy Parquet)                └─ Latency (p50/p95/p99)
     s3://netscope-raw/measurements/               │
                                                   ▼
                                        CLICKHOUSE OLAP WAREHOUSE
                                        ├─ measurements (raw)
                                        ├─ latency_windows (aggregates)
                                        ├─ target_daily_stats (MV)
                                        └─ incidents (lifecycle)
```

---

## 3. 9-Week Master Plan & Current Status

| Week | Phase / Milestone | Status | Key Verifications & Artifacts |
| :---: | :--- | :---: | :--- |
| **W1** | **Architecture, Probe MVP, Data Model** | ✅ **DONE** | Canonical Schema, Probe Engine (M1), Target Config, 14/14 Tests |
| **W2** | **Distributed Probes + Raw Storage Lake** | ✅ **DONE** | MinIO in Docker, Hive Parquet Writer (M4), Data Quality Engine |
| **W3** | **Kafka Ingestion Layer (M3)** | ✅ **DONE** | KRaft Broker, 3 Topics, Producer, TEST-005 Lag Recovery Experiment |
| **W4** | **Flink Real-Time Stream Engine (M5)** | ✅ **DONE** | Event-Time, Watermarks, Deduplication, Tumbling/Sliding Windows |
| **W5** | **ClickHouse Analytical Warehouse (M6)**| ✅ **DONE** | MergeTree DDL, Materialized Views, 100K Benchmark (27.2k rows/s) |
| **W6** | **Anomaly & Incident Engine (M7)** | 🟡 **NEXT** | Z-score, rolling baselines, controlled outage demo |
| **W7** | **Geographic & Traceroute Analysis (M8)**| ⚪ *Queue* | Traceroute hop latency, regional degradation classifier |
| **W8** | **Observability & Chaos Experiments (M9/10)**| ⚪ *Queue* | Prometheus, 5 Grafana dashboards, fault injection tests |
| **W9** | **Benchmarking & Final Polish (M11)** | ⚪ *Queue* | 1k–20k events/sec stress tests, Parquet vs JSON/CSV comparison |

---

## 4. Live Benchmark & Verification Results (Measured)

NetScope enforces an engineering rule: **Measure everything; report actual observed values.**

| Pipeline Stage | Metric | Measured Result |
| :--- | :--- | :--- |
| **Probe Telemetry** | Timing breakdown (Cloudflare) | DNS: 179ms · TCP: 20ms · TLS: 44ms · TTFB: 84ms |
| **Kafka Ingestion** | Topic Partitions | `measurements`: 6 · `probe_health`: 3 · `alerts`: 3 |
| **Kafka Fault Tolerance** | Consumer kill & recovery | Lag accumulated to 25+ during kill → Drained to 0 on restart with zero data loss |
| **Flink Window Latency** | 1-min Tumbling Window | Calculated `p50`, `p95`, `p99`, `availability` over streaming events |
| **Flink Watermarks** | Late-event handling | Events trailing behind watermark past allowed lateness dropped to side-output |
| **ClickHouse Ingestion** | Vectorized Bulk Insert | **100,000 rows in 3.67 seconds (27,277 rows/sec)** |
| **ClickHouse Queries** | Slowest Targets by p95 (100k rows) | **71.03 ms** |
| **ClickHouse Queries** | Protocol Stage Breakdown (100k rows)| **67.09 ms** |
| **ClickHouse Queries** | Regional Availability (100k rows) | **60.68 ms** |
| **ClickHouse Queries** | Materialized View Pre-aggregation | **50.27 ms** |
| **Regression Tests** | Automated unit test suite | **40 / 40 tests passing** in ~4 seconds |

---

## 5. Repository Structure

```
DE Project/
├── probe/
│   ├── netscope_probe/
│   │   ├── probe.py               # Core protocol measurement engine (DNS/TCP/TLS/HTTP)
│   │   └── writers/
│   │       └── minio_writer.py    # Hive-partitioned Snappy Parquet writer
│   ├── tests/
│   │   ├── test_probe.py          # Week 1 tests (TEST-001, TEST-002)
│   │   ├── test_week2.py          # Week 2 tests (Data Quality, Parquet roundtrip)
│   │   ├── test_week3.py          # Week 3 tests (Kafka producer, topic routing)
│   │   ├── test_week4.py          # Week 4 tests (Flink windows, watermarks, late events)
│   │   └── test_week5.py          # Week 5 tests (ClickHouse DDL, queries, inserts)
│   └── Dockerfile                 # Probe container image
│
├── schemas/
│   └── measurement.py             # Canonical dataclasses: Measurement & ProbeHeartbeat
│
├── targets/
│   └── targets.yaml               # Target configurations, timeouts, and payload caps
│
├── ingestion/
│   ├── producers/
│   │   └── kafka_producer.py      # Resilient Kafka producer
│   └── consumers/
│       └── kafka_to_minio.py      # Stream-to-lake archival consumer (at-least-once)
│
├── streaming/
│   └── flink_jobs/
│       ├── stream_processor.py    # Core Flink stream processor (windows, watermarks)
│       └── run_pipeline.py        # Live Kafka-to-Flink runner
│
├── warehouse/
│   ├── schemas/
│   │   └── init.sql               # ClickHouse DDL (MergeTree, codecs, Materialized Views)
│   ├── ingestion/
│   │   └── clickhouse_client.py   # High-speed vectorized ClickHouse client
│   └── queries/
│       └── benchmark_queries.py   # 100K benchmark and analytical query suite
│
├── data_quality/
│   └── checker.py                 # Automated dataset hygiene auditor
│
├── scripts/
│   ├── sweep_and_stats.py         # Multi-round target sweep & percentile reporter
│   ├── test_005_kafka_recovery.py # TEST-005 Kafka lag & recovery experiment
│   └── demo_week4_flink_streaming.py # Week 4 live streaming observable demo
│
├── docker-compose.yml             # MinIO, Kafka (KRaft), ClickHouse, & Probe stack
└── README.md
```

---

## 6. How to Run & Verify the Stack

### Prerequisites
- Python 3.11+
- Docker Desktop (WSL2 backend on Windows)

### 1. Start the Core Infrastructure
```powershell
docker compose up -d minio minio-init kafka kafka-init clickhouse
```

Web interfaces available:
- **MinIO S3 Console:** [http://localhost:9001](http://localhost:9001) (`minioadmin` / `minioadmin`)
- **ClickHouse HTTP:** [http://localhost:8123/ping](http://localhost:8123/ping)
- **Kafka Broker:** `localhost:9092`

### 2. Run the Full Test Suite (40 Tests)
```powershell
cd probe
python -m pytest tests/ -v -m "not integration"
```

### 3. Run the Kafka Lag & Fault-Tolerance Demo (TEST-005)
```powershell
python scripts/test_005_kafka_recovery.py
```

### 4. Run the Real-Time Flink Streaming Demo (TEST-006 & TEST-007)
```powershell
python scripts/demo_week4_flink_streaming.py
```

### 5. Run the ClickHouse 100K Benchmark & Historical Queries
```powershell
python warehouse/queries/benchmark_queries.py
```

---

## 7. Team Responsibilities

- **MEMBER_1 (Probe & Networking):** Network probing engine, protocol latency breakdown, multi-location deployment, traceroute engine.
- **MEMBER_2 (Data Platform):** Kafka ingestion, Flink real-time streaming, MinIO data lake, ClickHouse OLAP schema & query optimization.
- **MEMBER_3 (Analytics & Observability):** Data quality checker, statistical anomaly detection, incident state machine, Prometheus & Grafana dashboards.
