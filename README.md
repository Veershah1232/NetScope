# NETSCOPE — Distributed Internet Measurement & Analytics Platform

> **Status:** Weeks 1 – 9 Complete & Fully Verified ✅ | **Production-Ready Capstone**  
> **Team:** 3 Members · **Duration:** 9 Weeks · **Stack:** Python · Kafka (KRaft) · Flink · MinIO (S3) · ClickHouse · Prometheus · Grafana

---

## 1. What is NetScope?

**NetScope** is a distributed Internet observatory and telemetry platform. It continuously measures protocol-level performance (DNS resolution, TCP connection, TLS handshake, and HTTP TTFB) from geographically distributed probes, streaming telemetry events through an enterprise-grade data engineering pipeline.

The platform provides:
- **Decoupled high-throughput ingestion** via Apache Kafka (KRaft mode, 6 partitions, up to 9,250 events/sec sustained).
- **Real-time stream processing** with event-time semantics, bounded out-of-orderness watermarks, stream deduplication, and windowed statistical aggregations (p50/p95/p99) matching Apache Flink.
- **S3-compatible raw data lake** (MinIO) using Hive-partitioned Snappy Parquet files with 6.3x storage reduction over JSONL.
- **Analytical data warehouse** (ClickHouse) with columnar compression (`DoubleDelta`, `ZSTD`), `LowCardinality` dictionary encoding, and real-time Materialized Views achieving 27,277 rows/sec ingestion and 50–71 ms queries.
- **Statistical anomaly detection & incident lifecycle engine** using rolling baselines, dynamic z-scores, multi-signal composite scoring, and a 4-state deterministic finite state machine (`NORMAL` → `ACTIVE` → `RECOVERING` → `RESOLVED`).
- **Geographic correlation & traceroute analysis** distinguishing `LIKELY REGIONAL DEGRADATION` from `LIKELY GLOBAL OUTAGE` with MD5 path signature change tracking.
- **Full observability & fault tolerance** with a Prometheus exporter, provisioned Grafana dashboards, and verified recovery across 5 distinct failure scenarios.

---

## 2. End-to-End Architecture

```
                         DISTRIBUTED INTERNET TARGETS
                                       │
                  ┌────────────────────┼────────────────────┐
                  ▼                    ▼                    ▼
               Probe IN             Probe SG             Probe EU
              (india-01)            (sg-01)              (eu-01)
                  │                    │                    │
                  ├────────────────────┼────────────────────┤
                  │     Traceroute & Route Signature Hash   │
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
           STREAM-TO-LAKE CONSUMER           APACHE FLINK STREAM ENGINE
           (Manual commit on write)          ├─ Event-time extraction
                       │                     ├─ Bounded out-of-orderness watermarks
                       ▼                     ├─ Exact-once deduplication (UUID cache)
              MINIO DATA LAKE                ├─ 1-min tumbling / 5-min sliding windows
             (Snappy Parquet)                ├─ Latency percentiles (p50/p95/p99)
         s3://netscope-raw/measurements/     └─ Late-event DLQ side-outputs
                       │                               │
                       └───────────────┬───────────────┘
                                       ▼
                           CLICKHOUSE OLAP WAREHOUSE
                           ├─ measurements (raw columnar with DoubleDelta/ZSTD)
                           ├─ latency_windows (streaming aggregated windows)
                           ├─ target_daily_stats (SummingMergeTree MV)
                           └─ incidents (stateful incident lifecycle audit)
                                       │
                       ┌───────────────┴───────────────┐
                       ▼                               ▼
           STATISTICAL ANOMALY ENGINE         OBSERVABILITY SUITE
           ├─ Rolling baseline & Z-Score      ├─ Prometheus Exporter (:9090)
           ├─ Multi-signal composite scoring  ├─ Grafana Dashboards (:3000)
           ├─ Incident FSM lifecycle          └─ Chaos & Fault-Tolerance Suite
           └─ Geo Regional Correlation
```

---

## 3. 9-Week Master Plan & Deliverables

| Week | Phase / Milestone | Status | Key Deliverables & Verified Artifacts |
| :---: | :--- | :---: | :--- |
| **W1** | **Architecture, Probe MVP, Data Model** | ✅ **DONE** | Canonical Dataclasses (`Measurement`, `ProbeHeartbeat`), Probe Engine (`probe.py`), Targets Config (`targets.yaml`), 14 unit tests |
| **W2** | **Distributed Probes + Raw Storage Lake** | ✅ **DONE** | Standalone MinIO in Docker, Hive Parquet Writer (`minio_writer.py`), Automated Data Quality Checker (`checker.py`), 30 unit tests |
| **W3** | **Kafka Ingestion Layer** | ✅ **DONE** | KRaft Broker, 3 partitioned topics, Kafka Producer & Consumer (`kafka_to_minio.py`), TEST-005 Lag Recovery Experiment |
| **W4** | **Flink Real-Time Stream Engine** | ✅ **DONE** | Event-Time, Watermarks, Deduplication, Tumbling/Sliding Windows, Latency percentiles, Late-event DLQ side-output, Live streaming demo |
| **W5** | **ClickHouse Analytical Warehouse** | ✅ **DONE** | MergeTree DDL with `DoubleDelta`/`ZSTD` codecs, `SummingMergeTree` Materialized View, 100K benchmark (27,277 rows/s, 50–71 ms queries) |
| **W6** | **Anomaly & Incident Engine** | ✅ **DONE** | Controlled Chaos Server (`controlled_endpoint/`), Rolling Z-Score detector, 4-state Incident FSM, Outage simulation demo |
| **W7** | **Geographic & Traceroute Analysis** | ✅ **DONE** | Cross-probe Regional vs Global correlation, Traceroute engine with hop RTT and MD5 route signature hashing |
| **W8** | **Observability & Fault Tolerance** | ✅ **DONE** | Prometheus exporter, Grafana dashboards (`dashboard_overview.json`), 5-scenario fault tolerance suite (FT-1 to FT-5) |
| **W9** | **Benchmarking, Shootout & Final Demo** | ✅ **DONE** | Storage Shootout (Parquet vs CSV vs JSONL), Kafka stress test (9,250 events/s), Unified 18-step presentation orchestrator |

---

## 4. Measured Benchmark & Verification Results

NetScope strictly enforces data engineering rigor: **No synthetic or unmeasured numbers.** All metrics below were directly measured on the running system:

### Storage Shootout (50,000 Historical Measurements)
| Storage Format | File Size | Read Throughput | Selective Column Read Time | Compression vs JSONL |
| :--- | :---: | :---: | :---: | :---: |
| **Parquet (Snappy)** | **2.67 MB** | **1,221,729 rows/sec** | **4.90 ms** | **6.3x smaller (84.1% savings)** |
| **CSV** | 6.85 MB | 284,091 rows/sec | 176.32 ms | 2.4x smaller |
| **JSONL (Newline JSON)** | 16.77 MB | 84,746 rows/sec | 589.65 ms | 1.0x baseline |

### ClickHouse Analytical Warehouse (100,000 Rows Benchmark)
| Operation / Query | Volume | Measured Latency / Throughput |
| :--- | :---: | :---: |
| **Vectorized Bulk Insert** | 100,000 rows | **3.67 seconds (27,277 rows/sec)** |
| **Slowest Targets by p95 Latency** | 100,000 rows | **71.03 ms** |
| **Protocol Stage Breakdown (DNS/TCP/TLS/TTFB)** | 100,000 rows | **67.09 ms** |
| **Regional Availability Aggregate** | 100,000 rows | **60.68 ms** |
| **Materialized View Pre-Aggregation (`target_daily_stats`)** | 100,000 rows | **50.27 ms** |

### Streaming & Ingestion Throughput
| Pipeline Component | Metric | Measured Value |
| :--- | :--- | :--- |
| **Kafka Ingestion Rate** | Single-producer stress test | **9,250 events/sec sustained** |
| **Kafka Topic Partitions** | Partition distribution | `measurements` (6), `probe_health` (3), `alerts` (3) |
| **Kafka Consumer Recovery (TEST-005)** | Consumer kill & restart | Accumulated 25+ lag → Drained to 0 in 1.4s with 0 dropped events |
| **Flink Real-Time Windowing** | 1-min Tumbling Window | Calculated `p50`, `p95`, `p99`, `availability` over streaming events |
| **Flink Out-of-Orderness** | Late-event handling | Watermark delay drops out-of-order records to dedicated DLQ |

### Anomaly Detection & Incident Management
| Capability | Implementation | Observed Behavior |
| :--- | :--- | :--- |
| **Chaos Endpoint** | HTTP endpoint on port 8088 | Controlled injection of +650ms delay and 500 error rates |
| **Statistical Detector** | Rolling Mean + Z-Score Deviation | Flagged anomalous latency immediately upon chaos injection |
| **Incident FSM** | 4-Stage State Machine | `NORMAL` → `ACTIVE` (3 alerts) → `RECOVERING` → `RESOLVED` (5 healthy sweeps) |
| **Geographic Correlation** | Cross-probe degradation analyzer | Accurate classification: `LIKELY REGIONAL DEGRADATION` vs `LIKELY GLOBAL OUTAGE` |
| **Traceroute Engine** | Hop RTT & Route Signature | MD5 route signature (e.g. `8a9cd0363941`) detects network path flapping |

### Observability & Fault Tolerance
| Suite | Scope | Result |
| :--- | :--- | :--- |
| **Prometheus Exporter** | Custom metrics on port 9090 | Probe counts, Kafka consumer lag, Flink window metrics, active incidents |
| **Grafana Dashboards** | Provisioned dashboard on port 3000 | Multi-panel visual breakdown of latency, availability, and incident status |
| **Fault Tolerance (FT-1 to FT-5)**| Probe crash, consumer lag, Flink recovery, deduplication, DLQ | **5 / 5 Scenarios Passed** |
| **Automated Test Suite** | Full regression unit test suite | **52 / 52 Tests Passing** |

---

## 5. Repository Structure

```
DE Project/
├── probe/
│   ├── netscope_probe/
│   │   ├── probe.py                  # Core protocol measurement engine (DNS, TCP, TLS, TTFB)
│   │   └── writers/
│   │       └── minio_writer.py       # Hive-partitioned Snappy Parquet lake writer
│   ├── tests/
│   │   ├── test_probe.py             # W1: Schema validation & protocol measurement
│   │   ├── test_week2.py             # W2: Data quality checker & Parquet roundtrip
│   │   ├── test_week3.py             # W3: Kafka producer & topic routing
│   │   ├── test_week4.py             # W4: Flink windows, watermarks, late events
│   │   ├── test_week5.py             # W5: ClickHouse DDL, queries, bulk inserts
│   │   ├── test_week6.py             # W6: Statistical anomaly detector & incident FSM
│   │   ├── test_week7.py             # W7: Geographic correlation & traceroute signatures
│   │   └── test_week8.py             # W8: Observability exporter & metric tracking
│   ├── Dockerfile                    # Probe container image
│   └── pytest.ini                    # Pytest configuration
│
├── schemas/
│   └── measurement.py                # Canonical dataclasses: Measurement & ProbeHeartbeat
│
├── targets/
│   └── targets.yaml                  # Targets, timeouts, protocols, and response byte caps
│
├── ingestion/
│   ├── producers/
│   │   └── kafka_producer.py         # Resilient Kafka producer (keyed partitioning)
│   └── consumers/
│       └── kafka_to_minio.py         # Stream-to-lake consumer (at-least-once manual commit)
│
├── streaming/
│   └── flink_jobs/
│       ├── stream_processor.py       # Flink stream processor (windows, watermarks, dedup)
│       └── run_pipeline.py           # Live Kafka-to-Flink runner
│
├── warehouse/
│   ├── schemas/
│   │   └── init.sql                  # ClickHouse DDL (MergeTree, codecs, Materialized Views)
│   ├── ingestion/
│   │   └── clickhouse_client.py      # High-speed vectorized ClickHouse client
│   └── queries/
│       └── benchmark_queries.py      # 100K benchmark and analytical query suite
│
├── anomaly_engine/
│   ├── detectors/
│   │   └── statistical_detector.py   # Rolling baseline, Z-score, multi-signal composite score
│   └── incident_manager/
│       └── lifecycle.py              # 4-stage incident finite state machine
│
├── controlled_endpoint/
│   └── server.py                     # Chaos injection server (/inject/delay, /inject/error)
│
├── network_analysis/
│   ├── geographic/
│   │   └── correlation_engine.py     # Cross-probe regional vs global correlation
│   └── traceroute/
│       └── traceroute_engine.py      # Hop-by-hop traceroute parser & MD5 route hashing
│
├── observability/
│   ├── metrics_exporter/
│   │   └── exporter.py               # Prometheus metrics exporter
│   ├── prometheus/
│   │   └── prometheus.yml            # Prometheus scrape configuration
│   └── grafana/
│       ├── dashboards.yml            # Provisioning config
│       ├── datasources.yml           # Prometheus datasource config
│       └── dashboard_overview.json   # 5-panel NetScope overview dashboard
│
├── data_quality/
│   └── checker.py                    # Automated dataset hygiene auditor
│
├── benchmarks/
│   ├── storage_shootout.py           # 50,000-row format shootout (Parquet vs CSV vs JSONL)
│   └── stress_test.py                # High-throughput Kafka producer stress test
│
├── scripts/
│   ├── final_presentation.py         # 18-step master end-to-end presentation orchestrator
│   ├── test_005_kafka_recovery.py    # TEST-005 Kafka lag & recovery experiment
│   ├── test_014_fault_tolerance.py   # 5-scenario platform fault tolerance verification
│   ├── demo_week4_flink_streaming.py # Week 4 live streaming observable demo
│   ├── demo_week6_controlled_outage.py# Week 6 controlled chaos & incident demo
│   ├── demo_week7_geographic_traceroute.py # Week 7 geographic & traceroute demo
│   └── sweep_and_stats.py            # Multi-round target sweep & percentile reporter
│
├── docker-compose.yml                # Kafka (KRaft), MinIO, ClickHouse, Prometheus, Grafana
└── README.md
```

---

## 6. How to Run & Verify the Stack

### Prerequisites
- Python 3.11+
- Docker Desktop with WSL2 backend (Windows / Linux / macOS)

### 1. Launch All Infrastructure Services
```powershell
docker compose up -d
```
Service endpoints:
- **MinIO S3 Console:** [http://localhost:9001](http://localhost:9001) (`minioadmin` / `minioadmin`)
- **ClickHouse HTTP:** [http://localhost:8123/ping](http://localhost:8123/ping) (Native TCP on port `9009`)
- **Kafka Broker:** `localhost:9092`
- **Prometheus:** [http://localhost:9090](http://localhost:9090)
- **Grafana:** [http://localhost:3000](http://localhost:3000) (`admin` / `admin`)

### 2. Run the Complete Automated Test Suite (52 Tests)
```powershell
python -m pytest probe/tests/ -v -m "not integration"
```

### 3. Run the Master 18-Step End-to-End Presentation
Execute the unified demo script that steps through every capability built across all 9 weeks:
```powershell
python scripts/final_presentation.py
```

### 4. Run Individual Phase Demos & Benchmarks
- **Storage Shootout (Parquet vs CSV vs JSONL):**
  ```powershell
  python benchmarks/storage_shootout.py
  ```
- **Kafka High-Throughput Stress Test:**
  ```powershell
  python benchmarks/stress_test.py
  ```
- **ClickHouse 100K Benchmark & Historical Queries:**
  ```powershell
  python warehouse/queries/benchmark_queries.py
  ```
- **Real-Time Flink Streaming & Watermarks Demo:**
  ```powershell
  python scripts/demo_week4_flink_streaming.py
  ```
- **Controlled Chaos & Incident Lifecycle Demo:**
  ```powershell
  python scripts/demo_week6_controlled_outage.py
  ```
- **Geographic Correlation & Route Hash Demo:**
  ```powershell
  python scripts/demo_week7_geographic_traceroute.py
  ```
- **Fault Tolerance Suite (FT-1 to FT-5):**
  ```powershell
  python scripts/test_014_fault_tolerance.py
  ```

---

## 7. Team Contributions & Resume Impact

### MEMBER_1: Network Probing, Ingestion & Routing
- Engineered a multi-protocol active measurement probe in Python measuring DNS, TCP connect, TLS handshake, and HTTP TTFB with socket-level timeout guards and response payload caps.
- Implemented a cross-platform traceroute engine computing hop-by-hop latency and MD5 route signature hashing to detect network path flapping.
- Configured Apache Kafka (KRaft mode) ingestion with keyed partitioning across 6 partitions, achieving a sustained single-producer throughput of **9,250 events/sec**.
- Validated Kafka consumer recovery under lag accumulation (TEST-005), demonstrating zero message loss and instant drain upon consumer restart.

### MEMBER_2: Real-Time Streaming & Analytical Warehouse
- Architected an Apache Flink streaming engine handling event-time extraction, bounded out-of-orderness watermarks, and windowed statistical aggregations (p50/p95/p99) with dead-letter queue (DLQ) late event routing.
- Designed ClickHouse columnar analytical schema leveraging `DoubleDelta` and `ZSTD` compression codecs and `SummingMergeTree` Materialized Views, achieving **27,277 rows/sec** bulk ingestion.
- Benchmarked analytical query latency across 100,000 measurements, returning sub-75ms analytical query responses (fastest at **50.27 ms** via materialized views).
- Conducted a storage format shootout on 50,000 records demonstrating Snappy Parquet achieved **6.3x higher compression** and **1.22M rows/sec** read throughput compared to JSONL.

### MEMBER_3: Anomaly Detection, Incident Management & Observability
- Developed an explainable statistical anomaly detection engine using rolling baselines, dynamic Z-score thresholds, and multi-signal composite scoring.
- Implemented a 4-stage deterministic incident state machine (`NORMAL` → `ACTIVE` → `RECOVERING` → `RESOLVED`) managing incident lifecycles without alert flapping.
- Built a cross-probe correlation engine distinguishing `LIKELY REGIONAL DEGRADATION` from `LIKELY GLOBAL OUTAGE` using localized probe failure rates.
- Deployed end-to-end self-monitoring with a custom Prometheus metrics exporter and 5 provisioned Grafana dashboards tracking pipeline latency, availability, and active incidents.
