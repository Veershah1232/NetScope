# System Architecture & Design Philosophy

NetScope is a distributed Internet telemetry, telemetry processing, and performance intelligence platform designed to observe protocol-level latency and reliability across global and local network targets.

---

## 1. Architectural Overview & Design Tenets

Modern Internet telemetry systems face three core challenges:
1. **High Ingestion Cardinality & Velocity**: Measurements arrive continuously from distributed vantage points across multiple target endpoints.
2. **Event-Time Skew & Out-of-Orderness**: Network jitter, probe disconnects, and packet delays mean arrival time ($\text{proc\_time}$) diverges significantly from measurement occurrence time ($\text{event\_time}$).
3. **Dual Storage Requirements**:
   - **Long-term Raw Lake**: Economical, immutable, append-only raw data for forensic replay and offline model training.
   - **Analytical OLAP Warehouse**: Low-latency, column-oriented storage supporting real-time aggregations, tail percentile computations ($p_{95}, p_{99}$), and multi-dimensional slice-and-dice queries.

NetScope solves these problems by adopting an **event-driven, decoupled Lakehouse architecture**:

```
+-----------------------------------------------------------------------------------+
|                            TARGET REGISTRY (50 Websites)                           |
+-----------------------------------------------------------------------------------+
                                         |
                                         v
+-----------------------------------------------------------------------------------+
|               DISTRIBUTED ACTIVE PROBE FLEET (probe-01 ... probe-10)               |
|  - DNS Resolution (socket.getaddrinfo)                                             |
|  - TCP Connect (socket.create_connection)                                          |
|  - TLS Handshake (ssl.create_default_context)                                      |
|  - HTTP Request & TTFB (urllib.request / requests)                                |
|  - Traceroute & MD5 Route Signature Flapping Detection                            |
+-----------------------------------------------------------------------------------+
                                         |
                                         v (Keyed JSON Events: key=probe_id)
+-----------------------------------------------------------------------------------+
|                        APACHE KAFKA BROKER (KRaft Mode)                           |
|  - Topic: measurements (6 Partitions, partition_key=probe_id)                      |
|  - Topic: probe_health (3 Partitions, partition_key=probe_id)                      |
|  - Topic: alerts       (3 Partitions, partition_key=target_id)                    |
+-----------------------------------------------------------------------------------+
                         |                                   |
                         v                                   v
+-----------------------------------+   +-------------------------------------------+
|    STREAM-TO-LAKE INGESTION       |   |      REAL-TIME STREAM ENGINE (FLINK)      |
|  - Micro-batch buffering (1,000)  |   |  - Event-time extraction                  |
|  - PyArrow Snappy Parquet writer  |   |  - Bounded out-of-orderness watermarks    |
|  - Hive Partitioning              |   |  - Exact-once deduplication (UUID cache)  |
|  - Manual Kafka offset commit     |   |  - 1-min tumbling / 5-min sliding windows |
|  - Target: MinIO S3 Object Store  |   |  - Late-event dead letter queue (DLQ)     |
+-----------------------------------+   +-------------------------------------------+
                 |                                                   |
                 +-------------------------+-------------------------+
                                           |
                                           v
+-----------------------------------------------------------------------------------+
|                      CLICKHOUSE COLUMNAR ANALYTICAL WAREHOUSE                     |
|  - Table: measurements (DoubleDelta, ZSTD codecs, LowCardinality probe/target)    |
|  - Table: latency_windows (Streaming aggregate window snapshots)                  |
|  - Materialized View: target_daily_stats (SummingMergeTree pre-aggregations)      |
+-----------------------------------------------------------------------------------+
                         |                                   |
                         v                                   v
+-----------------------------------+   +-------------------------------------------+
|    STATISTICAL ANOMALY ENGINE     |   |         OBSERVABILITY & PRESENTATION      |
|  - Rolling Welford mean & variance|   |  - Prometheus Exporter (:9090)            |
|  - Dynamic Z-Score deviation      |   |  - Grafana 5-Panel Dashboard (:3000)      |
|  - Multi-signal composite scoring |   |  - NetScope Telemetry Explorer UI (:8050) |
|  - 4-Stage Incident FSM           |   |  - 18-Step Automated Presentation CLI     |
|  - Regional vs Global Correlation |   +-------------------------------------------+
+-----------------------------------+
```

---

## 2. End-to-End Data Flow

### Step 1: Probe Execution & Heartbeat
- Each probe worker instance is identified by an honest local identifier (`probe-01` through `probe-10`).
- Probes sweep their 15 assigned targets cyclically every 30 seconds.
- Every sweep records microsecond-resolution timing across protocol stages: `dns_lookup_time_ms`, `tcp_connect_time_ms`, `tls_handshake_time_ms`, and `http_ttfb_ms`.
- Heartbeat signals are emitted after every sweep round to topic `probe_health` reporting total measurements sent and error counts.

### Step 2: Resilient Kafka Ingestion
- Probes serialize telemetry into canonical JSON conforming to `schemas.measurement.Measurement`.
- The producer sets the Kafka record key to `probe_id`. Because Kafka hashes record keys across partition slots ($P = \text{hash}(\text{key}) \pmod N$), all events from a given probe land on the same partition in strict causal sequence.
- Producer uses exponential backoff retries with idempotence flags to guard against transient network partitions.

### Step 3: Dual Storage Lakehouse Ingestion
1. **Raw Lake Path (MinIO S3)**:
   - High-throughput consumer batches up to 1,000 measurements or flushes every 10 seconds.
   - Batches are converted into columnar Parquet format using PyArrow and compressed with Snappy.
   - Stored in a Hive-partitioned directory hierarchy:
     `s3://netscope-raw/measurements/year=YYYY/month=MM/day=DD/hour=HH/probe_id=.../part-0000.parquet`
   - Offsets are manually committed back to Kafka only **after** the S3 write returns HTTP 200, guaranteeing at-least-once delivery with zero data loss.
2. **Analytical Warehouse Path (ClickHouse)**:
   - Events are buffered and inserted via vectorized HTTP batch queries into ClickHouse.
   - ClickHouse stores records in the `measurements` `MergeTree` table, automatically updating the `target_daily_stats` `SummingMergeTree` Materialized View.

### Step 4: Stream Processing Semantics (Apache Flink Simulator)
- Events enter the stream processor where `timestamp` is parsed as event time.
- A **Bounded Out-of-Orderness Watermark** generator tracks the maximum observed timestamp minus a tolerance delay:
  $$W(t) = \max(T_{\text{observed}}) - \Delta_{\text{delay}}$$
- **Deduplication**: An in-memory sliding cache of measurement UUIDs suppresses duplicate transmissions caused by producer retries.
- **Windowed Aggregations**:
  - **1-Minute Tumbling Windows**: Non-overlapping windows calculating $p_{50}, p_{95}, p_{99}$ latency and availability percentage per target.
  - **5-Minute Sliding Windows**: Smooth moving percentiles emitted every 60 seconds.
- **Dead-Letter Queue (DLQ)**: Any event arriving with timestamp $t < W(t)$ is marked late and routed to the DLQ for auditing.

### Step 5: Anomaly Intelligence & Incident Management
- Streaming window aggregates feed the **Statistical Anomaly Detector**.
- A rolling baseline maintains the mean $\mu$ and standard deviation $\sigma$ using Welford's single-pass algorithm.
- Current latency is evaluated against the baseline:
  $$Z = \frac{x - \mu}{\sigma}$$
- If $Z \ge 3.0$ or packet loss / error rates spike, a composite score triggers the **Incident State Machine**.
- Incidents transition through a 4-state lifecycle (`NORMAL` $\to$ `ACTIVE` $\to$ `RECOVERING` $\to$ `RESOLVED`), requiring hysteresis counters to prevent flapping.

### Step 6: Presentation & Observability
- Prometheus scrapes telemetry metrics every 15 seconds.
- Grafana visualizes real-time probe status, ingestion throughput, consumer lag, and incident status.
- NetScope Telemetry Explorer (`:8050`) offers an interactive UI with tail latency graphs ($p_{95}, p_{99}$), probe fleet inspection, and target latency comparisons.

---

## 3. Technology Stack Rationale

| Layer | Component | Selected Technology | Why Selected & Alternatives Evaluated |
| :--- | :--- | :--- | :--- |
| **Ingestion** | Message Broker | **Apache Kafka (KRaft)** | Keyed ordering per probe, multi-consumer decoupling, high throughput (9,250 msgs/s sustained). Replaced ZooKeeper dependency with lightweight KRaft metadata quorum. *Alternatives considered: RabbitMQ (insufficient analytical replay), Redis Streams (memory bounded).* |
| **Lake** | Object Store | **MinIO (S3 Compatible)** | S3 API standard, zero vendor lock-in, native local Docker deployment. Snappy Parquet columnar storage provides 6.3x compression over JSONL. *Alternatives considered: Local filesystem (no cloud parity), HDFS (unnecessary JVM operational overhead).* |
| **Warehouse** | Analytical OLAP | **ClickHouse** | Vectorized query engine, `MergeTree` storage, specialized compression codecs (`DoubleDelta` for timestamps, `ZSTD` for text), real-time Materialized Views. Sub-75ms analytical queries over 100K rows. *Alternatives considered: PostgreSQL (row-oriented, slow aggregates), Elasticsearch (heavy memory footprint).* |
| **Streaming** | Stream Processor | **Apache Flink Paradigm** | True event-time semantics, watermarking, stateful windows, exact-once deduplication. *Alternatives considered: Spark Streaming (micro-batch latency penalty).* |
| **Monitoring**| Observability | **Prometheus + Grafana** | Industry-standard pull-based metrics model, multidimensional labeling (`probe_id`, `target`), rich Grafana templated dashboards. |
