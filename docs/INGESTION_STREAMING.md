# Ingestion & Real-Time Stream Processing

This document covers the NetScope messaging layer powered by **Apache Kafka (KRaft mode)** and the **real-time stream processing engine** implementing Apache Flink streaming semantics.

---

## 1. Apache Kafka Message Broker Architecture

NetScope utilizes Apache Kafka running in KRaft mode (Kafka Raft Metadata Quorum), eliminating external ZooKeeper dependencies while maintaining strong consistency and metadata replication.

### 1.1 Topic Topology & Partition Strategy

```
                                      APACHE KAFKA BROKER (:9092)
                                                 │
       ┌─────────────────────────────────────────┼─────────────────────────────────────────┐
       ▼                                         ▼                                         ▼
Topic: measurements                       Topic: probe_health                       Topic: alerts
├─ Partitions: 6                          ├─ Partitions: 3                          ├─ Partitions: 3
├─ Key: probe_id                          ├─ Key: probe_id                          ├─ Key: target_id
└─ Purpose: Raw measurement telemetry     └─ Purpose: Heartbeats & health metrics   └─ Purpose: Anomaly notifications
```

### 1.2 Keyed Partitioning & Causal Ordering Guarantee
In distributed telemetry, maintaining causal time ordering per probe is paramount. If consecutive measurements from `probe-01` were distributed round-robin across partitions, consumers could observe out-of-order latency spikes or false recovery transitions.

NetScope sets `key = probe_id.encode('utf-8')` for every measurement event:
$$\text{Partition Index} = \text{murmur2}(\text{probe\_id}) \pmod 6$$

This guarantees:
1. **Per-Probe Strict In-Order Processing**: All events from `probe-01` are written to and consumed from the exact same partition in monotonic order.
2. **Horizontal Consumer Parallelism**: Up to 6 consumer threads or processes can read concurrently from `measurements` without lock contention or thread race conditions.

### 1.3 Resilient Kafka Producer
Implemented in `ingestion/producers/kafka_producer.py`:
- **Automatic Retry Policy**: Configured with `retries=5` and exponential backoff.
- **Idempotent Delivery**: Prevents duplicate records on the broker if an ACK packet is dropped across the network.
- **Micro-Batch Buffering**: Uses `linger_ms=10` and `batch_size=16384` to batch network writes, achieving up to **9,250 events/sec sustained throughput**.

---

## 2. Stream-to-Lake Consumer & At-Least-Once Delivery

Implemented in `ingestion/consumers/kafka_to_minio.py`:

```
Kafka Topic [measurements]
          │
          ▼
   Consumer Poll()
          │
          ▼
   In-Memory Buffer (batch_size=1,000 OR max_wait=10s)
          │
          ▼
   Convert to PyArrow Table -> Snappy Parquet
          │
          ▼
   MinIO S3 PutObject() ---> HTTP 200 OK?
          │
          ├── YES: consumer.commit() (Advance Kafka Offset)
          │
          └── NO:  Retry write; Do NOT commit offset (Prevent Data Loss)
```

### Manual Offset Commit Mechanics
- Default Kafka auto-commit (`enable_auto_commit=True`) commits offsets periodically in the background. If a worker crashes after polling but before writing to MinIO, data is permanently lost.
- NetScope disables auto-commit (`enable_auto_commit=False`). Offsets are committed synchronously **only after** `minio_client.put_object()` returns an HTTP 200 acknowledgment.
- **Failure Recovery (TEST-005)**: When the consumer is terminated under heavy traffic, unprocessed events accumulate harmlessly as Kafka lag. Upon consumer restart, it resumes from the last committed offset, draining accumulated lag without dropping a single event.

---

## 3. Real-Time Stream Engine (Apache Flink Semantics)

Implemented in `streaming/flink_jobs/stream_processor.py`, NetScope simulates core Apache Flink streaming primitives:

### 3.1 Event-Time vs Processing-Time
- **Processing Time**: The local system clock of the processing node when an event happens to be read from Kafka. Vulnerable to network jitter, consumer restarts, and queue backpressure.
- **Event Time**: The timestamp recorded by the probe at the instant of measurement. NetScope extracts event time from the `timestamp` field of each `Measurement` record.

### 3.2 Bounded Out-of-Orderness Watermarking
Because distributed network packets arrive with variable transit delays, the stream processor cannot assume monotonic event-time arrivals. It implements a **Bounded Out-of-Orderness Watermark Generator**:
$$W(t) = \max_{e \in E}(e.\text{timestamp}) - \Delta_{\text{allowed\_lateness}}$$
- Default watermark delay: $\Delta = 5 \text{ seconds}$.
- An event with timestamp $t$ arriving when $W > t$ is classified as a **Late Event**.

### 3.3 Deduplication State Filter
To prevent duplicate records (e.g. from network retries) from polluting analytical averages, the engine maintains an in-memory sliding cache of `measurement_id` (UUIDv4) keys:
```python
if event["measurement_id"] in self.seen_ids:
    self.dropped_duplicates += 1
    return  # Discard duplicate event
self.seen_ids.add(event["measurement_id"])
```

### 3.4 Windowed Statistical Aggregations

The engine supports two distinct streaming window topologies:

#### 1. 1-Minute Tumbling Windows (Fixed & Non-Overlapping)
- Groups all events where:
  $$W_{\text{start}} \le \text{event\_time} < W_{\text{end}}$$
- Computes aggregated metrics per target:
  - **Availability Rate**: $\frac{\text{Successful Count}}{\text{Total Count}} \times 100\%$
  - **Median Latency ($p_{50}$)**: 50th percentile of total latency.
  - **Tail Latency ($p_{95}, p_{99}$)**: 95th and 99th percentiles reflecting outlier degraded paths.
  - **DNS, TCP, TLS Stage Means**: Protocol phase breakdown.

#### 2. 5-Minute Sliding Windows (Overlapping with 1-Minute Slide)
- Window size: 5 minutes. Slide interval: 1 minute.
- Provides smooth statistical moving averages for long-term anomaly baseline calculations.

### 3.5 Late Event Side-Outputs (Dead-Letter Queue / DLQ)
Events that arrive after the window has already closed ($t < W$) are not dropped silently. They are diverted into a dedicated **Dead-Letter Queue (DLQ)** side-output:
```python
if event_time < current_watermark:
    self.dlq.append(event)
    self.late_events_count += 1
```
This enables post-hoc forensic analysis and auditability of severely delayed network telemetry.
