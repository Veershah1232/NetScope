# NetScope Code API Reference

This document provides a comprehensive code reference of classes, methods, signatures, and data contracts across all Python packages in the NetScope repository.

---

## 1. Data Contracts & Schemas (`schemas/`)

### `schemas.measurement.Measurement`
The canonical immutable representation of an active network measurement.

```python
@dataclass
class Measurement:
    measurement_id: str          # UUIDv4 string
    probe_id: str                # e.g., 'probe-01'
    target: str                  # Domain or hostname
    target_category: str         # e.g., 'search', 'cdn', 'cloud'
    timestamp: str               # ISO 8601 UTC timestamp
    resolved_ip: Optional[str]   # IPv4/IPv6 string
    dns_lookup_time_ms: float    # Milliseconds for getaddrinfo
    tcp_connect_time_ms: float   # Milliseconds for TCP handshake
    tls_handshake_time_ms: float # Milliseconds for SSL wrap
    http_ttfb_ms: float          # Milliseconds to first response byte
    total_latency_ms: float      # Sum of all handshake stages
    http_status_code: int        # HTTP status code (e.g. 200, 301, 500)
    response_size_bytes: int     # Read payload bytes (capped)
    is_success: bool             # True if 2xx/3xx/4xx without network fault
    error_type: Optional[str]    # 'dns_failure', 'tcp_refused', 'connect_timeout', etc.
    error_message: Optional[str] # Detailed exception message if failed
```

#### Key Methods:
- `to_dict() -> Dict[str, Any]`: Serializes the dataclass to a JSON-compatible dictionary.
- `validate() -> List[str]`: Enforces schema validity (non-empty IDs, positive timings, valid ISO timestamps). Returns a list of validation failure strings (empty if valid).

### `schemas.measurement.ProbeHeartbeat`
Periodic health and operational telemetry emitted by probe workers.

```python
@dataclass
class ProbeHeartbeat:
    probe_id: str               # e.g., 'probe-01'
    timestamp: str              # ISO 8601 UTC timestamp
    status: str                 # 'healthy', 'degraded', 'unreachable'
    version: str                # Probe engine version ('0.1.0')
    measurements_sent: int      # Cumulative sent count
    failed_measurements: int    # Cumulative failed count
```

---

## 2. Active Probe Engine (`probe/netscope_probe/`)

### `probe.netscope_probe.probe`

#### `measure_target(target: Dict[str, Any], probe_id: str) -> Measurement`
Performs an active, multi-stage protocol measurement against a single target configuration dictionary.
- Instruments DNS, TCP, TLS, and HTTP TTFB.
- Applies socket timeout and response byte truncation.
- Classifies network errors into canonical failure types.

#### `load_targets_from_yaml(path: str, probe_id: Optional[str] = None) -> List[Dict[str, Any]]`
Parses target definitions from YAML file.
- If `probe_id` is supplied, filters targets to only those where `probe_id in target.get("probes", [])`.

#### `sweep_targets(targets: List[Dict[str, Any]], probe_id: str, producer: Optional[Any] = None) -> List[Measurement]`
Executes sequential or threaded measurements across all loaded targets, emits measurements to Kafka if a producer is supplied, and emits a `ProbeHeartbeat` upon sweep completion.

### `probe.netscope_probe.writers.minio_writer`

#### `records_to_parquet_bytes(records: List[Dict[str, Any]]) -> bytes`
Converts a list of measurement dictionaries into an in-memory Snappy-compressed Apache Parquet byte buffer using PyArrow.

#### `_partition_key(ts_str: str, probe_id: str) -> str`
Generates a standard Hive partition directory path from an ISO timestamp:
`year=YYYY/month=MM/day=DD/hour=HH/probe_id=...`

#### `write_partition_to_minio(client: Minio, bucket: str, partition_path: str, part_index: int, records: List[Dict[str, Any]]) -> str`
Serializes records and executes an S3 `put_object` call into MinIO under the generated Hive partition prefix.

---

## 3. Ingestion & Messaging (`ingestion/`)

### `ingestion.producers.kafka_producer.NetScopeKafkaProducer`
Resilient Kafka producer with retry policy and keyed routing.

```python
class NetScopeKafkaProducer:
    def __init__(self, bootstrap_servers: str = "localhost:9092")
    def send_measurement(self, measurement: Measurement) -> None
    def send_heartbeat(self, heartbeat: ProbeHeartbeat) -> None
    def send_alert(self, alert_payload: Dict[str, Any]) -> None
    def flush(self) -> None
    def close(self) -> None
```
- Keys `measurements` and `probe_health` topics by `probe_id`.
- Keys `alerts` topic by `target`.

### `ingestion.consumers.kafka_to_minio.KafkaToMinioConsumer`
Stream-to-lake consumer with manual offset commits.

```python
class KafkaToMinioConsumer:
    def __init__(self, bootstrap_servers: str, minio_client: Minio, bucket: str = "netscope-raw", batch_size: int = 1000)
    def consume_loop(self) -> None
    def flush_batch(self, buffer: List[Dict[str, Any]]) -> None
```
- Calls `consumer.commit()` **only after** `flush_batch()` succeeds.

---

## 4. Real-Time Streaming (`streaming/flink_jobs/`)

### `streaming.flink_jobs.stream_processor.StreamProcessor`
Simulates Apache Flink event-time streaming primitives.

```python
class StreamProcessor:
    def __init__(self, allowed_lateness_sec: float = 5.0, tumbling_window_sec: int = 60)
    def process_event(self, event: Dict[str, Any]) -> Optional[Dict[str, Any]]
    def calculate_percentile(self, values: List[float], p: float) -> float
    def get_dlq_events(self) -> List[Dict[str, Any]]
```

#### Core Methods:
- `_extract_event_time(event)`: Parses `timestamp` into epoch seconds.
- `_update_watermark(event_time)`: Updates monotonic watermark $W = \max(T) - \Delta$.
- `_is_late(event_time)`: Checks if $event\_time < W$.
- `_evaluate_windows()`: Triggers window closure when $W \ge W_{\text{end}}$ and computes $p_{50}, p_{95}, p_{99}$ metrics.

---

## 5. Analytical Warehouse (`warehouse/`)

### `warehouse.ingestion.clickhouse_client.ClickHouseClient`
High-speed HTTP client for ClickHouse OLAP operations.

```python
class ClickHouseClient:
    def __init__(self, host: str = "localhost", port: int = 8123, database: str = "netscope")
    def insert_measurements(self, measurements: List[Dict[str, Any]]) -> int
    def query(self, sql: str) -> List[Dict[str, Any]]
    def ping(self) -> bool
```

### `warehouse.queries.benchmark_queries`
- `query_slowest_targets(client: ClickHouseClient, limit: int = 10)`
- `query_protocol_breakdown(client: ClickHouseClient)`
- `query_regional_availability(client: ClickHouseClient)`
- `query_materialized_view(client: ClickHouseClient)`

---

## 6. Anomaly & Incident Engine (`anomaly_engine/`)

### `anomaly_engine.detectors.statistical_detector.StatisticalAnomalyDetector`
Online statistical anomaly scoring using Welford's baseline.

```python
class StatisticalAnomalyDetector:
    def __init__(self, z_threshold: float = 3.0)
    def update_baseline(self, target: str, latency_ms: float) -> Tuple[float, float]
    def compute_z_score(self, target: str, latency_ms: float) -> float
    def evaluate_measurement(self, measurement: Dict[str, Any]) -> Dict[str, Any]
```

### `anomaly_engine.incident_manager.lifecycle.IncidentLifecycleManager`
4-state incident finite state machine.

```python
class IncidentState(Enum):
    NORMAL = "NORMAL"
    ACTIVE = "ACTIVE"
    RECOVERING = "RECOVERING"
    RESOLVED = "RESOLVED"

class IncidentLifecycleManager:
    def __init__(self, active_threshold: int = 3, recover_threshold: int = 2, resolve_threshold: int = 5)
    def process_signal(self, target: str, is_anomalous: bool, details: Dict[str, Any]) -> IncidentState
    def get_active_incidents(self) -> Dict[str, Any]
```

---

## 7. Network Intelligence (`network_analysis/`)

### `network_analysis.geographic.correlation_engine.GeographicCorrelationEngine`
```python
class GeographicCorrelationEngine:
    def __init__(self, global_threshold: float = 0.70)
    def correlate_probe_failures(self, target: str, probe_results: Dict[str, bool]) -> str
```
- Returns `"LIKELY GLOBAL OUTAGE"` or `"LIKELY REGIONAL DEGRADATION"`.

### `network_analysis.traceroute.traceroute_engine.TracerouteEngine`
```python
class TracerouteEngine:
    def __init__(self, max_hops: int = 30, timeout_sec: int = 2)
    def execute_traceroute(self, target: str) -> List[Dict[str, Any]]
    def compute_route_signature(self, hops: List[Dict[str, Any]]) -> str
```
- Computes MD5 hash across non-empty intermediate hop IP addresses.

---

## 8. Observability & Quality (`observability/` & `data_quality/`)

### `observability.metrics_exporter.exporter.MetricsExporter`
Prometheus exporter publishing metrics on port 9090.
- `record_measurement(probe_id: str, target: str, is_success: bool, latency_ms: float)`
- `update_heartbeat(probe_id: str, is_healthy: bool)`
- `set_consumer_lag(topic: str, partition: int, lag: int)`

### `data_quality.checker.DataQualityChecker`
Validates dataset hygiene, non-negative timings, schema compliance, and monotonic timestamps.
- `validate_record(record: Dict[str, Any]) -> List[str]`
- `audit_batch(records: List[Dict[str, Any]]) -> Dict[str, Any]`

---

## 9. Scripts & User Dashboards (`scripts/`)

### `scripts.run_telemetry_pipeline`
Orchestrator daemon that:
1. Initializes ClickHouse tables (`init.sql`).
2. Spawns the Web UI server (`web_ui.py`) on port 8050.
3. Consumes messages from Kafka (`measurements`) in micro-batches.
4. Flushes batches to ClickHouse and MinIO Parquet partitions.

### `scripts.web_ui`
Flask web application serving the NetScope Telemetry Explorer:
- `GET /`: Dashboard homepage.
- `GET /api/summary`: Top-level fleet statistics (p50, p95, p99, total records, active probes).
- `GET /api/timeseries?target=<name>`: Time-series measurements and calculated p95/p99 thresholds.
- `GET /api/targets`: List of all 50 targets and 10 local probes.
- `GET /api/recent`: Recent 20 measurement logs.
