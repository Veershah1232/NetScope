# Observability, Operations & Fault Tolerance Guide

This document describes the observability infrastructure, the Web UI dashboard, operational commands for managing the 10-probe fleet, and the fault-tolerance verification suite.

---

## 1. Observability Suite

NetScope provides native observability across all pipeline components via Prometheus and Grafana.

### 1.1 Prometheus Exporter
Implemented in `observability/metrics_exporter/exporter.py`, exposing standard `/metrics` on port `9090`:
- `netscope_measurements_total{probe_id, target, status}`: Counter tracking emitted active probes.
- `netscope_probe_heartbeat{probe_id}`: Gauge reflecting 1 (healthy) or 0 (unreachable).
- `netscope_kafka_consumer_lag{topic, partition}`: Gauge tracking real-time unread messages.
- `netscope_flink_window_latency_p95_ms{target}`: Gauge emitting rolling windowed tail latency.
- `netscope_active_incidents{target, state}`: Gauge indicating active incident counts.

### 1.2 Grafana Provisioned Dashboards
Pre-configured in `observability/grafana/` and reachable at `http://localhost:3000` (`admin` / `admin`):
- **Overview Dashboard (`dashboard_overview.json`)**:
  - Live Ingestion Rate (events/sec)
  - Fleet Health & Heartbeat Status Table
  - Target Availability (%) Time-Series
  - Real-time p50, p95, p99 Latency Heatmap
  - Active Incidents & Severity Breakdown

---

## 2. NetScope Telemetry Explorer Web UI (`:8050`)

Implemented in `scripts/web_ui.py` and launched via `scripts/run_telemetry_pipeline.py`.

Accessible at **`http://localhost:8050`**, this custom lightweight dashboard provides operational insight without requiring full Grafana configuration:

```
+-----------------------------------------------------------------------------------------+
|  NETSCOPE TELEMETRY EXPLORER                                    Fleet: 10 Nodes Online  |
+-----------------------------------------------------------------------------------------+
| [ Total Records ]    [ Median p50 ]    [ Tail p95 ]    [ Peak p99 ]    [ Availability ] |
|     12,450 rows          34.2 ms          118.5 ms        245.0 ms          99.8%       |
+-----------------------------------------------------------------------------------------+
|  REAL-TIME LATENCY (ms) & PERCENTILE THRESHOLDS                                         |
|                                                                                         |
|  Latency (ms)                                                                           |
|   300 +                                                                                 |
|       |                                                - - - - - - - p99 Threshold (245)|
|   200 +                                                                                 |
|       |                                        - - - - - - - - - - - p95 Threshold (118)|
|   100 +                                                                                 |
|       |   /\    /\        /\            /\                                              |
|     0 +--/--\--/--\------/--\----------/--\-----------------------------------------    |
|          06:10  06:15   06:20         06:25                                   Time      |
+-----------------------------------------------------------------------------------------+
|  ACTIVE FLEET (10 Local Probes)              |  RECENT MEASUREMENT LOG                  |
|  - probe-01: 15 targets (100% OK)            |  06:25:12 | probe-01 | google.com | 24ms |
|  - probe-02: 15 targets (100% OK)            |  06:25:14 | probe-03 | github.com | 48ms |
|  - ...                                       |  06:25:16 | probe-08 | min.io     | 18ms |
+-----------------------------------------------------------------------------------------+
```

### Key UI Features:
- **Tail Latency Visualizer**: Real-time Chart.js time-series featuring dynamic horizontal thresholds for $p_{95}$ (amber) and $p_{99}$ (rose).
- **Target Filtering**: Instant dropdown selection to inspect any of the 50 targets.
- **Probe Fleet Inspection**: Live breakdown of all 10 local probe containers.

---

## 3. Fleet Operations Runbook

### Starting Infrastructure
```powershell
# Start core datastores & messaging brokers
docker compose up -d minio kafka clickhouse prometheus grafana
```

### Starting the 10 Local Probes (Continuous Mode B)
```powershell
docker compose up -d probe-1 probe-2 probe-3 probe-4 probe-5 probe-6 probe-7 probe-8 probe-9 probe-10
```

### Checking Probe Container Status & Logs
```powershell
# Check container status
docker ps --filter "name=netscope-probe"

# Stream logs from probe-01
docker logs netscope-probe-01 -f --tail 25

# Stream logs from probe-10
docker logs netscope-probe-10 -f --tail 25
```

### Stopping the 10 Probes
```powershell
docker compose stop probe-1 probe-2 probe-3 probe-4 probe-5 probe-6 probe-7 probe-8 probe-9 probe-10
```

### Launching the Ingestion Pipeline & Web UI
Run in your PowerShell terminal:
```powershell
python scripts/run_telemetry_pipeline.py
```
This script:
1. Connects to Kafka broker (`localhost:9092`) as consumer group `netscope-telemetry-lake`.
2. Flushes micro-batches to ClickHouse (`measurements` table).
3. Writes compressed Snappy Parquet batches to MinIO (`s3://netscope-raw`).
4. Serves the web dashboard on `http://localhost:8050`.

---

## 4. Fault Tolerance Verification (FT-1 to FT-5)

Executed via `python scripts/test_014_fault_tolerance.py`.

The platform undergoes rigorous verification across 5 real-world infrastructure failure scenarios:

| Scenario | Injected Failure Condition | Expected System Behavior | Result |
| :--- | :--- | :--- | :---: |
| **FT-1: Probe Crash & Recovery** | Probe process forcibly terminated mid-sweep. | Target measurements resume on next restart; zero corrupted Kafka records. | ✅ **PASSED** |
| **FT-2: Consumer Lag Recovery (TEST-005)** | Stream-to-lake consumer paused during high traffic. | Messages queue in Kafka partition; consumer drains accumulated lag to 0 with 0 lost events. | ✅ **PASSED** |
| **FT-3: ClickHouse Transient Outage** | ClickHouse temporarily unavailable during insert. | Ingestion buffer safely retains rows in-memory and retries with backoff until warehouse restores. | ✅ **PASSED** |
| **FT-4: Producer Retries & Exact-Once Dedup** | Duplicate messages with identical `measurement_id` sent to Kafka. | Flink state cache detects duplicate UUIDs and drops them from aggregate windows. | ✅ **PASSED** |
| **FT-5: Late Event Handling & DLQ** | Event with timestamp lagging behind watermark injected. | Flink engine detects event lateness and routes record to Dead-Letter Queue side-output. | ✅ **PASSED** |

---

## 5. Automated Test Suite

Run the full unit and regression test suite:
```powershell
python -m pytest probe/tests/ -v -m "not integration"
```
**Status**: 52 passed / 52 total.
- `test_probe.py`: Protocol timing, error tagging, and schema validation.
- `test_week2.py`: Data quality auditor, Parquet writer, Hive partitioning.
- `test_week3.py`: Kafka producer, topic routing, keyed partitioning.
- `test_week4.py`: Flink watermarks, tumbling windows, deduplication, DLQ.
- `test_week5.py`: ClickHouse DDL, queries, bulk ingestion.
- `test_week6.py`: Statistical anomaly detector, Z-scores, 4-state incident FSM.
- `test_week7.py`: Geographic correlation and traceroute route hashing.
- `test_week8.py`: Prometheus metrics and lag monitoring.
