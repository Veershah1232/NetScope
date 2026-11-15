# NETSCOPE — Distributed Internet Measurement & Analytics Platform

> **Team:** 3 members · **Duration:** 9 weeks · **Stack:** Python · Kafka · Flink · MinIO · ClickHouse · Grafana

---

## What is NetScope?

NetScope is a distributed Internet observatory. It continuously measures DNS, TCP, TLS, and HTTP performance from geographically distributed probes, processes those measurements through a real-time data engineering pipeline, and surfaces anomalies, incidents, and geographic performance patterns in live dashboards.

This is not a simple ping dashboard. The engineering goal is a production-grade data pipeline demonstrating:

- Real-time stream processing with Apache Flink
- Kafka-based decoupled ingestion with fault tolerance
- S3-compatible data lake (MinIO + Parquet)
- Analytical warehouse (ClickHouse)
- Statistical anomaly detection and incident lifecycle management
- End-to-end observability with Prometheus and Grafana

---

## Architecture

```
Probes (IN / SG / EU)
      │
      ▼
   Kafka
   ├─ measurements
   ├─ probe_health
   └─ alerts
      │
      ▼
   Flink (event-time + watermarks + windowing)
      │
   ┌──┴──────────┬──────────────┐
   ▼             ▼              ▼
 MinIO        ClickHouse    Incident
(Parquet)     (Analytics)   Engine
   └─────────────┴──────────────┘
                 │
              Grafana
                 │
         Prometheus + OTel
```

---

## Week-by-Week Plan

| Week | Deliverable | Status |
|------|-------------|--------|
| 1 | Probe MVP — single probe measures 10+ targets | 🟡 |
| 2 | Distributed probes + MinIO/Parquet raw data lake | ⬜ |
| 3 | Kafka ingestion + consumer failure/recovery demo | ⬜ |
| 4 | Flink streaming + event-time + watermarks | ⬜ |
| 5 | ClickHouse + historical analytics | ⬜ |
| 6 | Anomaly detection + incident lifecycle | ⬜ |
| 7 | Geographic analysis + traceroute | ⬜ |
| 8 | Observability + fault tolerance experiments | ⬜ |
| 9 | Benchmarking + final demo + documentation | ⬜ |

---

## Repository Structure

```
DE Project/
├── probe/
│   ├── netscope_probe/
│   │   ├── __init__.py
│   │   └── probe.py          ← core measurement engine (M1)
│   └── tests/
│       └── test_probe.py     ← TEST-001, TEST-002
│
├── schemas/
│   └── measurement.py        ← canonical Measurement + Heartbeat dataclasses
│
├── targets/
│   └── targets.yaml          ← ~20 measurement targets
│
├── scripts/
│   └── sweep_and_stats.py    ← multi-round sweep + p50/p95/p99 summary
│
├── docs/
│   └── architecture.md       ← (Week 1 deliverable)
│
└── README.md
```

---

## Quick Start — Week 1

### Prerequisites

```bash
pip install pyyaml pytest
```

### Run the probe against a single target

```bash
cd "DE Project"
python probe/netscope_probe/probe.py --probe-id india-01 --target github.com
```

### Run a full sweep over all targets

```bash
python probe/netscope_probe/probe.py \
    --probe-id india-01 \
    --targets-file targets/targets.yaml \
    --interval 30 \
    --output results/measurements.jsonl
```

### Run multi-round sweep + statistics

```bash
python scripts/sweep_and_stats.py \
    --probe-id india-01 \
    --rounds 10 \
    --interval 30 \
    --output results/week1.jsonl
```

### Run tests

```bash
cd probe
python -m pytest tests/ -v                      # unit tests only
python -m pytest tests/ -v -m integration       # + live network tests
```

---

## Measurement Schema

```json
{
  "measurement_id": "uuid-v4",
  "probe_id": "india-01",
  "target_id": "github-com",
  "target": "github.com",
  "timestamp": "2026-11-16T00:00:00.000Z",
  "dns_ms": 18.4,
  "tcp_ms": 32.1,
  "tls_ms": 47.2,
  "ttfb_ms": 81.3,
  "total_ms": 104.8,
  "http_status": 200,
  "response_bytes": 18291,
  "error_type": null,
  "timed_out": false
}
```

---

## Team

| Member | Primary Area |
|--------|-------------|
| MEMBER_1 | Probe & Networking |
| MEMBER_2 | Data Platform (Kafka · Flink · Storage) |
| MEMBER_3 | Analytics & Observability |

---

## Technology Stack

| Component | Technology |
|-----------|-----------|
| Probe | Python 3.11+ |
| Ingestion | Apache Kafka |
| Stream Processing | Apache Flink |
| Data Lake | MinIO + Parquet |
| Analytical DB | ClickHouse |
| Dashboards | Grafana |
| Monitoring | Prometheus + OpenTelemetry |
| Containerization | Docker + Docker Compose |

---

## Important Notes

- **Be respectful of Internet services.** Normal HTTP/DNS/TCP requests at ~30s intervals only.
- **Never commit secrets.** Use environment variables or config files outside source control.
- **Measure everything.** Report actual observed values, not theoretical limits.
- **Week 9 is benchmarking only.** No new features in the final week.
