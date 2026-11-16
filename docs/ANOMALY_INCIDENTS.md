# Anomaly Detection, Incident Lifecycle & Network Intelligence

This document details NetScope's analytical intelligence suite: statistical anomaly scoring, deterministic incident lifecycle state machine, chaos injection testing, geographic correlation, and route signature flapping analysis.

---

## 1. Statistical Anomaly Detection Engine

Implemented in `anomaly_engine/detectors/statistical_detector.py`.

### 1.1 Welford's Algorithm for Single-Pass Baseline
Rather than storing thousands of historical raw samples in memory to calculate standard deviation, NetScope uses **Welford's algorithm** for numerically stable, online rolling mean and variance:

For each new observation $x$ at step $k$:
$$M_k = M_{k-1} + \frac{x - M_{k-1}}{k}$$
$$S_k = S_{k-1} + (x - M_{k-1})(x - M_k)$$
$$\sigma_k = \sqrt{\frac{S_k}{k - 1}}$$

- **Space Complexity**: $O(1)$ per target.
- **Time Complexity**: $O(1)$ update per measurement.

### 1.2 Dynamic Z-Score Deviation
When a new measurement arrives, its standardized deviation is computed:
$$Z = \frac{x - \mu}{\sigma}$$
- If $Z \ge 3.0$, the measurement deviates by more than three standard deviations from historical behavior and is flagged as an anomaly.

### 1.3 Multi-Signal Composite Anomaly Scoring
Relying solely on latency produces false alarms during harmless network bursts. NetScope evaluates a weighted composite score across three independent telemetry vectors:

$$S_{\text{composite}} = w_{\text{lat}} \cdot \min(Z / 5.0, 1.0) + w_{\text{err}} \cdot R_{\text{error}} + w_{\text{loss}} \cdot R_{\text{loss}}$$
- Default Weights: $w_{\text{lat}} = 0.5$, $w_{\text{err}} = 0.3$, $w_{\text{loss}} = 0.2$.
- If $S_{\text{composite}} \ge 0.65$, an actionable alert is dispatched to Kafka topic `alerts`.

---

## 2. Incident Lifecycle State Machine (FSM)

Implemented in `anomaly_engine/incident_manager/lifecycle.py`.

Raw alerting systems suffer from **alert fatigue and flapping** (bouncing between OK and ALERT every few seconds). NetScope implements a 4-state deterministic Finite State Machine (FSM) with transition hysteresis:

```
                  +--------------------+
                  |       NORMAL       |
                  +--------------------+
                            │
                            │ 3 Consecutive Anomaly Alerts
                            ▼
                  +--------------------+
                  |       ACTIVE       | ◄───┐
                  +--------------------+     │
                            │                │ Anomaly recurred
                            │ 2 Clean Sweeps │ during recovery
                            ▼                │
                  +--------------------+     │
                  |     RECOVERING     | ────┘
                  +--------------------+
                            │
                            │ 5 Clean Sweeps in a row
                            ▼
                  +--------------------+
                  |      RESOLVED      |
                  +--------------------+
```

### State Transitions & Hysteresis Guards
1. **`NORMAL` $\to$ `ACTIVE`**: Requires **3 consecutive anomalous sweeps**. A single transient packet spike cannot trigger an incident.
2. **`ACTIVE` $\to$ `RECOVERING`**: When targets return below threshold for **2 consecutive sweeps**, the incident transitions to `RECOVERING` (suppressing escalated alerts while continuing active monitoring).
3. **`RECOVERING` $\to$ `ACTIVE`**: If an anomaly recurs while in recovery, the system immediately reverts to `ACTIVE`.
4. **`RECOVERING` $\to$ `RESOLVED`**: Requires **5 consecutive healthy sweeps** to declare complete resolution and archive the incident record.

---

## 3. Controlled Chaos Injection Server

Implemented in `controlled_endpoint/server.py`.

To deterministically verify the anomaly detector and incident FSM without waiting for a real-world outage, NetScope includes a local chaos server running on port `8088`:
- `GET /health`: Returns 200 OK with normal sub-5ms response.
- `POST /inject/delay?ms=650`: Injects artificial network latency into subsequent requests.
- `POST /inject/error?rate=0.8`: Injects 80% HTTP 500 internal server errors.
- `POST /reset`: Clears all chaos conditions back to baseline.

---

## 4. Geographic Correlation Engine

Implemented in `network_analysis/geographic/correlation_engine.py`.

When target degradation is observed, NetScope correlates failure distributions across multiple probes to identify the scope of the event:

$$\text{Global Failure Ratio} = \frac{\text{Probes observing failure}}{\text{Total active probes measuring target}}$$

- **Case A: `LIKELY REGIONAL DEGRADATION`**
  - Only probes in a specific geographic cluster or subnet report timeouts, while the remaining probes report healthy latencies.
  - *Diagnosis*: Localized ISP fiber cut, regional transit congestion, or BGP route leak.
- **Case B: `LIKELY GLOBAL OUTAGE`**
  - $\ge 70\%$ of all probes across the fleet simultaneously report failures.
  - *Diagnosis*: Target origin server collapse, CDN edge failure, or global DNS outage.

---

## 5. Traceroute & MD5 Route Signature Flapping

Implemented in `network_analysis/traceroute/traceroute_engine.py`.

NetScope captures hop-by-hop network routing using platform ICMP/UDP traceroute and generates an MD5 hash of the sequence of intermediate Autonomous System (AS) IP hops:

$$\text{Route Signature} = \text{MD5}\Big(\text{hop}_1 \to \text{hop}_2 \to \dots \to \text{hop}_n\Big)$$

### Path Flapping Detection
- Stable routes produce consistent MD5 signatures (e.g. `8a9cd0363941`).
- If an upstream transit provider shifts traffic due to BGP flapping or link maintenance, the hop sequence changes.
- NetScope flags **Route Flapping Events** whenever consecutive sweeps produce alternating route hashes accompanied by latency shifts.
