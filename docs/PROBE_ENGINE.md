# NetScope Active Probe Engine

The NetScope Active Probe Engine (`probe/netscope_probe/probe.py`) is a lightweight, non-intrusive network telemetry agent responsible for conducting high-precision, protocol-level active measurements against designated Internet targets.

---

## 1. Protocol Measurement Phases

Unlike basic HTTP ping utilities that report only total elapsed wall-clock time, NetScope instruments every stage of the networking handshake down to microsecond resolution:

```
[Target Sweep Initiated]
          │
          ▼
   1. DNS Resolution        socket.getaddrinfo()
          │                 Records: dns_lookup_time_ms, resolved_ip
          ▼
   2. TCP Connect           socket.create_connection()
          │                 Records: tcp_connect_time_ms
          ▼
   3. TLS Handshake         ssl.create_default_context().wrap_socket()
          │                 Records: tls_handshake_time_ms, tls_version
          ▼
   4. HTTP Request & TTFB   Send GET / HTTP/1.1 -> Receive First Byte
          │                 Records: http_ttfb_ms, http_status_code
          ▼
   5. Response Truncation   Stream payload up to max_bytes cap
                            Records: response_size_bytes, total_latency_ms
```

### Measurement Breakdown Details
1. **DNS Resolution (`dns_lookup_time_ms`)**:
   - Resolved using standard POSIX `socket.getaddrinfo` with socket type `SOCK_STREAM`.
   - Captures the IPv4/IPv6 address of the target and records name server resolution time.
2. **TCP Connection (`tcp_connect_time_ms`)**:
   - Opens a raw TCP socket to the resolved IP address on the designated port (80 or 443).
   - Measures pure transport-layer round-trip time ($1 \times \text{RTT}$) before any application negotiation occurs.
3. **TLS Handshake (`tls_handshake_time_ms`)**:
   - For HTTPS targets, wraps the raw TCP socket in an SSL context (`ssl.create_default_context()`).
   - Measures cryptographic handshake negotiation, certificate validation, and cipher establishment ($1\text{--}2 \times \text{RTT}$).
4. **Time-to-First-Byte (`http_ttfb_ms`)**:
   - Emits a standard HTTP `GET` request with headers `User-Agent: NetScope-Probe/0.1.0` and `Connection: close`.
   - Measures elapsed time until the server returns the first byte of the HTTP response header.
5. **Total Latency (`total_latency_ms`)**:
   - The sum of all active protocol stages:
     $$\text{Total Latency} = T_{\text{DNS}} + T_{\text{TCP}} + T_{\text{TLS}} + T_{\text{TTFB}}$$

---

## 2. Safety Guards & Fault Handling

To prevent probe threads from hanging or exhausting host resources, the probe enforces strict safety boundaries:

- **Socket-Level Timeouts**:
  - Each target defines a timeout (default: 10 seconds in `targets.yaml`).
  - `socket.settimeout(timeout)` prevents hung connections or half-open socket leaks.
- **Response Byte Capping (`max_bytes`)**:
  - The probe measures latency, not content download bandwidth.
  - Payloads are capped at `max_bytes` (default: 65,536 bytes / 64 KB). After 64 KB is read from the socket stream, the socket is immediately closed.
- **Categorical Error Classification**:
  When a measurement fails, the probe sets `is_success = False` and populates `error_type` with one of the following canonical error tags:
  - `dns_failure`: Name resolution failed (`socket.gaierror`).
  - `tcp_refused`: Remote host rejected the TCP handshake (`ConnectionRefusedError`).
  - `connect_timeout`: Host failed to respond within target timeout (`socket.timeout`).
  - `tls_error`: SSL certificate validation failure or handshake mismatch (`ssl.SSLError`).
  - `http_error`: Target returned an HTTP status code $\ge 500$.

---

## 3. Honest Probe Fleet & Local Identification

To maintain engineering transparency, probe instances running locally on a workstation do not claim synthetic cloud geographic vantage points. The fleet is explicitly named:
- **`probe-01` through `probe-10`**
- Each container sets `PROBE_ID=probe-XX` via environment variables.
- Heartbeats broadcast `probe_id`, current version (`0.1.0`), health status (`healthy`), and cumulative success/failure metrics.

---

## 4. Balanced Target Sharding Formula

The fleet monitors **50 distinct Internet targets** defined in `targets/targets.yaml`. Each probe is assigned a balanced subset of targets satisfying the following mathematical constraints:
- **10 total probes**
- **15 targets assigned to each probe**
- **No target monitored by more than 3 probes**

### Mathematical Assignment Formula
For 50 targets with index $i \in \{0, 1, \dots, 49\}$, the 3 probes assigned to target $i$ are computed cyclically:
$$p_1 = (3i \bmod 10) + 1$$
$$p_2 = ((3i + 1) \bmod 10) + 1$$
$$p_3 = ((3i + 2) \bmod 10) + 1$$

$$\text{Total assignments} = 50 \text{ targets} \times 3 \text{ probes/target} = 150 \text{ slots}$$
$$\text{Targets per probe} = \frac{150}{10} = 15 \text{ targets/probe}$$

### Probe Target Filter Logic
In `probe/netscope_probe/probe.py`, `load_targets_from_yaml(path, probe_id)` filters targets dynamically:
```python
if probe_id:
    assigned_probes = t.get("probes", [])
    if assigned_probes and probe_id not in assigned_probes:
        continue  # Skip target not assigned to this probe
```

---

## 5. Probe Execution Modes

### Mode A: Standalone Single Sweep (Development & Test)
Run a single sweep from your terminal to inspect immediate timing output:
```powershell
python probe/netscope_probe/probe.py --targets targets/targets.yaml --probe-id probe-01 --rounds 1
```

### Mode B: Continuous Multi-Probe Fleet (Docker Compose)
Launch all 10 probes to run continuously in the background:
```powershell
docker compose up -d probe-1 probe-2 probe-3 probe-4 probe-5 probe-6 probe-7 probe-8 probe-9 probe-10
```

To view real-time logs from any individual probe:
```powershell
docker logs netscope-probe-01 -f --tail 20
```

To stop all 10 probes:
```powershell
docker compose stop probe-1 probe-2 probe-3 probe-4 probe-5 probe-6 probe-7 probe-8 probe-9 probe-10
```
