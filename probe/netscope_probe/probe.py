"""
NetScope Probe Engine — M1 (Week 2 revision)

Changes from Week 1:
  - Respects per-target `timeout` and `max_bytes` from targets.yaml
  - Skips targets where `enabled: false`
  - Response body read is capped at max_bytes (default 65 536) to avoid
    downloading multi-MB pages (Flipkart, IRCTC etc.)
  - All timing and logic unchanged
"""

from __future__ import annotations

import argparse
import json
import logging
import socket
import ssl
import time
import uuid
from datetime import datetime, timezone
from typing import Optional

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from schemas.measurement import Measurement, ProbeHeartbeat

logger = logging.getLogger("netscope.probe")

DEFAULT_TIMEOUT_S    = 10.0
DEFAULT_INTERVAL_S   = 30
DEFAULT_PROBE_ID     = "local-01"
DEFAULT_MAX_BYTES    = 65_536   # 64 KB cap on response body read
PROBE_VERSION        = "0.1.0"


# ---------------------------------------------------------------------------
# Measurement engine
# ---------------------------------------------------------------------------

def measure_target(
    probe_id: str,
    target_id: str,
    host: str,
    scheme: str = "https",
    path: str = "/",
    port: Optional[int] = None,
    timeout: float = DEFAULT_TIMEOUT_S,
    max_bytes: int = DEFAULT_MAX_BYTES,
) -> Measurement:
    """
    Perform DNS → TCP → TLS → HTTP measurement for one target.
    Returns a Measurement regardless of success or failure.
    All timings are in milliseconds.
    """
    m = Measurement(
        probe_id=probe_id,
        target_id=target_id,
        target=host,
        timestamp=datetime.now(timezone.utc).isoformat(),
    )

    if port is None:
        port = 443 if scheme == "https" else 80

    t0 = time.perf_counter()

    # ------------------------------------------------------------------
    # Stage 1: DNS resolution
    # ------------------------------------------------------------------
    dns_start = time.perf_counter()
    try:
        addrs = socket.getaddrinfo(host, port, proto=socket.IPPROTO_TCP)
        m.dns_ms = round((time.perf_counter() - dns_start) * 1000, 2)

        if not addrs:
            m.error_type = "DNS_NO_RESULTS"
            return m

        addr_family, _, _, _, sockaddr = addrs[0]

    except socket.gaierror as exc:
        m.dns_ms = round((time.perf_counter() - dns_start) * 1000, 2)
        m.error_type = f"DNS_FAILED: {exc}"
        return m
    except Exception as exc:
        m.error_type = f"DNS_ERROR: {exc}"
        return m

    # ------------------------------------------------------------------
    # Stage 2: TCP connection
    # ------------------------------------------------------------------
    try:
        sock = socket.socket(addr_family, socket.SOCK_STREAM)
        sock.settimeout(timeout)

        tcp_start = time.perf_counter()
        sock.connect(sockaddr)
        m.tcp_ms = round((time.perf_counter() - tcp_start) * 1000, 2)

    except socket.timeout:
        m.timed_out = True
        m.error_type = "TCP_TIMEOUT"
        return m
    except ConnectionRefusedError:
        m.error_type = "TCP_REFUSED"
        return m
    except Exception as exc:
        m.error_type = f"TCP_ERROR: {exc}"
        return m

    # ------------------------------------------------------------------
    # Stage 3: TLS handshake (HTTPS only)
    # ------------------------------------------------------------------
    if scheme == "https":
        try:
            ctx = ssl.create_default_context()
            tls_start = time.perf_counter()
            tls_sock = ctx.wrap_socket(sock, server_hostname=host)
            m.tls_ms = round((time.perf_counter() - tls_start) * 1000, 2)
            conn_sock = tls_sock
        except ssl.SSLError as exc:
            m.error_type = f"TLS_ERROR: {exc}"
            sock.close()
            return m
        except socket.timeout:
            m.timed_out = True
            m.error_type = "TLS_TIMEOUT"
            sock.close()
            return m
        except Exception as exc:
            m.error_type = f"TLS_ERROR: {exc}"
            sock.close()
            return m
    else:
        conn_sock = sock

    # ------------------------------------------------------------------
    # Stage 4: HTTP request (TTFB + total, capped at max_bytes)
    # ------------------------------------------------------------------
    try:
        http_request = (
            f"GET {path} HTTP/1.1\r\n"
            f"Host: {host}\r\n"
            f"User-Agent: NetScope-Probe/{PROBE_VERSION}\r\n"
            f"Accept: */*\r\n"
            f"Connection: close\r\n"
            f"\r\n"
        )
        conn_sock.settimeout(timeout)
        conn_sock.sendall(http_request.encode())

        http_start = time.perf_counter()
        response_data = b""
        first_byte_captured = False

        while len(response_data) < max_bytes:
            try:
                chunk = conn_sock.recv(4096)
                if not chunk:
                    break
                if not first_byte_captured:
                    m.ttfb_ms = round((time.perf_counter() - http_start) * 1000, 2)
                    first_byte_captured = True
                response_data += chunk
            except socket.timeout:
                m.timed_out = True
                m.error_type = "HTTP_TIMEOUT"
                break

        m.total_ms = round((time.perf_counter() - t0) * 1000, 2)
        m.response_bytes = len(response_data)   # bytes actually read (may be capped)

        # Parse HTTP status line
        if response_data:
            try:
                status_line = response_data.split(b"\r\n")[0].decode(errors="replace")
                parts = status_line.split(" ", 2)
                if len(parts) >= 2:
                    m.http_status = int(parts[1])
            except (ValueError, IndexError):
                m.error_type = "HTTP_STATUS_PARSE_ERROR"

    except socket.timeout:
        m.timed_out = True
        m.error_type = "HTTP_TIMEOUT"
    except Exception as exc:
        m.error_type = f"HTTP_ERROR: {exc}"
    finally:
        try:
            conn_sock.close()
        except Exception:
            pass

    return m


# ---------------------------------------------------------------------------
# Target loader
# ---------------------------------------------------------------------------

def load_targets_from_yaml(path: str) -> list[dict]:
    """Load enabled targets from targets.yaml."""
    try:
        import yaml
    except ImportError:
        raise ImportError("PyYAML required: pip install pyyaml")

    with open(path, "r") as f:
        data = yaml.safe_load(f)

    all_targets = data.get("targets", [])
    # Filter out disabled targets
    enabled = [t for t in all_targets if t.get("enabled", True)]
    skipped = len(all_targets) - len(enabled)
    if skipped:
        logger.info("Skipping %d disabled targets", skipped)
    return enabled


def load_targets_from_hosts(hosts: list[str]) -> list[dict]:
    return [
        {"id": h.replace(".", "-"), "host": h, "scheme": "https", "path": "/"}
        for h in hosts
    ]


# ---------------------------------------------------------------------------
# Probe sweep
# ---------------------------------------------------------------------------

def run_sweep(
    probe_id: str,
    targets: list[dict],
    default_timeout: float = DEFAULT_TIMEOUT_S,
    default_max_bytes: int = DEFAULT_MAX_BYTES,
    output_file: Optional[str] = None,
) -> list[Measurement]:
    results: list[Measurement] = []

    for t in targets:
        host = t.get("host", "")
        if not host:
            logger.warning("Target missing 'host', skipping: %s", t)
            continue

        # Per-target overrides
        timeout   = t.get("timeout", default_timeout)
        max_bytes = t.get("max_bytes", default_max_bytes)

        logger.info("Measuring %s (timeout=%.0fs, max_bytes=%d) ...",
                    host, timeout, max_bytes)

        m = measure_target(
            probe_id=probe_id,
            target_id=t.get("id", host.replace(".", "-")),
            host=host,
            scheme=t.get("scheme", "https"),
            path=t.get("path", "/"),
            port=t.get("port"),
            timeout=timeout,
            max_bytes=max_bytes,
        )

        errors = m.validate()
        if errors:
            logger.warning("Validation errors for %s: %s", host, errors)

        results.append(m)
        _print_measurement(m)

        if output_file:
            _append_jsonl(output_file, m.to_dict())

    return results


def _print_measurement(m: Measurement) -> None:
    status_str  = str(m.http_status) if m.http_status else "N/A"
    error_str   = f"  ⚠ {m.error_type}" if m.error_type else ""
    timeout_str = "  ⏱ TIMED OUT" if m.timed_out else ""
    print(
        f"\n[{m.probe_id}] → {m.target}\n"
        f"  DNS:    {_fmt_ms(m.dns_ms)}\n"
        f"  TCP:    {_fmt_ms(m.tcp_ms)}\n"
        f"  TLS:    {_fmt_ms(m.tls_ms)}\n"
        f"  TTFB:   {_fmt_ms(m.ttfb_ms)}\n"
        f"  Total:  {_fmt_ms(m.total_ms)}\n"
        f"  Status: {status_str}  Bytes: {m.response_bytes or 'N/A'}"
        f"{error_str}{timeout_str}"
    )


def _fmt_ms(val: Optional[float]) -> str:
    return f"{val:.1f} ms" if val is not None else "—"


def _append_jsonl(path: str, record: dict) -> None:
    with open(path, "a") as f:
        f.write(json.dumps(record) + "\n")


# ---------------------------------------------------------------------------
# Heartbeat
# ---------------------------------------------------------------------------

def emit_heartbeat(
    probe_id: str,
    measurements_sent: int,
    failed: int,
    output_file: Optional[str] = None,
) -> ProbeHeartbeat:
    hb = ProbeHeartbeat(
        probe_id=probe_id,
        measurements_sent=measurements_sent,
        failed_measurements=failed,
    )
    logger.info("HEARTBEAT: %s", json.dumps(hb.to_dict()))
    if output_file:
        _append_jsonl(output_file, {"_type": "heartbeat", **hb.to_dict()})
    return hb


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="NetScope Probe")
    parser.add_argument("--probe-id",     default=os.environ.get("PROBE_ID", DEFAULT_PROBE_ID))
    parser.add_argument("--target",       help="Single hostname")
    parser.add_argument("--targets-file", default="targets/targets.yaml")
    parser.add_argument("--interval",     type=float, default=float(os.environ.get("PROBE_INTERVAL", "0")))
    parser.add_argument("--timeout",      type=float, default=DEFAULT_TIMEOUT_S)
    parser.add_argument("--max-bytes",    type=int,   default=DEFAULT_MAX_BYTES)
    parser.add_argument("--output",       default=os.environ.get("PROBE_OUTPUT", ""))
    parser.add_argument("--log-level",    default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    if args.target:
        targets = load_targets_from_hosts([args.target])
    elif args.targets_file:
        targets = load_targets_from_yaml(args.targets_file)
    else:
        parser.error("Provide --target or --targets-file")

    output = args.output or None
    if output:
        os.makedirs(os.path.dirname(output) if os.path.dirname(output) else ".", exist_ok=True)

    logger.info("NetScope Probe %s | probe_id=%s | targets=%d | interval=%.0fs",
                PROBE_VERSION, args.probe_id, len(targets), args.interval)

    total_sent, total_failed, round_num = 0, 0, 0

    while True:
        round_num += 1
        logger.info("--- Sweep round %d ---", round_num)

        results = run_sweep(
            probe_id=args.probe_id,
            targets=targets,
            default_timeout=args.timeout,
            default_max_bytes=args.max_bytes,
            output_file=output,
        )

        sent   = len(results)
        failed = sum(1 for r in results if not r.is_success())
        total_sent   += sent
        total_failed += failed

        emit_heartbeat(args.probe_id, total_sent, total_failed, output_file=output)

        if args.interval <= 0:
            break

        logger.info("Next sweep in %.0f seconds...", args.interval)
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
