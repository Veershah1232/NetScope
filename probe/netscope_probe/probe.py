"""
NetScope Probe Engine — M1

Measures DNS, TCP, TLS, and HTTP performance for a single target.

Usage:
    python -m netscope_probe.probe --probe-id india-01 --target github.com
    python -m netscope_probe.probe --probe-id india-01 --targets-file targets/targets.yaml --interval 30
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
from urllib.parse import urlparse

import urllib.request
import urllib.error

# Add project root to path so schemas can be imported
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from schemas.measurement import Measurement, ProbeHeartbeat

logger = logging.getLogger("netscope.probe")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DEFAULT_TIMEOUT_S = 10.0      # hard timeout per measurement
DEFAULT_INTERVAL_S = 30       # seconds between sweep rounds
DEFAULT_PROBE_ID = "local-01"
PROBE_VERSION = "0.1.0"


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
) -> Measurement:
    """
    Perform a full DNS → TCP → TLS → HTTP measurement for a single target.

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

    url = f"{scheme}://{host}:{port}{path}" if port not in (80, 443) else f"{scheme}://{host}{path}"

    t0 = time.perf_counter()

    # ------------------------------------------------------------------
    # Stage 1: DNS resolution
    # ------------------------------------------------------------------
    try:
        dns_start = time.perf_counter()
        addrs = socket.getaddrinfo(host, port, proto=socket.IPPROTO_TCP)
        dns_end = time.perf_counter()
        m.dns_ms = round((dns_end - dns_start) * 1000, 2)

        if not addrs:
            m.error_type = "DNS_NO_RESULTS"
            return m

        addr_family, _, _, _, sockaddr = addrs[0]
        resolved_ip = sockaddr[0]

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
        tcp_end = time.perf_counter()
        m.tcp_ms = round((tcp_end - tcp_start) * 1000, 2)

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
            tls_end = time.perf_counter()
            m.tls_ms = round((tls_end - tls_start) * 1000, 2)
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
    # Stage 4: HTTP request (TTFB + total)
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

        # Read until we get the first byte of the response body (TTFB)
        response_data = b""
        first_byte = False
        ttfb_ms_local = None

        while True:
            try:
                chunk = conn_sock.recv(4096)
                if not chunk:
                    break
                if not first_byte:
                    ttfb_ms_local = round((time.perf_counter() - http_start) * 1000, 2)
                    first_byte = True
                response_data += chunk
            except socket.timeout:
                m.timed_out = True
                m.error_type = "HTTP_TIMEOUT"
                break

        http_end = time.perf_counter()
        m.total_ms = round((http_end - t0) * 1000, 2)
        m.ttfb_ms = ttfb_ms_local

        # Parse status line
        if response_data:
            status_line = response_data.split(b"\r\n")[0].decode(errors="replace")
            parts = status_line.split(" ", 2)
            if len(parts) >= 2:
                try:
                    m.http_status = int(parts[1])
                except ValueError:
                    m.error_type = "HTTP_STATUS_PARSE_ERROR"
            m.response_bytes = len(response_data)

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
    """Load target list from targets.yaml."""
    try:
        import yaml
    except ImportError:
        raise ImportError("PyYAML is required: pip install pyyaml")

    with open(path, "r") as f:
        data = yaml.safe_load(f)
    return data.get("targets", [])


def load_targets_from_hosts(hosts: list[str]) -> list[dict]:
    """Build a minimal target list from bare hostnames."""
    targets = []
    for host in hosts:
        targets.append({
            "id": host.replace(".", "-"),
            "host": host,
            "scheme": "https",
            "path": "/",
        })
    return targets


# ---------------------------------------------------------------------------
# Probe sweep (one round over all targets)
# ---------------------------------------------------------------------------

def run_sweep(
    probe_id: str,
    targets: list[dict],
    timeout: float = DEFAULT_TIMEOUT_S,
    output_file: Optional[str] = None,
) -> list[Measurement]:
    """Measure all targets once. Returns list of Measurement objects."""
    results: list[Measurement] = []

    for t in targets:
        host = t.get("host", "")
        if not host:
            logger.warning("Target missing 'host', skipping: %s", t)
            continue

        logger.info("Measuring %s ...", host)
        m = measure_target(
            probe_id=probe_id,
            target_id=t.get("id", host.replace(".", "-")),
            host=host,
            scheme=t.get("scheme", "https"),
            path=t.get("path", "/"),
            port=t.get("port"),
            timeout=timeout,
        )

        # Validate
        errors = m.validate()
        if errors:
            logger.warning("Measurement validation errors for %s: %s", host, errors)

        results.append(m)
        _print_measurement(m)

        if output_file:
            _append_jsonl(output_file, m.to_dict())

    return results


def _print_measurement(m: Measurement) -> None:
    """Pretty-print a measurement to stdout."""
    status_str = str(m.http_status) if m.http_status else "N/A"
    error_str = f"  ERROR: {m.error_type}" if m.error_type else ""
    timeout_str = "  TIMED OUT" if m.timed_out else ""

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
    if val is None:
        return "—"
    return f"{val:.1f} ms"


def _append_jsonl(path: str, record: dict) -> None:
    with open(path, "a") as f:
        f.write(json.dumps(record) + "\n")


# ---------------------------------------------------------------------------
# Heartbeat
# ---------------------------------------------------------------------------

def emit_heartbeat(probe_id: str, measurements_sent: int, failed: int) -> ProbeHeartbeat:
    hb = ProbeHeartbeat(
        probe_id=probe_id,
        measurements_sent=measurements_sent,
        failed_measurements=failed,
    )
    logger.info("HEARTBEAT: %s", json.dumps(hb.to_dict()))
    return hb


# ---------------------------------------------------------------------------
# CLI entrypoint
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="NetScope Probe — measures Internet targets and reports timings."
    )
    parser.add_argument("--probe-id", default=DEFAULT_PROBE_ID, help="Unique probe identifier")
    parser.add_argument("--target", help="Single target hostname (e.g. github.com)")
    parser.add_argument("--targets-file", help="Path to targets.yaml")
    parser.add_argument("--interval", type=float, default=0,
                        help="Seconds between sweeps (0 = single run)")
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT_S,
                        help="Per-measurement timeout in seconds")
    parser.add_argument("--output", help="Append measurements as JSONL to this file")
    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    # Build target list
    if args.target:
        targets = load_targets_from_hosts([args.target])
    elif args.targets_file:
        targets = load_targets_from_yaml(args.targets_file)
    else:
        parser.error("Provide --target or --targets-file")

    logger.info("NetScope Probe %s starting. probe_id=%s, targets=%d",
                PROBE_VERSION, args.probe_id, len(targets))

    total_sent = 0
    total_failed = 0
    round_num = 0

    while True:
        round_num += 1
        logger.info("--- Sweep round %d ---", round_num)

        results = run_sweep(
            probe_id=args.probe_id,
            targets=targets,
            timeout=args.timeout,
            output_file=args.output,
        )

        sent = len(results)
        failed = sum(1 for r in results if not r.is_success())
        total_sent += sent
        total_failed += failed

        # Heartbeat after each sweep
        emit_heartbeat(args.probe_id, total_sent, total_failed)

        if args.interval <= 0:
            break

        logger.info("Sleeping %.0f seconds until next sweep...", args.interval)
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
