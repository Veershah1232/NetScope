"""
NetScope Traceroute Engine — M8 / FR_14

Collects hop-by-hop route path and latency from probe to destination targets.
Features:
- Cross-platform support (invokes system tracert on Windows or traceroute on Linux)
- Fast timeout and hop limit guards (prevents hanging on firewall blocks)
- Structured Hop output: hop number, IP, round-trip times (ms)
- Route signature hashing: detects route changes over time
- Graceful degradation: handles blocked ICMP/UDP hops (* * * Request timed out)
"""

from __future__ import annotations

import hashlib
import logging
import platform
import re
import subprocess
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger("netscope.traceroute")


@dataclass
class Hop:
    hop_num: int
    ip: str                       # Resolved IP or "*" if timed out
    rtt_ms: Optional[float]       # Average round-trip time in ms (or None if timed out)


@dataclass
class RoutePath:
    probe_id: str
    target_id: str
    target: str
    timestamp: str
    hops: list[Hop]
    total_hops: int
    destination_reached: bool
    route_hash: str               # MD5 hash of sequential IP hops to detect route shifts

    def to_dict(self) -> dict:
        d = asdict(self)
        d["hops"] = [asdict(h) for h in self.hops]
        return d


def compute_route_hash(hops: list[Hop]) -> str:
    """Computes a stable hash representing the network path signature."""
    path_signature = "->".join(f"{h.hop_num}:{h.ip}" for h in hops if h.ip != "*")
    return hashlib.md5(path_signature.encode("utf-8")).hexdigest()[:12]


def run_traceroute(
    probe_id: str,
    target_id: str,
    host: str,
    max_hops: int = 15,
    timeout_per_hop_sec: float = 2.0,
) -> RoutePath:
    """
    Executes a traceroute against the target host and returns structured RoutePath.
    """
    is_windows = platform.system().lower() == "windows"
    timeout_ms = int(timeout_per_hop_sec * 1000)

    if is_windows:
        cmd = ["tracert", "-d", "-h", str(max_hops), "-w", str(timeout_ms), host]
    else:
        cmd = ["traceroute", "-n", "-m", str(max_hops), "-w", str(int(timeout_per_hop_sec)), host]

    logger.info("Executing traceroute for %s (probe=%s, max_hops=%d)...", host, probe_id, max_hops)

    hops: list[Hop] = []
    destination_reached = False

    try:
        proc = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=max(30.0, max_hops * timeout_per_hop_sec * 3),
            check=False,
        )
        output = proc.stdout

        # Parse output line by line
        # Typical Windows line: "  1     2 ms     2 ms     2 ms  172.31.64.3"
        # Or timeout:          "  3     *        *        *     Request timed out."
        for line in output.splitlines():
            line = line.strip()
            if not line:
                continue

            # Look for lines starting with hop number
            m = re.match(r"^(\d+)\s+(.+)$", line)
            if not m:
                continue

            hop_num = int(m.group(1))
            rest = m.group(2)

            if "Request timed out" in rest or "* * *" in rest:
                hops.append(Hop(hop_num=hop_num, ip="*", rtt_ms=None))
                continue

            # Extract latencies and IP address
            ip_match = re.search(r"(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})", rest)
            ip_str = ip_match.group(1) if ip_match else "*"

            times_ms = [float(t) for t in re.findall(r"([0-9]+(?:\.[0-9]+)?)\s*ms", rest)]
            avg_rtt = round(sum(times_ms) / len(times_ms), 2) if times_ms else None

            hops.append(Hop(hop_num=hop_num, ip=ip_str, rtt_ms=avg_rtt))

            # If destination host/IP matches or reached trace complete
            if ip_str != "*" and ip_str in host:
                destination_reached = True

    except Exception as exc:
        logger.warning("Traceroute execution error for %s: %s", host, exc)

    route_hash = compute_route_hash(hops)

    return RoutePath(
        probe_id=probe_id,
        target_id=target_id,
        target=host,
        timestamp=datetime.now(timezone.utc).isoformat(),
        hops=hops,
        total_hops=len(hops),
        destination_reached=destination_reached,
        route_hash=route_hash,
    )
