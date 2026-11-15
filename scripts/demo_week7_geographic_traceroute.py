"""
TEST-012 & TEST-013: Geographic & Route Analysis Live Demonstration

Demonstrates:
1. Live cross-probe geographic diagnosis:
   - Case A: Localized degradation in India -> Diagnosed as 'LIKELY REGIONAL DEGRADATION'
   - Case B: Synchronized degradation across IN, SG, EU -> Diagnosed as 'LIKELY GLOBAL OUTAGE'
2. Real-time traceroute execution against a live target (Cloudflare 1.1.1.1):
   - Hop-by-hop latency and router IP capture
   - Route signature hashing
"""

from __future__ import annotations

import logging
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from network_analysis.geographic.correlation_engine import (
    GeographicCorrelationEngine,
    RegionalObservation,
)
from network_analysis.traceroute.traceroute_engine import run_traceroute

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("netscope.demo_week7")


def run_demo() -> None:
    logger.info("=== STARTING WEEK 7 GEOGRAPHIC & ROUTE ANALYSIS OBSERVABLE ===")

    engine = GeographicCorrelationEngine()

    # -----------------------------------------------------------------------
    # Case A: Simulated Regional Degradation
    # -----------------------------------------------------------------------
    logger.info("\n--- Demonstration A: Regional Isolation ---")
    regional_obs = [
        RegionalObservation(probe_id="india-01", region="India", avg_latency_ms=640.0, p95_ms=710.0, is_degraded=True),
        RegionalObservation(probe_id="sg-01", region="Singapore", avg_latency_ms=105.0, p95_ms=115.0, is_degraded=False),
        RegionalObservation(probe_id="eu-01", region="Europe", avg_latency_ms=118.0, p95_ms=128.0, is_degraded=False),
        RegionalObservation(probe_id="us-01", region="US-East", avg_latency_ms=132.0, p95_ms=140.0, is_degraded=False),
    ]

    diag_a = engine.diagnose_target("github-com", "github.com", regional_obs)
    print(
        f"\n🌍 [GEOGRAPHIC DIAGNOSIS - CASE A]\n"
        f"   Target:         {diag_a.target}\n"
        f"   Classification: {diag_a.classification}\n"
        f"   Degraded:       {', '.join(diag_a.degraded_regions)}\n"
        f"   Healthy:        {', '.join(diag_a.healthy_regions)}\n"
        f"   Confidence:     {diag_a.confidence_score * 100}%\n"
        f"   Assessment:     {diag_a.summary}"
    )

    # -----------------------------------------------------------------------
    # Case B: Simulated Global Outage
    # -----------------------------------------------------------------------
    logger.info("\n--- Demonstration B: Global Origin Failure ---")
    global_obs = [
        RegionalObservation(probe_id="india-01", region="India", avg_latency_ms=720.0, p95_ms=780.0, is_degraded=True),
        RegionalObservation(probe_id="sg-01", region="Singapore", avg_latency_ms=690.0, p95_ms=740.0, is_degraded=True),
        RegionalObservation(probe_id="eu-01", region="Europe", avg_latency_ms=710.0, p95_ms=750.0, is_degraded=True),
        RegionalObservation(probe_id="us-01", region="US-East", avg_latency_ms=680.0, p95_ms=730.0, is_degraded=True),
    ]

    diag_b = engine.diagnose_target("github-com", "github.com", global_obs)
    print(
        f"\n🌍 [GEOGRAPHIC DIAGNOSIS - CASE B]\n"
        f"   Target:         {diag_b.target}\n"
        f"   Classification: {diag_b.classification}\n"
        f"   Degraded:       {', '.join(diag_b.degraded_regions)}\n"
        f"   Healthy:        {', '.join(diag_b.healthy_regions)}\n"
        f"   Confidence:     {diag_b.confidence_score * 100}%\n"
        f"   Assessment:     {diag_b.summary}"
    )

    # -----------------------------------------------------------------------
    # Part 2: Real-World Traceroute Hop-by-Hop Telemetry
    # -----------------------------------------------------------------------
    logger.info("\n--- Demonstration C: Live Network Traceroute (Google DNS 8.8.8.8) ---")
    route = run_traceroute(
        probe_id="india-01",
        target_id="dns-google",
        host="8.8.8.8",
        max_hops=4,
        timeout_per_hop_sec=1.0,
    )

    print(
        f"\n🛰 [TRACEROUTE ROUTE PATH]\n"
        f"   Probe:       {route.probe_id} -> Destination: {route.target}\n"
        f"   Timestamp:   {route.timestamp}\n"
        f"   Route Hash:  {route.route_hash}\n"
        f"   Total Hops:  {route.total_hops}\n"
        f"   Hops Captured:"
    )
    for hop in route.hops:
        latency_str = f"{hop.rtt_ms:.1f} ms" if hop.rtt_ms is not None else "* (Timed out)"
        print(f"      Hop {hop.hop_num:>2}: {hop.ip:<18} | Latency: {latency_str}")

    logger.info("\n=== WEEK 7 GEOGRAPHIC & ROUTE ANALYSIS OBSERVABLE COMPLETED ===")


if __name__ == "__main__":
    run_demo()
