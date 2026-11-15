"""
CONTROLLED OUTAGE & ANOMALY RECOVERY DEMO — M7 / TEST-010 & TEST-011

The Mandatory Week 6 Live Outage & Recovery Demonstration:
1. Starts the in-process Controlled Test Endpoint on localhost:8080
2. Collects baseline measurements (Normal: ~15-20ms latency, Status 200, NORMAL)
3. Injects 600ms artificial latency delay into the controlled server
4. Probes measure the degraded endpoint -> Anomaly detected via Threshold, Baseline Ratio & Z-Score
5. Incident Manager fires: Transition to ACTIVE incident with CRITICAL severity
6. Removes the injected delay (/reset)
7. Probes observe normalized latency
8. Incident Manager transitions: ACTIVE -> RECOVERING -> RESOLVED
"""

from __future__ import annotations

import json
import logging
import threading
import time
import urllib.request
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from controlled_endpoint.server import run_controlled_server
from anomaly_engine.detectors.statistical_detector import StatisticalAnomalyDetector
from anomaly_engine.incident_manager.lifecycle import IncidentLifecycleManager, Incident
from probe.netscope_probe.probe import measure_target

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("netscope.controlled_outage_demo")

SERVER_PORT = 8088
SERVER_URL = f"http://127.0.0.1:{SERVER_PORT}"


def send_control_post(endpoint: str, data: dict) -> None:
    req = urllib.request.Request(
        f"{SERVER_URL}{endpoint}",
        data=json.dumps(data).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=3.0) as resp:
        resp.read()


def run_demo() -> None:
    logger.info("=== STARTING WEEK 6: CONTROLLED OUTAGE & AUTO-RECOVERY DEMO ===")

    # 1. Start Controlled Server in background thread on dedicated port
    server_thread = threading.Thread(target=run_controlled_server, args=(SERVER_PORT,), daemon=True)
    server_thread.start()
    time.sleep(1.0)  # Wait for socket to bind

    # 2. Initialize Anomaly Detector & Incident Manager
    detector = StatisticalAnomalyDetector(
        static_threshold_ms=300.0,
        rolling_baseline_multiplier=2.0,
        z_score_threshold=2.5,
        min_baseline_samples=3,
    )

    incident_log: list[str] = []

    def on_event(event_type: str, inc: Incident):
        msg = f"[{event_type}] Incident #{inc.incident_id[:8]} on {inc.target} (Severity: {inc.severity}, Status: {inc.status})"
        print(f"\n📢 INCIDENT EVENT: {msg}")
        incident_log.append(event_type)

    manager = IncidentLifecycleManager(
        recovery_rounds_threshold=2,
        on_incident_event=on_event,
    )

    # -----------------------------------------------------------------------
    # PHASE 1: Baseline Establishment (Normal Operation)
    # -----------------------------------------------------------------------
    logger.info("--- PHASE 1: Establishing Baseline (~20ms latency) ---")
    send_control_post("/reset", {})

    for i in range(5):
        m = measure_target("india-01", "netscope-ctrl", "127.0.0.1", scheme="http", path="/health", port=SERVER_PORT)
        eval_res = detector.evaluate_window(
            target_id="netscope-ctrl",
            avg_latency_ms=m.total_ms or 20.0,
            p95_ms=m.total_ms or 20.0,
            failure_count=0 if m.is_success() else 1,
            availability_pct=100.0 if m.is_success() else 0.0,
        )
        manager.evaluate_target_state("netscope-ctrl", "Controlled Endpoint", "India", "india-01", eval_res)
        print(f"Round {i+1} Normal: latency={m.total_ms}ms | Score: {eval_res.anomaly_score} | Status: {eval_res.severity}")
        time.sleep(0.3)

    assert len(manager.active_incidents) == 0, "No incidents should be active during normal baseline!"

    # -----------------------------------------------------------------------
    # PHASE 2: Inject Outage (+650ms Latency)
    # -----------------------------------------------------------------------
    logger.info("\n💥 --- PHASE 2: Injecting +650ms Artificial Latency ---")
    send_control_post("/inject/delay", {"delay_ms": 650.0})

    for i in range(3):
        m = measure_target("india-01", "netscope-ctrl", "127.0.0.1", scheme="http", path="/health", port=SERVER_PORT)
        eval_res = detector.evaluate_window(
            target_id="netscope-ctrl",
            avg_latency_ms=m.total_ms or 650.0,
            p95_ms=m.total_ms or 650.0,
            failure_count=0 if m.is_success() else 1,
            availability_pct=100.0 if m.is_success() else 0.0,
        )
        inc = manager.evaluate_target_state("netscope-ctrl", "Controlled Endpoint", "India", "india-01", eval_res)
        print(f"Degraded Round {i+1}: latency={m.total_ms}ms | Score: {eval_res.anomaly_score} | Severity: {eval_res.severity}")
        for r in eval_res.reasons:
            print(f"   ↳ Evidence: {r}")
        time.sleep(0.3)

    assert len(manager.active_incidents) == 1, "Incident must be OPEN and ACTIVE!"
    active_inc = list(manager.active_incidents.values())[0]
    assert active_inc.status == "ACTIVE"
    logger.info("Verified: Anomaly caught and active incident opened.")

    # -----------------------------------------------------------------------
    # PHASE 3: Remove Outage & Observe Self-Healing Recovery
    # -----------------------------------------------------------------------
    logger.info("\n🩺 --- PHASE 3: Removing Outage (/reset) -> Self-Healing ---")
    send_control_post("/reset", {})

    for i in range(3):
        m = measure_target("india-01", "netscope-ctrl", "127.0.0.1", scheme="http", path="/health", port=SERVER_PORT)
        eval_res = detector.evaluate_window(
            target_id="netscope-ctrl",
            avg_latency_ms=m.total_ms or 20.0,
            p95_ms=m.total_ms or 20.0,
            failure_count=0 if m.is_success() else 1,
            availability_pct=100.0 if m.is_success() else 0.0,
        )
        manager.evaluate_target_state("netscope-ctrl", "Controlled Endpoint", "India", "india-01", eval_res)
        print(f"Recovery Round {i+1}: latency={m.total_ms}ms | Score: {eval_res.anomaly_score} | Severity: {eval_res.severity}")
        time.sleep(0.3)

    assert len(manager.active_incidents) == 0, "Incident should be cleared from active!"
    assert len(manager.resolved_incidents) == 1, "Incident must transition to RESOLVED!"
    resolved_inc = manager.resolved_incidents[0]
    assert resolved_inc.status == "RESOLVED"
    assert resolved_inc.end_time is not None

    logger.info("=== WEEK 6 CONTROLLED OUTAGE DEMO COMPLETED & VERIFIED SUCCESSFULLY ===")


if __name__ == "__main__":
    run_demo()
