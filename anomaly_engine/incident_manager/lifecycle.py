"""
NetScope Stateful Incident Lifecycle Engine — M7 / FR_11

Manages the incident state machine:
  NORMAL -> ANOMALY -> INCIDENT_OPEN -> ACTIVE -> RECOVERING -> RESOLVED

Rules:
- 1st Anomaly detection triggers: INCIDENT_OPEN -> ACTIVE
- Sustained anomalies maintain: ACTIVE
- Return to normal parameters transitions: ACTIVE -> RECOVERING
- Sustained normal observations for recovery_window_rounds transitions: RECOVERING -> RESOLVED
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, Callable

from anomaly_engine.detectors.statistical_detector import AnomalyEvaluation

logger = logging.getLogger("netscope.incident_manager")


@dataclass
class Incident:
    incident_id: str
    target_id: str
    target: str
    region: str
    start_time: str
    status: str                         # "ACTIVE" | "RECOVERING" | "RESOLVED"
    severity: str                       # "WARNING" | "CRITICAL"
    max_anomaly_score: float = 0.0
    end_time: Optional[str] = None
    affected_probes: set[str] = field(default_factory=set)
    evidence: list[str] = field(default_factory=list)
    recovery_rounds: int = 0

    def to_dict(self) -> dict:
        return {
            "incident_id": self.incident_id,
            "target_id": self.target_id,
            "target": self.target,
            "region": self.region,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "status": self.status,
            "severity": self.severity,
            "anomaly_score": self.max_anomaly_score,
            "affected_probes": list(self.affected_probes),
            "evidence": "; ".join(self.evidence[-5:]),
        }


class IncidentLifecycleManager:
    def __init__(
        self,
        recovery_rounds_threshold: int = 2,  # Must be healthy for 2 consecutive rounds to resolve
        on_incident_event: Optional[Callable[[str, Incident], None]] = None,
    ):
        self.recovery_rounds_threshold = recovery_rounds_threshold
        self.on_incident_event = on_incident_event

        # Key: (target_id, region) -> active Incident
        self.active_incidents: dict[tuple[str, str], Incident] = {}
        self.resolved_incidents: list[Incident] = []

    def evaluate_target_state(
        self,
        target_id: str,
        target: str,
        region: str,
        probe_id: str,
        evaluation: AnomalyEvaluation,
    ) -> Optional[Incident]:
        """
        Transitions incident state machine based on incoming anomaly evaluation.
        """
        key = (target_id, region)
        incident = self.active_incidents.get(key)
        now_iso = datetime.now(timezone.utc).isoformat()

        if evaluation.is_anomaly:
            if incident is None:
                # --- State Transition: NORMAL -> ANOMALY -> INCIDENT_OPEN / ACTIVE ---
                incident = Incident(
                    incident_id=str(uuid.uuid4()),
                    target_id=target_id,
                    target=target,
                    region=region,
                    start_time=now_iso,
                    status="ACTIVE",
                    severity=evaluation.severity,
                    max_anomaly_score=evaluation.anomaly_score,
                    affected_probes={probe_id},
                    evidence=list(evaluation.reasons),
                )
                self.active_incidents[key] = incident
                logger.warning(
                    "🔥 [INCIDENT OPENED] ID: %s | Target: %s (%s) | Severity: %s | Score: %.2f",
                    incident.incident_id,
                    target,
                    region,
                    incident.severity,
                    incident.max_anomaly_score,
                )
                if self.on_incident_event:
                    self.on_incident_event("OPENED", incident)

            else:
                # --- State: INCIDENT ACTIVE (Update evidence & score) ---
                incident.affected_probes.add(probe_id)
                incident.evidence.extend(evaluation.reasons)
                incident.max_anomaly_score = max(incident.max_anomaly_score, evaluation.anomaly_score)
                if evaluation.severity == "CRITICAL" and incident.severity != "CRITICAL":
                    incident.severity = "CRITICAL"

                if incident.status == "RECOVERING":
                    logger.info("Incident %s relapsed back to ACTIVE.", incident.incident_id)
                    incident.status = "ACTIVE"
                    incident.recovery_rounds = 0
                    if self.on_incident_event:
                        self.on_incident_event("RELAPSED", incident)

        else:
            # Current window is NORMAL
            if incident is not None:
                incident.recovery_rounds += 1

                if incident.status == "ACTIVE":
                    # --- State Transition: ACTIVE -> RECOVERING ---
                    incident.status = "RECOVERING"
                    logger.info("🟡 [INCIDENT RECOVERING] ID: %s | Target: %s", incident.incident_id, target)
                    if self.on_incident_event:
                        self.on_incident_event("RECOVERING", incident)

                if incident.recovery_rounds >= self.recovery_rounds_threshold:
                    # --- State Transition: RECOVERING -> RESOLVED ---
                    incident.status = "RESOLVED"
                    incident.end_time = now_iso
                    self.active_incidents.pop(key, None)
                    self.resolved_incidents.append(incident)
                    logger.info("✅ [INCIDENT RESOLVED] ID: %s | Target: %s | Duration: %s -> %s",
                                incident.incident_id, target, incident.start_time, incident.end_time)
                    if self.on_incident_event:
                        self.on_incident_event("RESOLVED", incident)

        return incident
