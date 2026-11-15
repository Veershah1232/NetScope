"""
NetScope Geographic Diagnosis & Correlation Engine — M8 / FR_13

Analyzes cross-probe telemetry across geographically distributed vantage points
(e.g. India, Singapore, Europe) to determine the scope of performance anomalies:

Classifications:
1. "LIKELY REGIONAL DEGRADATION"
   - When only 1 probe region experiences abnormal latency / errors while
     the remaining probe regions remain healthy (green).
2. "LIKELY GLOBAL OUTAGE"
   - When multiple or all probe regions experience correlated degradation
     at the target simultaneously.
3. "NORMAL PERFORMANCE"
   - All probe regions observing healthy baseline latency.
4. "INSUFFICIENT CROSS-PROBE DATA"
   - When only single-probe telemetry is available.

Engineering Principle:
Never claim definitive root cause (e.g. 'ISP X cut fiber'); use precise,
verifiable probabilistic language: 'Likely Regional Degradation'.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger("netscope.geographic_engine")


@dataclass
class RegionalObservation:
    probe_id: str
    region: str
    avg_latency_ms: float
    p95_ms: float
    is_degraded: bool
    availability_pct: float = 100.0


@dataclass
class GeographicDiagnosis:
    target_id: str
    target: str
    classification: str             # "NORMAL" | "LIKELY REGIONAL DEGRADATION" | "LIKELY GLOBAL OUTAGE"
    summary: str
    degraded_regions: list[str] = field(default_factory=list)
    healthy_regions: list[str] = field(default_factory=list)
    confidence_score: float = 0.0   # 0.0 to 1.0


class GeographicCorrelationEngine:
    def __init__(self, regional_threshold_ratio: float = 0.5):
        """
        regional_threshold_ratio:
          If degraded_regions / total_regions <= 0.5 -> REGIONAL
          If degraded_regions / total_regions > 0.5  -> GLOBAL
        """
        self.regional_threshold_ratio = regional_threshold_ratio

    def diagnose_target(
        self,
        target_id: str,
        target: str,
        observations: list[RegionalObservation],
    ) -> GeographicDiagnosis:
        if not observations:
            return GeographicDiagnosis(
                target_id=target_id,
                target=target,
                classification="NORMAL",
                summary="No observations available.",
                confidence_score=0.0,
            )

        if len(observations) == 1:
            obs = observations[0]
            if obs.is_degraded:
                return GeographicDiagnosis(
                    target_id=target_id,
                    target=target,
                    classification="LIKELY REGIONAL DEGRADATION",
                    summary=f"Degradation observed from single vantage point ({obs.region} - {obs.probe_id}). Cross-region correlation unavailable.",
                    degraded_regions=[obs.region],
                    confidence_score=0.5,
                )
            return GeographicDiagnosis(
                target_id=target_id,
                target=target,
                classification="NORMAL",
                summary=f"Normal performance from {obs.region}.",
                healthy_regions=[obs.region],
                confidence_score=1.0,
            )

        degraded = [o.region for o in observations if o.is_degraded]
        healthy = [o.region for o in observations if not o.is_degraded]
        total = len(observations)

        if not degraded:
            return GeographicDiagnosis(
                target_id=target_id,
                target=target,
                classification="NORMAL",
                summary=f"All {total} regions reporting normal performance.",
                healthy_regions=healthy,
                confidence_score=1.0,
            )

        degraded_ratio = len(degraded) / total

        if degraded_ratio <= self.regional_threshold_ratio:
            # Regional issue: e.g. India red (600ms), but SG and EU green (110ms, 120ms)
            return GeographicDiagnosis(
                target_id=target_id,
                target=target,
                classification="LIKELY REGIONAL DEGRADATION",
                summary=(
                    f"Degradation isolated to region(s): {', '.join(degraded)}. "
                    f"Remaining region(s) {', '.join(healthy)} remain healthy. "
                    f"Indicates localized ISP/transit degradation rather than origin outage."
                ),
                degraded_regions=degraded,
                healthy_regions=healthy,
                confidence_score=round(1.0 - (len(degraded) / (total * 2)), 2),
            )
        else:
            # Global issue: e.g. India (700ms), SG (680ms), EU (720ms) all red
            return GeographicDiagnosis(
                target_id=target_id,
                target=target,
                classification="LIKELY GLOBAL OUTAGE",
                summary=(
                    f"Degradation observed across {len(degraded)} of {total} regions ({', '.join(degraded)}). "
                    f"Correlated global performance collapse suggests origin server failure or CDN edge outage."
                ),
                degraded_regions=degraded,
                healthy_regions=healthy,
                confidence_score=round(degraded_ratio, 2),
            )
