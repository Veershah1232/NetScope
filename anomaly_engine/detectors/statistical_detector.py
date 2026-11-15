"""
NetScope Statistical Anomaly Detectors — M7 / FR_10

Implements explainable statistical techniques:
1. Static Threshold Detection (e.g. p95 > 500ms)
2. Rolling Baseline Comparison (e.g. current > 2.0x of rolling mean)
3. Z-Score Deviation (z = (x - mean) / std_dev, flagged if |z| > 3.0)
4. Multi-Signal Composite Anomaly Score:
     anomaly_score = latency_score + error_score + availability_score + timeout_score
   Classification:
     0.0 - 2.0  -> NORMAL
     2.0 - 5.0  -> WARNING
     5.0+       -> CRITICAL
"""

from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class AnomalyEvaluation:
    is_anomaly: bool
    severity: str           # "NORMAL" | "WARNING" | "CRITICAL"
    anomaly_score: float
    reasons: list[str] = field(default_factory=list)
    z_score: Optional[float] = None
    baseline_ratio: Optional[float] = None


class RollingStats:
    """Maintains a sliding history window for rolling mean and standard deviation."""

    def __init__(self, max_samples: int = 30):
        self.max_samples = max_samples
        self.samples: deque[float] = deque(maxlen=max_samples)

    def add(self, value: float) -> None:
        self.samples.append(value)

    @property
    def count(self) -> int:
        return len(self.samples)

    @property
    def mean(self) -> float:
        return sum(self.samples) / len(self.samples) if self.samples else 0.0

    @property
    def std_dev(self) -> float:
        if len(self.samples) < 2:
            return 0.0
        avg = self.mean
        variance = sum((x - avg) ** 2 for x in self.samples) / (len(self.samples) - 1)
        return math.sqrt(variance)


class StatisticalAnomalyDetector:
    def __init__(
        self,
        static_threshold_ms: float = 400.0,
        rolling_baseline_multiplier: float = 2.0,
        z_score_threshold: float = 3.0,
        min_baseline_samples: int = 5,
    ):
        self.static_threshold_ms = static_threshold_ms
        self.rolling_baseline_multiplier = rolling_baseline_multiplier
        self.z_score_threshold = z_score_threshold
        self.min_baseline_samples = min_baseline_samples

        # Per-target rolling latency statistics
        self.history: dict[str, RollingStats] = {}

    def evaluate_window(
        self,
        target_id: str,
        avg_latency_ms: float,
        p95_ms: float,
        failure_count: int,
        availability_pct: float,
    ) -> AnomalyEvaluation:
        """
        Evaluate a single time-window aggregate against statistical criteria.
        Calculates individual signal scores and combines into a composite anomaly score.
        """
        reasons: list[str] = []
        latency_score = 0.0
        error_score = 0.0
        availability_score = 0.0

        if target_id not in self.history:
            self.history[target_id] = RollingStats()

        stats = self.history[target_id]

        # 1. Static Threshold check
        if p95_ms > self.static_threshold_ms:
            reasons.append(f"p95 latency ({p95_ms:.1f}ms) exceeded static threshold ({self.static_threshold_ms:.1f}ms)")
            latency_score += min(3.0, (p95_ms / self.static_threshold_ms) * 1.5)

        # 2. Rolling Baseline & Z-Score (only if enough baseline history and meaningful latency)
        baseline_ratio = None
        z_score = None

        if stats.count >= self.min_baseline_samples and stats.mean >= 5.0:
            mean = stats.mean
            std = stats.std_dev

            # Baseline ratio
            if mean > 0:
                baseline_ratio = round(avg_latency_ms / mean, 2)
                if baseline_ratio >= self.rolling_baseline_multiplier:
                    reasons.append(f"Latency {baseline_ratio}x above rolling average ({mean:.1f}ms)")
                    latency_score += min(3.0, baseline_ratio)

            # Z-score
            if std > 2.0:  # Avoid division by near-zero variance
                z_score = round((avg_latency_ms - mean) / std, 2)
                if abs(z_score) >= self.z_score_threshold:
                    reasons.append(f"Z-score {z_score:+.2f} exceeds standard deviation threshold (±{self.z_score_threshold})")
                    latency_score += min(2.5, abs(z_score) * 0.5)

        # 3. HTTP Error / Failure signal
        if failure_count > 0:
            reasons.append(f"Detected {failure_count} failed requests")
            error_score += min(3.0, failure_count * 1.0)

        # 4. Availability signal
        if availability_pct < 100.0:
            drop = 100.0 - availability_pct
            reasons.append(f"Availability degraded to {availability_pct:.1f}%")
            availability_score += min(4.0, (drop / 20.0) * 2.0)

        # Update historical baseline with current observation
        # (Only update baseline if current event isn't an extreme outlier, to prevent poisoning baseline)
        if latency_score < 3.0:
            stats.add(avg_latency_ms)

        total_score = round(latency_score + error_score + availability_score, 2)

        # Classification
        if total_score >= 5.0:
            severity = "CRITICAL"
            is_anomaly = True
        elif total_score >= 2.0:
            severity = "WARNING"
            is_anomaly = True
        else:
            severity = "NORMAL"
            is_anomaly = False

        return AnomalyEvaluation(
            is_anomaly=is_anomaly,
            severity=severity,
            anomaly_score=total_score,
            reasons=reasons,
            z_score=z_score,
            baseline_ratio=baseline_ratio,
        )
