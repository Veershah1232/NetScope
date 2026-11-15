"""
Canonical measurement schema for NetScope.

All probes must produce measurements that conform to this dataclass.
Downstream components (Kafka producer, MinIO writer, Flink, ClickHouse)
all consume this structure.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Optional


@dataclass
class Measurement:
    """A single Internet performance measurement from one probe to one target."""

    # --- Identity ---
    measurement_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    probe_id: str = ""          # e.g. "india-01"
    target_id: str = ""         # e.g. "github-com"
    target: str = ""            # e.g. "github.com"
    timestamp: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    # --- Timing (ms, None if stage failed) ---
    dns_ms: Optional[float] = None
    tcp_ms: Optional[float] = None
    tls_ms: Optional[float] = None
    ttfb_ms: Optional[float] = None
    total_ms: Optional[float] = None

    # --- HTTP result ---
    http_status: Optional[int] = None
    response_bytes: Optional[int] = None

    # --- Error state ---
    error_type: Optional[str] = None   # e.g. "TIMEOUT", "DNS_FAILED", "TCP_REFUSED"
    timed_out: bool = False

    def to_dict(self) -> dict:
        return asdict(self)

    def is_success(self) -> bool:
        return (
            self.error_type is None
            and not self.timed_out
            and self.http_status is not None
            and 200 <= self.http_status < 400
        )

    def validate(self) -> list[str]:
        """Return a list of validation errors (empty list = valid)."""
        errors: list[str] = []
        if not self.measurement_id:
            errors.append("measurement_id is required")
        if not self.probe_id:
            errors.append("probe_id is required")
        if not self.target_id:
            errors.append("target_id is required")
        if not self.target:
            errors.append("target is required")
        if not self.timestamp:
            errors.append("timestamp is required")
        # Basic timestamp format check
        try:
            datetime.fromisoformat(self.timestamp.replace("Z", "+00:00"))
        except ValueError:
            errors.append(f"timestamp format invalid: {self.timestamp}")
        # Timing sanity: nothing should be negative
        for field_name in ("dns_ms", "tcp_ms", "tls_ms", "ttfb_ms", "total_ms"):
            val = getattr(self, field_name)
            if val is not None and val < 0:
                errors.append(f"{field_name} must be non-negative, got {val}")
        return errors


@dataclass
class ProbeHeartbeat:
    """Periodic health signal emitted by each probe."""

    probe_id: str
    timestamp: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    status: str = "healthy"          # "healthy" | "degraded" | "error"
    version: str = "0.1.0"
    measurements_sent: int = 0
    failed_measurements: int = 0

    def to_dict(self) -> dict:
        return asdict(self)
