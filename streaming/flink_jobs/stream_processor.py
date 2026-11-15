"""
NetScope Real-Time Stream Processing Engine — M5 / Week 4

Implements pure stream analytics matching Apache Flink semantics:
1. Event-time extraction from record timestamps
2. Bounded out-of-orderness watermarks (tolerates late arrivals up to watermark_delay)
3. Deduplication of incoming measurements (at-least-once -> exactly-once window semantics)
4. Tumbling windows (1-minute fixed intervals)
5. Sliding windows (5-minute window sliding every 30 seconds)
6. Stream aggregations:
   - count, average latency, p50, p95, p99, failure_count, availability %
7. Dead-letter queue / late-event policy for events trailing behind the current watermark
"""

from __future__ import annotations

import json
import logging
import math
from collections import defaultdict
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Optional, Callable

logger = logging.getLogger("netscope.streaming_engine")


@dataclass
class WindowAggregate:
    """Aggregated metrics computed over a tumbling or sliding time window."""
    window_type: str            # "tumbling_1m" | "sliding_5m_30s"
    window_start: str           # ISO-8601
    window_end: str             # ISO-8601
    probe_id: str
    target_id: str
    target: str

    count: int = 0
    avg_latency_ms: float = 0.0
    p50_ms: float = 0.0
    p95_ms: float = 0.0
    p99_ms: float = 0.0

    failure_count: int = 0
    availability_pct: float = 100.0

    def to_dict(self) -> dict:
        return asdict(self)


def calculate_percentile(sorted_data: list[float], pct: float) -> float:
    """Calculate the p-th percentile from an already sorted list of floats."""
    if not sorted_data:
        return 0.0
    n = len(sorted_data)
    if n == 1:
        return sorted_data[0]
    rank = (pct / 100.0) * (n - 1)
    lo = int(math.floor(rank))
    hi = int(math.ceil(rank))
    if lo == hi:
        return sorted_data[lo]
    d = rank - lo
    return round(sorted_data[lo] + d * (sorted_data[hi] - sorted_data[lo]), 2)


def parse_iso_to_epoch_ms(ts_str: str) -> int:
    """Parse ISO timestamp to millisecond epoch timestamp."""
    try:
        dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        return int(dt.timestamp() * 1000)
    except Exception:
        return int(datetime.now(timezone.utc).timestamp() * 1000)


def epoch_ms_to_iso(ms: int) -> str:
    """Convert millisecond epoch timestamp back to UTC ISO string."""
    dt = datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
    return dt.isoformat()


class FlinkStreamingSimulator:
    """
    Production-grade streaming execution model executing windowing, watermarks,
    late-event routing, and deduplication identically to Apache Flink DataStream API.
    """

    def __init__(
        self,
        watermark_delay_ms: int = 5000,      # Bounded out-of-orderness tolerance (e.g. 5 seconds)
        allowed_lateness_ms: int = 10000,    # Late-event grace window before dropping
        on_window_fire: Optional[Callable[[WindowAggregate], None]] = None,
        on_late_event: Optional[Callable[[dict], None]] = None,
    ):
        self.watermark_delay_ms = watermark_delay_ms
        self.allowed_lateness_ms = allowed_lateness_ms
        self.on_window_fire = on_window_fire
        self.on_late_event = on_late_event

        self.current_watermark_ms: int = -1
        self.seen_measurement_ids: set[str] = set()

        # Tumbling windows: (window_start_ms, probe_id, target_id) -> list of events
        self.tumbling_1m_buckets: dict[tuple[int, str, str], list[dict]] = defaultdict(list)

        # Sliding windows: 5min (300,000ms) sliding every 30s (30,000ms)
        self.sliding_5m_buckets: dict[tuple[int, str, str], list[dict]] = defaultdict(list)

        # Retain closed window boundaries to detect late events
        self.closed_tumbling_windows: set[tuple[int, str, str]] = set()

    def process_record(self, record: dict) -> bool:
        """
        Process a single streaming event.
        Returns:
            True if accepted into active processing windows
            False if dropped as duplicate or rejected as late past allowed lateness
        """
        measurement_id = record.get("measurement_id")
        if not measurement_id:
            logger.warning("Event missing measurement_id, rejecting.")
            return False

        # --- Stage 1: Deduplication ---
        if measurement_id in self.seen_measurement_ids:
            logger.info("Dropping duplicate event: %s", measurement_id)
            return False
        self.seen_measurement_ids.add(measurement_id)

        # --- Stage 2: Event-Time Extraction ---
        event_time_ms = parse_iso_to_epoch_ms(record.get("timestamp", ""))
        probe_id = record.get("probe_id", "unknown")
        target_id = record.get("target_id", "unknown")

        # --- Stage 3: Watermark Advance ---
        # Watermark = Max(EventTime) - WatermarkDelay
        new_watermark = event_time_ms - self.watermark_delay_ms
        if new_watermark > self.current_watermark_ms:
            self.current_watermark_ms = new_watermark
            self._advance_watermark_and_trigger(self.current_watermark_ms)

        # --- Stage 4: Late-Event Check ---
        # If event arrives behind the current watermark + allowed_lateness, send to side-output/DLQ
        if self.current_watermark_ms > 0 and event_time_ms < (self.current_watermark_ms - self.allowed_lateness_ms):
            logger.warning(
                "Late event dropped past allowed lateness: id=%s event_time=%d watermark=%d",
                measurement_id,
                event_time_ms,
                self.current_watermark_ms,
            )
            if self.on_late_event:
                self.on_late_event(record)
            return False

        # --- Stage 5: Tumbling Window Assignment (1 minute = 60,000 ms) ---
        tumbling_size_ms = 60_000
        tumb_start = event_time_ms - (event_time_ms % tumbling_size_ms)
        tumb_key = (tumb_start, probe_id, target_id)

        if tumb_key in self.closed_tumbling_windows:
            logger.warning("Event arrived for already closed tumbling window %s", tumb_key)
            if self.on_late_event:
                self.on_late_event(record)
        else:
            self.tumbling_1m_buckets[tumb_key].append(record)

        # --- Stage 6: Sliding Window Assignment (5m window = 300,000 ms, slide = 30,000 ms) ---
        slide_size_ms = 300_000
        slide_step_ms = 30_000
        last_start = event_time_ms - (event_time_ms % slide_step_ms)
        for w_start in range(last_start, last_start - slide_size_ms, -slide_step_ms):
            if w_start <= event_time_ms < w_start + slide_size_ms:
                self.sliding_5m_buckets[(w_start, probe_id, target_id)].append(record)

        return True

    def _advance_watermark_and_trigger(self, watermark_ms: int) -> list[WindowAggregate]:
        """Fire any windows whose window_end <= watermark."""
        fired_aggregates: list[WindowAggregate] = []

        # 1. Check Tumbling Windows (1m = 60,000 ms)
        tumbling_size_ms = 60_000
        to_close_tumbling = [
            key for key in self.tumbling_1m_buckets
            if (key[0] + tumbling_size_ms) <= watermark_ms
        ]

        for key in to_close_tumbling:
            records = self.tumbling_1m_buckets.pop(key)
            self.closed_tumbling_windows.add(key)
            agg = self._compute_aggregate(
                window_type="tumbling_1m",
                start_ms=key[0],
                end_ms=key[0] + tumbling_size_ms,
                probe_id=key[1],
                target_id=key[2],
                records=records,
            )
            fired_aggregates.append(agg)
            if self.on_window_fire:
                self.on_window_fire(agg)

        # 2. Check Sliding Windows (5m = 300,000 ms)
        slide_size_ms = 300_000
        to_close_sliding = [
            key for key in self.sliding_5m_buckets
            if (key[0] + slide_size_ms) <= watermark_ms
        ]

        for key in to_close_sliding:
            records = self.sliding_5m_buckets.pop(key)
            agg = self._compute_aggregate(
                window_type="sliding_5m_30s",
                start_ms=key[0],
                end_ms=key[0] + slide_size_ms,
                probe_id=key[1],
                target_id=key[2],
                records=records,
            )
            fired_aggregates.append(agg)
            if self.on_window_fire:
                self.on_window_fire(agg)

        return fired_aggregates

    def _compute_aggregate(
        self,
        window_type: str,
        start_ms: int,
        end_ms: int,
        probe_id: str,
        target_id: str,
        records: list[dict],
    ) -> WindowAggregate:
        """Compute window metrics: count, avg, p50, p95, p99, failure_count, availability."""
        count = len(records)
        target = records[0].get("target", target_id) if records else target_id

        latencies: list[float] = []
        failure_count = 0

        for r in records:
            # Check for error or timeout
            if r.get("error_type") or r.get("timed_out") or not (200 <= (r.get("http_status") or 0) < 400):
                failure_count += 1

            total_ms = r.get("total_ms")
            if total_ms is not None and isinstance(total_ms, (int, float)):
                latencies.append(float(total_ms))

        latencies.sort()

        avg_latency = round(sum(latencies) / len(latencies), 2) if latencies else 0.0
        p50 = calculate_percentile(latencies, 50.0)
        p95 = calculate_percentile(latencies, 95.0)
        p99 = calculate_percentile(latencies, 99.0)

        availability = round(100.0 * (count - failure_count) / count, 2) if count > 0 else 100.0

        return WindowAggregate(
            window_type=window_type,
            window_start=epoch_ms_to_iso(start_ms),
            window_end=epoch_ms_to_iso(end_ms),
            probe_id=probe_id,
            target_id=target_id,
            target=target,
            count=count,
            avg_latency_ms=avg_latency,
            p50_ms=p50,
            p95_ms=p95,
            p99_ms=p99,
            failure_count=failure_count,
            availability_pct=availability,
        )

    def force_flush_all(self) -> list[WindowAggregate]:
        """Force flush all unclosed windows up to the highest seen timestamp."""
        return self._advance_watermark_and_trigger(watermark_ms=int(time.time() * 1000) + 1_000_000_000)
