from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Optional


HISTOGRAM_BUCKETS = ("1", "2-5", "6-10", "11-20", "21-50", "51-100", "100+")


class AlertLevel(str, Enum):
    OK      = "OK"       # PSI < 0.10
    WARNING = "WARNING"  # 0.10 ≤ PSI < 0.20
    ALERT   = "ALERT"    # PSI ≥ 0.20


@dataclass
class DriftSignal:
    """Distribution snapshot emitted by the Flink DriftSignalFunction for one window."""
    window_start_ms: int
    window_end_ms: int
    unique_users: int
    total_events: int
    count_histogram: Dict[str, int]  # bucket → user count

    @classmethod
    def from_dict(cls, d: dict) -> "DriftSignal":
        return cls(
            window_start_ms=d["windowStartMs"],
            window_end_ms=d["windowEndMs"],
            unique_users=d["uniqueUsers"],
            total_events=d["totalEvents"],
            count_histogram={k: int(v) for k, v in d["countHistogram"].items()},
        )

    def as_distribution(self) -> Dict[str, float]:
        """Normalise histogram to a probability distribution (sums to 1.0)."""
        total = sum(self.count_histogram.values())
        if total == 0:
            return {b: 0.0 for b in HISTOGRAM_BUCKETS}
        return {b: self.count_histogram.get(b, 0) / total for b in HISTOGRAM_BUCKETS}


@dataclass
class DriftReport:
    window_start_ms: int
    window_end_ms: int
    psi: float
    level: AlertLevel
    baseline_distribution: Dict[str, float]
    current_distribution: Dict[str, float]
    unique_users: int
    total_events: int
    message: str = ""

    @property
    def window_start_iso(self) -> str:
        from datetime import datetime, timezone
        return datetime.fromtimestamp(self.window_start_ms / 1000, tz=timezone.utc).isoformat()
