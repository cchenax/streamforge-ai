"""
Population Stability Index (PSI) drift detector.

PSI measures how much a distribution has shifted relative to a baseline:
  PSI < 0.10  → stable (OK)
  PSI 0.10–0.20 → moderate shift (WARNING)
  PSI ≥ 0.20  → significant drift (ALERT)
"""
from __future__ import annotations

import math
from typing import Dict

from drift_monitor.models import AlertLevel, DriftReport, DriftSignal, HISTOGRAM_BUCKETS

# Small epsilon prevents log(0) when a bucket is empty in one distribution.
_EPSILON = 1e-4

WARNING_THRESHOLD = 0.10
ALERT_THRESHOLD   = 0.20


def compute_psi(baseline: Dict[str, float], current: Dict[str, float]) -> float:
    """Return the PSI between two normalised bucket distributions."""
    psi = 0.0
    for bucket in HISTOGRAM_BUCKETS:
        b = max(baseline.get(bucket, 0.0), _EPSILON)
        c = max(current.get(bucket, 0.0), _EPSILON)
        psi += (c - b) * math.log(c / b)
    return psi


def evaluate(signal: DriftSignal, baseline: DriftSignal) -> DriftReport:
    """Compare *signal* against *baseline* and produce a DriftReport."""
    baseline_dist = baseline.as_distribution()
    current_dist  = signal.as_distribution()
    psi           = compute_psi(baseline_dist, current_dist)

    if psi < WARNING_THRESHOLD:
        level = AlertLevel.OK
        msg   = f"PSI={psi:.4f} — distribution stable"
    elif psi < ALERT_THRESHOLD:
        level = AlertLevel.WARNING
        msg   = f"PSI={psi:.4f} — moderate feature drift detected"
    else:
        level = AlertLevel.ALERT
        msg   = f"PSI={psi:.4f} — significant feature drift! Investigate data pipeline."

    return DriftReport(
        window_start_ms=signal.window_start_ms,
        window_end_ms=signal.window_end_ms,
        psi=psi,
        level=level,
        baseline_distribution=baseline_dist,
        current_distribution=current_dist,
        unique_users=signal.unique_users,
        total_events=signal.total_events,
        message=msg,
    )
