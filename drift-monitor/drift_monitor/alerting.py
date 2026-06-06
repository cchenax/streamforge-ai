"""
Alert routing for drift reports.

Three channels:
  1. Structured log (always on)
  2. Prometheus gauge — scraped by Grafana (enabled when prometheus-client is installed)
  3. HTTP webhook — POST JSON payload to DRIFT_WEBHOOK_URL if set
"""
from __future__ import annotations

import json
import logging
import os
import urllib.request
from typing import Optional

from drift_monitor.models import AlertLevel, DriftReport

LOG = logging.getLogger(__name__)

_WEBHOOK_URL: Optional[str] = os.getenv("DRIFT_WEBHOOK_URL")
_WEBHOOK_TIMEOUT = int(os.getenv("DRIFT_WEBHOOK_TIMEOUT_SEC", "5"))

# ── Prometheus metrics (optional) ────────────────────────────────────────────
try:
    from prometheus_client import Gauge, Counter
    _psi_gauge = Gauge(
        "feature_drift_psi",
        "Population Stability Index of the user-event-count distribution",
    )
    _alerts_total = Counter(
        "feature_drift_alerts_total",
        "Number of drift alerts fired, by level",
        labelnames=["level"],
    )
    _PROMETHEUS_AVAILABLE = True
except ImportError:
    _PROMETHEUS_AVAILABLE = False


# ── Public API ────────────────────────────────────────────────────────────────

def dispatch(report: DriftReport) -> None:
    """Route a DriftReport to all configured alert channels."""
    _log(report)
    _prometheus(report)
    if report.level != AlertLevel.OK:
        _webhook(report)


# ── Channels ──────────────────────────────────────────────────────────────────

def _log(report: DriftReport) -> None:
    level_map = {
        AlertLevel.OK:      logging.DEBUG,
        AlertLevel.WARNING: logging.WARNING,
        AlertLevel.ALERT:   logging.ERROR,
    }
    LOG.log(
        level_map[report.level],
        "[%s] %s | window=%s | users=%d | events=%d | buckets=%s",
        report.level.value,
        report.message,
        report.window_start_iso,
        report.unique_users,
        report.total_events,
        _format_distribution_diff(report),
    )


def _prometheus(report: DriftReport) -> None:
    if not _PROMETHEUS_AVAILABLE:
        return
    _psi_gauge.set(report.psi)
    if report.level != AlertLevel.OK:
        _alerts_total.labels(level=report.level.value).inc()


def _webhook(report: DriftReport) -> None:
    if not _WEBHOOK_URL:
        return
    payload = {
        "level":        report.level.value,
        "psi":          round(report.psi, 6),
        "message":      report.message,
        "window_start": report.window_start_iso,
        "unique_users": report.unique_users,
        "total_events": report.total_events,
        "baseline":     report.baseline_distribution,
        "current":      report.current_distribution,
    }
    try:
        data = json.dumps(payload).encode()
        req  = urllib.request.Request(
            _WEBHOOK_URL,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=_WEBHOOK_TIMEOUT) as resp:
            LOG.debug("Webhook delivered: HTTP %d", resp.status)
    except Exception as exc:
        LOG.warning("Webhook delivery failed: %s", exc)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _format_distribution_diff(report: DriftReport) -> str:
    parts = []
    for bucket in report.current_distribution:
        b = report.baseline_distribution.get(bucket, 0.0)
        c = report.current_distribution.get(bucket, 0.0)
        if abs(c - b) > 0.01:
            parts.append(f"{bucket}: {b:.2%}→{c:.2%}")
    return ", ".join(parts) or "no notable bucket shift"
