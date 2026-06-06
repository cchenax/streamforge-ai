import pytest
from drift_monitor.detector import compute_psi, evaluate, WARNING_THRESHOLD, ALERT_THRESHOLD
from drift_monitor.models import AlertLevel, DriftSignal, HISTOGRAM_BUCKETS


def _make_signal(histogram: dict) -> DriftSignal:
    return DriftSignal(
        window_start_ms=0,
        window_end_ms=60_000,
        unique_users=sum(histogram.values()),
        total_events=sum(histogram.values()),
        count_histogram={b: histogram.get(b, 0) for b in HISTOGRAM_BUCKETS},
    )


class TestComputePsi:
    def test_identical_distributions_near_zero(self):
        dist = {b: 1.0 / len(HISTOGRAM_BUCKETS) for b in HISTOGRAM_BUCKETS}
        assert compute_psi(dist, dist) < 1e-9

    def test_moderate_shift_triggers_warning(self):
        baseline = _make_signal({"1": 80, "2-5": 20}).as_distribution()
        current  = _make_signal({"1": 50, "2-5": 50}).as_distribution()
        psi = compute_psi(baseline, current)
        assert WARNING_THRESHOLD <= psi < ALERT_THRESHOLD

    def test_large_shift_triggers_alert(self):
        baseline = _make_signal({"1": 100}).as_distribution()
        current  = _make_signal({"100+": 100}).as_distribution()
        psi = compute_psi(baseline, current)
        assert psi >= ALERT_THRESHOLD

    def test_psi_non_negative(self):
        baseline = _make_signal({"1": 60, "2-5": 30, "6-10": 10}).as_distribution()
        current  = _make_signal({"1": 70, "2-5": 20, "6-10": 10}).as_distribution()
        assert compute_psi(baseline, current) >= 0.0


class TestEvaluate:
    def test_stable_returns_ok(self):
        sig = _make_signal({"1": 80, "2-5": 20})
        report = evaluate(sig, sig)
        assert report.level == AlertLevel.OK
        assert report.psi < WARNING_THRESHOLD

    def test_shifted_returns_warning_or_alert(self):
        baseline = _make_signal({"1": 90, "2-5": 10})
        current  = _make_signal({"1": 10, "2-5": 90})
        report = evaluate(current, baseline)
        assert report.level in (AlertLevel.WARNING, AlertLevel.ALERT)

    def test_report_carries_window_metadata(self):
        sig = _make_signal({"1": 100})
        sig.window_start_ms = 1_000_000
        sig.window_end_ms   = 1_060_000
        report = evaluate(sig, sig)
        assert report.window_start_ms == 1_000_000
        assert report.window_end_ms   == 1_060_000
        assert report.unique_users    == 100
