"""
Baseline distribution store.

Supports two backends:
  - "file"  — JSON file on local disk (default, good for dev/testing)
  - "minio" — JSON object in a MinIO/S3 bucket (production)

The baseline is automatically bootstrapped from the first N windows of data
when no prior baseline exists.
"""
from __future__ import annotations

import json
import logging
import os
from typing import Optional

from drift_monitor.models import DriftSignal, HISTOGRAM_BUCKETS

LOG = logging.getLogger(__name__)

_BASELINE_OBJECT_KEY = "drift-monitor/baseline.json"


def _empty_signal() -> DriftSignal:
    return DriftSignal(
        window_start_ms=0,
        window_end_ms=0,
        unique_users=0,
        total_events=0,
        count_histogram={b: 0 for b in HISTOGRAM_BUCKETS},
    )


class BaselineStore:
    """
    Loads and saves the reference distribution used by the drift detector.

    Args:
        backend:        "file" or "minio"
        path_or_bucket: local file path (backend="file") or bucket name (backend="minio")
        minio_endpoint: MinIO endpoint URL, e.g. "http://minio:9000"
        minio_access_key / minio_secret_key: credentials
        warmup_windows: number of windows to average before accepting the baseline
    """

    def __init__(
        self,
        backend: str = "file",
        path_or_bucket: str = "/tmp/drift-baseline.json",
        minio_endpoint: str = "http://localhost:9000",
        minio_access_key: str = "minioadmin",
        minio_secret_key: str = "minioadmin",
        warmup_windows: int = 5,
    ):
        self._backend       = backend
        self._path_or_bucket = path_or_bucket
        self._minio_endpoint = minio_endpoint
        self._minio_access_key = minio_access_key
        self._minio_secret_key = minio_secret_key
        self._warmup_windows = warmup_windows
        self._warmup_buffer: list[DriftSignal] = []
        self._baseline: Optional[DriftSignal] = self._load()

        if self._backend == "minio":
            self._ensure_minio_client()

    # ── Public API ───────────────────────────────────────────────────────────

    @property
    def is_ready(self) -> bool:
        return self._baseline is not None

    @property
    def baseline(self) -> Optional[DriftSignal]:
        return self._baseline

    def ingest_warmup(self, signal: DriftSignal) -> bool:
        """
        Feed a warmup window. Returns True once the baseline is established
        (after warmup_windows signals have been collected).
        """
        if self._baseline is not None:
            return True
        self._warmup_buffer.append(signal)
        LOG.info("Warmup %d/%d", len(self._warmup_buffer), self._warmup_windows)
        if len(self._warmup_buffer) >= self._warmup_windows:
            self._baseline = self._average(self._warmup_buffer)
            self._save(self._baseline)
            LOG.info("Baseline established from %d warmup windows", self._warmup_windows)
            return True
        return False

    def update(self, signal: DriftSignal, ema_alpha: float = 0.1) -> None:
        """
        Continuously update the baseline with an exponential moving average
        so it slowly adapts to genuine distribution shifts.
        """
        if self._baseline is None:
            return
        for bucket in HISTOGRAM_BUCKETS:
            old = self._baseline.count_histogram.get(bucket, 0)
            new = signal.count_histogram.get(bucket, 0)
            self._baseline.count_histogram[bucket] = int(old * (1 - ema_alpha) + new * ema_alpha)
        self._baseline.unique_users = int(
            self._baseline.unique_users * (1 - ema_alpha) + signal.unique_users * ema_alpha
        )
        self._save(self._baseline)

    # ── Persistence ──────────────────────────────────────────────────────────

    def _load(self) -> Optional[DriftSignal]:
        try:
            if self._backend == "file":
                return self._load_file()
            return None  # MinIO client not initialised yet; loaded lazily
        except Exception as exc:
            LOG.warning("Could not load baseline: %s", exc)
            return None

    def _load_file(self) -> Optional[DriftSignal]:
        if not os.path.exists(self._path_or_bucket):
            return None
        with open(self._path_or_bucket, "r", encoding="utf-8") as f:
            return DriftSignal.from_dict(json.load(f))

    def _save(self, signal: DriftSignal) -> None:
        try:
            data = {
                "windowStartMs":  signal.window_start_ms,
                "windowEndMs":    signal.window_end_ms,
                "uniqueUsers":    signal.unique_users,
                "totalEvents":    signal.total_events,
                "countHistogram": signal.count_histogram,
            }
            if self._backend == "file":
                with open(self._path_or_bucket, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2)
            elif self._backend == "minio":
                self._save_minio(json.dumps(data).encode())
        except Exception as exc:
            LOG.error("Failed to persist baseline: %s", exc)

    def _ensure_minio_client(self) -> None:
        from minio import Minio
        self._minio = Minio(
            self._minio_endpoint.replace("http://", "").replace("https://", ""),
            access_key=self._minio_access_key,
            secret_key=self._minio_secret_key,
            secure=self._minio_endpoint.startswith("https://"),
        )
        if not self._minio.bucket_exists(self._path_or_bucket):
            self._minio.make_bucket(self._path_or_bucket)
        # Lazy-load if we skipped it at init time
        if self._baseline is None:
            self._baseline = self._load_minio()

    def _load_minio(self) -> Optional[DriftSignal]:
        try:
            response = self._minio.get_object(self._path_or_bucket, _BASELINE_OBJECT_KEY)
            return DriftSignal.from_dict(json.loads(response.read()))
        except Exception:
            return None

    def _save_minio(self, data: bytes) -> None:
        import io
        self._minio.put_object(
            self._path_or_bucket, _BASELINE_OBJECT_KEY,
            io.BytesIO(data), len(data), content_type="application/json"
        )

    # ── Helpers ──────────────────────────────────────────────────────────────

    @staticmethod
    def _average(signals: list[DriftSignal]) -> DriftSignal:
        n = len(signals)
        histogram = {b: int(sum(s.count_histogram.get(b, 0) for s in signals) / n)
                     for b in HISTOGRAM_BUCKETS}
        return DriftSignal(
            window_start_ms=signals[0].window_start_ms,
            window_end_ms=signals[-1].window_end_ms,
            unique_users=int(sum(s.unique_users for s in signals) / n),
            total_events=int(sum(s.total_events for s in signals) / n),
            count_histogram=histogram,
        )
