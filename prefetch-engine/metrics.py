"""
Prometheus metrics definitions for the prefetch engine.

Expose via HTTP (pull model) by setting METRICS_PORT (default 8000),
or push to a Pushgateway by setting METRICS_PUSHGATEWAY_URL.
"""

import os

from prometheus_client import Counter, Gauge, Histogram, start_http_server, push_to_gateway, REGISTRY

# ── Labels ───────────────────────────────────────────────────────────────────
_JOB_LABEL = "job_id"

# ── Counters ─────────────────────────────────────────────────────────────────
files_prefetched_total = Counter(
    "prefetch_engine_files_prefetched_total",
    "Number of files successfully copied into the local cache.",
    [_JOB_LABEL],
)

files_skipped_total = Counter(
    "prefetch_engine_files_skipped_total",
    "Number of candidate files skipped because the source was not found.",
    [_JOB_LABEL],
)

cache_hits_total = Counter(
    "prefetch_engine_cache_hits_total",
    "Number of ML-job file reads served from the local cache.",
    [_JOB_LABEL],
)

cache_misses_total = Counter(
    "prefetch_engine_cache_misses_total",
    "Number of ML-job file reads that were not found in the local cache.",
    [_JOB_LABEL],
)

minio_uploads_total = Counter(
    "prefetch_engine_minio_uploads_total",
    "Number of MinIO upload attempts.",
    ["status"],  # "success" | "skipped" | "error"
)

# ── Gauges ───────────────────────────────────────────────────────────────────
candidates_total = Gauge(
    "prefetch_engine_candidates_total",
    "Size of the candidate file pool evaluated in the last selection pass.",
)

hot_files_selected = Gauge(
    "prefetch_engine_hot_files_selected",
    "Number of hot files chosen for prefetch in the last selection pass.",
)

# ── Histograms ────────────────────────────────────────────────────────────────
prefetch_duration_seconds = Histogram(
    "prefetch_engine_prefetch_duration_seconds",
    "Wall-clock time spent prefetching all hot files for one run.",
    [_JOB_LABEL],
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)

job_duration_seconds = Histogram(
    "prefetch_engine_job_duration_seconds",
    "Wall-clock time spent in the simulated ML job for one run.",
    [_JOB_LABEL],
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)

minio_upload_duration_seconds = Histogram(
    "prefetch_engine_minio_upload_duration_seconds",
    "Wall-clock time spent uploading processed records to MinIO.",
    buckets=(0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)


# ── Server helpers ────────────────────────────────────────────────────────────

def start_metrics_server() -> int | None:
    """
    Start the Prometheus pull-model HTTP server if METRICS_PORT is set.

    Returns the port number on success, or None if disabled.
    """
    raw = os.environ.get("METRICS_PORT", "").strip()
    if not raw:
        return None
    port = int(raw)
    start_http_server(port)
    print(f"[METRICS] Prometheus metrics exposed on :{port}/metrics")
    return port


def push_metrics(job_id: str) -> None:
    """
    Push metrics to a Prometheus Pushgateway if METRICS_PUSHGATEWAY_URL is set.
    """
    url = os.environ.get("METRICS_PUSHGATEWAY_URL", "").strip()
    if not url:
        return
    push_to_gateway(url, job=f"prefetch-engine-{job_id}", registry=REGISTRY)
    print(f"[METRICS] Pushed metrics to Pushgateway at {url}")
