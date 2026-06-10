"""
drift-monitor entry point.

Starts the Prometheus HTTP server (if prometheus-client is installed) and
then runs the Kafka consumer loop.

Usage:
    python -m drift_monitor.main

All configuration via environment variables — see config.example.env.
"""
from __future__ import annotations

import logging
import os

from drift_monitor.baseline_store import BaselineStore
from drift_monitor import consumer

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
)
LOG = logging.getLogger("drift_monitor")


def _start_prometheus_server() -> None:
    try:
        from prometheus_client import start_http_server
        port = int(os.getenv("PROMETHEUS_PORT", "8000"))
        start_http_server(port)
        LOG.info("Prometheus metrics available on :%d/metrics", port)
    except ImportError:
        LOG.info("prometheus-client not installed — metrics endpoint disabled")


def main() -> None:
    _start_prometheus_server()

    store = BaselineStore(
        backend           = os.getenv("BASELINE_BACKEND",         "file"),
        path_or_bucket    = os.getenv("BASELINE_PATH_OR_BUCKET",  "/tmp/drift-baseline.json"),
        minio_endpoint    = os.getenv("MINIO_ENDPOINT",           "http://localhost:9000"),
        minio_access_key  = os.getenv("MINIO_ACCESS_KEY",         "minioadmin"),
        minio_secret_key  = os.getenv("MINIO_SECRET_KEY",         "minioadmin"),
        warmup_windows    = int(os.getenv("BASELINE_WARMUP_WINDOWS", "5")),
    )

    if store.is_ready:
        LOG.info("Baseline loaded from storage — skipping warmup")
    else:
        LOG.info("No baseline found — collecting %s warmup windows",
                 os.getenv("BASELINE_WARMUP_WINDOWS", "5"))

    consumer.run(store)


if __name__ == "__main__":
    main()
