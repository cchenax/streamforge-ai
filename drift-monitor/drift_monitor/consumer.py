"""
Kafka consumer loop for the feature.drift.signals topic.

Each message is a JSON-serialised DriftSignal emitted by the Flink
DriftSignalFunction. The loop:
  1. Warms up the baseline if not yet established.
  2. Evaluates PSI once the baseline is ready.
  3. Dispatches alerts and optionally updates the baseline (EMA).
"""
from __future__ import annotations

import json
import logging
import os
from typing import Optional

from kafka import KafkaConsumer  # type: ignore[import]

from drift_monitor import alerting, detector
from drift_monitor.baseline_store import BaselineStore
from drift_monitor.models import DriftSignal

LOG = logging.getLogger(__name__)


def run(store: BaselineStore) -> None:
    bootstrap     = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    drift_topic   = os.getenv("KAFKA_DRIFT_SIGNALS_TOPIC", "feature.drift.signals")
    consumer_group = os.getenv("DRIFT_CONSUMER_GROUP", "drift-monitor")
    ema_alpha     = float(os.getenv("BASELINE_EMA_ALPHA", "0.05"))
    # Update baseline EMA after every N non-alert windows to adapt to slow drift.
    ema_update_interval = int(os.getenv("BASELINE_EMA_UPDATE_INTERVAL", "10"))

    LOG.info("Connecting to Kafka %s, topic=%s, group=%s", bootstrap, drift_topic, consumer_group)

    consumer = KafkaConsumer(
        drift_topic,
        bootstrap_servers=bootstrap,
        group_id=consumer_group,
        auto_offset_reset="latest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        enable_auto_commit=True,
    )

    ok_window_count = 0

    for message in consumer:
        try:
            signal = DriftSignal.from_dict(message.value)
        except (KeyError, TypeError) as exc:
            LOG.warning("Malformed drift signal, skipping: %s — %s", exc, message.value)
            continue

        if not store.is_ready:
            store.ingest_warmup(signal)
            continue

        report = detector.evaluate(signal, store.baseline)
        alerting.dispatch(report)

        from drift_monitor.models import AlertLevel
        if report.level == AlertLevel.OK:
            ok_window_count += 1
            if ok_window_count % ema_update_interval == 0:
                store.update(signal, ema_alpha=ema_alpha)
        else:
            ok_window_count = 0
