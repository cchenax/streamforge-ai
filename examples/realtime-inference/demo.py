#!/usr/bin/env python3
"""
StreamForge AI — Real-time Inference Pipeline Demo

Demonstrates the full loop end-to-end without running Flink:

  1. [PRODUCE]   Publish synthetic UserEventCount records to Kafka.
  2. [INFER]     Call the model server directly for each record (mirrors
                 what InferencePipelineJob does via async HTTP).
  3. [CONSUME]   Read InferencePrediction records back from Kafka and
                 print a summary report.

When Kafka is unavailable the demo runs in "offline" mode — it generates
records, calls the model server, and prints results without touching Kafka.

Usage
-----
  # Offline (no Kafka, no Flink needed):
  python examples/realtime-inference/demo.py

  # Against a live stack (see docker-compose.yml):
  KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \\
  MODEL_SERVER_URL=http://localhost:8080/predict \\
  python examples/realtime-inference/demo.py --records 200

  # Print raw prediction JSON:
  python examples/realtime-inference/demo.py --verbose

Environment variables
---------------------
  KAFKA_BOOTSTRAP_SERVERS   default: localhost:9092
  KAFKA_SOURCE_TOPIC        default: user.event.counts
  KAFKA_SINK_TOPIC          default: user.event.predictions
  MODEL_SERVER_URL          default: http://localhost:8080/predict
  MODEL_SERVER_TIMEOUT_S    default: 2
"""

from __future__ import annotations

import argparse
import json
import os
import random
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import List, Optional, Tuple

import urllib.request
import urllib.error

# ---------------------------------------------------------------------------
# Data classes mirroring Java POJOs
# ---------------------------------------------------------------------------

@dataclass
class UserEventCount:
    userId:        str
    count:         int
    windowStartMs: int
    windowEndMs:   int


@dataclass
class InferencePrediction:
    userId:             str
    windowStartMs:      int
    windowEndMs:        int
    eventCount:         int
    label:              str
    confidence:         float
    inferenceLatencyMs: int
    predictedAtMs:      int


# ---------------------------------------------------------------------------
# Synthetic data generator
# ---------------------------------------------------------------------------

def _generate_records(n: int, window_size_s: int = 60) -> List[UserEventCount]:
    now_ms      = int(time.time() * 1000)
    window_ms   = window_size_s * 1000
    user_ids    = [f"user_{i:04d}" for i in range(max(1, n // 5))]
    records     = []
    for i in range(n):
        window_start = now_ms - (n - i) * window_ms
        records.append(UserEventCount(
            userId        = random.choice(user_ids),
            count         = random.randint(0, 50),
            windowStartMs = window_start,
            windowEndMs   = window_start + window_ms,
        ))
    return records


# ---------------------------------------------------------------------------
# Model server client
# ---------------------------------------------------------------------------

def _call_model_server(
        record: UserEventCount,
        url: str,
        timeout_s: float = 2.0,
) -> InferencePrediction:
    payload = json.dumps(asdict(record)).encode()
    req     = urllib.request.Request(
        url,
        data    = payload,
        headers = {"Content-Type": "application/json"},
        method  = "POST",
    )
    t0 = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            body       = json.loads(resp.read())
            latency_ms = int((time.perf_counter() - t0) * 1000)
            return InferencePrediction(
                userId             = record.userId,
                windowStartMs      = record.windowStartMs,
                windowEndMs        = record.windowEndMs,
                eventCount         = record.count,
                label              = body["label"],
                confidence         = body["confidence"],
                inferenceLatencyMs = latency_ms,
                predictedAtMs      = int(time.time() * 1000),
            )
    except Exception as exc:
        latency_ms = int((time.perf_counter() - t0) * 1000)
        return InferencePrediction(
            userId             = record.userId,
            windowStartMs      = record.windowStartMs,
            windowEndMs        = record.windowEndMs,
            eventCount         = record.count,
            label              = "error",
            confidence         = 0.0,
            inferenceLatencyMs = latency_ms,
            predictedAtMs      = int(time.time() * 1000),
        )


# ---------------------------------------------------------------------------
# Optional Kafka helpers (kafka-python; gracefully skipped if unavailable)
# ---------------------------------------------------------------------------

def _try_produce(records: List[UserEventCount], bootstrap: str, topic: str) -> bool:
    try:
        from kafka import KafkaProducer  # type: ignore
        producer = KafkaProducer(
            bootstrap_servers=bootstrap,
            value_serializer=lambda v: json.dumps(asdict(v)).encode(),
        )
        for r in records:
            producer.send(topic, value=r)
        producer.flush()
        producer.close()
        return True
    except Exception as exc:
        print(f"[demo] Kafka produce skipped ({exc.__class__.__name__}: {exc})")
        return False


def _try_publish_predictions(
        preds: List[InferencePrediction], bootstrap: str, topic: str) -> bool:
    try:
        from kafka import KafkaProducer  # type: ignore
        producer = KafkaProducer(
            bootstrap_servers=bootstrap,
            value_serializer=lambda v: json.dumps(asdict(v)).encode(),
        )
        for p in preds:
            producer.send(topic, value=p)
        producer.flush()
        producer.close()
        return True
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

def _print_report(
        records: List[UserEventCount],
        predictions: List[InferencePrediction],
        total_s: float,
        kafka_ok: bool,
        model_server_url: str,
) -> None:
    label_counts: dict[str, int] = {"low": 0, "medium": 0, "high": 0, "error": 0}
    latencies: List[int] = []
    for p in predictions:
        label_counts[p.label] = label_counts.get(p.label, 0) + 1
        latencies.append(p.inferenceLatencyMs)

    avg_lat  = sum(latencies) / len(latencies) if latencies else 0
    p99_lat  = sorted(latencies)[int(len(latencies) * 0.99)] if latencies else 0
    errors   = label_counts.get("error", 0)
    sep      = "=" * 70

    print(f"\n{sep}")
    print("  StreamForge AI — Real-time Inference Pipeline Demo")
    print(sep)
    print(f"  Model server  : {model_server_url}")
    print(f"  Kafka         : {'connected' if kafka_ok else 'offline (demo mode)'}")
    print(f"  Records in    : {len(records):,}")
    print(f"  Predictions   : {len(predictions):,}")
    print()
    print("  ── Label distribution ────────────────────────────")
    for label in ("low", "medium", "high", "error"):
        n   = label_counts.get(label, 0)
        pct = n / len(predictions) * 100 if predictions else 0
        bar = "█" * int(pct / 2)
        print(f"  {label:8s} {n:5,}  ({pct:5.1f}%)  {bar}")
    print()
    print("  ── Inference latency ─────────────────────────────")
    print(f"  avg           : {avg_lat:.1f} ms")
    print(f"  p99           : {p99_lat} ms")
    print(f"  errors        : {errors}")
    print()
    print(f"  ── Total demo time: {total_s:.2f}s ──")
    print(sep + "\n")


# ---------------------------------------------------------------------------
# Main demo
# ---------------------------------------------------------------------------

def run_demo(
        n_records: int = 100,
        window_size_s: int = 60,
        model_server_url: str = "http://localhost:8080/predict",
        model_timeout_s: float = 2.0,
        kafka_bootstrap: str = "localhost:9092",
        source_topic: str = "user.event.counts",
        sink_topic: str = "user.event.predictions",
        verbose: bool = False,
) -> None:
    t0 = time.perf_counter()

    # Stage 1: Generate synthetic records
    print(f"\n[demo] Stage 1/3 — generating {n_records} synthetic UserEventCount records …")
    records = _generate_records(n_records, window_size_s)

    # Stage 2: Optionally produce to Kafka
    print(f"[demo] Stage 2/3 — publishing to Kafka topic '{source_topic}' …")
    kafka_ok = _try_produce(records, kafka_bootstrap, source_topic)

    # Stage 3: Call model server for each record
    print(f"[demo] Stage 3/3 — calling model server ({model_server_url}) …")
    predictions: List[InferencePrediction] = []
    for i, rec in enumerate(records):
        pred = _call_model_server(rec, model_server_url, model_timeout_s)
        predictions.append(pred)
        if verbose:
            print(f"  [{i+1:4d}/{n_records}] userId={pred.userId} "
                  f"count={pred.eventCount} → {pred.label} "
                  f"({pred.confidence:.3f}) {pred.inferenceLatencyMs}ms")

    # Optionally write predictions back to Kafka
    if kafka_ok:
        _try_publish_predictions(predictions, kafka_bootstrap, sink_topic)

    total_s = time.perf_counter() - t0
    _print_report(records, predictions, total_s, kafka_ok, model_server_url)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="StreamForge AI real-time inference pipeline demo"
    )
    p.add_argument("--records",    type=int,   default=100,
                   help="Number of synthetic UserEventCount records (default: 100)")
    p.add_argument("--window",     type=int,   default=60,
                   help="Window size in seconds (default: 60)")
    p.add_argument("--verbose",    action="store_true",
                   help="Print each prediction as it arrives")
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    run_demo(
        n_records        = args.records,
        window_size_s    = args.window,
        model_server_url = os.environ.get("MODEL_SERVER_URL",       "http://localhost:8080/predict"),
        model_timeout_s  = float(os.environ.get("MODEL_SERVER_TIMEOUT_S", "2")),
        kafka_bootstrap  = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        source_topic     = os.environ.get("KAFKA_SOURCE_TOPIC",      "user.event.counts"),
        sink_topic       = os.environ.get("KAFKA_SINK_TOPIC",        "user.event.predictions"),
        verbose          = args.verbose,
    )


if __name__ == "__main__":
    main()
