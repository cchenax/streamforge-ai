#!/usr/bin/env python3
"""
StreamForge AI — Model Server

Lightweight FastAPI inference endpoint consumed by InferencePipelineJob.

Endpoint
--------
  POST /predict
    Body : {"userId": str, "count": int, "windowStartMs": int, "windowEndMs": int}
    Reply: {"label": "low"|"medium"|"high", "confidence": float}

The model is a simple threshold classifier that ships as a stand-in for any
real serialized model (scikit-learn, ONNX, TorchScript, etc.).  Swap
_load_model() to load from disk/S3 without changing the serving code.

Usage
-----
  pip install fastapi uvicorn
  python examples/realtime-inference/model_server.py          # port 8080
  MODEL_SERVER_PORT=9090 python examples/realtime-inference/model_server.py
"""

from __future__ import annotations

import math
import os
import time
from dataclasses import dataclass
from typing import Literal

import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Request / response schemas
# ---------------------------------------------------------------------------

class PredictRequest(BaseModel):
    userId:        str
    count:         int
    windowStartMs: int
    windowEndMs:   int


class PredictResponse(BaseModel):
    label:      Literal["low", "medium", "high"]
    confidence: float


# ---------------------------------------------------------------------------
# Model (replace with a real serialized model as needed)
# ---------------------------------------------------------------------------

@dataclass
class ThresholdModel:
    """Classifies activity level by event count per window."""
    low_max:    int   = 5
    medium_max: int   = 20

    def predict(self, count: int, window_duration_ms: int) -> tuple[str, float]:
        # Normalise by window length to be rate-based, not count-based
        window_s     = max(window_duration_ms / 1000, 1)
        rate_per_min = count / window_s * 60

        if rate_per_min <= self.low_max:
            label      = "low"
            # sigmoid-shaped confidence: higher away from threshold
            raw        = self.low_max - rate_per_min
            confidence = _sigmoid(raw / self.low_max * 4)
        elif rate_per_min <= self.medium_max:
            label      = "medium"
            mid        = (self.low_max + self.medium_max) / 2
            confidence = _sigmoid((1 - abs(rate_per_min - mid) / mid) * 4)
        else:
            label      = "high"
            raw        = rate_per_min - self.medium_max
            confidence = _sigmoid(raw / self.medium_max * 4)

        return label, round(max(0.5, min(1.0, confidence)), 4)


def _sigmoid(x: float) -> float:
    return 1.0 / (1.0 + math.exp(-x))


def _load_model() -> ThresholdModel:
    """Load model from disk/env; returns built-in threshold model as default."""
    low_max    = int(os.environ.get("MODEL_LOW_THRESHOLD",    "5"))
    medium_max = int(os.environ.get("MODEL_MEDIUM_THRESHOLD", "20"))
    return ThresholdModel(low_max=low_max, medium_max=medium_max)


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app   = FastAPI(title="StreamForge Model Server", version="1.0.0")
model = _load_model()

_requests_total = 0
_errors_total   = 0
_latency_sum_ms = 0.0


@app.post("/predict", response_model=PredictResponse)
def predict(req: PredictRequest) -> PredictResponse:
    global _requests_total, _errors_total, _latency_sum_ms

    t0 = time.perf_counter()
    _requests_total += 1

    window_duration_ms = max(req.windowEndMs - req.windowStartMs, 1)
    label, confidence  = model.predict(req.count, window_duration_ms)

    _latency_sum_ms += (time.perf_counter() - t0) * 1000
    return PredictResponse(label=label, confidence=confidence)


@app.get("/healthz")
def healthz() -> dict:
    return {"status": "ok"}


@app.get("/metrics")
def metrics() -> dict:
    avg_latency = (_latency_sum_ms / _requests_total) if _requests_total else 0.0
    return {
        "requests_total":   _requests_total,
        "errors_total":     _errors_total,
        "avg_latency_ms":   round(avg_latency, 3),
    }


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    port = int(os.environ.get("MODEL_SERVER_PORT", "8080"))
    print(f"[model-server] Starting on port {port} (thresholds: "
          f"low≤{model.low_max} ev/min, medium≤{model.medium_max} ev/min)")
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
