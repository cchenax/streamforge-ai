# Real-time Inference Pipeline

End-to-end example wiring **Apache Flink async I/O** to a **model server** for
sub-second per-record inference at streaming scale.

```
Kafka (user.event.counts)
       │
       ▼
 InferencePipelineJob        ← Flink job (Java)
  └─ AsyncDataStream.unorderedWait()
        │  HTTP POST /predict  (non-blocking, up to 100 in-flight)
        ▼
  model-server:8080           ← FastAPI (Python)
        │
        ▼
 Kafka (user.event.predictions)
```

## Components

| File | Description |
|------|-------------|
| `model_server.py` | FastAPI inference endpoint (`POST /predict`) |
| `demo.py` | End-to-end demo: generates records → calls model server → prints report |
| `docker-compose.yml` | Kafka + model-server + demo runner |
| `Dockerfile` | Model-server image |
| [`InferencePipelineJob.java`](../../stream-processor/src/main/java/ai/streamforge/processor/InferencePipelineJob.java) | Flink async-I/O job |
| [`ModelServerAsyncFunction.java`](../../stream-processor/src/main/java/ai/streamforge/processor/inference/ModelServerAsyncFunction.java) | Flink `RichAsyncFunction` — non-blocking HTTP |

## Quick start

### Offline demo (no Kafka, no Flink)

```bash
pip install fastapi uvicorn
# Terminal 1 — start model server
python examples/realtime-inference/model_server.py

# Terminal 2 — run demo
python examples/realtime-inference/demo.py --records 200 --verbose
```

### Docker Compose (model server + Kafka + demo)

```bash
docker compose -f examples/realtime-inference/docker-compose.yml up --build
```

### Full pipeline (CDC → Flink → model server)

```bash
docker compose \
  -f deploy/cdc-flink-minio-demo/docker-compose.yml \
  -f examples/realtime-inference/docker-compose.yml \
  up
```

## Environment variables

### Flink job (`InferencePipelineJob`)

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka brokers |
| `KAFKA_SOURCE_TOPIC` | `user.event.counts` | Input topic (UserEventCount JSON) |
| `KAFKA_SINK_TOPIC` | `user.event.predictions` | Output topic (InferencePrediction JSON) |
| `KAFKA_CONSUMER_GROUP` | `flink-inference-pipeline` | Consumer group |
| `MODEL_SERVER_URL` | `http://localhost:8080/predict` | Model server endpoint |
| `MODEL_SERVER_TIMEOUT_MS` | `2000` | Per-request HTTP timeout |
| `ASYNC_MAX_CONCURRENT` | `100` | Max in-flight async requests |
| `ASYNC_TIMEOUT_MS` | `5000` | Flink async-I/O timeout |

### Model server

| Variable | Default | Description |
|---|---|---|
| `MODEL_SERVER_PORT` | `8080` | Listening port |
| `MODEL_LOW_THRESHOLD` | `5` | Events/min below → "low" |
| `MODEL_MEDIUM_THRESHOLD` | `20` | Events/min below → "medium"; above → "high" |

## API

### `POST /predict`

```json
// Request
{"userId": "user_0042", "count": 12, "windowStartMs": 1700000000000, "windowEndMs": 1700000060000}

// Response
{"label": "medium", "confidence": 0.8713}
```

Labels: `low` | `medium` | `high`

### `GET /healthz`
Returns `{"status": "ok"}`.

### `GET /metrics`
Returns `{"requests_total": N, "errors_total": N, "avg_latency_ms": N}`.

## Design notes

- **No blocking threads**: `AsyncDataStream.unorderedWait()` keeps Flink task
  threads free while waiting for HTTP responses. Up to `ASYNC_MAX_CONCURRENT`
  requests are in-flight simultaneously.
- **Fault tolerance**: timeout and HTTP errors emit `label="error"` so the
  pipeline never stalls; errors are counted in Flink metrics
  (`model_server.errors_total`).
- **Swap the model**: replace `_load_model()` in `model_server.py` with any
  loader (pickle, ONNX runtime, TorchScript) — the serving contract is just
  the JSON request/response schema above.
