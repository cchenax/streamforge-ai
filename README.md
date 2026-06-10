

# StreamForge AI

## 1. Project Overview

StreamForge AI is a real-time data pipeline platform for AI and analytics workloads. It focuses on:

- CDC ingestion from operational databases
- stream processing for feature generation
- object-storage-based data sinking
- storage-aware prefetching for ML workloads

## 2. Motivation

- Provide a minimal but realistic open-source AI data pipeline
- Support local development and demo environments
- Showcase best practices in streaming, storage, and pipeline orchestration
- Demonstrate architecture leadership and contributor collaboration

## 3. Non-goals

- Full production-grade multi-tenant platform
- Large-scale distributed control plane
- Enterprise authentication / authorization in v0.1

## Architecture Summary

### 4.1 Ingestion layer
Uses Debezium to capture row-level changes from MySQL/Postgres and publish them to Kafka topics.

### 4.2 Streaming layer
Uses Apache Flink to:
- consume CDC events
- perform cleaning and transformation
- compute simple feature aggregations
- write processed outputs to storage

### 4.3 Storage layer
MinIO/S3-compatible storage is the initial storage target (and is optionally exercised by the prefetch demo).
Future versions may support Iceberg table sinks for incremental analytics.

### 4.4 Prefetch layer
A lightweight prefetch engine analyzes expected access patterns and pulls selected objects into a hot cache area before an ML job starts (implemented in `prefetch-engine/`).

## 5. Initial module boundaries

- `stream-processor/`
- `prefetch-engine/`
- `deploy/`

## 6. Design decisions

### Why Kafka
Kafka is a widely adopted event backbone and works naturally with Debezium and Flink.

### Why Flink
Flink provides strong streaming semantics, checkpointing, and event processing flexibility.

### Why MinIO first
MinIO is simple for local demos and provides an S3-compatible interface.

### Why prefetch demo
Prefetching is a practical optimization for AI pipelines with repeated object access and training cold starts.

## Core Features

This repo focuses on an MVP demo set that illustrates the intended architecture:
- MySQL -> Kafka CDC ingestion via Debezium (see `deploy/cdc-mysql-kafka-debezium/`)
- End-to-end demo: MySQL -> Debezium -> Kafka -> Flink -> MinIO (see `deploy/cdc-flink-minio-demo/`)
- Storage-aware prefetching demo for ML workloads (see `prefetch-engine/`)
- Optional MinIO upload of processed outputs from the prefetch demo (see `prefetch-engine/README.md`)

Planned next:
- Additional storage sinks (e.g., Iceberg)
- Metrics and observability
- Benchmark scenarios

## Roadmap

### v0.1 Local demo (MVP)
- [x] Storage-aware prefetch demo (`prefetch-engine/`)
- [x] MySQL -> Kafka CDC ingestion via Debezium (`deploy/cdc-mysql-kafka-debezium/`)
- [x] End-to-end demo: MySQL -> Debezium -> Kafka -> Flink -> MinIO (`deploy/cdc-flink-minio-demo/`)
- [x] Runnable local stack (Docker Compose) for the full demo (`deploy/cdc-flink-minio-demo/docker-compose.yml`)

### v0.2 Streaming + sinks
- [x] Flink stream processor job + example (`stream-processor/`)
- [x] MinIO/S3 sink with an output layout and naming conventions (`deploy/cdc-flink-minio-demo/feature-sink/`)
- [x] Iceberg sink support (optional)

### v0.3 Hardening + integrations
- [x] Schema evolution handling
- [x] Metrics and observability
- [x] Benchmark scenarios
- [x] Training-job integration example
- [x] Backfill / replay tooling for historical reprocessing
- [x] Data quality checks (basic validation + drift signals)
- [x] Cost/performance tuning guide (Flink, Kafka, MinIO, Iceberg)

### v0.4 Lakehouse + governance
- [x] Iceberg-first sink mode (partitioning, compaction, snapshots)
- [x] Table/catalog integration (REST catalog or Hive Metastore)
- [x] Data lineage basics (job -> dataset -> feature artifacts)
- [x] Access patterns for offline/online features (example layouts)

### v0.5 Platformization
- [x] Pipeline configuration as code (YAML) + validation
- [x] Simple control-plane API for starting/stopping pipelines
- [x] Web UI for demo environments (status, logs, artifacts)
- [x] Basic authn/authz for local multi-user demos

### v0.6 Expanded sources & feature serving
- [ ] Multi-source CDC support (PostgreSQL, MongoDB)
- [ ] Feature store integration (Feast, Tecton-compatible interface)
- [ ] Online feature serving API with caching
- [ ] Schema registry integration (Confluent Schema Registry or Apicurio)
- [ ] Stream-to-batch joins (Flink with Iceberg dimension tables)

### v0.7 ML/AI deeper integration
- [ ] Real-time feature drift detection & alerting
- [ ] Streaming windowed feature examples (rolling aggregates, sessionization)
- [ ] Real-time inference pipeline example (Flink + model server)
- [ ] Training data sampling/downsampling for active learning
- [ ] Metadata catalog for features & pipelines (OpenLineage/Marquez)

### v0.8 Enterprise readiness
- [ ] Kubernetes operator for cluster deployment
- [ ] Secrets management (Vault integration or Kubernetes Secrets)
- [ ] Fine-grained RBAC for pipelines/datasets
- [ ] Audit logging (API, job, and data access events)
- [ ] High-availability mode for control plane & critical services

### v0.9 AI/RAG & agent workloads
- [ ] First-class RAG pipeline template (ingest → embed → index → retrieve) on top of `rag-engine/`
- [ ] Pluggable embedding model adapters (local, OpenAI-compatible, HuggingFace) with batching
- [ ] Vector store sinks (pgvector, Qdrant, Milvus) alongside existing Iceberg/MinIO sinks
- [ ] Hybrid retrieval (BM25 + vector) with reranker hooks
- [ ] Agent workflow orchestration examples (`agent-workflow/`) wired to streaming features
- [ ] Tool/function-calling integration with control-plane API for pipeline actions
- [ ] Streaming-to-RAG freshness SLOs (latency/lag dashboards per index)
- [ ] LLM-output evaluation harness (groundedness, hallucination, retrieval recall) + benchmarks
- [ ] PII/safety filters on ingestion and retrieval paths
- [ ] Cost/token telemetry for embedding + inference per pipeline

### Roadmap Links
- GitHub issues: https://github.com/cchenax/streamforge-ai/issues
- GitHub projects (if/when populated): https://github.com/cchenax/streamforge-ai/projects

## Contribution Links
- Contribution guide: [`CONTRIBUTING.md`](./CONTRIBUTING.md)
- Start a topic/track progress: https://github.com/cchenax/streamforge-ai/issues/new/choose
- Open PRs to `main`: https://github.com/cchenax/streamforge-ai/pulls
