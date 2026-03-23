# StreamForge AI

## Project Overview

StreamForge AI is a real-time data pipeline platform for AI and analytics workloads. It focuses on:

- CDC ingestion from operational databases
- stream processing for feature generation
- object-storage-based data sinking
- storage-aware prefetching for ML workloads

## Motivation

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
Planned: Uses Apache Flink to:
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

- `ingestion-service/`
- `stream-processor/`
- `storage-sink/`
- `prefetch-engine/`
- `deploy/`
- `examples/`

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
- Storage-aware prefetching demo for ML workloads (see `prefetch-engine/`)
- Optional MinIO upload of processed outputs from the prefetch demo (see `prefetch-engine/README.md`)

Planned next (not yet included as runnable code here):
- A streaming/feature-generation job (e.g., with Apache Flink)
- Additional storage sinks (e.g., Iceberg)

## Roadmap Links

Track progress:
- GitHub issues: https://github.com/iLvDallas/streamforge-ai/issues
- GitHub projects (if/when populated): https://github.com/cchenax/streamforge-ai/projects

- Iceberg sink support
- schema evolution handling
- metrics and observability
- benchmark scenarios
- training-job integration

## Contribution Links
- Contribution guide: [`CONTRIBUTING.md`](./CONTRIBUTING.md)
- Start a topic/track progress: https://github.com/iLvDallas/streamforge-ai/issues/new/choose
- Open PRs to `main`: https://github.com/iLvDallas/streamforge-ai/pulls