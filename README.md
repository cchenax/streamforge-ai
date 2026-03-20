# StreamForge AI Architecture

## 1. Overview

StreamForge AI is a real-time data pipeline platform for AI and analytics workloads. It focuses on:

- CDC ingestion from operational databases
- stream processing for feature generation
- object-storage-based data sinking
- storage-aware prefetching for ML workloads

## 2. Goals

- Provide a minimal but realistic open-source AI data pipeline
- Support local development and demo environments
- Showcase best practices in streaming, storage, and pipeline orchestration
- Demonstrate architecture leadership and contributor collaboration

## 3. Non-goals

- Full production-grade multi-tenant platform
- Large-scale distributed control plane
- Enterprise authentication / authorization in v0.1

## 4. High-level architecture

### 4.1 Ingestion layer
Uses Debezium to capture row-level changes from MySQL/Postgres and publish them to Kafka topics.

### 4.2 Streaming layer
Uses Apache Flink to:
- consume CDC events
- perform cleaning and transformation
- compute simple feature aggregations
- write processed outputs to storage

### 4.3 Storage layer
Uses MinIO as the initial object storage target.
Future versions may support Iceberg table sinks for incremental analytics.

### 4.4 Prefetch layer
A lightweight prefetch engine analyzes expected access patterns and pulls selected objects into a hot cache area before an ML job starts.

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

## 7. MVP scope

v0.1 will include:
- MySQL CDC to Kafka
- one Flink aggregation job
- one MinIO sink
- one end-to-end Docker Compose demo

For the initial CDC demo, see `deploy/cdc-mysql-kafka-debezium/`.

## 8. Future improvements

- Iceberg sink support
- schema evolution handling
- metrics and observability
- benchmark scenarios
- training-job integration