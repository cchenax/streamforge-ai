# Local Development Environment

This stack provides a local development environment for StreamForge AI with:
- MySQL
- Kafka
- Zookeeper
- Debezium (Kafka Connect)
- MinIO

## Start services

```bash
cd deploy/local-dev
docker compose up -d
```

## Acceptance criteria
Services can start successfully with `docker compose up`.

## Verify services are running

```bash
docker compose ps
```

Expected running services:
- `streamforge-zookeeper`
- `streamforge-kafka`
- `streamforge-mysql`
- `streamforge-debezium`
- `streamforge-minio`

`streamforge-connector-setup` may exit after attempting connector creation, which is expected.

## Optional quick checks

- Debezium API:
  - `http://localhost:8083/connectors`
- MinIO API health:
  - `http://localhost:9000/minio/health/live`
- MinIO Console:
  - `http://localhost:9001` (`minioadmin` / `minioadmin`)

## Stop services

```bash
docker compose down
```

To also remove persisted MySQL/MinIO data:

```bash
docker compose down -v
```
