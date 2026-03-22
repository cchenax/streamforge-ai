# End-to-End Demo: Source DB -> CDC Kafka -> Flink -> MinIO

This demo shows a complete runnable flow:
1. Source DB update in MySQL
2. CDC event emitted into Kafka by Debezium
3. Flink processing: user event count in time window
4. Output persisted to MinIO

## Acceptance criteria
- A MySQL update generates CDC records in topic `streamforge.streamforge.customers`.
- Flink job produces aggregated feature records in topic `streamforge.features.user_event_counts`.
- Feature sink writes those records to MinIO bucket `processed`.
- Demo steps are documented and reproducible using the commands below.

## Prerequisites
- Docker + Docker Compose

## Step 1: Build Flink job jar
From repo root:

```bash
docker run --rm \
  -v "${PWD}/stream-processor:/ws" \
  -w /ws \
  maven:3.9-eclipse-temurin-17 \
  mvn -DskipTests package
```

Expected artifact:
- `stream-processor/target/stream-processor-0.1.0-SNAPSHOT.jar`

## Step 2: Start demo stack

```bash
cd deploy/cdc-flink-minio-demo
docker compose up -d --build
```

Wait for:
- Kafka ready at `kafka:9092` (inside network)
- Kafka Connect ready at `http://localhost:8083`
- Flink Web UI at `http://localhost:8081`
- MinIO Console at `http://localhost:9001` (`minioadmin` / `minioadmin`)

## Step 3: Verify CDC topic receives DB updates
Generate source DB changes:

```bash
docker compose exec mysql mysql -uroot -proot -D streamforge -e \
  "INSERT INTO customers (id, name, email) VALUES (101, 'Alice', 'alice@example.com');"

docker compose exec mysql mysql -uroot -proot -D streamforge -e \
  "UPDATE customers SET email='alice.new@example.com' WHERE id=101;"
```

Read CDC events:

```bash
docker compose exec kafka bash -lc \
  "kafka-console-consumer --bootstrap-server kafka:9092 --topic streamforge.streamforge.customers --from-beginning --max-messages 10"
```

Look for Debezium records containing `"op":"c"` and `"op":"u"`.

## Step 4: Submit Flink processing job

```bash
docker compose exec jobmanager flink run -d /opt/flink/usrlib/stream-processor-0.1.0-SNAPSHOT.jar \
  --bootstrap.servers kafka:9092 \
  --input.topic streamforge.streamforge.customers \
  --output.topic streamforge.features.user_event_counts \
  --window.seconds 30 \
  --startup.mode earliest
```

Check jobs:

```bash
docker compose exec jobmanager flink list
```

## Step 5: Verify Flink output topic

```bash
docker compose exec kafka bash -lc \
  "kafka-console-consumer --bootstrap-server kafka:9092 --topic streamforge.features.user_event_counts --from-beginning --max-messages 10"
```

Expected JSON example:

```json
{"feature":"user_event_count","user_id":"101","window_start":"...","window_end":"...","event_count":2,"last_op":"u","last_source_ts_ms":...}
```

## Step 6: Verify output files in MinIO
The `feature-sink` service consumes the Flink output topic and writes each feature event as a JSON object in MinIO.

List objects:

```bash
docker compose run --rm minio-init /bin/sh -c \
  "/usr/bin/mc alias set local http://minio:9000 minioadmin minioadmin && /usr/bin/mc ls --recursive local/processed"
```

You should see files under a path similar to:
- `streamforge/features/YYYY/MM/DD/...json`

Open MinIO console:
- URL: `http://localhost:9001`
- User: `minioadmin`
- Password: `minioadmin`

## Cleanup

```bash
docker compose down -v
```
