# Stream Processor (Flink)

This module contains a basic Apache Flink job for StreamForge AI that:
- reads CDC events from Kafka (Debezium JSON format),
- performs a simple aggregation,
- and produces processed feature output.

## Implemented example
`count events per user in a time window`

The job consumes records from a CDC topic, extracts a user identifier from payload fields, and emits feature records with event counts for each user in a tumbling processing-time window.

## Source and sink
- **Input topic (default)**: `streamforge.streamforge.customers`
- **Output topic (default)**: `streamforge.features.user_event_counts`

The output is JSON lines, e.g.:

```json
{"feature":"user_event_count","user_id":"3","window_start":"2026-03-21T06:20:00Z","window_end":"2026-03-21T06:21:00Z","event_count":4,"last_op":"u","last_source_ts_ms":1711002065000}
```

## Build
Requires Java 17 and Maven.

```bash
cd stream-processor
mvn -DskipTests package
```

The fat jar is produced under:

`target/stream-processor-0.1.0-SNAPSHOT.jar`

## Run
Run in a Flink environment with Kafka reachable from the Flink job manager/task managers.

```bash
flink run target/stream-processor-0.1.0-SNAPSHOT.jar \
  --bootstrap.servers localhost:9092 \
  --input.topic streamforge.streamforge.customers \
  --output.topic streamforge.features.user_event_counts \
  --group.id streamforge-flink-user-count \
  --window.seconds 60 \
  --startup.mode earliest
```

## Supported CDC fields
The parser expects a Debezium-like envelope (JSON) and reads:
- `payload.op` (counts only non-delete events),
- `payload.ts_ms` (or falls back to `ts_ms` at the root, then `payload.source.ts_ms`),
- `payload.after.user_id` or `payload.after.uid` or fallback `payload.after.id`,
- additional identifier field aliases like `payload.after.userId` / `payload.after.userID`,
- and (optionally) nested identifiers under `payload.after.user.*`.

For schema evolution resilience, if the identifier field is renamed upstream, the parser will
also fall back to the first top-level field that ends with `*_id`.

## Notes
- This is an MVP processor intended for local/demo usage.
- Aggregation currently uses **processing time** tumbling windows.
- Output is written to Kafka and also printed to stdout for quick verification.
