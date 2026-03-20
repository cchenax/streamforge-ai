# CDC: MySQL -> Kafka (Debezium)

This directory contains a minimal, runnable demo that sets up CDC ingestion from **MySQL** into **Kafka topics** using **Debezium MySQL connector** (via Kafka Connect).

## Acceptance criteria

1. Insert/update/delete events appear in Kafka.
2. Example source table and topic names are documented.

## Architecture (at a glance)

1. MySQL emits row-level change events to its binary log (`binlog`).
2. Debezium MySQL connector tails the binlog.
3. Kafka Connect publishes change events to Kafka topics.

## Example source table

The demo creates this table in the `streamforge` MySQL database:

`streamforge.customers`

## Example Kafka topic names

Debezium’s default topic naming is:

`<database.server.name>.<database.name>.<table.name>`

In this demo:

- `database.server.name = streamforge`
- `database.name = streamforge`
- `table.name = customers`

So the main data-change topic is expected to be:

`streamforge.streamforge.customers`

If `include.schema.changes=true`, Debezium also emits schema change events to an additional topic for schema changes. You can confirm exact topic names by listing Kafka topics (see below).

## How to run

Prerequisites:

- Docker + Docker Compose

Run the stack:

```bash
cd deploy/cdc-mysql-kafka-debezium
docker compose up -d
```

Wait until:

- Kafka is reachable at `kafka:9092` (from inside the compose network)
- Kafka Connect REST API is reachable at `http://localhost:8083`

## Create the Debezium connector

When you run `docker compose up -d`, the compose stack includes a small `connector-setup` helper container that POSTs `./connector-config.json` into the Kafka Connect REST API.

The connector configuration is stored in `./connector-config.json`.

If the connector does not appear, you can create it manually (in another terminal):

```bash
cd deploy/cdc-mysql-kafka-debezium
curl -sS -X POST -H "Content-Type: application/json" \
  --data @connector-config.json \
  http://localhost:8083/connectors
```

You can check connector status:

```bash
curl -sS http://localhost:8083/connectors/debezium-mysql/status | jq
```

If you do not have `jq`, just run without `| jq`.

## Verify Kafka topics exist

List topics (from inside the `kafka` container):

```bash
docker compose exec kafka bash -lc "kafka-topics --bootstrap-server kafka:9092 --list"
```

Look for:

- `streamforge.streamforge.customers`

## Verify insert/update/delete events

1. Start a consumer on the expected data-change topic:

```bash
docker compose exec kafka bash -lc "kafka-console-consumer --bootstrap-server kafka:9092 --topic streamforge.streamforge.customers --from-beginning --max-messages 20"
```

2. Generate MySQL changes (insert, update, delete):

```bash
# Insert
docker compose exec mysql mysql -uroot -proot -D streamforge -e \
  \"INSERT INTO customers (id, name, email) VALUES (3, 'Carol', 'carol@example.com');\"

# Update
docker compose exec mysql mysql -uroot -proot -D streamforge -e \
  \"UPDATE customers SET email='carol.new@example.com' WHERE id=3;\"

# Delete
docker compose exec mysql mysql -uroot -proot -D streamforge -e \
  \"DELETE FROM customers WHERE id=3;\"
```

3. In the consumer output, you should see events for:

- Insert: `"op":"c"`
- Update: `"op":"u"`
- Delete: `"op":"d"`

The exact JSON shape depends on Debezium version and converters, but the `op` field (create/update/delete) is the key signal to confirm the acceptance criteria.

Because `tombstones.on.delete=true`, you may also observe delete "tombstone" records (often represented with a `null` value) after the `op:"d"` event.

## Notes / troubleshooting

- If no events appear:
  - Check MySQL binlog settings (binlog must be `ROW`).
  - Ensure the Debezium user has replication privileges (this demo grants them in `mysql/init/01_init_streamforge.sql`).
  - Re-check the topic name by running the Kafka topic listing step.

