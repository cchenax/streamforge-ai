import json
import os
import time
from datetime import datetime, timezone
from io import BytesIO

from kafka import KafkaConsumer
from minio import Minio


def env(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value not in (None, "") else default


def ensure_bucket(client: Minio, bucket: str) -> None:
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)


def build_object_key(prefix: str) -> str:
    now = datetime.now(timezone.utc)
    return f"{prefix}/{now.strftime('%Y/%m/%d/%H%M%S')}-{int(time.time() * 1000)}.json"


def main() -> None:
    kafka_servers = env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    kafka_topic = env("KAFKA_TOPIC", "streamforge.features.user_event_counts")
    kafka_group = env("KAFKA_GROUP_ID", "streamforge-feature-minio-sink")

    minio_endpoint = env("MINIO_ENDPOINT", "minio:9000")
    minio_access_key = env("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret_key = env("MINIO_SECRET_KEY", "minioadmin")
    minio_bucket = env("MINIO_BUCKET", "processed")
    minio_prefix = env("MINIO_PREFIX", "streamforge/features")

    print(f"[SINK] Starting Kafka->MinIO sink topic={kafka_topic} endpoint={minio_endpoint}")

    consumer = KafkaConsumer(
        kafka_topic,
        bootstrap_servers=[kafka_servers],
        group_id=kafka_group,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: v.decode("utf-8"),
    )

    minio_client = Minio(
        endpoint=minio_endpoint,
        access_key=minio_access_key,
        secret_key=minio_secret_key,
        secure=False,
    )
    ensure_bucket(minio_client, minio_bucket)

    for msg in consumer:
        raw_value = msg.value
        try:
            payload = json.loads(raw_value)
        except json.JSONDecodeError:
            payload = {"raw": raw_value}

        payload["sink_received_at"] = datetime.now(timezone.utc).isoformat()
        data = (json.dumps(payload, separators=(",", ":")) + "\n").encode("utf-8")
        key = build_object_key(minio_prefix)

        minio_client.put_object(
            bucket_name=minio_bucket,
            object_name=key,
            data=BytesIO(data),
            length=len(data),
            content_type="application/json",
        )

        print(f"[SINK] Wrote feature event to minio://{minio_bucket}/{key}")


if __name__ == "__main__":
    main()
