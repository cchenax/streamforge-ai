"""
Avro serializer / deserializer using the Confluent wire format.

Wire format (Confluent Schema Registry framing)
-----------------------------------------------
    Byte 0       : magic byte = 0x00
    Bytes 1-4    : schema ID, big-endian uint32
    Bytes 5+     : Avro binary-encoded payload

This is the de-facto standard used by Confluent Platform, Confluent Cloud,
and Apicurio Registry (in compat mode).  The Java Flink SerDe counterparts
(``AvroConfluentDeserializationSchema`` / ``AvroConfluentSerializationSchema``)
implement the same framing.

Requirements
------------
    pip install fastavro       # Avro encode/decode
    pip install requests       # needed by SchemaRegistryClient (not avro_serde itself)

Usage
-----
    from schema_registry.config import RegistryConfig
    from schema_registry.client import SchemaRegistryClient
    from schema_registry.avro_serde import AvroSerializer, AvroDeserializer

    client = SchemaRegistryClient(RegistryConfig.from_env())
    topic  = "user-events"

    serializer   = AvroSerializer(client, topic)
    deserializer = AvroDeserializer(client)

    raw   = serializer.serialize({"user_id": "42", "event_type": "click"})
    event = deserializer.deserialize(raw)  # → {"user_id": "42", "event_type": "click"}

    # Batch round-trip
    records = [{"user_id": str(i), "event_type": "view"} for i in range(10)]
    payloads = [serializer.serialize(r) for r in records]
    decoded  = [deserializer.deserialize(p) for p in payloads]
"""
from __future__ import annotations

import io
import struct
from typing import Any, Dict, Optional

from config import RegistryConfig, SubjectNameStrategy
from client import SchemaRegistryClient, SchemaNotFoundError

# Confluent wire-format constants
_MAGIC_BYTE: int = 0
_HEADER_FMT: str = ">bI"   # big-endian: signed byte + unsigned 4-byte int
_HEADER_SIZE: int = 5       # 1 (magic) + 4 (schema ID)


def _require_fastavro():
    try:
        import fastavro
        return fastavro
    except ImportError as exc:
        raise ImportError(
            "fastavro is required for Avro serialization: pip install fastavro"
        ) from exc


# ---------------------------------------------------------------------------
# AvroSerializer
# ---------------------------------------------------------------------------

class AvroSerializer:
    """
    Serialize Python dicts to Confluent-framed Avro binary.

    The schema is registered (or looked up) in the registry on the first
    ``serialize()`` call and cached for the lifetime of the serializer.

    Parameters
    ----------
    client          : Connected SchemaRegistryClient.
    topic           : Kafka topic name (used to derive the default subject name).
    schema_str      : Avro schema JSON.  If omitted the serializer attempts
                      to look up the latest version from the registry.
    strategy        : Subject naming strategy (default: TOPIC_NAME).
    is_key          : If True encode as a key subject (``{topic}-key``).
    auto_register   : Register the schema if not already present (default: True).
    """

    def __init__(
        self,
        client: SchemaRegistryClient,
        topic: str,
        schema_str: Optional[str] = None,
        *,
        strategy: SubjectNameStrategy = SubjectNameStrategy.TOPIC_NAME,
        is_key: bool = False,
        auto_register: bool = True,
    ) -> None:
        self._client = client
        self._topic = topic
        self._strategy = strategy
        self._is_key = is_key
        self._auto_register = auto_register
        self._schema_str = schema_str
        self._schema_id: Optional[int] = None
        self._parsed_schema = None   # fastavro parsed schema object

    @property
    def _subject(self) -> str:
        record_name = ""
        if self._parsed_schema is not None:
            ns = self._parsed_schema.get("namespace", "")
            name = self._parsed_schema.get("name", "")
            record_name = f"{ns}.{name}" if ns else name
        return self._strategy.subject(
            topic=self._topic,
            record_name=record_name,
            is_key=self._is_key,
        )

    def _ensure_initialized(self) -> None:
        if self._schema_id is not None:
            return
        fastavro = _require_fastavro()
        if self._schema_str is None:
            # Fetch the latest version from the registry
            rs = self._client.get_schema(self._subject, "latest")
            self._schema_str = rs.schema_str
            self._schema_id = rs.schema_id
        else:
            if self._auto_register:
                self._schema_id = self._client.get_or_register(self._subject, self._schema_str)
            else:
                rs = self._client.get_schema(self._subject, "latest")
                self._schema_id = rs.schema_id
        self._parsed_schema = fastavro.parse_schema(
            __import__("json").loads(self._schema_str)
        )

    def serialize(self, record: Dict[str, Any]) -> bytes:
        """
        Serialize *record* to Confluent-framed Avro bytes.

        Parameters
        ----------
        record : dict matching the registered Avro schema.

        Returns
        -------
        bytes
        """
        self._ensure_initialized()
        fastavro = _require_fastavro()
        buf = io.BytesIO()
        buf.write(struct.pack(_HEADER_FMT, _MAGIC_BYTE, self._schema_id))
        fastavro.schemaless_writer(buf, self._parsed_schema, record)
        return buf.getvalue()

    @property
    def schema_id(self) -> Optional[int]:
        """The registry schema ID used for serialization (available after first serialize call)."""
        return self._schema_id


# ---------------------------------------------------------------------------
# AvroDeserializer
# ---------------------------------------------------------------------------

class AvroDeserializer:
    """
    Deserialize Confluent-framed Avro bytes to Python dicts.

    The schema is resolved per-message using the embedded schema ID.
    Multiple schema versions are handled transparently via reader/writer
    schema resolution (fastavro supports Avro schema evolution).

    Parameters
    ----------
    client        : Connected SchemaRegistryClient.
    reader_schema : Optional reader schema (for schema evolution / projection).
                    If None, the writer schema is used as-is.
    fallback_json : If True, attempt JSON decode when the magic byte is absent.
                    Useful during migration from JSON to Avro.
    """

    def __init__(
        self,
        client: SchemaRegistryClient,
        *,
        reader_schema: Optional[str] = None,
        fallback_json: bool = True,
    ) -> None:
        self._client = client
        self._reader_schema_str = reader_schema
        self._fallback_json = fallback_json
        self._parsed_reader: Optional[object] = None
        self._parsed_writers: Dict[int, object] = {}   # schema_id → parsed schema

    def _get_writer_schema(self, schema_id: int):
        if schema_id not in self._parsed_writers:
            fastavro = _require_fastavro()
            schema_str = self._client.get_schema_by_id(schema_id)
            self._parsed_writers[schema_id] = fastavro.parse_schema(
                __import__("json").loads(schema_str)
            )
        return self._parsed_writers[schema_id]

    def _get_reader_schema(self):
        if self._reader_schema_str is None:
            return None
        if self._parsed_reader is None:
            fastavro = _require_fastavro()
            self._parsed_reader = fastavro.parse_schema(
                __import__("json").loads(self._reader_schema_str)
            )
        return self._parsed_reader

    def deserialize(self, data: Optional[bytes]) -> Optional[Dict[str, Any]]:
        """
        Deserialize *data* from Confluent-framed Avro bytes.

        Parameters
        ----------
        data : Raw bytes from Kafka.  ``None`` → returns ``None``.

        Returns
        -------
        dict or None

        Raises
        ------
        ValueError  if the magic byte is wrong and ``fallback_json`` is False.
        """
        if data is None:
            return None
        if len(data) < _HEADER_SIZE:
            raise ValueError(
                f"Message too short ({len(data)} bytes); expected at least {_HEADER_SIZE}"
            )

        magic, schema_id = struct.unpack_from(_HEADER_FMT, data, 0)

        if magic != _MAGIC_BYTE:
            if self._fallback_json:
                import json
                return json.loads(data)
            raise ValueError(
                f"Invalid magic byte: expected 0x{_MAGIC_BYTE:02x}, got 0x{magic:02x}"
            )

        fastavro = _require_fastavro()
        writer_schema = self._get_writer_schema(schema_id)
        reader_schema = self._get_reader_schema()

        payload = io.BytesIO(data[_HEADER_SIZE:])
        return fastavro.schemaless_reader(payload, writer_schema, reader_schema)


# ---------------------------------------------------------------------------
# Convenience factory
# ---------------------------------------------------------------------------

def make_serde_pair(
    topic: str,
    schema_str: str,
    config: Optional[RegistryConfig] = None,
    client: Optional[SchemaRegistryClient] = None,
) -> tuple[AvroSerializer, AvroDeserializer]:
    """
    Convenience factory that returns a matched (serializer, deserializer) pair.

    >>> ser, de = make_serde_pair("my-topic", open("schemas/cdc_event_v2.avsc").read())
    >>> de.deserialize(ser.serialize({"op": "c", "ts_ms": 0, "after": None}))
    {'op': 'c', 'ts_ms': 0, 'after': None}
    """
    if client is None:
        client = SchemaRegistryClient(config or RegistryConfig())
    ser = AvroSerializer(client, topic, schema_str)
    de = AvroDeserializer(client)
    return ser, de
