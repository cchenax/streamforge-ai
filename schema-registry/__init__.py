"""
streamforge-ai schema-registry — Confluent Schema Registry + Apicurio integration.

Supports both registry backends via a single unified client, Confluent
wire-format Avro SerDe, and an offline schema compatibility checker.

Quick start — Confluent Schema Registry
----------------------------------------
    from schema_registry.config import RegistryConfig
    from schema_registry.client import SchemaRegistryClient
    from schema_registry.avro_serde import AvroSerializer, AvroDeserializer

    cfg    = RegistryConfig(url="http://localhost:8081")
    client = SchemaRegistryClient(cfg)

    schema = open("schemas/cdc_event_v2.avsc").read()
    sid    = client.register_schema("user-events-value", schema)

    ser = AvroSerializer(client, "user-events")
    de  = AvroDeserializer(client)

    raw = ser.serialize({"op": "c", "ts_ms": 1234567890, "after": None})
    obj = de.deserialize(raw)

Quick start — Apicurio Registry (compat mode)
----------------------------------------------
    from schema_registry.config import RegistryConfig, RegistryBackend

    cfg = RegistryConfig(
        url="http://localhost:8080",
        backend=RegistryBackend.APICURIO,
    )
    # All other calls are identical — the client prepends /apis/ccompat/v6 automatically.

Quick start — offline compatibility check (CI/CD)
--------------------------------------------------
    from schema_registry.evolve import CompatibilityChecker

    ok = CompatibilityChecker.is_compatible(new_schema_str, old_schema_str, "FULL")
    report = CompatibilityChecker.analyze(new_schema_str, old_schema_str)
    print(report.summary())

No-server development
---------------------
    from schema_registry.client import MockSchemaRegistryClient
    client = MockSchemaRegistryClient()
    # identical API; stores everything in memory
"""
from __future__ import annotations

import sys
from pathlib import Path

# Allow ``import client`` / ``import config`` etc. when running inside the package dir
_HERE = str(Path(__file__).parent)
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from config import RegistryConfig, RegistryBackend, SubjectNameStrategy
from client import (
    SchemaRegistryClient,
    MockSchemaRegistryClient,
    RegisteredSchema,
    SchemaRegistryError,
    SchemaNotFoundError,
    IncompatibleSchemaError,
)
from avro_serde import AvroSerializer, AvroDeserializer, make_serde_pair
from evolve import CompatibilityChecker, EvolutionReport, FieldChange, FieldChangeKind

__all__ = [
    # config
    "RegistryConfig",
    "RegistryBackend",
    "SubjectNameStrategy",
    # client
    "SchemaRegistryClient",
    "MockSchemaRegistryClient",
    "RegisteredSchema",
    "SchemaRegistryError",
    "SchemaNotFoundError",
    "IncompatibleSchemaError",
    # serde
    "AvroSerializer",
    "AvroDeserializer",
    "make_serde_pair",
    # evolution
    "CompatibilityChecker",
    "EvolutionReport",
    "FieldChange",
    "FieldChangeKind",
]
