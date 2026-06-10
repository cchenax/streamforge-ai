"""
Unit tests for the StreamForge schema-registry package.

No running registry server is required — all HTTP calls are mocked.

Run:
    cd schema-registry
    python -m pytest tests.py -v
"""
from __future__ import annotations

import io
import json
import os
import struct
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Make the package importable when run from within the schema-registry dir
sys.path.insert(0, str(Path(__file__).parent))

from config import RegistryConfig, RegistryBackend, SubjectNameStrategy
from client import (
    SchemaRegistryClient,
    MockSchemaRegistryClient,
    RegisteredSchema,
    SchemaRegistryError,
    SchemaNotFoundError,
    IncompatibleSchemaError,
    _LruCache,
)
from avro_serde import AvroSerializer, AvroDeserializer, _MAGIC_BYTE, _HEADER_SIZE
from evolve import (
    CompatibilityChecker,
    EvolutionReport,
    FieldChangeKind,
)

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

SCHEMA_DIR = Path(__file__).parent / "schemas"

SIMPLE_STRING_SCHEMA = '{"type": "string"}'
RECORD_V1 = json.dumps({
    "namespace": "ai.streamforge.test",
    "type": "record",
    "name": "Event",
    "fields": [
        {"name": "id",    "type": "string"},
        {"name": "value", "type": "int"},
    ],
})
# V2 adds a nullable field with default — BACKWARD-compatible with V1
RECORD_V2 = json.dumps({
    "namespace": "ai.streamforge.test",
    "type": "record",
    "name": "Event",
    "fields": [
        {"name": "id",    "type": "string"},
        {"name": "value", "type": "int"},
        {"name": "label", "type": ["null", "string"], "default": None},
    ],
})
# V3 drops "value" — FORWARD-breaking, BACKWARD-compatible
RECORD_V3_DROP = json.dumps({
    "namespace": "ai.streamforge.test",
    "type": "record",
    "name": "Event",
    "fields": [
        {"name": "id",    "type": "string"},
        {"name": "label", "type": ["null", "string"], "default": None},
    ],
})
# V_BAD adds required field (no default) — BACKWARD-breaking
RECORD_V_REQUIRED = json.dumps({
    "namespace": "ai.streamforge.test",
    "type": "record",
    "name": "Event",
    "fields": [
        {"name": "id",    "type": "string"},
        {"name": "value", "type": "int"},
        {"name": "required_field", "type": "string"},   # no default!
    ],
})


def _mock_response(status_code: int, body: dict | list) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    resp.content = b"x"
    resp.json.return_value = body
    resp.text = json.dumps(body)
    resp.reason = "OK" if status_code < 400 else "Error"
    return resp


def _make_client(session) -> SchemaRegistryClient:
    cfg = RegistryConfig(url="http://registry:8081")
    return SchemaRegistryClient(cfg, session=session)


# ===========================================================================
# RegistryConfig tests
# ===========================================================================

class TestRegistryConfig:
    def test_defaults(self):
        cfg = RegistryConfig()
        assert cfg.url == "http://localhost:8081"
        assert cfg.backend == RegistryBackend.CONFLUENT
        assert cfg.cache_size == 512
        assert cfg.ssl_verify is True
        assert cfg.auth is None

    def test_api_prefix_confluent(self):
        cfg = RegistryConfig(backend=RegistryBackend.CONFLUENT)
        assert cfg.api_prefix == ""

    def test_api_prefix_apicurio(self):
        cfg = RegistryConfig(backend=RegistryBackend.APICURIO)
        assert cfg.api_prefix == "/apis/ccompat/v6"

    def test_auth_basic(self):
        cfg = RegistryConfig(username="alice", password="secret")
        assert cfg.auth == ("alice", "secret")

    def test_auth_api_key_takes_precedence(self):
        cfg = RegistryConfig(username="u", password="p", api_key="key", api_secret="sec")
        assert cfg.auth == ("key", "sec")

    def test_auth_none_when_no_creds(self):
        cfg = RegistryConfig()
        assert cfg.auth is None

    def test_from_env(self, monkeypatch):
        monkeypatch.setenv("SCHEMA_REGISTRY_URL", "http://sr.example.com:8081")
        monkeypatch.setenv("SCHEMA_REGISTRY_BACKEND", "apicurio")
        monkeypatch.setenv("SCHEMA_REGISTRY_USERNAME", "u")
        monkeypatch.setenv("SCHEMA_REGISTRY_PASSWORD", "p")
        monkeypatch.setenv("SCHEMA_REGISTRY_SSL_VERIFY", "false")
        monkeypatch.setenv("SCHEMA_REGISTRY_TIMEOUT", "20")
        monkeypatch.setenv("SCHEMA_REGISTRY_CACHE_SIZE", "256")
        cfg = RegistryConfig.from_env()
        assert cfg.url == "http://sr.example.com:8081"
        assert cfg.backend == RegistryBackend.APICURIO
        assert cfg.auth == ("u", "p")
        assert cfg.ssl_verify is False
        assert cfg.timeout_seconds == 20
        assert cfg.cache_size == 256

    def test_from_env_defaults_when_empty(self, monkeypatch):
        for k in [
            "SCHEMA_REGISTRY_URL", "SCHEMA_REGISTRY_BACKEND",
            "SCHEMA_REGISTRY_USERNAME", "SCHEMA_REGISTRY_PASSWORD",
        ]:
            monkeypatch.delenv(k, raising=False)
        cfg = RegistryConfig.from_env()
        assert cfg.url == "http://localhost:8081"
        assert cfg.backend == RegistryBackend.CONFLUENT

    def test_from_yaml(self, tmp_path):
        yaml = pytest.importorskip("yaml")
        cfg_file = tmp_path / "sr.yaml"
        cfg_file.write_text(
            "schema_registry:\n"
            "  url: http://apicurio:8080\n"
            "  backend: apicurio\n"
            "  cache_size: 128\n"
        )
        cfg = RegistryConfig.from_yaml(cfg_file)
        assert cfg.url == "http://apicurio:8080"
        assert cfg.backend == RegistryBackend.APICURIO
        assert cfg.cache_size == 128

    def test_repr(self):
        cfg = RegistryConfig(url="http://x:8081")
        assert "x:8081" in repr(cfg)
        assert "confluent" in repr(cfg)

    def test_trailing_slash_stripped(self):
        cfg = RegistryConfig.from_env.__func__(RegistryConfig)  # type: ignore[attr-defined]
        # directly test constructor
        cfg = RegistryConfig(url="http://host:8081/")
        # url is set as-is in __init__; stripping happens in from_env / from_yaml
        # We test that the client strips it:
        client = SchemaRegistryClient(RegistryConfig(url="http://host:8081/"))
        assert not client._base.endswith("/")


# ===========================================================================
# SubjectNameStrategy tests
# ===========================================================================

class TestSubjectNameStrategy:
    def test_topic_name_value(self):
        assert SubjectNameStrategy.TOPIC_NAME.subject("my-topic") == "my-topic-value"

    def test_topic_name_key(self):
        assert SubjectNameStrategy.TOPIC_NAME.subject("my-topic", is_key=True) == "my-topic-key"

    def test_record_name(self):
        assert SubjectNameStrategy.RECORD_NAME.subject(
            record_name="ai.sf.CdcEvent"
        ) == "ai.sf.CdcEvent"

    def test_topic_record_name(self):
        s = SubjectNameStrategy.TOPIC_RECORD_NAME.subject(
            topic="events", record_name="MyRecord"
        )
        assert s == "events-MyRecord"

    def test_record_name_fallback_no_record(self):
        s = SubjectNameStrategy.RECORD_NAME.subject(topic="t")
        assert "value" in s


# ===========================================================================
# LRU cache tests
# ===========================================================================

class TestLruCache:
    def test_basic_put_get(self):
        c = _LruCache(3)
        c.put(1, "a")
        assert c.get(1) == "a"

    def test_eviction(self):
        c = _LruCache(2)
        c.put(1, "a")
        c.put(2, "b")
        c.put(3, "c")   # evicts 1
        assert c.get(1) is None
        assert c.get(2) == "b"
        assert c.get(3) == "c"

    def test_access_refreshes_order(self):
        c = _LruCache(2)
        c.put(1, "a")
        c.put(2, "b")
        c.get(1)        # refresh 1
        c.put(3, "c")   # should evict 2, not 1
        assert c.get(1) == "a"
        assert c.get(2) is None

    def test_overwrite(self):
        c = _LruCache(2)
        c.put(1, "a")
        c.put(1, "b")
        assert c.get(1) == "b"
        assert len(c) == 1


# ===========================================================================
# SchemaRegistryClient (mocked HTTP) tests
# ===========================================================================

class TestSchemaRegistryClientHTTP:
    def _session(self, status_code, body):
        s = MagicMock()
        s.request.return_value = _mock_response(status_code, body)
        return s

    def test_register_schema_returns_id(self):
        s = self._session(200, {"id": 7})
        client = _make_client(s)
        sid = client.register_schema("my-subject", RECORD_V1)
        assert sid == 7
        s.request.assert_called_once()
        payload = json.loads(s.request.call_args.kwargs["json"] if "json" in s.request.call_args.kwargs
                             else s.request.call_args[1]["json"])
        assert payload["schema"] == RECORD_V1
        assert payload["schemaType"] == "AVRO"

    def test_get_schema_by_id(self):
        s = self._session(200, {"schema": RECORD_V1})
        client = _make_client(s)
        result = client.get_schema_by_id(42)
        assert result == RECORD_V1

    def test_get_schema_by_id_cached(self):
        s = self._session(200, {"schema": RECORD_V1})
        client = _make_client(s)
        client.get_schema_by_id(42)
        client.get_schema_by_id(42)   # second call — should not hit server
        assert s.request.call_count == 1

    def test_get_schema_latest(self):
        body = {"id": 1, "schema": RECORD_V1, "subject": "s", "version": 1, "schemaType": "AVRO"}
        s = self._session(200, body)
        client = _make_client(s)
        rs = client.get_schema("my-subject")
        assert rs.schema_id == 1
        assert rs.schema_str == RECORD_V1
        assert rs.version == 1

    def test_get_schema_caches_by_id_too(self):
        body = {"id": 5, "schema": RECORD_V1, "subject": "s", "version": 1, "schemaType": "AVRO"}
        s = self._session(200, body)
        client = _make_client(s)
        client.get_schema("my-subject")
        # Now get_schema_by_id(5) should not hit server
        result = client.get_schema_by_id(5)
        assert result == RECORD_V1
        assert s.request.call_count == 1

    def test_check_compatibility_true(self):
        s = self._session(200, {"is_compatible": True})
        client = _make_client(s)
        assert client.check_compatibility("subj", RECORD_V2) is True

    def test_check_compatibility_false(self):
        s = self._session(200, {"is_compatible": False})
        client = _make_client(s)
        assert client.check_compatibility("subj", RECORD_V_REQUIRED) is False

    def test_check_compatibility_subject_not_found(self):
        s = MagicMock()
        s.request.return_value = _mock_response(404, {"error_code": 40401, "message": "Not found"})
        client = _make_client(s)
        # No versions yet → trivially compatible
        assert client.check_compatibility("new-subj", RECORD_V1) is True

    def test_assert_compatible_raises(self):
        s = self._session(200, {"is_compatible": False})
        client = _make_client(s)
        with pytest.raises(IncompatibleSchemaError):
            client.assert_compatible("subj", RECORD_V_REQUIRED)

    def test_set_compatibility(self):
        s = self._session(200, {"compatibility": "FULL"})
        client = _make_client(s)
        client.set_compatibility("subj", "FULL")
        body = s.request.call_args.kwargs.get("json") or s.request.call_args[1].get("json")
        assert body["compatibility"] == "FULL"

    def test_get_compatibility(self):
        s = self._session(200, {"compatibilityLevel": "BACKWARD"})
        client = _make_client(s)
        assert client.get_compatibility("subj") == "BACKWARD"

    def test_list_subjects(self):
        s = self._session(200, ["events-value", "counts-value"])
        client = _make_client(s)
        assert client.list_subjects() == ["events-value", "counts-value"]

    def test_list_versions(self):
        s = self._session(200, [1, 2, 3])
        client = _make_client(s)
        assert client.list_versions("my-subj") == [1, 2, 3]

    def test_delete_subject(self):
        s = self._session(200, [1, 2])
        client = _make_client(s)
        deleted = client.delete_subject("old-subj")
        assert deleted == [1, 2]

    def test_4xx_raises_schema_registry_error(self):
        s = MagicMock()
        s.request.return_value = _mock_response(
            422, {"error_code": 42201, "message": "Invalid schema"}
        )
        client = _make_client(s)
        with pytest.raises(SchemaRegistryError) as exc_info:
            client.register_schema("s", "not-valid-schema")
        assert exc_info.value.status_code == 422

    def test_404_raises_schema_not_found(self):
        s = MagicMock()
        s.request.return_value = _mock_response(
            404, {"error_code": 40401, "message": "Subject not found"}
        )
        client = _make_client(s)
        with pytest.raises(SchemaNotFoundError):
            client.get_schema("no-such-subject")

    def test_5xx_retries(self):
        fail = _mock_response(503, {"message": "unavailable"})
        ok   = _mock_response(200, {"id": 1})
        s = MagicMock()
        s.request.side_effect = [fail, fail, ok]
        client = _make_client(s)
        with patch("client.time.sleep"):   # don't actually sleep
            sid = client.register_schema("s", RECORD_V1)
        assert sid == 1
        assert s.request.call_count == 3

    def test_apicurio_url_prefix(self):
        cfg = RegistryConfig(url="http://apicurio:8080", backend=RegistryBackend.APICURIO)
        client = SchemaRegistryClient(cfg, session=self._session(200, {"id": 1}))
        assert "/apis/ccompat/v6" in client._base


# ===========================================================================
# MockSchemaRegistryClient tests
# ===========================================================================

class TestMockSchemaRegistryClient:
    def test_register_and_retrieve(self):
        c = MockSchemaRegistryClient()
        sid = c.register_schema("t-value", RECORD_V1)
        assert c.get_schema_by_id(sid) == RECORD_V1

    def test_idempotent_register(self):
        c = MockSchemaRegistryClient()
        sid1 = c.register_schema("t-value", RECORD_V1)
        sid2 = c.register_schema("t-value", RECORD_V1)
        assert sid1 == sid2

    def test_multiple_versions(self):
        c = MockSchemaRegistryClient()
        c.register_schema("t-value", RECORD_V1)
        c.register_schema("t-value", RECORD_V2)
        assert c.list_versions("t-value") == [1, 2]
        assert c.get_schema("t-value", "1").schema_str == RECORD_V1
        assert c.get_schema("t-value", "latest").schema_str == RECORD_V2

    def test_get_unknown_id_raises(self):
        c = MockSchemaRegistryClient()
        with pytest.raises(SchemaNotFoundError):
            c.get_schema_by_id(999)

    def test_get_unknown_subject_raises(self):
        c = MockSchemaRegistryClient()
        with pytest.raises(SchemaNotFoundError):
            c.get_schema("no-such-subject")

    def test_compatibility_check_delegates_to_evolve(self):
        c = MockSchemaRegistryClient()
        c.register_schema("t-value", RECORD_V1)
        c.set_compatibility("t-value", "BACKWARD")
        assert c.check_compatibility("t-value", RECORD_V2) is True

    def test_list_subjects(self):
        c = MockSchemaRegistryClient()
        c.register_schema("a-value", RECORD_V1)
        c.register_schema("b-value", RECORD_V1)
        assert set(c.list_subjects()) == {"a-value", "b-value"}

    def test_delete_subject(self):
        c = MockSchemaRegistryClient()
        c.register_schema("old", RECORD_V1)
        deleted = c.delete_subject("old")
        assert deleted == [1]
        assert "old" not in c.list_subjects()

    def test_get_or_register(self):
        c = MockSchemaRegistryClient()
        sid1 = c.get_or_register("t-value", RECORD_V1)
        sid2 = c.get_or_register("t-value", RECORD_V1)   # same schema
        assert sid1 == sid2


# ===========================================================================
# AvroSerializer / AvroDeserializer tests
# ===========================================================================

def _avro_schema_str():
    """Return a simple testable Avro schema string."""
    return json.dumps({
        "type": "record",
        "name": "Ping",
        "namespace": "test",
        "fields": [
            {"name": "id",    "type": "string"},
            {"name": "value", "type": "long"},
        ],
    })


@pytest.fixture
def avro_client():
    """MockSchemaRegistryClient pre-loaded with a simple Avro schema."""
    c = MockSchemaRegistryClient()
    c.register_schema("test-topic-value", _avro_schema_str())
    return c


class TestAvroSerde:
    pytest.importorskip("fastavro", reason="fastavro not installed")

    def test_magic_byte_present(self, avro_client):
        ser = AvroSerializer(avro_client, "test-topic", _avro_schema_str())
        raw = ser.serialize({"id": "x", "value": 1})
        assert raw[0] == _MAGIC_BYTE

    def test_schema_id_in_header(self, avro_client):
        schema_id = avro_client.register_schema("test-topic-value", _avro_schema_str())
        ser = AvroSerializer(avro_client, "test-topic", _avro_schema_str())
        raw = ser.serialize({"id": "abc", "value": 99})
        _, embedded_id = struct.unpack_from(">bI", raw, 0)
        assert embedded_id == schema_id

    def test_round_trip(self, avro_client):
        ser = AvroSerializer(avro_client, "test-topic", _avro_schema_str())
        de  = AvroDeserializer(avro_client)
        record = {"id": "user-42", "value": 1234567890}
        assert de.deserialize(ser.serialize(record)) == record

    def test_deserialize_none_returns_none(self, avro_client):
        de = AvroDeserializer(avro_client)
        assert de.deserialize(None) is None

    def test_deserialize_too_short_raises(self, avro_client):
        de = AvroDeserializer(avro_client)
        with pytest.raises(ValueError, match="too short"):
            de.deserialize(b"\x00\x01\x02")

    def test_wrong_magic_byte_fallback_json(self, avro_client):
        de = AvroDeserializer(avro_client, fallback_json=True)
        payload = json.dumps({"hello": "world"}).encode()
        result = de.deserialize(payload)
        assert result == {"hello": "world"}

    def test_wrong_magic_byte_no_fallback_raises(self, avro_client):
        de = AvroDeserializer(avro_client, fallback_json=False)
        payload = b"\x01" + b"\x00" * 4 + b"data"
        with pytest.raises(ValueError, match="magic byte"):
            de.deserialize(payload)

    def test_multiple_records(self, avro_client):
        ser = AvroSerializer(avro_client, "test-topic", _avro_schema_str())
        de  = AvroDeserializer(avro_client)
        records = [{"id": str(i), "value": i * 10} for i in range(20)]
        decoded  = [de.deserialize(ser.serialize(r)) for r in records]
        assert decoded == records

    def test_subject_name_strategy_topic_name(self, avro_client):
        ser = AvroSerializer(
            avro_client, "events",
            _avro_schema_str(),
            strategy=SubjectNameStrategy.TOPIC_NAME,
        )
        ser.serialize({"id": "1", "value": 0})  # triggers initialization
        assert avro_client.list_subjects()   # schema must be registered


# ===========================================================================
# CompatibilityChecker (evolve.py) tests
# ===========================================================================

class TestCompatibilityChecker:
    def test_same_schema_is_full(self):
        r = CompatibilityChecker.analyze(RECORD_V1, RECORD_V1)
        assert r.is_full

    def test_add_nullable_field_is_full(self):
        r = CompatibilityChecker.analyze(RECORD_V2, RECORD_V1)
        assert r.is_backward
        assert r.is_forward
        assert r.is_full

    def test_add_required_field_breaks_backward(self):
        r = CompatibilityChecker.analyze(RECORD_V_REQUIRED, RECORD_V1)
        assert not r.is_backward
        assert r.breaking_fields

    def test_drop_field_breaks_forward(self):
        r = CompatibilityChecker.analyze(RECORD_V3_DROP, RECORD_V2)
        assert not r.is_forward
        assert r.is_backward   # new schema drops; OLD can still read new data

    def test_type_widening_is_backward(self):
        old = json.dumps({"type": "record", "name": "T", "namespace": "n",
                          "fields": [{"name": "x", "type": "int"}]})
        new = json.dumps({"type": "record", "name": "T", "namespace": "n",
                          "fields": [{"name": "x", "type": "long"}]})
        r = CompatibilityChecker.analyze(new, old)
        assert r.is_backward
        kinds = {c.kind for c in r.changes}
        assert FieldChangeKind.TYPE_WIDENED in kinds

    def test_incompatible_type_change_is_breaking(self):
        old = json.dumps({"type": "record", "name": "T", "namespace": "n",
                          "fields": [{"name": "x", "type": "int"}]})
        new = json.dumps({"type": "record", "name": "T", "namespace": "n",
                          "fields": [{"name": "x", "type": "string"}]})
        r = CompatibilityChecker.analyze(new, old)
        assert not r.is_backward
        assert not r.is_forward

    def test_is_compatible_none_always_true(self):
        assert CompatibilityChecker.is_compatible(RECORD_V_REQUIRED, RECORD_V1, "NONE")

    def test_is_compatible_backward_add_nullable(self):
        assert CompatibilityChecker.is_compatible(RECORD_V2, RECORD_V1, "BACKWARD")

    def test_is_compatible_full_add_nullable(self):
        assert CompatibilityChecker.is_compatible(RECORD_V2, RECORD_V1, "FULL")

    def test_is_compatible_backward_required_field_fails(self):
        assert not CompatibilityChecker.is_compatible(RECORD_V_REQUIRED, RECORD_V1, "BACKWARD")

    def test_summary_full(self):
        r = CompatibilityChecker.analyze(RECORD_V2, RECORD_V1)
        assert "FULL" in r.summary()

    def test_summary_breaking(self):
        r = CompatibilityChecker.analyze(RECORD_V_REQUIRED, RECORD_V1)
        assert "BREAKING" in r.summary()

    def test_invalid_json_raises(self):
        with pytest.raises(json.JSONDecodeError):
            CompatibilityChecker.analyze("not-json", RECORD_V1)

    def test_transitive_suffix_stripped(self):
        assert CompatibilityChecker.is_compatible(RECORD_V2, RECORD_V1, "BACKWARD_TRANSITIVE")


# ===========================================================================
# Avro schema files
# ===========================================================================

class TestAvroSchemaFiles:
    @pytest.mark.parametrize("fname", [
        "cdc_event_v1.avsc",
        "cdc_event_v2.avsc",
        "cdc_event_v3.avsc",
        "user_event_count.avsc",
    ])
    def test_schema_file_exists_and_parses(self, fname):
        path = SCHEMA_DIR / fname
        assert path.exists(), f"{fname} not found in schemas/"
        schema = json.loads(path.read_text())
        assert schema["type"] == "record"
        assert "name" in schema
        assert "fields" in schema

    def test_cdc_event_versions_are_backward_compatible(self):
        v1 = (SCHEMA_DIR / "cdc_event_v1.avsc").read_text()
        v2 = (SCHEMA_DIR / "cdc_event_v2.avsc").read_text()
        v3 = (SCHEMA_DIR / "cdc_event_v3.avsc").read_text()

        assert CompatibilityChecker.is_compatible(v2, v1, "BACKWARD"), "V2 should be BACKWARD-compatible with V1"
        assert CompatibilityChecker.is_compatible(v3, v2, "BACKWARD"), "V3 should be BACKWARD-compatible with V2"
        assert CompatibilityChecker.is_compatible(v3, v1, "BACKWARD"), "V3 should be BACKWARD-compatible with V1"

    def test_user_event_count_has_expected_fields(self):
        schema = json.loads((SCHEMA_DIR / "user_event_count.avsc").read_text())
        field_names = {f["name"] for f in schema["fields"]}
        assert {"user_id", "event_count", "window_start_ms", "window_end_ms"} <= field_names


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
