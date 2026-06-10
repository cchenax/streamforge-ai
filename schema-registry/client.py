"""
SchemaRegistryClient — unified REST client for Confluent Schema Registry and
Apicurio Registry (Confluent-compat mode).

Both backends expose the same Confluent Schema Registry REST API surface:
  • Confluent SR (self-hosted / Cloud): served at the registry root
  • Apicurio v2+: served at /apis/ccompat/v6  (compat mode)

The RegistryConfig.api_prefix property handles the URL difference transparently,
so all methods work identically for both backends.

Usage
-----
    from schema_registry.config import RegistryConfig
    from schema_registry.client import SchemaRegistryClient

    client = SchemaRegistryClient(RegistryConfig.from_env())

    # Register a schema
    schema_id = client.register_schema("user-events-value", open("schemas/cdc_event_v2.avsc").read())

    # Fetch by ID (cached after first call)
    schema_str = client.get_schema_by_id(schema_id)

    # Check compatibility before deploying a new version
    ok = client.check_compatibility("user-events-value", new_schema_str)

Error handling
--------------
    SchemaRegistryError   — 4xx/5xx HTTP response; carries .status_code and .message
    SchemaNotFoundError   — 404 (subject or schema ID not found)
    IncompatibleSchemaError — compatibility check returned False
"""
from __future__ import annotations

import json
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Dict, List, Optional

from config import RegistryConfig


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class SchemaRegistryError(Exception):
    """Raised when the registry returns a non-2xx response."""
    def __init__(self, message: str, status_code: int = 0, error_code: int = 0):
        super().__init__(message)
        self.status_code = status_code
        self.error_code = error_code  # Confluent error_code field


class SchemaNotFoundError(SchemaRegistryError):
    """Raised when the requested schema / subject / version does not exist."""


class IncompatibleSchemaError(SchemaRegistryError):
    """Raised when a schema fails the registered compatibility check."""


# ---------------------------------------------------------------------------
# Value objects
# ---------------------------------------------------------------------------

@dataclass
class RegisteredSchema:
    """A schema version returned from the registry."""
    schema_id: int
    schema_str: str
    subject: str
    version: int
    schema_type: str = "AVRO"


# ---------------------------------------------------------------------------
# LRU cache
# ---------------------------------------------------------------------------

class _LruCache:
    """Simple LRU cache backed by an OrderedDict."""

    def __init__(self, capacity: int) -> None:
        self._cap = max(1, capacity)
        self._data: OrderedDict = OrderedDict()

    def get(self, key):
        if key not in self._data:
            return None
        self._data.move_to_end(key)
        return self._data[key]

    def put(self, key, value) -> None:
        if key in self._data:
            self._data.move_to_end(key)
        self._data[key] = value
        if len(self._data) > self._cap:
            self._data.popitem(last=False)

    def __len__(self) -> int:
        return len(self._data)


# ---------------------------------------------------------------------------
# SchemaRegistryClient
# ---------------------------------------------------------------------------

class SchemaRegistryClient:
    """
    HTTP client for Confluent Schema Registry / Apicurio (compat mode).

    All HTTP calls use ``requests`` with automatic retry on 5xx responses
    (up to 3 attempts with exponential back-off).  Schema strings fetched by
    ID are cached in an LRU cache of ``config.cache_size`` entries.

    Parameters
    ----------
    config : RegistryConfig
        Connection details (URL, auth, backend, cache size).
    session : optional requests.Session
        Inject a pre-configured session (useful for testing).
    """

    _RETRY_ATTEMPTS = 3
    _RETRY_BASE_DELAY = 0.25  # seconds

    def __init__(
        self,
        config: Optional[RegistryConfig] = None,
        *,
        session=None,
    ) -> None:
        self._config = config or RegistryConfig()
        self._base = self._config.url.rstrip("/") + self._config.api_prefix
        self._by_id_cache: _LruCache = _LruCache(self._config.cache_size)
        self._subject_cache: _LruCache = _LruCache(self._config.cache_size)
        self._session = session  # injected in tests; lazy-init in production

    # ------------------------------------------------------------------
    # Session
    # ------------------------------------------------------------------

    def _get_session(self):
        if self._session is not None:
            return self._session
        try:
            import requests
        except ImportError as exc:
            raise ImportError("requests is required: pip install requests") from exc
        s = requests.Session()
        s.headers.update({"Content-Type": "application/vnd.schemaregistry.v1+json"})
        if self._config.auth:
            s.auth = self._config.auth
        return s

    # ------------------------------------------------------------------
    # Internal HTTP helpers
    # ------------------------------------------------------------------

    def _request(self, method: str, path: str, **kwargs) -> dict:
        """Execute an HTTP request with retry-on-5xx and raise on error."""
        url = f"{self._base}{path}"
        session = self._get_session()
        last_exc = None

        for attempt in range(self._RETRY_ATTEMPTS):
            try:
                resp = session.request(
                    method,
                    url,
                    verify=self._config.ssl_verify,
                    timeout=self._config.timeout_seconds,
                    **kwargs,
                )
            except Exception as exc:
                last_exc = exc
                time.sleep(self._RETRY_BASE_DELAY * (2 ** attempt))
                continue

            if resp.status_code < 300:
                if not resp.content:
                    return {}
                return resp.json()

            # 5xx → retry
            if resp.status_code >= 500:
                last_exc = _make_error(resp)
                time.sleep(self._RETRY_BASE_DELAY * (2 ** attempt))
                continue

            # 4xx → raise immediately (no retry)
            raise _make_error(resp)

        raise (last_exc or SchemaRegistryError("Max retries exceeded"))

    # ------------------------------------------------------------------
    # Schema registration
    # ------------------------------------------------------------------

    def register_schema(
        self,
        subject: str,
        schema_str: str,
        schema_type: str = "AVRO",
        normalize: bool = False,
    ) -> int:
        """
        Register a schema under *subject*.

        If the same schema is already registered the registry returns the
        existing ID without creating a new version.

        Parameters
        ----------
        subject     : Subject name, e.g. ``"user-events-value"``.
        schema_str  : JSON string of the schema.
        schema_type : ``"AVRO"`` (default), ``"JSON"``, or ``"PROTOBUF"``.
        normalize   : Ask the registry to normalize the schema before storing.

        Returns
        -------
        int
            The globally unique schema ID.
        """
        body: Dict = {"schema": schema_str, "schemaType": schema_type}
        if normalize:
            body["normalize"] = True
        result = self._request("POST", f"/subjects/{subject}/versions", json=body)
        schema_id: int = result["id"]
        # Populate ID cache immediately so the caller doesn't need a second fetch
        self._by_id_cache.put(schema_id, schema_str)
        return schema_id

    # ------------------------------------------------------------------
    # Schema retrieval
    # ------------------------------------------------------------------

    def get_schema_by_id(self, schema_id: int) -> str:
        """
        Return the schema string for a globally-unique ID.

        Cached — subsequent calls with the same ID skip the HTTP round-trip.
        """
        cached = self._by_id_cache.get(schema_id)
        if cached is not None:
            return cached
        result = self._request("GET", f"/schemas/ids/{schema_id}")
        schema_str: str = result["schema"]
        self._by_id_cache.put(schema_id, schema_str)
        return schema_str

    def get_schema(self, subject: str, version: str = "latest") -> RegisteredSchema:
        """
        Return a ``RegisteredSchema`` for a subject + version.

        Parameters
        ----------
        subject : Subject name.
        version : Integer version number or ``"latest"`` (default).

        Returns
        -------
        RegisteredSchema
        """
        cache_key = f"{subject}:{version}"
        cached = self._subject_cache.get(cache_key)
        if cached is not None:
            return cached

        result = self._request("GET", f"/subjects/{subject}/versions/{version}")
        rs = RegisteredSchema(
            schema_id=result["id"],
            schema_str=result["schema"],
            subject=result.get("subject", subject),
            version=result.get("version", 0),
            schema_type=result.get("schemaType", "AVRO"),
        )
        self._subject_cache.put(cache_key, rs)
        self._by_id_cache.put(rs.schema_id, rs.schema_str)
        return rs

    # ------------------------------------------------------------------
    # Compatibility
    # ------------------------------------------------------------------

    def check_compatibility(
        self,
        subject: str,
        schema_str: str,
        version: str = "latest",
        schema_type: str = "AVRO",
    ) -> bool:
        """
        Test whether *schema_str* is compatible with the registered version.

        Uses the ``/compatibility/`` endpoint; does **not** register anything.

        Returns
        -------
        bool  ``True`` if compatible, ``False`` otherwise.

        Raises
        ------
        SchemaNotFoundError  if the subject has no registered versions yet
                             (treat a brand-new subject as always compatible).
        """
        body: Dict = {"schema": schema_str, "schemaType": schema_type}
        try:
            result = self._request(
                "POST",
                f"/compatibility/subjects/{subject}/versions/{version}",
                json=body,
            )
        except SchemaNotFoundError:
            # No versions yet → trivially compatible
            return True
        return bool(result.get("is_compatible", False))

    def assert_compatible(
        self,
        subject: str,
        schema_str: str,
        version: str = "latest",
    ) -> None:
        """Like :meth:`check_compatibility` but raises :exc:`IncompatibleSchemaError` on failure."""
        if not self.check_compatibility(subject, schema_str, version):
            raise IncompatibleSchemaError(
                f"Schema is not compatible with {subject!r} version {version!r}",
                status_code=409,
            )

    # ------------------------------------------------------------------
    # Compatibility level config
    # ------------------------------------------------------------------

    def set_compatibility(self, subject: str, level: str) -> None:
        """
        Set the compatibility level for *subject*.

        Parameters
        ----------
        level : ``"BACKWARD"``, ``"BACKWARD_TRANSITIVE"``, ``"FORWARD"``,
                ``"FORWARD_TRANSITIVE"``, ``"FULL"``, ``"FULL_TRANSITIVE"``,
                or ``"NONE"``.
        """
        self._request("PUT", f"/config/{subject}", json={"compatibility": level.upper()})

    def get_compatibility(self, subject: str) -> str:
        """Return the current compatibility level for *subject*."""
        try:
            result = self._request("GET", f"/config/{subject}")
        except SchemaNotFoundError:
            # Subject has no explicit config — return the global default
            result = self._request("GET", "/config")
        return result.get("compatibilityLevel", result.get("compatibility", "NONE"))

    # ------------------------------------------------------------------
    # Listing / deletion
    # ------------------------------------------------------------------

    def list_subjects(self) -> List[str]:
        """Return all subject names registered in the registry."""
        result = self._request("GET", "/subjects")
        return list(result) if isinstance(result, list) else []

    def list_versions(self, subject: str) -> List[int]:
        """Return all version numbers for *subject*."""
        result = self._request("GET", f"/subjects/{subject}/versions")
        return list(result) if isinstance(result, list) else []

    def delete_subject(self, subject: str, permanent: bool = False) -> List[int]:
        """
        Soft-delete all versions of *subject* (or hard-delete if *permanent* is True).

        Returns the list of deleted version numbers.
        """
        path = f"/subjects/{subject}"
        if permanent:
            path += "?permanent=true"
        result = self._request("DELETE", path)
        return list(result) if isinstance(result, list) else []

    def delete_version(
        self, subject: str, version: int, permanent: bool = False
    ) -> int:
        """Delete a specific version of *subject*. Returns the deleted version number."""
        path = f"/subjects/{subject}/versions/{version}"
        if permanent:
            path += "?permanent=true"
        result = self._request("DELETE", path)
        return int(result) if isinstance(result, int) else version

    # ------------------------------------------------------------------
    # Convenience
    # ------------------------------------------------------------------

    def get_or_register(self, subject: str, schema_str: str) -> int:
        """
        Return the schema ID, registering the schema first if not already present.

        Safe to call repeatedly — idempotent when schema content is unchanged.
        """
        try:
            rs = self.get_schema(subject, "latest")
            if rs.schema_str == schema_str:
                return rs.schema_id
        except SchemaNotFoundError:
            pass
        return self.register_schema(subject, schema_str)

    def __repr__(self) -> str:
        return (
            f"SchemaRegistryClient(url={self._config.url!r}, "
            f"backend={self._config.backend.value!r})"
        )


# ---------------------------------------------------------------------------
# Mock client (no server required — for local development and testing)
# ---------------------------------------------------------------------------

class MockSchemaRegistryClient(SchemaRegistryClient):
    """
    In-memory mock that satisfies the full SchemaRegistryClient interface
    without any network calls.

    Useful for local development, unit tests, and CI pipelines without a
    running registry.

    >>> client = MockSchemaRegistryClient()
    >>> sid = client.register_schema("my-topic-value", '{"type":"string"}')
    >>> client.get_schema_by_id(sid)
    '{"type":"string"}'
    """

    def __init__(self) -> None:
        super().__init__(session=_MockSession())
        self._store: Dict[str, List[dict]] = {}   # subject → [{"id": ..., "schema": ...}]
        self._id_map: Dict[int, str] = {}          # global id → schema_str
        self._next_id: int = 1
        self._compat: Dict[str, str] = {}          # subject → level

    def register_schema(
        self,
        subject: str,
        schema_str: str,
        schema_type: str = "AVRO",
        normalize: bool = False,
    ) -> int:
        versions = self._store.setdefault(subject, [])
        # Idempotent: return existing ID if schema unchanged
        for entry in versions:
            if entry["schema"] == schema_str:
                return entry["id"]
        schema_id = self._next_id
        self._next_id += 1
        versions.append({"id": schema_id, "schema": schema_str, "schemaType": schema_type})
        self._id_map[schema_id] = schema_str
        self._by_id_cache.put(schema_id, schema_str)
        return schema_id

    def get_schema_by_id(self, schema_id: int) -> str:
        cached = self._by_id_cache.get(schema_id)
        if cached is not None:
            return cached
        if schema_id not in self._id_map:
            raise SchemaNotFoundError(f"Schema {schema_id} not found", status_code=404, error_code=40403)
        return self._id_map[schema_id]

    def get_schema(self, subject: str, version: str = "latest") -> RegisteredSchema:
        versions = self._store.get(subject, [])
        if not versions:
            raise SchemaNotFoundError(f"Subject {subject!r} not found", status_code=404, error_code=40401)
        if version == "latest":
            entry = versions[-1]
            ver_num = len(versions)
        else:
            ver_num = int(version)
            if ver_num < 1 or ver_num > len(versions):
                raise SchemaNotFoundError(
                    f"Version {ver_num} of {subject!r} not found", status_code=404, error_code=40402
                )
            entry = versions[ver_num - 1]
        return RegisteredSchema(
            schema_id=entry["id"],
            schema_str=entry["schema"],
            subject=subject,
            version=ver_num,
            schema_type=entry.get("schemaType", "AVRO"),
        )

    def check_compatibility(
        self,
        subject: str,
        schema_str: str,
        version: str = "latest",
        schema_type: str = "AVRO",
    ) -> bool:
        if subject not in self._store:
            return True
        # Delegate to the offline checker if fastavro is available
        try:
            from evolve import CompatibilityChecker
            existing = self.get_schema(subject, version)
            level = self._compat.get(subject, "BACKWARD")
            return CompatibilityChecker.is_compatible(schema_str, existing.schema_str, level)
        except Exception:
            return True

    def set_compatibility(self, subject: str, level: str) -> None:
        self._compat[subject] = level.upper()

    def get_compatibility(self, subject: str) -> str:
        return self._compat.get(subject, "BACKWARD")

    def list_subjects(self) -> List[str]:
        return list(self._store.keys())

    def list_versions(self, subject: str) -> List[int]:
        return list(range(1, len(self._store.get(subject, [])) + 1))

    def delete_subject(self, subject: str, permanent: bool = False) -> List[int]:
        versions = self._store.pop(subject, [])
        if permanent:
            for entry in versions:
                self._id_map.pop(entry["id"], None)
        return list(range(1, len(versions) + 1))

    def delete_version(self, subject: str, version: int, permanent: bool = False) -> int:
        versions = self._store.get(subject, [])
        if version < 1 or version > len(versions):
            raise SchemaNotFoundError(f"Version {version} not found", 404)
        entry = versions.pop(version - 1)
        if permanent:
            self._id_map.pop(entry["id"], None)
        return version


class _MockSession:
    """Placeholder session that will never be used in MockSchemaRegistryClient."""
    def request(self, *args, **kwargs):
        raise RuntimeError("MockSchemaRegistryClient does not make HTTP calls")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _make_error(resp) -> SchemaRegistryError:
    """Parse a Confluent-style error response into the right exception type."""
    try:
        body = resp.json()
        message = body.get("message", resp.text)
        error_code = int(body.get("error_code", 0))
    except Exception:
        message = resp.text or resp.reason
        error_code = 0

    if resp.status_code == 404:
        return SchemaNotFoundError(message, status_code=404, error_code=error_code)
    if resp.status_code == 409:
        return IncompatibleSchemaError(message, status_code=409, error_code=error_code)
    return SchemaRegistryError(
        f"HTTP {resp.status_code}: {message}", status_code=resp.status_code, error_code=error_code
    )
