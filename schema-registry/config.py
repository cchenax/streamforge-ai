"""
RegistryConfig — configuration for Confluent Schema Registry or Apicurio Registry.

Environment variables
---------------------
    SCHEMA_REGISTRY_URL           http://localhost:8081
    SCHEMA_REGISTRY_BACKEND       confluent | apicurio  (default: confluent)
    SCHEMA_REGISTRY_USERNAME      HTTP Basic Auth username
    SCHEMA_REGISTRY_PASSWORD      HTTP Basic Auth password
    SCHEMA_REGISTRY_API_KEY       Confluent Cloud API key  (overrides username/password)
    SCHEMA_REGISTRY_API_SECRET    Confluent Cloud API secret
    SCHEMA_REGISTRY_SSL_VERIFY    true | false  (default: true)
    SCHEMA_REGISTRY_TIMEOUT       seconds       (default: 10)
    SCHEMA_REGISTRY_CACHE_SIZE    int           (default: 512)

YAML snippet (feature_store.yaml or schema_registry.yaml)
----------------------------------------------------------
    schema_registry:
      url: http://localhost:8081
      backend: confluent        # or apicurio
      username: ~
      password: ~
      ssl_verify: true
      timeout_seconds: 10
      cache_size: 512
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class RegistryBackend(str, Enum):
    """Supported schema registry backends."""
    CONFLUENT = "confluent"   # Confluent Schema Registry (self-hosted or Confluent Cloud)
    APICURIO  = "apicurio"   # Apicurio Registry v2 (compat mode uses /apis/ccompat/v6)


class SubjectNameStrategy(str, Enum):
    """
    How a subject name is derived from topic/record context.

    Mirrors Confluent's three standard strategies:
      TOPIC_NAME        → ``{topic}-value`` / ``{topic}-key``
      RECORD_NAME       → ``{record_namespace}.{record_name}``
      TOPIC_RECORD_NAME → ``{topic}-{record_namespace}.{record_name}``
    """
    TOPIC_NAME        = "topic_name"
    RECORD_NAME       = "record_name"
    TOPIC_RECORD_NAME = "topic_record_name"

    def subject(
        self,
        topic: str = "",
        record_name: str = "",
        is_key: bool = False,
    ) -> str:
        suffix = "key" if is_key else "value"
        if self == SubjectNameStrategy.TOPIC_NAME:
            return f"{topic}-{suffix}"
        if self == SubjectNameStrategy.RECORD_NAME:
            return record_name or f"unknown-{suffix}"
        # TOPIC_RECORD_NAME
        return f"{topic}-{record_name}" if record_name else f"{topic}-{suffix}"


@dataclass
class RegistryConfig:
    """
    Connection parameters for a schema registry.

    All fields have sensible defaults so the code works without configuration
    when the registry is reachable at localhost:8081.
    """
    url: str = "http://localhost:8081"
    backend: RegistryBackend = RegistryBackend.CONFLUENT
    username: Optional[str] = None
    password: Optional[str] = None
    api_key: Optional[str] = None     # Confluent Cloud — takes precedence over username
    api_secret: Optional[str] = None  # Confluent Cloud
    ssl_verify: bool = True
    timeout_seconds: int = 10
    cache_size: int = 512
    subject_name_strategy: SubjectNameStrategy = SubjectNameStrategy.TOPIC_NAME

    # ------------------------------------------------------------------
    # Constructors
    # ------------------------------------------------------------------

    @classmethod
    def from_env(cls) -> "RegistryConfig":
        """Build config from environment variables."""
        backend_raw = os.environ.get("SCHEMA_REGISTRY_BACKEND", "confluent").lower()
        backend = (
            RegistryBackend.APICURIO
            if "apicurio" in backend_raw
            else RegistryBackend.CONFLUENT
        )
        return cls(
            url=os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081").rstrip("/"),
            backend=backend,
            username=os.environ.get("SCHEMA_REGISTRY_USERNAME"),
            password=os.environ.get("SCHEMA_REGISTRY_PASSWORD"),
            api_key=os.environ.get("SCHEMA_REGISTRY_API_KEY"),
            api_secret=os.environ.get("SCHEMA_REGISTRY_API_SECRET"),
            ssl_verify=os.environ.get("SCHEMA_REGISTRY_SSL_VERIFY", "true").lower() != "false",
            timeout_seconds=int(os.environ.get("SCHEMA_REGISTRY_TIMEOUT", "10")),
            cache_size=int(os.environ.get("SCHEMA_REGISTRY_CACHE_SIZE", "512")),
        )

    @classmethod
    def from_yaml(cls, path) -> "RegistryConfig":
        """Load config from a YAML file under the ``schema_registry`` key."""
        try:
            import yaml
        except ImportError as exc:
            raise ImportError("pyyaml is required: pip install pyyaml") from exc
        with open(path) as f:
            raw = yaml.safe_load(f) or {}
        sr = raw.get("schema_registry", {})
        backend_raw = sr.get("backend", "confluent").lower()
        backend = (
            RegistryBackend.APICURIO
            if "apicurio" in backend_raw
            else RegistryBackend.CONFLUENT
        )
        return cls(
            url=sr.get("url", "http://localhost:8081").rstrip("/"),
            backend=backend,
            username=sr.get("username"),
            password=sr.get("password"),
            api_key=sr.get("api_key"),
            api_secret=sr.get("api_secret"),
            ssl_verify=sr.get("ssl_verify", True),
            timeout_seconds=int(sr.get("timeout_seconds", 10)),
            cache_size=int(sr.get("cache_size", 512)),
        )

    # ------------------------------------------------------------------
    # Derived helpers
    # ------------------------------------------------------------------

    @property
    def api_prefix(self) -> str:
        """
        URL path prefix for API calls.

        Confluent SR uses the root (``/``).
        Apicurio v2 exposes its Confluent-compat layer at ``/apis/ccompat/v6``.
        """
        return "/apis/ccompat/v6" if self.backend == RegistryBackend.APICURIO else ""

    @property
    def auth(self) -> Optional[tuple]:
        """Return ``(user, password)`` tuple for HTTP Basic Auth, or ``None``."""
        if self.api_key and self.api_secret:
            return (self.api_key, self.api_secret)
        if self.username and self.password:
            return (self.username, self.password)
        return None

    def __repr__(self) -> str:
        return (
            f"RegistryConfig(url={self.url!r}, backend={self.backend.value!r}, "
            f"cache_size={self.cache_size})"
        )
