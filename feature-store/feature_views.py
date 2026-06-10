"""
Feature view definitions — Feast + Tecton-compatible.

Three view types mirror both ecosystems:

  BatchFeatureView    — offline storage (MinIO / Iceberg)
  StreamFeatureView   — real-time stream (Kafka → Redis)
  OnDemandFeatureView — request-time computation on top of other views
"""
from __future__ import annotations

import datetime
from dataclasses import dataclass, field
from typing import Callable, List, Optional

from entities import Entity


# ---------------------------------------------------------------------------
# Feature descriptor
# ---------------------------------------------------------------------------

@dataclass
class Feature:
    """A single named feature column with type metadata."""

    name: str
    dtype: str = "FLOAT"
    description: str = ""

    @classmethod
    def int(cls, name: str, description: str = "") -> "Feature":
        return cls(name=name, dtype="INT64", description=description)

    @classmethod
    def float(cls, name: str, description: str = "") -> "Feature":
        return cls(name=name, dtype="FLOAT", description=description)

    @classmethod
    def string(cls, name: str, description: str = "") -> "Feature":
        return cls(name=name, dtype="STRING", description=description)

    def __repr__(self) -> str:
        return f"Feature({self.name!r}, dtype={self.dtype!r})"


# ---------------------------------------------------------------------------
# Data source pointer
# ---------------------------------------------------------------------------

@dataclass
class DataSource:
    """
    Pointer to a backing data source.

    source_type : "minio" | "redis" | "kafka" | "iceberg"
    config      : source-specific connection parameters
    """

    source_type: str
    config: dict = field(default_factory=dict)

    @classmethod
    def minio(
        cls,
        bucket: str = "processed",
        prefix: str = "streamforge/features",
        endpoint: str = "localhost:9000",
    ) -> "DataSource":
        return cls("minio", {"bucket": bucket, "prefix": prefix, "endpoint": endpoint})

    @classmethod
    def redis(cls, host: str = "localhost", port: int = 6379, db: int = 0) -> "DataSource":
        return cls("redis", {"host": host, "port": port, "db": db})

    @classmethod
    def kafka(
        cls,
        topic: str = "user.event.counts",
        bootstrap_servers: str = "localhost:9092",
    ) -> "DataSource":
        return cls("kafka", {"topic": topic, "bootstrap_servers": bootstrap_servers})

    @classmethod
    def iceberg(cls, catalog_uri: str = "thrift://localhost:9083", warehouse: str = "s3a://processed/") -> "DataSource":
        return cls("iceberg", {"catalog_uri": catalog_uri, "warehouse": warehouse})

    def __repr__(self) -> str:
        return f"DataSource({self.source_type!r})"


# ---------------------------------------------------------------------------
# BatchFeatureView
# ---------------------------------------------------------------------------

class BatchFeatureView:
    """
    Feast-compatible batch (offline) feature view backed by MinIO or Iceberg.

    Analogous to:
      Feast  — FeatureView with a batch source
      Tecton — BatchFeatureView

    Parameters
    ----------
    name        : unique view name
    entities    : list of Entity objects this view joins on
    features    : list of Feature columns produced by this view
    source      : DataSource pointing to offline storage (default: MinIO)
    ttl         : how long online cache entries remain valid
    online      : whether features should be materialized to online store
    tags        : arbitrary key-value metadata
    description : human-readable description
    """

    def __init__(
        self,
        name: str,
        entities: List[Entity],
        features: List[Feature],
        source: Optional[DataSource] = None,
        ttl: datetime.timedelta = datetime.timedelta(days=7),
        online: bool = True,
        tags: Optional[dict] = None,
        description: str = "",
    ) -> None:
        self.name = name
        self.entities = entities
        self.features = features
        self.source = source or DataSource.minio()
        self.ttl = ttl
        self.online = online
        self.tags = tags or {}
        self.description = description
        self._transform_fn: Optional[Callable] = None

    @property
    def join_keys(self) -> List[str]:
        return [e.join_key for e in self.entities]

    def __repr__(self) -> str:
        return f"BatchFeatureView(name={self.name!r}, features={[f.name for f in self.features]})"


# ---------------------------------------------------------------------------
# StreamFeatureView
# ---------------------------------------------------------------------------

class StreamFeatureView:
    """
    Real-time feature view backed by Kafka → Redis pipeline.

    Analogous to:
      Feast  — StreamFeatureView
      Tecton — StreamFeatureView

    Parameters
    ----------
    name        : unique view name
    entities    : list of Entity objects this view joins on
    features    : list of Feature columns emitted per event
    source      : DataSource pointing to Kafka topic
    online      : if True, features are served from Redis online store
    ttl         : Redis key TTL for online entries
    tags        : arbitrary key-value metadata
    description : human-readable description
    """

    def __init__(
        self,
        name: str,
        entities: List[Entity],
        features: List[Feature],
        source: Optional[DataSource] = None,
        online: bool = True,
        ttl: datetime.timedelta = datetime.timedelta(hours=24),
        tags: Optional[dict] = None,
        description: str = "",
    ) -> None:
        self.name = name
        self.entities = entities
        self.features = features
        self.source = source or DataSource.kafka()
        self.online = online
        self.ttl = ttl
        self.tags = tags or {}
        self.description = description
        self._transform_fn: Optional[Callable] = None

    @property
    def join_keys(self) -> List[str]:
        return [e.join_key for e in self.entities]

    def __repr__(self) -> str:
        return f"StreamFeatureView(name={self.name!r}, features={[f.name for f in self.features]})"


# ---------------------------------------------------------------------------
# OnDemandFeatureView
# ---------------------------------------------------------------------------

class OnDemandFeatureView:
    """
    Request-time computed features derived from upstream views.

    The transform function receives a merged dict of all upstream feature
    values and returns a dict of newly computed features.

    Analogous to:
      Feast  — OnDemandFeatureView
      Tecton — OnDemandFeatureView

    Parameters
    ----------
    name         : unique view name
    sources      : list of BatchFeatureView / StreamFeatureView to draw inputs from
    features     : list of Feature columns this view produces
    transform_fn : callable(inputs: dict) -> dict
    tags         : arbitrary key-value metadata
    description  : human-readable description
    """

    def __init__(
        self,
        name: str,
        sources: List,
        features: List[Feature],
        transform_fn: Optional[Callable] = None,
        tags: Optional[dict] = None,
        description: str = "",
    ) -> None:
        self.name = name
        self.sources = sources
        self.features = features
        self.transform_fn = transform_fn
        self.tags = tags or {}
        self.description = description

    def compute(self, inputs: dict) -> dict:
        """Apply the transformation to merged upstream inputs."""
        if self.transform_fn is None:
            return {}
        return self.transform_fn(inputs)

    def __repr__(self) -> str:
        return f"OnDemandFeatureView(name={self.name!r}, features={[f.name for f in self.features]})"
