"""
Entity definitions — Feast-compatible.

An Entity identifies the primary object (e.g. user) for which features
are computed.  Every FeatureView must reference one or more entities so
the store knows which join key to use when serving.
"""
from __future__ import annotations

from dataclasses import dataclass, field


class ValueType:
    INT64 = "INT64"
    FLOAT = "FLOAT"
    STRING = "STRING"
    DOUBLE = "DOUBLE"
    BOOL = "BOOL"
    BYTES = "BYTES"
    UNIX_TIMESTAMP = "UNIX_TIMESTAMP"


@dataclass
class Entity:
    """
    Feast-compatible Entity.

    Parameters
    ----------
    name : str
        Unique name for this entity (e.g. "user").
    join_key : str
        Column name used to join features (e.g. "user_id").
    value_type : str
        Type of the join key (default INT64).
    description : str
        Human-readable description.
    tags : dict
        Arbitrary key-value metadata.
    """

    name: str
    join_key: str
    value_type: str = ValueType.INT64
    description: str = ""
    tags: dict = field(default_factory=dict)

    def __repr__(self) -> str:
        return f"Entity(name={self.name!r}, join_key={self.join_key!r})"
