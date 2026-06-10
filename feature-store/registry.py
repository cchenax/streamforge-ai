"""
In-memory registry for StreamForge feature store objects.

In production (Feast uses SQLite / PostgreSQL / GCS backed registry).
For StreamForge we use a fast in-memory dict with optional JSON dump.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Optional, Union

from entities import Entity
from feature_views import BatchFeatureView, StreamFeatureView, OnDemandFeatureView
from feature_service import FeatureService

FeatureViewType = Union[BatchFeatureView, StreamFeatureView, OnDemandFeatureView]


class Registry:
    """
    Central registry that tracks entities, feature views, and feature services.

    All apply() calls are idempotent — re-applying with the same name
    overwrites the previous definition.
    """

    def __init__(self) -> None:
        self._entities: Dict[str, Entity] = {}
        self._feature_views: Dict[str, FeatureViewType] = {}
        self._feature_services: Dict[str, FeatureService] = {}

    # ------------------------------------------------------------------
    # Entities
    # ------------------------------------------------------------------

    def apply_entity(self, entity: Entity) -> None:
        self._entities[entity.name] = entity

    def get_entity(self, name: str) -> Entity:
        if name not in self._entities:
            raise KeyError(f"Entity not found: {name!r}")
        return self._entities[name]

    def list_entities(self) -> List[Entity]:
        return list(self._entities.values())

    # ------------------------------------------------------------------
    # Feature views
    # ------------------------------------------------------------------

    def apply_feature_view(self, view: FeatureViewType) -> None:
        self._feature_views[view.name] = view

    def get_feature_view(self, name: str) -> FeatureViewType:
        if name not in self._feature_views:
            raise KeyError(f"FeatureView not found: {name!r}")
        return self._feature_views[name]

    def list_feature_views(self) -> List[FeatureViewType]:
        return list(self._feature_views.values())

    # ------------------------------------------------------------------
    # Feature services
    # ------------------------------------------------------------------

    def apply_feature_service(self, service: FeatureService) -> None:
        self._feature_services[service.name] = service

    def get_feature_service(self, name: str) -> FeatureService:
        if name not in self._feature_services:
            raise KeyError(f"FeatureService not found: {name!r}")
        return self._feature_services[name]

    def list_feature_services(self) -> List[FeatureService]:
        return list(self._feature_services.values())

    # ------------------------------------------------------------------
    # Serialization (for debugging / persistence)
    # ------------------------------------------------------------------

    def dump(self) -> dict:
        return {
            "entities": [e.name for e in self.list_entities()],
            "feature_views": [v.name for v in self.list_feature_views()],
            "feature_services": [s.name for s in self.list_feature_services()],
        }

    def save(self, path: Union[str, Path]) -> None:
        """Persist registry index to a JSON file."""
        with open(path, "w") as f:
            json.dump(self.dump(), f, indent=2)
