"""
FeatureService — groups related feature views into a single serving unit.

Feast-compatible: FeatureService is the primary artifact used at serving time.
Tecton-compatible: equivalent to FeatureService.
"""
from __future__ import annotations

from typing import List, Optional, Union

from feature_views import BatchFeatureView, StreamFeatureView, OnDemandFeatureView

FeatureViewType = Union[BatchFeatureView, StreamFeatureView, OnDemandFeatureView]


class FeatureService:
    """
    A named collection of feature views served together.

    Parameters
    ----------
    name        : unique service name used in get_features() lookups
    features    : list of feature view objects to include
    description : human-readable description
    tags        : arbitrary key-value metadata
    owner       : team or individual responsible for this service
    """

    def __init__(
        self,
        name: str,
        features: List[FeatureViewType],
        description: str = "",
        tags: Optional[dict] = None,
        owner: str = "",
    ) -> None:
        self.name = name
        self.features = features
        self.description = description
        self.tags = tags or {}
        self.owner = owner

    @property
    def feature_view_names(self) -> List[str]:
        return [f.name for f in self.features]

    def __repr__(self) -> str:
        return f"FeatureService(name={self.name!r}, views={self.feature_view_names})"
