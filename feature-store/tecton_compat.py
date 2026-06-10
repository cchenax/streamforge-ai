"""
Tecton-compatible decorator API for defining feature views.

This module provides decorators that mirror Tecton's Python SDK surface,
layering cleanly on top of StreamForge's core primitives.

Usage
-----
    import datetime
    from feature_store.tecton_compat import (
        batch_feature_view,
        stream_feature_view,
        on_demand_feature_view,
    )
    from feature_store.entities import Entity
    from feature_store.feature_views import Feature, DataSource

    user = Entity(name="user", join_key="user_id")

    @batch_feature_view(
        sources=[DataSource.minio()],
        entities=[user],
        online=True,
        offline=True,
        ttl=datetime.timedelta(days=7),
        description="Per-user event statistics computed by StreamForge Flink job",
    )
    def user_event_stats(records):
        # records is list[dict] from the offline store
        return records  # pass-through; add transformations here

    @stream_feature_view(
        source=DataSource.kafka(topic="user.event.counts"),
        entities=[user],
        features=[Feature.float("event_rate")],
    )
    def realtime_event_rate(event):
        return {"event_rate": event.get("event_count", 0) / 30.0}

    @on_demand_feature_view(
        sources=[user_event_stats],
        features=[Feature.float("event_rate_normalized")],
    )
    def normalized_event_rate(inputs):
        return {"event_rate_normalized": min(inputs.get("event_rate", 0.0) / 10.0, 1.0)}
"""
from __future__ import annotations

import datetime
import functools
from typing import Callable, List, Optional

from feature_views import (
    BatchFeatureView,
    StreamFeatureView,
    OnDemandFeatureView,
    Feature,
    DataSource,
)
from entities import Entity


# ---------------------------------------------------------------------------
# Decorators
# ---------------------------------------------------------------------------

def batch_feature_view(
    sources: List,
    entities: List[Entity],
    features: Optional[List[Feature]] = None,
    online: bool = True,
    offline: bool = True,
    ttl: datetime.timedelta = datetime.timedelta(days=7),
    description: str = "",
    tags: Optional[dict] = None,
    mode: str = "python",
    feature_start_time: Optional[datetime.datetime] = None,
) -> Callable:
    """
    Decorator — turns a Python function into a Tecton-style BatchFeatureView.

    The decorated function receives raw offline records (list[dict]) and
    should return transformed records (list[dict]).  The function name
    becomes the view name.

    Parameters mirror Tecton's @batch_feature_view:
      sources            — list of DataSource objects (first is used)
      entities           — list of Entity this view is keyed on
      features           — list of Feature columns (inferred from fn if omitted)
      online / offline   — materialization targets
      ttl                — how long online cache entries stay valid
      mode               — "python" (default); reserved for future Spark/PySpark
      feature_start_time — earliest timestamp the view covers (metadata only)
    """
    def decorator(fn: Callable) -> BatchFeatureView:
        view_features = features or []
        source = sources[0] if sources else DataSource.minio()
        view = BatchFeatureView(
            name=fn.__name__,
            entities=entities,
            features=view_features,
            source=source,
            ttl=ttl,
            online=online,
            tags=tags or {},
            description=description or (fn.__doc__ or "").strip(),
        )
        view._transform_fn = fn
        functools.update_wrapper(view, fn)
        return view

    return decorator


def stream_feature_view(
    source: Optional[DataSource] = None,
    entities: Optional[List[Entity]] = None,
    features: Optional[List[Feature]] = None,
    online: bool = True,
    ttl: datetime.timedelta = datetime.timedelta(hours=24),
    description: str = "",
    tags: Optional[dict] = None,
    mode: str = "python",
) -> Callable:
    """
    Decorator — turns a Python function into a Tecton-style StreamFeatureView.

    The decorated function receives a single Kafka event record (dict) and
    should return a dict of computed feature values.

    Parameters mirror Tecton's @stream_feature_view:
      source   — DataSource.kafka(...)
      entities — list of Entity this view is keyed on
      features — list of Feature columns produced per event
      online   — if True, events are written to online store via push()
      ttl      — Redis key TTL for online entries
      mode     — "python" (default)
    """
    def decorator(fn: Callable) -> StreamFeatureView:
        view_features = features or []
        view = StreamFeatureView(
            name=fn.__name__,
            entities=entities or [],
            features=view_features,
            source=source or DataSource.kafka(),
            online=online,
            ttl=ttl,
            tags=tags or {},
            description=description or (fn.__doc__ or "").strip(),
        )
        view._transform_fn = fn
        functools.update_wrapper(view, fn)
        return view

    return decorator


def on_demand_feature_view(
    sources: List,
    features: List[Feature],
    mode: str = "python",
    description: str = "",
    tags: Optional[dict] = None,
) -> Callable:
    """
    Decorator — turns a Python function into a Tecton-style OnDemandFeatureView.

    The decorated function receives a merged dict of all upstream feature
    values and should return a dict of newly computed features.

    Parameters mirror Tecton's @on_demand_feature_view:
      sources  — list of BatchFeatureView / StreamFeatureView to pull inputs from
      features — list of Feature columns this view produces
      mode     — "python" (default)
    """
    def decorator(fn: Callable) -> OnDemandFeatureView:
        view = OnDemandFeatureView(
            name=fn.__name__,
            sources=sources,
            features=features,
            transform_fn=fn,
            tags=tags or {},
            description=description or (fn.__doc__ or "").strip(),
        )
        functools.update_wrapper(view, fn)
        return view

    return decorator
