"""
streamforge-ai feature store — Feast + Tecton-compatible interface.

Quick start (Feast style)
-------------------------
    from feature_store.store import FeatureStore
    from feature_store.entities import Entity
    from feature_store.feature_views import BatchFeatureView, StreamFeatureView, Feature, DataSource
    from feature_store.feature_service import FeatureService
    import datetime

    user = Entity(name="user", join_key="user_id")

    user_event_stats = BatchFeatureView(
        name="user_event_stats",
        entities=[user],
        features=[
            Feature.int("event_count"),
            Feature.float("event_rate"),
            Feature.int("window_start_ms"),
            Feature.int("window_end_ms"),
        ],
        source=DataSource.minio(),
        ttl=datetime.timedelta(days=7),
    )

    recommendation_service = FeatureService(
        name="recommendation_service",
        features=[user_event_stats],
    )

    store = FeatureStore(project="streamforge")
    store.apply([user, user_event_stats, recommendation_service])

    fv = store.get_online_features(
        features=["user_event_stats:event_count"],
        entity_rows=[{"user_id": 42}],
    )
    print(fv.to_dict())

Quick start (Tecton style)
--------------------------
    from feature_store.tecton_compat import batch_feature_view, on_demand_feature_view

    @batch_feature_view(sources=[DataSource.minio()], entities=[user], online=True)
    def user_event_stats(records):
        return records

    @on_demand_feature_view(
        sources=[user_event_stats],
        features=[Feature.float("event_rate_normalized")],
    )
    def normalized_rate(inputs):
        return {"event_rate_normalized": min(inputs.get("event_rate", 0.0) / 10.0, 1.0)}
"""

from store import FeatureStore
from entities import Entity, ValueType
from feature_views import BatchFeatureView, StreamFeatureView, OnDemandFeatureView, Feature, DataSource
from feature_service import FeatureService
from config import StoreConfig

__all__ = [
    "FeatureStore",
    "Entity",
    "ValueType",
    "BatchFeatureView",
    "StreamFeatureView",
    "OnDemandFeatureView",
    "Feature",
    "DataSource",
    "FeatureService",
    "StoreConfig",
]
