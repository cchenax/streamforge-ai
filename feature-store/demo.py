"""
StreamForge Feature Store — end-to-end demo.

Demonstrates both Feast-compatible and Tecton-compatible APIs against
the existing StreamForge MinIO + Redis infrastructure (falls back to
in-memory stubs when the services are not running).

Run:
    cd feature-store
    python demo.py
"""
from __future__ import annotations

import datetime
import sys
import time

# ---------------------------------------------------------------------------
# 1. Feast-compatible API
# ---------------------------------------------------------------------------

print("=" * 60)
print("StreamForge Feature Store Demo")
print("=" * 60)

from store import FeatureStore
from entities import Entity
from feature_views import BatchFeatureView, StreamFeatureView, OnDemandFeatureView, Feature, DataSource
from feature_service import FeatureService

# --- Declare objects ---

user = Entity(
    name="user",
    join_key="user_id",
    description="StreamForge platform user",
)

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
    description="Per-user tumbling-window event aggregates (MinIO offline store)",
)

realtime_user_events = StreamFeatureView(
    name="realtime_user_events",
    entities=[user],
    features=[
        Feature.int("event_count"),
        Feature.float("event_rate"),
    ],
    source=DataSource.kafka(topic="user.event.counts"),
    ttl=datetime.timedelta(hours=24),
    description="Real-time Kafka stream features (Redis online store)",
)

normalized_rate_view = OnDemandFeatureView(
    name="normalized_event_rate",
    sources=[user_event_stats],
    features=[Feature.float("event_rate_normalized")],
    transform_fn=lambda inputs: {
        "event_rate_normalized": round(min(inputs.get("event_rate", 0.0) / 10.0, 1.0), 4)
    },
    description="Event rate scaled to [0, 1] (request-time computation)",
)

recommendation_service = FeatureService(
    name="recommendation_service",
    features=[user_event_stats, realtime_user_events],
    description="Features used by the recommendation model",
    owner="ml-platform",
)

# --- Register everything ---

store = FeatureStore(project="streamforge")
store.apply([
    user,
    user_event_stats,
    realtime_user_events,
    normalized_rate_view,
    recommendation_service,
])

print(f"\n[registry] {store}")

# ---------------------------------------------------------------------------
# 2. Online serving  — Feast-compatible
# ---------------------------------------------------------------------------

print("\n--- get_online_features() [Feast-compatible] ---")
t0 = time.perf_counter()
fv = store.get_online_features(
    features=[
        "user_event_stats:event_count",
        "user_event_stats:event_rate",
        "user_event_stats:window_start_ms",
    ],
    entity_rows=[{"user_id": 1}, {"user_id": 42}, {"user_id": 99}],
)
elapsed_ms = (time.perf_counter() - t0) * 1000
print(f"  result: {fv}  ({elapsed_ms:.1f} ms)")
d = fv.to_dict()
for uid, ec, er in zip(d.get("user_id", []), d.get("event_count", []), d.get("event_rate", [])):
    print(f"  user_id={uid}  event_count={ec}  event_rate={er}")

# ---------------------------------------------------------------------------
# 3. Online serving  — Tecton-compatible
# ---------------------------------------------------------------------------

print("\n--- get_features() [Tecton-compatible] ---")
t0 = time.perf_counter()
fv2 = store.get_features(
    join_keys={"user_id": [1, 42, 99]},
    feature_service_name="recommendation_service",
)
elapsed_ms = (time.perf_counter() - t0) * 1000
print(f"  result: {fv2}  ({elapsed_ms:.1f} ms)")

# ---------------------------------------------------------------------------
# 4. Tecton decorator API
# ---------------------------------------------------------------------------

print("\n--- Tecton decorator API ---")
from tecton_compat import batch_feature_view, stream_feature_view, on_demand_feature_view

user2 = Entity(name="user2", join_key="user_id")


@batch_feature_view(
    sources=[DataSource.minio()],
    entities=[user2],
    online=True,
    offline=True,
    ttl=datetime.timedelta(days=7),
    description="Decorated batch view: passthrough with filter",
)
def active_user_stats(records):
    """Return only records with event_count > 0."""
    return [r for r in records if r.get("event_count", 0) > 0]


@stream_feature_view(
    source=DataSource.kafka(topic="user.event.counts"),
    entities=[user2],
    features=[Feature.float("events_per_minute")],
    description="Decorated stream view: convert event_count to per-minute rate",
)
def realtime_event_rate(event: dict) -> dict:
    """Scale 30-second window count to per-minute rate."""
    return {"events_per_minute": event.get("event_count", 0) * 2.0}


@on_demand_feature_view(
    sources=[active_user_stats],
    features=[Feature.float("event_rate_normalized")],
    description="Decorated on-demand view: normalize event_rate to [0, 1]",
)
def normalized_event_rate_v2(inputs: dict) -> dict:
    """Normalize event_rate to [0, 1] (cap at 10 events/s)."""
    return {"event_rate_normalized": round(min(inputs.get("event_rate", 0.0) / 10.0, 1.0), 4)}


print(f"  BatchFeatureView : {active_user_stats}")
print(f"  StreamFeatureView: {realtime_event_rate}")
print(f"  OnDemandView     : {normalized_event_rate_v2}")

store2 = FeatureStore(project="streamforge-tecton")
store2.apply([user2, active_user_stats, realtime_event_rate, normalized_event_rate_v2])

fv3 = store2.get_online_features(
    features=["active_user_stats:event_count", "active_user_stats:event_rate"],
    entity_rows=[{"user_id": 42}, {"user_id": 7}],
)
print(f"  decorated view result: {fv3}")

# ---------------------------------------------------------------------------
# 5. Historical features  — RetrievalJob
# ---------------------------------------------------------------------------

print("\n--- get_historical_features() [Feast-compatible] ---")
job = store.get_historical_features(
    entity_df={"user_id": [1, 42, 99]},
    features=["user_event_stats:event_count", "user_event_stats:event_rate"],
)
print(f"  RetrievalJob: {job}")
try:
    import pandas as pd
    df = job.to_df()
    print(f"  DataFrame shape: {df.shape}")
    if not df.empty:
        print(f"  Sample row: {df.iloc[0].to_dict()}")
except ImportError:
    print("  (pandas not installed — skipping .to_df() check)")

# ---------------------------------------------------------------------------
# 6. Materialization
# ---------------------------------------------------------------------------

print("\n--- materialize() offline → online ---")
end_date = datetime.date.today()
start_date = end_date - datetime.timedelta(days=1)
store.materialize(start_date=start_date, end_date=end_date)

# ---------------------------------------------------------------------------
# 7. Stats
# ---------------------------------------------------------------------------

print("\n--- store.stats() ---")
import json
stats = store.stats()
print(json.dumps(stats, indent=2, default=str))

print("\n[demo] Done.")
