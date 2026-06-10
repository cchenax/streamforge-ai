"""
Unit tests for the StreamForge feature store (no external services required).

Run:
    cd feature-store
    python -m pytest tests.py -v
"""
from __future__ import annotations

import datetime
import sys
from pathlib import Path

import pytest

# Make the feature-store package importable
sys.path.insert(0, str(Path(__file__).parent))

from entities import Entity, ValueType
from feature_views import BatchFeatureView, StreamFeatureView, OnDemandFeatureView, Feature, DataSource
from feature_service import FeatureService
from registry import Registry
from store import FeatureStore, FeatureVector, _parse_feature_refs


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def user_entity():
    return Entity(name="user", join_key="user_id")


@pytest.fixture
def batch_view(user_entity):
    return BatchFeatureView(
        name="user_event_stats",
        entities=[user_entity],
        features=[Feature.int("event_count"), Feature.float("event_rate")],
        source=DataSource.minio(),
    )


@pytest.fixture
def stream_view(user_entity):
    return StreamFeatureView(
        name="realtime_events",
        entities=[user_entity],
        features=[Feature.float("event_rate")],
        source=DataSource.kafka(),
    )


@pytest.fixture
def on_demand_view(batch_view):
    return OnDemandFeatureView(
        name="normalized_rate",
        sources=[batch_view],
        features=[Feature.float("event_rate_normalized")],
        transform_fn=lambda inputs: {
            "event_rate_normalized": min(inputs.get("event_rate", 0.0) / 10.0, 1.0)
        },
    )


@pytest.fixture
def feature_service(batch_view, stream_view):
    return FeatureService(
        name="recommendation_service",
        features=[batch_view, stream_view],
    )


@pytest.fixture
def store(user_entity, batch_view, stream_view, on_demand_view, feature_service):
    s = FeatureStore(project="test")
    s.apply([user_entity, batch_view, stream_view, on_demand_view, feature_service])
    return s


# ---------------------------------------------------------------------------
# Entity tests
# ---------------------------------------------------------------------------

class TestEntity:
    def test_defaults(self, user_entity):
        assert user_entity.name == "user"
        assert user_entity.join_key == "user_id"
        assert user_entity.value_type == ValueType.INT64

    def test_repr(self, user_entity):
        assert "user" in repr(user_entity)


# ---------------------------------------------------------------------------
# Feature tests
# ---------------------------------------------------------------------------

class TestFeature:
    def test_int(self):
        f = Feature.int("event_count")
        assert f.dtype == "INT64"
        assert f.name == "event_count"

    def test_float(self):
        f = Feature.float("event_rate")
        assert f.dtype == "FLOAT"

    def test_string(self):
        f = Feature.string("country")
        assert f.dtype == "STRING"


# ---------------------------------------------------------------------------
# DataSource tests
# ---------------------------------------------------------------------------

class TestDataSource:
    def test_minio_defaults(self):
        ds = DataSource.minio()
        assert ds.source_type == "minio"
        assert ds.config["bucket"] == "processed"

    def test_redis_defaults(self):
        ds = DataSource.redis()
        assert ds.source_type == "redis"
        assert ds.config["port"] == 6379

    def test_kafka_defaults(self):
        ds = DataSource.kafka()
        assert ds.source_type == "kafka"
        assert "user.event.counts" in ds.config["topic"]

    def test_iceberg(self):
        ds = DataSource.iceberg()
        assert ds.source_type == "iceberg"


# ---------------------------------------------------------------------------
# FeatureView tests
# ---------------------------------------------------------------------------

class TestBatchFeatureView:
    def test_join_keys(self, batch_view, user_entity):
        assert batch_view.join_keys == ["user_id"]

    def test_repr(self, batch_view):
        assert "user_event_stats" in repr(batch_view)
        assert "event_count" in repr(batch_view)

    def test_default_source_is_minio(self, user_entity):
        view = BatchFeatureView(
            name="v", entities=[user_entity], features=[Feature.int("x")]
        )
        assert view.source.source_type == "minio"


class TestStreamFeatureView:
    def test_join_keys(self, stream_view, user_entity):
        assert stream_view.join_keys == ["user_id"]

    def test_default_source_is_kafka(self, user_entity):
        view = StreamFeatureView(
            name="s", entities=[user_entity], features=[Feature.float("rate")]
        )
        assert view.source.source_type == "kafka"


class TestOnDemandFeatureView:
    def test_compute(self, on_demand_view):
        result = on_demand_view.compute({"event_rate": 5.0})
        assert "event_rate_normalized" in result
        assert result["event_rate_normalized"] == pytest.approx(0.5)

    def test_compute_clamps_at_one(self, on_demand_view):
        result = on_demand_view.compute({"event_rate": 100.0})
        assert result["event_rate_normalized"] == pytest.approx(1.0)

    def test_compute_no_transform_returns_empty(self, batch_view):
        view = OnDemandFeatureView(name="v", sources=[batch_view], features=[])
        assert view.compute({"x": 1}) == {}


# ---------------------------------------------------------------------------
# FeatureService tests
# ---------------------------------------------------------------------------

class TestFeatureService:
    def test_feature_view_names(self, feature_service):
        names = feature_service.feature_view_names
        assert "user_event_stats" in names
        assert "realtime_events" in names

    def test_repr(self, feature_service):
        assert "recommendation_service" in repr(feature_service)


# ---------------------------------------------------------------------------
# Registry tests
# ---------------------------------------------------------------------------

class TestRegistry:
    def test_apply_and_get_entity(self, user_entity):
        reg = Registry()
        reg.apply_entity(user_entity)
        assert reg.get_entity("user") is user_entity

    def test_missing_entity_raises(self):
        reg = Registry()
        with pytest.raises(KeyError):
            reg.get_entity("missing")

    def test_apply_and_get_feature_view(self, batch_view):
        reg = Registry()
        reg.apply_feature_view(batch_view)
        assert reg.get_feature_view("user_event_stats") is batch_view

    def test_missing_feature_view_raises(self):
        reg = Registry()
        with pytest.raises(KeyError):
            reg.get_feature_view("ghost")

    def test_apply_and_get_feature_service(self, feature_service):
        reg = Registry()
        reg.apply_feature_service(feature_service)
        assert reg.get_feature_service("recommendation_service") is feature_service

    def test_dump_structure(self, user_entity, batch_view, feature_service):
        reg = Registry()
        reg.apply_entity(user_entity)
        reg.apply_feature_view(batch_view)
        reg.apply_feature_service(feature_service)
        d = reg.dump()
        assert "entities" in d
        assert "feature_views" in d
        assert "feature_services" in d

    def test_idempotent_apply(self, user_entity):
        reg = Registry()
        reg.apply_entity(user_entity)
        reg.apply_entity(user_entity)
        assert len(reg.list_entities()) == 1


# ---------------------------------------------------------------------------
# FeatureStore tests
# ---------------------------------------------------------------------------

class TestFeatureStore:
    def test_repr_contains_project(self, store):
        assert "test" in repr(store)

    def test_apply_registers_objects(self, store):
        assert any(e.name == "user" for e in store.list_entities())
        assert any(v.name == "user_event_stats" for v in store.list_feature_views())
        assert any(s.name == "recommendation_service" for s in store.list_feature_services())

    def test_apply_unknown_type_raises(self):
        s = FeatureStore(project="x")
        with pytest.raises(TypeError):
            s.apply(["not-a-valid-object"])

    def test_get_online_features_returns_feature_vector(self, store):
        fv = store.get_online_features(
            features=["user_event_stats:event_count", "user_event_stats:event_rate"],
            entity_rows=[{"user_id": 1}, {"user_id": 42}],
        )
        assert isinstance(fv, FeatureVector)
        assert len(fv) == 2

    def test_feature_vector_to_dict_has_all_features(self, store):
        fv = store.get_online_features(
            features=["user_event_stats:event_count"],
            entity_rows=[{"user_id": 1}],
        )
        d = fv.to_dict()
        assert "event_count" in d
        assert len(d["event_count"]) == 1

    def test_get_online_features_wildcard(self, store):
        fv = store.get_online_features(
            features=["user_event_stats:*"],
            entity_rows=[{"user_id": 42}],
        )
        d = fv.to_dict()
        assert "event_count" in d
        assert "event_rate" in d

    def test_get_features_tecton_alias(self, store):
        fv = store.get_features(
            join_keys={"user_id": [1, 42, 99]},
            feature_service_name="recommendation_service",
        )
        assert isinstance(fv, FeatureVector)
        assert len(fv) == 3

    def test_get_features_unknown_service_raises(self, store):
        with pytest.raises(KeyError):
            store.get_features(join_keys={"user_id": [1]}, feature_service_name="ghost")

    def test_get_historical_features_returns_retrieval_job(self, store):
        from store import RetrievalJob
        job = store.get_historical_features(
            entity_df={"user_id": [1, 42]},
            features=["user_event_stats:event_count"],
        )
        assert isinstance(job, RetrievalJob)

    def test_stats_has_expected_keys(self, store):
        stats = store.stats()
        assert "project" in stats
        assert "registry" in stats
        assert "online_store" in stats
        assert "offline_store" in stats


# ---------------------------------------------------------------------------
# Feature reference parsing
# ---------------------------------------------------------------------------

class TestParseFeatureRefs:
    def test_view_colon_feature(self, store):
        parsed = _parse_feature_refs(["user_event_stats:event_count"], store._registry)
        assert ("user_event_stats", "event_count") in parsed

    def test_wildcard_expands_all_features(self, store):
        parsed = _parse_feature_refs(["user_event_stats:*"], store._registry)
        names = [fn for _, fn in parsed]
        assert "event_count" in names
        assert "event_rate" in names

    def test_global_wildcard(self, store):
        parsed = _parse_feature_refs(["*"], store._registry)
        assert len(parsed) > 0

    def test_bare_feature_name(self, store):
        parsed = _parse_feature_refs(["event_count"], store._registry)
        assert ("", "event_count") in parsed


# ---------------------------------------------------------------------------
# Tecton decorator API
# ---------------------------------------------------------------------------

class TestTectonCompat:
    def test_batch_feature_view_decorator(self, user_entity):
        from tecton_compat import batch_feature_view

        @batch_feature_view(
            sources=[DataSource.minio()],
            entities=[user_entity],
            features=[Feature.int("event_count")],
        )
        def my_batch_view(records):
            return records

        assert isinstance(my_batch_view, BatchFeatureView)
        assert my_batch_view.name == "my_batch_view"
        assert my_batch_view._transform_fn is not None

    def test_stream_feature_view_decorator(self, user_entity):
        from tecton_compat import stream_feature_view

        @stream_feature_view(
            source=DataSource.kafka(),
            entities=[user_entity],
            features=[Feature.float("rate")],
        )
        def my_stream_view(event):
            return {"rate": event.get("count", 0) / 30.0}

        assert isinstance(my_stream_view, StreamFeatureView)
        assert my_stream_view.name == "my_stream_view"

    def test_on_demand_feature_view_decorator(self, batch_view):
        from tecton_compat import on_demand_feature_view

        @on_demand_feature_view(
            sources=[batch_view],
            features=[Feature.float("normalized")],
        )
        def my_odv(inputs):
            return {"normalized": inputs.get("event_rate", 0.0) / 10.0}

        assert isinstance(my_odv, OnDemandFeatureView)
        result = my_odv.compute({"event_rate": 5.0})
        assert result["normalized"] == pytest.approx(0.5)

    def test_decorated_view_registered_in_store(self, user_entity):
        from tecton_compat import batch_feature_view

        @batch_feature_view(
            sources=[DataSource.minio()],
            entities=[user_entity],
            features=[Feature.int("count")],
        )
        def decorated_stats(records):
            return records

        s = FeatureStore(project="tecton-test")
        s.apply([user_entity, decorated_stats])
        assert s.get_feature_view("decorated_stats") is decorated_stats


# ---------------------------------------------------------------------------
# StoreConfig tests
# ---------------------------------------------------------------------------

class TestStoreConfig:
    def test_defaults(self):
        from config import StoreConfig, OnlineStoreConfig, OfflineStoreConfig
        cfg = StoreConfig()
        assert cfg.project == "streamforge"
        assert cfg.online_store.type == "redis"
        assert cfg.offline_store.type == "minio"
        assert cfg.online_store.port == 6379

    def test_save_and_reload(self, tmp_path):
        from config import StoreConfig
        cfg = StoreConfig(project="myproject")
        cfg.online_store.host = "redis-prod"
        cfg.offline_store.bucket = "ml-features"
        yaml_path = tmp_path / "feature_store.yaml"
        cfg.save(yaml_path)
        assert yaml_path.exists()
        loaded = StoreConfig.from_yaml(yaml_path)
        assert loaded.project == "myproject"
        assert loaded.online_store.host == "redis-prod"
        assert loaded.offline_store.bucket == "ml-features"

    def test_from_yaml_missing_file(self, tmp_path):
        from config import StoreConfig
        with pytest.raises(FileNotFoundError):
            StoreConfig.from_yaml(tmp_path / "nonexistent.yaml")

    def test_partial_yaml(self, tmp_path):
        from config import StoreConfig
        yaml_path = tmp_path / "feature_store.yaml"
        yaml_path.write_text("project: partial\n")
        cfg = StoreConfig.from_yaml(yaml_path)
        assert cfg.project == "partial"
        assert cfg.online_store.type == "redis"

    def test_registry_path_relative(self, tmp_path):
        from config import StoreConfig
        cfg = StoreConfig(registry="registry.json")
        path = cfg.registry_path(relative_to=tmp_path)
        assert path == tmp_path / "registry.json"

    def test_repr(self):
        from config import StoreConfig
        cfg = StoreConfig()
        r = repr(cfg)
        assert "streamforge" in r
        assert "redis" in r

    def test_store_accepts_config(self, tmp_path):
        from config import StoreConfig
        cfg = StoreConfig(project="cfg-project")
        store = FeatureStore(config=cfg)
        assert store.project == "cfg-project"


# ---------------------------------------------------------------------------
# CLI tests
# ---------------------------------------------------------------------------

class TestCLI:
    def test_init_creates_yaml(self, tmp_path):
        from cli import main
        import os
        orig = os.getcwd()
        os.chdir(tmp_path)
        try:
            rc = main(["init", "--project", "cli-test"])
            assert rc == 0
            assert (tmp_path / "feature_store.yaml").exists()
        finally:
            os.chdir(orig)

    def test_init_no_overwrite_without_force(self, tmp_path):
        from cli import main
        import os
        orig = os.getcwd()
        os.chdir(tmp_path)
        try:
            main(["init"])
            rc = main(["init"])
            assert rc == 1
        finally:
            os.chdir(orig)

    def test_init_force_overwrites(self, tmp_path):
        from cli import main
        import os
        orig = os.getcwd()
        os.chdir(tmp_path)
        try:
            main(["init", "--project", "first"])
            rc = main(["init", "--project", "second", "--force"])
            assert rc == 0
            from config import StoreConfig
            cfg = StoreConfig.from_yaml(tmp_path / "feature_store.yaml")
            assert cfg.project == "second"
        finally:
            os.chdir(orig)

    def test_apply_and_list(self, tmp_path):
        from cli import main
        import os
        repo = tmp_path / "feature_repo.py"
        repo.write_text(
            "import sys, os; sys.path.insert(0, os.path.dirname(__file__))\n"
            "from entities import Entity\n"
            "from feature_views import BatchFeatureView, Feature, DataSource\n"
            "user = Entity(name='user', join_key='user_id')\n"
            "stats = BatchFeatureView(\n"
            "    name='stats', entities=[user],\n"
            "    features=[Feature.int('count')], source=DataSource.minio(),\n"
            ")\n"
        )
        orig = os.getcwd()
        os.chdir(tmp_path)
        try:
            rc = main(["apply", str(repo)])
            assert rc == 0
        finally:
            os.chdir(orig)

    def test_apply_missing_file(self, tmp_path):
        from cli import main
        rc = main(["apply", str(tmp_path / "nonexistent.py")])
        assert rc == 1

    def test_stats_no_config(self, tmp_path):
        from cli import main
        import os
        orig = os.getcwd()
        os.chdir(tmp_path)
        try:
            rc = main(["stats"])
            assert rc == 0
        finally:
            os.chdir(orig)

    def test_no_command_returns_nonzero(self):
        from cli import main
        rc = main([])
        assert rc == 1

    def test_materialize_bad_date(self, tmp_path):
        from cli import main
        import os
        orig = os.getcwd()
        os.chdir(tmp_path)
        try:
            rc = main(["materialize", "--start", "bad", "--end", "2026-05-25"])
            assert rc == 1
        finally:
            os.chdir(orig)


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
