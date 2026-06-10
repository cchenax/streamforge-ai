"""
FeatureStore — the main entry point for StreamForge's feature store.

Feast-compatible API surface
-----------------------------
  store = FeatureStore(project="streamforge")
  store.apply([entity, batch_view, stream_view, service])

  # Online serving
  fv = store.get_online_features(
      features=["user_event_stats:event_count", "user_event_stats:event_rate"],
      entity_rows=[{"user_id": 42}, {"user_id": 99}],
  )
  fv.to_dict()   # → {"event_count": [12, 7], "event_rate": [0.4, 0.23]}
  fv.to_df()     # → pandas DataFrame

  # Historical / offline
  job = store.get_historical_features(
      entity_df=pd.DataFrame({"user_id": [42, 99], "event_timestamp": [...]}),
      features=["user_event_stats:event_count"],
  )
  df = job.to_df()

  # Materialize offline → online
  store.materialize(start_date=date(2026, 5, 1), end_date=date(2026, 5, 25))

  # Push stream records
  store.push("user_event_stats_stream", records, to="online")

Tecton-compatible alias
-----------------------
  store.get_features(
      join_keys={"user_id": [1, 42, 99]},
      feature_service_name="recommendation_service",
  )
"""
from __future__ import annotations

import datetime
import sys
from pathlib import Path
from typing import Dict, List, Optional, Union

# Expose existing feature-read-paths readers
_READERS_DIR = Path(__file__).parent.parent / "examples" / "feature-read-paths"
if str(_READERS_DIR) not in sys.path:
    sys.path.insert(0, str(_READERS_DIR))

from registry import Registry
from entities import Entity
from feature_views import BatchFeatureView, StreamFeatureView, OnDemandFeatureView, Feature
from feature_service import FeatureService

try:
    from config import StoreConfig
    _CONFIG_AVAILABLE = True
except ImportError:
    _CONFIG_AVAILABLE = False

try:
    from offline_batch_reader import OfflineBatchReader
    from online_lowlatency_reader import OnlineFeatureReader
    _READERS_AVAILABLE = True
except ImportError:
    _READERS_AVAILABLE = False

RegistryObject = Union[Entity, BatchFeatureView, StreamFeatureView, OnDemandFeatureView, FeatureService]


# ---------------------------------------------------------------------------
# FeatureVector — result of online serving
# ---------------------------------------------------------------------------

class FeatureVector:
    """
    Result of get_online_features() or get_features().

    Feast-compatible interface: .to_dict() and .to_df().
    """

    def __init__(self, results: List[dict], feature_names: List[str]) -> None:
        self._results = results
        self._feature_names = feature_names

    def to_dict(self) -> Dict[str, list]:
        """Return {feature_name: [value, ...]} mapping (Feast-style)."""
        return {name: [r.get(name) for r in self._results] for name in self._feature_names}

    def to_df(self):
        """Return a pandas DataFrame. Requires pandas."""
        try:
            import pandas as pd
        except ImportError as exc:
            raise ImportError("pandas is required for to_df()") from exc
        return pd.DataFrame(self._results)

    def __len__(self) -> int:
        return len(self._results)

    def __repr__(self) -> str:
        return f"FeatureVector(rows={len(self._results)}, features={self._feature_names})"


# ---------------------------------------------------------------------------
# RetrievalJob — lazy historical retrieval
# ---------------------------------------------------------------------------

class RetrievalJob:
    """
    Lazy offline / historical feature retrieval.

    Feast-compatible: .to_df(), .to_arrow(), .persist().
    """

    def __init__(
        self,
        entity_df,
        features: List[str],
        offline_reader,
        registry: Registry,
    ) -> None:
        self._entity_df = entity_df
        self._features = features
        self._offline_reader = offline_reader
        self._registry = registry
        self._cached: Optional[list] = None

    # ------------------------------------------------------------------
    # Materialise
    # ------------------------------------------------------------------

    def to_df(self):
        import pandas as pd
        records = self._fetch()
        return pd.DataFrame(records) if records else pd.DataFrame()

    def to_arrow(self):
        try:
            import pyarrow as pa
        except ImportError as exc:
            raise ImportError("pyarrow is required for to_arrow()") from exc
        return pa.Table.from_pandas(self.to_df())

    def persist(self, destination, allow_overwrite: bool = False) -> None:
        """Write results to a file path or object with a .write() method."""
        df = self.to_df()
        if hasattr(destination, "write"):
            destination.write(df.to_dict(orient="records"))
        elif isinstance(destination, (str, Path)):
            df.to_parquet(str(destination))
        else:
            raise ValueError(f"Unsupported destination: {type(destination)}")

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _fetch(self) -> List[dict]:
        if self._cached is not None:
            return self._cached
        if self._offline_reader is None:
            return []

        entity_data = _normalize_entity_df(self._entity_df)
        entity_user_ids = set(int(u) for u in entity_data.get("user_id", []))

        timestamps = entity_data.get("event_timestamp", [])
        if timestamps:
            try:
                import pandas as pd
                ts = pd.to_datetime(timestamps)
                start = ts.min().date() - datetime.timedelta(days=1)
                end = ts.max().date()
            except Exception:
                end = datetime.date.today()
                start = end - datetime.timedelta(days=7)
        else:
            end = datetime.date.today()
            start = end - datetime.timedelta(days=7)

        try:
            records = self._offline_reader.read_date_range(start, end)
        except Exception:
            records = []

        filtered = [r for r in records if r.get("user_id") in entity_user_ids]
        self._cached = filtered
        return filtered


# ---------------------------------------------------------------------------
# FeatureStore
# ---------------------------------------------------------------------------

class FeatureStore:
    """
    StreamForge FeatureStore — Feast-compatible + Tecton-compatible.

    Wraps the existing offline (MinIO/S3) and online (Redis) readers with
    a declarative registry of entities, feature views, and feature services.

    Parameters
    ----------
    project   : logical project / namespace (used as a metadata label)
    repo_path : optional path for persisting registry snapshots
    """

    def __init__(
        self,
        project: str = "streamforge",
        repo_path: Optional[str] = None,
        config: Optional["StoreConfig"] = None,
    ) -> None:
        if config is not None:
            self.project = config.project
            self.repo_path = config.registry_path().parent
        else:
            self.project = project
            self.repo_path = Path(repo_path) if repo_path else None
        self._config = config
        self._registry = Registry()
        self._online_reader: Optional[object] = None
        self._offline_reader: Optional[object] = None
        self._init_readers()

    def _init_readers(self) -> None:
        if not _READERS_AVAILABLE:
            return
        try:
            self._online_reader = OnlineFeatureReader()
        except Exception:
            pass
        try:
            self._offline_reader = OfflineBatchReader()
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Registry operations  (Feast: apply / list_*)
    # ------------------------------------------------------------------

    def apply(self, objects: List[RegistryObject]) -> None:
        """Register entities, feature views, and feature services."""
        for obj in objects:
            if isinstance(obj, Entity):
                self._registry.apply_entity(obj)
            elif isinstance(obj, (BatchFeatureView, StreamFeatureView, OnDemandFeatureView)):
                self._registry.apply_feature_view(obj)
            elif isinstance(obj, FeatureService):
                self._registry.apply_feature_service(obj)
            else:
                raise TypeError(f"Cannot apply object of type {type(obj).__name__!r}")
        if self.repo_path:
            self._registry.save(self.repo_path / "registry.json")

    def list_entities(self) -> List[Entity]:
        return self._registry.list_entities()

    def list_feature_views(self) -> List:
        return self._registry.list_feature_views()

    def list_feature_services(self) -> List[FeatureService]:
        return self._registry.list_feature_services()

    def get_feature_view(self, name: str):
        return self._registry.get_feature_view(name)

    def get_feature_service(self, name: str) -> FeatureService:
        return self._registry.get_feature_service(name)

    # ------------------------------------------------------------------
    # Online serving  (Feast: get_online_features)
    # ------------------------------------------------------------------

    def get_online_features(
        self,
        features: List[str],
        entity_rows: List[Dict],
    ) -> FeatureVector:
        """
        Fetch real-time features for a list of entity rows.

        Parameters
        ----------
        features : list[str]
            Feature references — "view_name:feature_name" or "view_name:*".
        entity_rows : list[dict]
            Each dict must include the join key, e.g. {"user_id": 42}.

        Returns
        -------
        FeatureVector
        """
        parsed_refs = _parse_feature_refs(features, self._registry)
        feature_names = list(dict.fromkeys(fn for _, fn in parsed_refs))  # preserve order, dedupe

        user_ids = [int(row["user_id"]) for row in entity_rows if "user_id" in row]
        raw_batch: Dict[int, dict] = {}
        if self._online_reader and user_ids:
            try:
                raw_batch = self._online_reader.get_batch(user_ids)
            except Exception:
                pass

        results = []
        for row in entity_rows:
            uid = row.get("user_id")
            feat_data = raw_batch.get(int(uid), {}) if uid is not None else {}
            results.append({**row, **feat_data})

        # Apply OnDemandFeatureView transforms
        results = self._apply_on_demand_transforms(results, parsed_refs)

        return FeatureVector(results, feature_names)

    def _apply_on_demand_transforms(
        self, results: List[dict], parsed_refs: List[tuple]
    ) -> List[dict]:
        """Run any OnDemandFeatureView transforms requested in the feature refs."""
        on_demand_views = {
            name: self._registry.get_feature_view(name)
            for name, _ in parsed_refs
            if name and name in {v.name for v in self._registry.list_feature_views()}
            and isinstance(self._registry.get_feature_view(name), OnDemandFeatureView)
        }
        if not on_demand_views:
            return results
        return [
            {**row, **{view.name: view.compute(row) for view in on_demand_views.values()}}
            for row in results
        ]

    # ------------------------------------------------------------------
    # Tecton-compatible alias
    # ------------------------------------------------------------------

    def get_features(
        self,
        join_keys: Dict[str, List],
        feature_service_name: str,
    ) -> FeatureVector:
        """
        Tecton-compatible get_features().

        Parameters
        ----------
        join_keys : dict[str, list]
            e.g. {"user_id": [1, 42, 99]}
        feature_service_name : str
            Name of a registered FeatureService.
        """
        service = self._registry.get_feature_service(feature_service_name)
        feature_refs = [
            f"{view.name}:*"
            for view in service.features
        ]
        key_names = list(join_keys.keys())
        key_values = list(join_keys.values())
        entity_rows = [
            dict(zip(key_names, row_vals))
            for row_vals in zip(*key_values)
        ]
        return self.get_online_features(feature_refs, entity_rows)

    # ------------------------------------------------------------------
    # Historical / offline retrieval  (Feast: get_historical_features)
    # ------------------------------------------------------------------

    def get_historical_features(
        self,
        entity_df,
        features: List[str],
        full_feature_names: bool = False,
    ) -> RetrievalJob:
        """
        Lazy historical feature retrieval from the offline store.

        Parameters
        ----------
        entity_df : pd.DataFrame or dict-of-lists
            Must contain join key column (e.g. "user_id") and optionally
            "event_timestamp" for point-in-time correct lookups.
        features : list[str]
            Feature references in "view_name:feature_name" format.

        Returns
        -------
        RetrievalJob
            Call .to_df() to materialise results.
        """
        return RetrievalJob(entity_df, features, self._offline_reader, self._registry)

    # ------------------------------------------------------------------
    # Materialization  (Feast: materialize / materialize_incremental)
    # ------------------------------------------------------------------

    def materialize(
        self,
        start_date: datetime.date,
        end_date: datetime.date,
        feature_views: Optional[List[str]] = None,
    ) -> None:
        """
        Read offline features for [start_date, end_date] and push to online store.

        Parameters
        ----------
        start_date, end_date : datetime.date
            Inclusive date range.
        feature_views : list[str] | None
            Restrict to named views; None = all registered batch views.
        """
        from materialization import materialize_offline_to_online
        materialize_offline_to_online(
            offline_reader=self._offline_reader,
            online_reader=self._online_reader,
            start_date=start_date,
            end_date=end_date,
            registry=self._registry,
            feature_view_names=feature_views,
        )

    def materialize_incremental(
        self,
        end_date: datetime.date,
        feature_views: Optional[List[str]] = None,
    ) -> None:
        """Materialize the last 24 hours up to end_date."""
        start_date = end_date - datetime.timedelta(days=1)
        self.materialize(start_date, end_date, feature_views)

    # ------------------------------------------------------------------
    # Push (stream ingest)  (Feast: push)
    # ------------------------------------------------------------------

    def push(
        self,
        feature_view_name: str,
        df,
        to: str = "online",
    ) -> None:
        """
        Push externally computed feature values into the store.

        Parameters
        ----------
        feature_view_name : str
            Must match a registered StreamFeatureView name.
        df : pd.DataFrame or list[dict]
        to : "online" (Redis) — offline push not yet supported
        """
        from materialization import push_to_online
        records = _df_to_records(df)
        if to == "online":
            pushed = push_to_online(records, self._online_reader)
            print(f"[push] Wrote {pushed} records to online store for {feature_view_name!r}")
        else:
            raise ValueError(f"push(to={to!r}) is not supported")

    # ------------------------------------------------------------------
    # Diagnostics
    # ------------------------------------------------------------------

    def stats(self) -> dict:
        online_stats = {}
        if self._online_reader:
            try:
                online_stats = self._online_reader.stats()
            except Exception:
                pass
        return {
            "project": self.project,
            "registry": self._registry.dump(),
            "online_store": online_stats,
            "offline_store": {
                "available": self._offline_reader is not None
                and getattr(self._offline_reader, "is_live", False)
            },
        }

    def __repr__(self) -> str:
        reg = self._registry.dump()
        return (
            f"FeatureStore(project={self.project!r}, "
            f"entities={reg['entities']}, "
            f"views={reg['feature_views']}, "
            f"services={reg['feature_services']})"
        )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _parse_feature_refs(
    features: List[str], registry: Registry
) -> List[tuple]:
    """Parse "view:feature" refs → list of (view_name, feature_name)."""
    parsed = []
    for ref in features:
        if ref == "*":
            for view in registry.list_feature_views():
                for f in view.features:
                    parsed.append((view.name, f.name))
        elif ":" in ref:
            view_name, feat_name = ref.rsplit(":", 1)
            if feat_name == "*":
                try:
                    view = registry.get_feature_view(view_name)
                    for f in view.features:
                        parsed.append((view_name, f.name))
                except KeyError:
                    pass
            else:
                parsed.append((view_name, feat_name))
        else:
            parsed.append(("", ref))
    return parsed


def _normalize_entity_df(entity_df) -> dict:
    """Coerce entity_df (DataFrame or dict) to a plain dict of lists."""
    if isinstance(entity_df, dict):
        return entity_df
    try:
        return entity_df.to_dict(orient="list")
    except AttributeError:
        return {}


def _df_to_records(df) -> List[dict]:
    """Convert DataFrame or list[dict] to list[dict]."""
    if isinstance(df, list):
        return df
    try:
        return df.to_dict(orient="records")
    except AttributeError:
        return list(df)
