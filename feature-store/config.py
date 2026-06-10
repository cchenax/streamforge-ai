"""
StoreConfig — load and validate feature_store.yaml.

Usage
-----
    config = StoreConfig.from_yaml("feature_store.yaml")
    config = StoreConfig.from_yaml()          # searches CWD → parents
    StoreConfig().save("feature_store.yaml")  # write defaults
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Optional


_YAML_FILENAME = "feature_store.yaml"


# ---------------------------------------------------------------------------
# Sub-configs
# ---------------------------------------------------------------------------

@dataclass
class OnlineStoreConfig:
    type: str = "redis"
    host: str = "localhost"
    port: int = 6379
    db: int = 0


@dataclass
class OfflineStoreConfig:
    type: str = "minio"
    endpoint: str = "localhost:9000"
    access_key: str = "minioadmin"
    secret_key: str = "minioadmin"
    bucket: str = "processed"
    prefix: str = "streamforge/features"


# ---------------------------------------------------------------------------
# StoreConfig
# ---------------------------------------------------------------------------

@dataclass
class StoreConfig:
    """
    Parsed contents of feature_store.yaml.

    All fields have sensible defaults so the store works out-of-the-box
    without any configuration file.
    """
    project: str = "streamforge"
    registry: str = "registry.json"
    provider: str = "local"
    online_store: OnlineStoreConfig = field(default_factory=OnlineStoreConfig)
    offline_store: OfflineStoreConfig = field(default_factory=OfflineStoreConfig)

    # ------------------------------------------------------------------
    # Constructors
    # ------------------------------------------------------------------

    @classmethod
    def from_yaml(cls, path: Optional[str | Path] = None) -> "StoreConfig":
        """
        Load config from a YAML file.

        If *path* is omitted, search from CWD upward for feature_store.yaml.
        Raises FileNotFoundError if no file is found.
        """
        try:
            import yaml
        except ImportError as exc:
            raise ImportError("PyYAML is required: pip install pyyaml") from exc

        resolved = cls._find_config(path)
        with open(resolved) as f:
            raw = yaml.safe_load(f) or {}

        online_raw = raw.get("online_store", {})
        offline_raw = raw.get("offline_store", {})

        online = OnlineStoreConfig(**{
            k: v for k, v in online_raw.items()
            if k in OnlineStoreConfig.__dataclass_fields__
        })
        offline = OfflineStoreConfig(**{
            k: v for k, v in offline_raw.items()
            if k in OfflineStoreConfig.__dataclass_fields__
        })

        return cls(
            project=raw.get("project", "streamforge"),
            registry=raw.get("registry", "registry.json"),
            provider=raw.get("provider", "local"),
            online_store=online,
            offline_store=offline,
        )

    @classmethod
    def _find_config(cls, path: Optional[str | Path]) -> Path:
        if path is not None:
            p = Path(path)
            if not p.exists():
                raise FileNotFoundError(f"Config not found: {p}")
            return p
        # Walk CWD → ancestors
        candidate = Path.cwd()
        for _ in range(10):
            target = candidate / _YAML_FILENAME
            if target.exists():
                return target
            parent = candidate.parent
            if parent == candidate:
                break
            candidate = parent
        raise FileNotFoundError(
            f"{_YAML_FILENAME!r} not found in {Path.cwd()} or any parent. "
            "Run: python -m feature_store init"
        )

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def save(self, path: str | Path = _YAML_FILENAME) -> None:
        """Write config to a YAML file."""
        try:
            import yaml
        except ImportError as exc:
            raise ImportError("PyYAML is required: pip install pyyaml") from exc

        data = {
            "project": self.project,
            "registry": self.registry,
            "provider": self.provider,
            "online_store": asdict(self.online_store),
            "offline_store": asdict(self.offline_store),
        }
        with open(path, "w") as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False)

    # ------------------------------------------------------------------
    # Convenience
    # ------------------------------------------------------------------

    def registry_path(self, relative_to: Optional[Path] = None) -> Path:
        base = relative_to or Path.cwd()
        p = Path(self.registry)
        return p if p.is_absolute() else base / p

    def __repr__(self) -> str:
        return (
            f"StoreConfig(project={self.project!r}, "
            f"online={self.online_store.type!r}, "
            f"offline={self.offline_store.type!r})"
        )
