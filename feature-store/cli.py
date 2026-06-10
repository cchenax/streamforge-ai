"""
StreamForge Feature Store CLI.

Usage
-----
    python -m feature_store <command> [options]

Commands
--------
    init                Create feature_store.yaml in the current directory
    apply  <repo.py>    Register all entities/views/services defined in repo.py
    plan   <repo.py>    Dry-run apply — show what would change without writing
    materialize         Read offline store → push to online store
    materialize-incremental
                        Materialize the last 24 h up to today
    list                List registered entities, views, and services
    describe <type> <name>
                        Show details of an entity, feature-view, or feature-service
    stats               Show store health (registry + online/offline availability)

Examples
--------
    python -m feature_store init
    python -m feature_store apply feature_repo.py
    python -m feature_store plan  feature_repo.py
    python -m feature_store list
    python -m feature_store describe view user_event_stats
    python -m feature_store materialize --start 2026-05-01 --end 2026-05-25
    python -m feature_store materialize-incremental --days 3
    python -m feature_store stats
"""
from __future__ import annotations

import argparse
import importlib.util
import sys
import textwrap
from pathlib import Path
from typing import List


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main(argv: List[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if not hasattr(args, "func"):
        parser.print_help()
        return 1

    return args.func(args)


# ---------------------------------------------------------------------------
# Sub-command implementations
# ---------------------------------------------------------------------------

def _cmd_init(args) -> int:
    target = Path.cwd() / "feature_store.yaml"
    if target.exists() and not args.force:
        print(f"[init] {target} already exists. Use --force to overwrite.")
        return 1

    sys.path.insert(0, str(Path(__file__).parent))
    from config import StoreConfig
    cfg = StoreConfig(project=args.project)
    cfg.save(target)
    print(f"[init] Created {target}")
    print(f"       project : {cfg.project}")
    print(f"       online  : {cfg.online_store.type} @ {cfg.online_store.host}:{cfg.online_store.port}")
    print(f"       offline : {cfg.offline_store.type} @ {cfg.offline_store.endpoint}/{cfg.offline_store.bucket}")
    return 0


def _cmd_apply(args) -> int:
    store, objects = _load_repo(args.repo)
    if objects is None:
        return 1
    store.apply(objects)
    reg = store._registry.dump()
    print(f"[apply] Applied to project={store.project!r}")
    _print_registry(reg)
    return 0


def _cmd_plan(args) -> int:
    store, objects = _load_repo(args.repo)
    if objects is None:
        return 1

    reg = store._registry.dump()
    current_entities = set(reg.get("entities", []))
    current_views = set(reg.get("feature_views", []))
    current_services = set(reg.get("feature_services", []))

    from entities import Entity
    from feature_views import BatchFeatureView, StreamFeatureView, OnDemandFeatureView
    from feature_service import FeatureService

    new_entities, new_views, new_services = set(), set(), set()
    for obj in objects:
        if isinstance(obj, Entity):
            new_entities.add(obj.name)
        elif isinstance(obj, (BatchFeatureView, StreamFeatureView, OnDemandFeatureView)):
            new_views.add(obj.name)
        elif isinstance(obj, FeatureService):
            new_services.add(obj.name)

    print(f"[plan] Project: {store.project!r} (dry run — no changes written)")
    _print_diff("Entities", current_entities, new_entities)
    _print_diff("Feature views", current_views, new_views)
    _print_diff("Feature services", current_services, new_services)
    return 0


def _cmd_materialize(args) -> int:
    import datetime
    store = _load_store()
    try:
        start = datetime.date.fromisoformat(args.start)
        end = datetime.date.fromisoformat(args.end)
    except ValueError as exc:
        print(f"[materialize] Invalid date: {exc}")
        return 1
    views = args.views.split(",") if args.views else None
    store.materialize(start, end, views)
    return 0


def _cmd_materialize_incremental(args) -> int:
    import datetime
    store = _load_store()
    end = datetime.date.today()
    start = end - datetime.timedelta(days=args.days)
    print(f"[materialize-incremental] {start} → {end}")
    store.materialize(start, end)
    return 0


def _cmd_list(args) -> int:
    store = _load_store()
    reg = store._registry.dump()
    _print_registry(reg)
    return 0


def _cmd_describe(args) -> int:
    store = _load_store()
    kind = args.kind.lower()
    name = args.name

    if kind in ("entity", "e"):
        try:
            obj = store._registry.get_entity(name)
        except KeyError:
            print(f"[describe] Entity not found: {name!r}")
            return 1
        print(f"Entity: {obj.name}")
        print(f"  join_key   : {obj.join_key}")
        print(f"  value_type : {obj.value_type}")
        if obj.description:
            print(f"  description: {obj.description}")
        if obj.tags:
            print(f"  tags       : {obj.tags}")

    elif kind in ("view", "feature-view", "featureview", "v"):
        try:
            obj = store._registry.get_feature_view(name)
        except KeyError:
            print(f"[describe] Feature view not found: {name!r}")
            return 1
        print(f"{type(obj).__name__}: {obj.name}")
        print(f"  entities : {[e.name for e in obj.entities]}")
        print(f"  features : {[f.name + ':' + f.dtype for f in obj.features]}")
        if hasattr(obj, "source"):
            print(f"  source   : {obj.source}")
        if hasattr(obj, "ttl"):
            print(f"  ttl      : {obj.ttl}")
        if obj.description:
            print(f"  description: {obj.description}")
        if obj.tags:
            print(f"  tags     : {obj.tags}")

    elif kind in ("service", "feature-service", "featureservice", "s"):
        try:
            obj = store._registry.get_feature_service(name)
        except KeyError:
            print(f"[describe] Feature service not found: {name!r}")
            return 1
        print(f"FeatureService: {obj.name}")
        print(f"  views   : {obj.feature_view_names}")
        if obj.owner:
            print(f"  owner   : {obj.owner}")
        if obj.description:
            print(f"  description: {obj.description}")
        if obj.tags:
            print(f"  tags    : {obj.tags}")

    else:
        print(f"[describe] Unknown type {kind!r}. Use: entity | view | service")
        return 1

    return 0


def _cmd_stats(args) -> int:
    store = _load_store()
    s = store.stats()
    print(f"Project  : {s['project']}")
    reg = s["registry"]
    print(f"Registry : {len(reg['entities'])} entities, "
          f"{len(reg['feature_views'])} views, "
          f"{len(reg['feature_services'])} services")
    os_ = s.get("online_store", {})
    print(f"Online   : {'available' if os_ else 'unavailable (stub mode)'}")
    off = s.get("offline_store", {})
    print(f"Offline  : {'available' if off.get('available') else 'unavailable (stub mode)'}")
    return 0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_store():
    """Load FeatureStore from the nearest feature_store.yaml."""
    _add_store_path()
    from store import FeatureStore
    try:
        from config import StoreConfig
        cfg = StoreConfig.from_yaml()
        return FeatureStore(config=cfg)
    except FileNotFoundError:
        return FeatureStore()


def _load_repo(repo_path: str):
    """
    Import a feature repo Python file and collect all registry objects.
    Returns (FeatureStore, list_of_objects) or (None, None) on error.
    """
    _add_store_path()
    path = Path(repo_path)
    if not path.exists():
        print(f"[error] File not found: {path}")
        return None, None

    spec = importlib.util.spec_from_file_location("_feature_repo", path)
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
    except Exception as exc:
        print(f"[error] Failed to import {path}: {exc}")
        return None, None

    from entities import Entity
    from feature_views import BatchFeatureView, StreamFeatureView, OnDemandFeatureView
    from feature_service import FeatureService
    from store import FeatureStore

    objects = [
        v for v in vars(module).values()
        if isinstance(v, (Entity, BatchFeatureView, StreamFeatureView,
                          OnDemandFeatureView, FeatureService))
    ]

    store = _load_store()
    return store, objects


def _add_store_path() -> None:
    here = str(Path(__file__).parent)
    if here not in sys.path:
        sys.path.insert(0, here)


def _print_registry(reg: dict) -> None:
    print(f"  entities ({len(reg['entities'])}): {', '.join(reg['entities']) or '—'}")
    print(f"  views    ({len(reg['feature_views'])}): {', '.join(reg['feature_views']) or '—'}")
    print(f"  services ({len(reg['feature_services'])}): {', '.join(reg['feature_services']) or '—'}")


def _print_diff(label: str, current: set, incoming: set) -> None:
    added = incoming - current
    removed = current - incoming
    unchanged = current & incoming
    print(f"\n  {label}:")
    for name in sorted(unchanged):
        print(f"    ~ {name}  (unchanged)")
    for name in sorted(added):
        print(f"    + {name}  (new)")
    for name in sorted(removed):
        print(f"    - {name}  (removed)")
    if not added and not removed and not unchanged:
        print("    (none)")


# ---------------------------------------------------------------------------
# Argument parser
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m feature_store",
        description="StreamForge Feature Store CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""\
            examples:
              python -m feature_store init
              python -m feature_store apply feature_repo.py
              python -m feature_store plan  feature_repo.py
              python -m feature_store list
              python -m feature_store describe view user_event_stats
              python -m feature_store materialize --start 2026-05-01 --end 2026-05-25
              python -m feature_store materialize-incremental --days 3
              python -m feature_store stats
        """),
    )
    sub = parser.add_subparsers(metavar="<command>")

    # init
    p = sub.add_parser("init", help="Create feature_store.yaml in current directory")
    p.add_argument("--project", default="streamforge", help="Project name")
    p.add_argument("--force", action="store_true", help="Overwrite existing file")
    p.set_defaults(func=_cmd_init)

    # apply
    p = sub.add_parser("apply", help="Register objects from a feature repo file")
    p.add_argument("repo", metavar="REPO.PY", help="Path to feature repo Python file")
    p.set_defaults(func=_cmd_apply)

    # plan
    p = sub.add_parser("plan", help="Dry-run apply — show what would change")
    p.add_argument("repo", metavar="REPO.PY", help="Path to feature repo Python file")
    p.set_defaults(func=_cmd_plan)

    # materialize
    p = sub.add_parser("materialize", help="Offline → online materialization")
    p.add_argument("--start", required=True, metavar="YYYY-MM-DD", help="Start date (inclusive)")
    p.add_argument("--end", required=True, metavar="YYYY-MM-DD", help="End date (inclusive)")
    p.add_argument("--views", default="", metavar="VIEW1,VIEW2", help="Restrict to specific views")
    p.set_defaults(func=_cmd_materialize)

    # materialize-incremental
    p = sub.add_parser("materialize-incremental", help="Materialize the last N days")
    p.add_argument("--days", type=int, default=1, help="Number of days back (default: 1)")
    p.set_defaults(func=_cmd_materialize_incremental)

    # list
    p = sub.add_parser("list", help="List all registered objects")
    p.set_defaults(func=_cmd_list)

    # describe
    p = sub.add_parser("describe", help="Show details of a registry object")
    p.add_argument("kind", choices=["entity", "view", "service"],
                   help="Object type to describe")
    p.add_argument("name", help="Object name")
    p.set_defaults(func=_cmd_describe)

    # stats
    p = sub.add_parser("stats", help="Show store health and connection status")
    p.set_defaults(func=_cmd_stats)

    return parser
