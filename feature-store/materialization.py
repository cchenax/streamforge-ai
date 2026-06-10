"""
Offline-to-online materialization for StreamForge feature store.

materialize_offline_to_online()
    Reads a date range from MinIO (offline) and writes latest-per-user
    snapshots to Redis (online).

push_to_online()
    Write pre-computed records directly to Redis; used by FeatureStore.push().
"""
from __future__ import annotations

import datetime
import json
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional

# Reach the layout_spec helpers from the existing examples
_LAYOUT_PATH = Path(__file__).parent.parent / "examples" / "feature-read-paths"
if str(_LAYOUT_PATH) not in sys.path:
    sys.path.insert(0, str(_LAYOUT_PATH))


def materialize_offline_to_online(
    offline_reader,
    online_reader,
    start_date: datetime.date,
    end_date: datetime.date,
    registry,
    feature_view_names: Optional[List[str]] = None,
) -> int:
    """
    Read offline features for [start_date, end_date] and write the latest
    snapshot per user to the online store.

    Returns the number of user snapshots written.
    """
    if offline_reader is None:
        print("[materialize] No offline reader — skipping")
        return 0

    print(f"[materialize] Reading {start_date} → {end_date} from offline store …")
    t0 = time.perf_counter()

    try:
        records = offline_reader.read_date_range(start_date, end_date)
    except Exception as exc:
        print(f"[materialize] Offline read failed: {exc}")
        return 0

    print(f"[materialize] Read {len(records):,} records in {time.perf_counter() - t0:.2f}s")

    # Keep only the most-recent window snapshot per user
    latest_per_user: Dict[int, dict] = {}
    for r in records:
        uid = r["user_id"]
        if uid not in latest_per_user or r["window_start_ms"] > latest_per_user[uid]["window_start_ms"]:
            latest_per_user[uid] = r

    count = push_to_online(list(latest_per_user.values()), online_reader)
    print(
        f"[materialize] Wrote {count} user snapshots to online store "
        f"({time.perf_counter() - t0:.2f}s total)"
    )
    return count


def push_to_online(records: List[dict], online_reader) -> int:
    """
    Write feature records to Redis.

    Each record must have at minimum: user_id, event_count, window_start_ms.
    Returns the number of records written (0 when Redis is unavailable).
    """
    if online_reader is None:
        return 0

    redis_client = getattr(online_reader, "_redis", None)
    if redis_client is None:
        # Stub backend — update in-memory store directly
        stub = getattr(online_reader, "_stub", None)
        if stub is not None:
            _push_to_stub(records, stub)
            return len(records)
        return 0

    from layout_spec import (
        online_user_key,
        online_hist_key,
        ONLINE_USER_TTL,
        ONLINE_HIST_TTL,
    )

    now = time.time()
    pipe = redis_client.pipeline(transaction=False)
    for r in records:
        uid = r["user_id"]
        mapping = {
            "event_count": str(r.get("event_count", 0)),
            "window_start_ms": str(r.get("window_start_ms", 0)),
            "window_end_ms": str(r.get("window_end_ms", r.get("window_start_ms", 0) + 30_000)),
            "event_rate": str(r.get("event_rate", 0.0)),
            "updated_at": str(now),
        }
        pipe.hset(online_user_key(uid), mapping=mapping)
        pipe.expire(online_user_key(uid), ONLINE_USER_TTL)
        pipe.zadd(online_hist_key(uid), {json.dumps(r): r.get("window_start_ms", 0)})
        pipe.expire(online_hist_key(uid), ONLINE_HIST_TTL)

    pipe.execute()
    return len(records)


def _push_to_stub(records: List[dict], stub) -> None:
    """Update the in-memory stub store (used when Redis is unavailable)."""
    now = time.time()
    for r in records:
        uid = r["user_id"]
        entry = dict(r, updated_at=now)
        stub._latest[uid] = entry
        hist = stub._history.setdefault(uid, [])
        hist.append(entry)
        stub._history[uid] = sorted(hist, key=lambda x: x.get("window_start_ms", 0))
