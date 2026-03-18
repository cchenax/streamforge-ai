import os
import shutil
import time
from dataclasses import dataclass
from pathlib import Path
from typing import List


@dataclass
class FileStat:
    """Represents basic access statistics for a candidate object."""

    uri: str
    recent_access_count: int
    last_access_epoch: float

    @property
    def score(self) -> float:
        # Simple scoring function:
        #   - prioritize recent access count
        #   - lightly weight recency
        return float(self.recent_access_count) + 0.000001 * self.last_access_epoch


def select_hot_files(candidates: List[FileStat], top_n: int) -> List[FileStat]:
    """Select top-N hot files based on the score."""
    if top_n <= 0:
        return []
    # Sort descending by score
    return sorted(candidates, key=lambda c: c.score, reverse=True)[:top_n]


def prefetch_files(hot_files: List[FileStat], cache_dir: Path, simulate_latency_s: float = 0.05) -> None:
    """
    Simulate prefetch by copying local files into a cache directory.

    In a real deployment this would pull from MinIO/S3 instead of the local filesystem.
    """
    cache_dir = Path(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)

    for f in hot_files:
        src = Path(f.uri.replace("file://", ""))
        dst = cache_dir / src.name

        if not src.exists():
            # Skip missing files in the demo; in production this should be logged/alerted.
            continue

        # Simulate remote IO latency
        time.sleep(simulate_latency_s)
        shutil.copy2(src, dst)


def run_simulated_ml_job(cache_dir: Path, hot_files: List[FileStat]) -> None:
    """
    Simulate an ML job that consumes prefetched files.

    For now this just reports cache hits vs misses.
    """
    cache_dir = Path(cache_dir)
    hits = 0
    misses = 0

    for f in hot_files:
        src = Path(f.uri.replace("file://", ""))
        cached = cache_dir / src.name
        if cached.exists():
            hits += 1
        else:
            misses += 1

    print(f"[ML JOB] cache hits={hits}, misses={misses}, total={len(hot_files)}")


def _build_demo_manifest(tmp_dir: Path) -> List[FileStat]:
    """
    Build a small demo manifest backed by local files.

    We create a few small text files and attach synthetic access stats.
    """
    tmp_dir.mkdir(parents=True, exist_ok=True)

    demo_files = []
    now = time.time()

    specs = [
        ("feature_batch_A.txt", 100, now - 60),
        ("feature_batch_B.txt", 80, now - 120),
        ("feature_batch_C.txt", 30, now - 10),
        ("feature_batch_D.txt", 5, now - 5),
    ]

    for name, access_count, last_access in specs:
        path = tmp_dir / name
        path.write_text(f"demo data for {name}\n")
        demo_files.append(
            FileStat(
                uri=f"file://{path}",
                recent_access_count=access_count,
                last_access_epoch=last_access,
            )
        )

    return demo_files


def main() -> None:
    """
    Run the end-to-end prefetch demo:
    - build a demo manifest
    - select hot files
    - prefetch into cache
    - run a simulated ML job
    """
    base = Path(os.environ.get("STREAMFORGE_DEMO_DIR", "/tmp/streamforge-demo"))
    manifest_dir = base / "manifest"
    cache_dir = base / "prefetch-cache"

    print(f"[DEMO] using base directory: {base}")

    candidates = _build_demo_manifest(manifest_dir)
    hot_files = select_hot_files(candidates, top_n=3)

    print("[DEMO] selected hot files (top 3 by score):")
    for f in hot_files:
        print(f"  - {f.uri} (recent_access_count={f.recent_access_count})")

    print(f"[DEMO] prefetching into cache: {cache_dir}")
    prefetch_files(hot_files, cache_dir)

    print("[DEMO] running simulated ML job")
    run_simulated_ml_job(cache_dir, hot_files)


if __name__ == "__main__":
    main()

