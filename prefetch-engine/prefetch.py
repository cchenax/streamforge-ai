import os
import shutil
import time
import json
from datetime import datetime, timezone
from dataclasses import dataclass
from io import BytesIO
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

    # Note: kept as printing only in the original demo.


@dataclass
class ProcessedRecord:
    """
    A single processed output record for a consumed input object.

    Output format is NDJSON (one JSON object per line).
    """

    job_id: str
    input_uri: str
    cache_hit: bool
    processed_at_epoch: float


def build_processed_records(job_id: str, cache_dir: Path, hot_files: List[FileStat]) -> List[ProcessedRecord]:
    """
    Build processed records from the ML job simulation.

    In the real pipeline this would be the transformed/feature-enriched output.
    """
    cache_dir = Path(cache_dir)
    now = time.time()

    records: List[ProcessedRecord] = []
    for f in hot_files:
        src = Path(f.uri.replace("file://", ""))
        cached = cache_dir / src.name
        records.append(
            ProcessedRecord(
                job_id=job_id,
                input_uri=f.uri,
                cache_hit=cached.exists(),
                processed_at_epoch=now,
            )
        )

    return records


def _utc_run_id() -> str:
    # Example: run-20260319T104455Z
    return datetime.now(timezone.utc).strftime("run-%Y%m%dT%H%M%SZ")


def _env(name: str, default: str) -> str:
    v = os.environ.get(name)
    return v if v is not None and v != "" else default


def _env_bool(name: str, default: bool) -> bool:
    v = os.environ.get(name)
    if v is None:
        return default
    v = v.strip().lower()
    if v in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if v in {"0", "false", "f", "no", "n", "off"}:
        return False
    return default


def object_key_for_processed_records(*, run_id: str, part_id: int, prefix: str) -> str:
    """
    File naming convention for processed outputs in MinIO.

    Key format:
      {prefix}/processed/{run_id}/part-{part_id:05d}.jsonl
    """
    prefix = prefix.strip("/")
    return f"{prefix}/processed/{run_id}/part-{part_id:05d}.jsonl"


def _records_to_ndjson(records: List[ProcessedRecord]) -> bytes:
    # One JSON object per line, no surrounding array wrapper.
    lines = [json.dumps(r.__dict__, separators=(",", ":")) for r in records]
    return ("\n".join(lines) + "\n").encode("utf-8")


def upload_processed_records_to_minio(records: List[ProcessedRecord]) -> str | None:
    """
    Upload NDJSON processed records to MinIO.

    Returns the MinIO object key if upload was attempted, else None.
    """
    # If the user doesn't configure MinIO, don't fail the demo; just skip upload.
    endpoint = os.environ.get("MINIO_ENDPOINT")
    access_key = os.environ.get("MINIO_ACCESS_KEY")
    secret_key = os.environ.get("MINIO_SECRET_KEY")
    bucket = os.environ.get("MINIO_BUCKET", "processed")
    prefix = os.environ.get("MINIO_PREFIX", "streamforge")

    if not endpoint or not access_key or not secret_key:
        print("[MINIO] Skipping upload (set MINIO_ENDPOINT/MINIO_ACCESS_KEY/MINIO_SECRET_KEY to enable).")
        return None

    try:
        from minio import Minio
    except ImportError as e:
        raise RuntimeError(
            "MinIO upload requested but `minio` dependency is missing. "
            "Run: pip install -r requirements.txt in `prefetch-engine/`."
        ) from e

    secure = _env_bool("MINIO_SECURE", False)

    client = Minio(
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure,
    )

    # Create bucket if needed (MinIO is forgiving; this keeps demo friction low).
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)

    # Use job_id from the first record; fall back safely.
    job_id = records[0].job_id if records else _utc_run_id()
    part_id = int(os.environ.get("MINIO_PART_ID", "0"))

    object_key = object_key_for_processed_records(run_id=job_id, part_id=part_id, prefix=prefix)
    payload = _records_to_ndjson(records)

    data_stream = BytesIO(payload)
    content_type = "application/x-ndjson"

    client.put_object(
        bucket_name=bucket,
        object_name=object_key,
        data=data_stream,
        length=len(payload),
        content_type=content_type,
    )

    print(f"[MINIO] Uploaded processed records to bucket={bucket} key={object_key}")
    return object_key


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
    job_id = os.environ.get("STREAMFORGE_JOB_ID", _utc_run_id())
    # Keep the original cache hit/miss reporting.
    run_simulated_ml_job(cache_dir, hot_files)

    # Build and upload processed outputs (NDJSON).
    records = build_processed_records(job_id=job_id, cache_dir=cache_dir, hot_files=hot_files)
    upload_processed_records_to_minio(records)


if __name__ == "__main__":
    main()

