import os
import time
import shutil
import tempfile
from pathlib import Path
from typing import List, Tuple

# Import from prefetch module
from prefetch import FileStat, prefetch_files, select_hot_files

# Simulation constants
REMOTE_LATENCY = 0.2  # 200ms per file access (simulating S3/MinIO)
LOCAL_LATENCY = 0.01   # 10ms per file access (simulating local disk/cache)
NUM_FILES = 5
FILE_SIZE_KB = 100

def create_mock_files(base_dir: Path, count: int) -> List[FileStat]:
    """Create real files on disk to simulate source data."""
    base_dir.mkdir(parents=True, exist_ok=True)
    stats = []
    for i in range(count):
        file_path = base_dir / f"data_part_{i}.bin"
        # Write some dummy data
        with open(file_path, "wb") as f:
            f.write(os.urandom(FILE_SIZE_KB * 1024))
        
        # Create FileStat with high score to ensure they are picked
        stats.append(FileStat(
            uri=f"file://{file_path.absolute()}",
            recent_access_count=10,
            last_access_epoch=time.time()
        ))
    return stats

def simulate_ml_job_no_prefetch(hot_files: List[FileStat]) -> float:
    """
    Simulate processing files directly from 'remote' storage.
    In this scenario, every file access incurs the full remote latency.
    """
    start_time = time.time()
    print(f"  -> Processing {len(hot_files)} files without prefetch...")
    for f in hot_files:
        # Simulate high latency access for each file
        time.sleep(REMOTE_LATENCY)
        # Simulate some processing time
        time.sleep(LOCAL_LATENCY) 
    return time.time() - start_time

def simulate_ml_job_with_prefetch(cache_dir: Path, hot_files: List[FileStat]) -> Tuple[float, float, int]:
    """
    Simulate processing files from local cache.
    1. Prefetch phase: Pulls files from remote to local (incurs latency).
    2. Job phase: Accesses local files (low latency).
    """
    # 1. Prefetch phase
    prefetch_start = time.time()
    print(f"  -> Prefetching {len(hot_files)} files...")
    prefetch_files(hot_files, cache_dir, simulate_latency_s=REMOTE_LATENCY)
    prefetch_duration = time.time() - prefetch_start
    
    # 2. ML Job phase
    job_start = time.time()
    print(f"  -> Running ML job on prefetched files...")
    hits = 0
    for f in hot_files:
        src = Path(f.uri.replace("file://", ""))
        cached = cache_dir / src.name
        if cached.exists():
            hits += 1
            # Simulate low latency local access
            time.sleep(LOCAL_LATENCY)
        else:
            # Fallback to remote if miss
            time.sleep(REMOTE_LATENCY)
            time.sleep(LOCAL_LATENCY)
    
    job_duration = time.time() - job_start
    return prefetch_duration, job_duration, hits

def run_benchmark():
    """
    Main benchmark runner.
    Compares the total end-to-end time of direct remote access vs prefetching.
    """
    print("=" * 60)
    print("      StreamForge AI: Prefetch vs No-Prefetch Benchmark")
    print("=" * 60)
    
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        source_dir = tmp_path / "source"
        cache_dir = tmp_path / "cache"
        
        # 1. Setup mock data
        hot_files = create_mock_files(source_dir, NUM_FILES)
        print(f"[SETUP] Created {NUM_FILES} mock files in {source_dir}")
        
        # 2. Run No-Prefetch Scenario
        print("\n[SCENARIO 1] No Prefetch (Direct Remote Access)")
        no_prefetch_time = simulate_ml_job_no_prefetch(hot_files)
        print(f"  Total time: {no_prefetch_time:.4f}s")
        
        # 3. Run Prefetch Scenario
        print("\n[SCENARIO 2] With Prefetch (Staged to Local Cache)")
        prefetch_dur, job_dur, hits = simulate_ml_job_with_prefetch(cache_dir, hot_files)
        total_prefetch_time = prefetch_dur + job_dur
        print(f"  Prefetch phase: {prefetch_dur:.4f}s")
        print(f"  ML Job phase:   {job_dur:.4f}s (Cache hits: {hits}/{NUM_FILES})")
        print(f"  Total time:     {total_prefetch_time:.4f}s")
        
        # 4. Results Comparison
        print("\n" + "=" * 60)
        print("                 BENCHMARK SUMMARY")
        print("=" * 60)
        print(f"Direct Access (No Prefetch): {no_prefetch_time:.4f}s")
        print(f"Optimized Access (Prefetch): {total_prefetch_time:.4f}s")
        
        # In this simple sequential simulation, the total time might be slightly higher 
        # with prefetching because we prefetch then process. 
        # However, the key insight is the "ML Job phase" latency reduction.
        
        reduction = ((no_prefetch_time - job_dur) / no_prefetch_time) * 100
        print(f"\nML Job Execution Latency Reduction: {reduction:.1f}%")
        print("-" * 60)
        print("Note: In production, prefetching happens in the background or")
        print("in a previous pipeline stage, making 'Prefetch phase' time")
        print("largely transparent to the actual ML training job.")
        print("=" * 60)

if __name__ == "__main__":
    run_benchmark()
