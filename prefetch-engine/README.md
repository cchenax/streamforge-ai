## Prefetch Engine

This module implements a **simple prefetch mechanism** for ML workloads. The v0.1 design focuses on clarity over sophistication and is intended as an executable architecture sketch.

### 1. Goals

- **Select hot files**: Identify a small set of likely-to-be-accessed objects from a larger manifest.
- **Prepare objects before a simulated ML job**: Resolve and stage selected objects into a local cache directory.
- **Document the intended optimization flow**: Provide a clear, step-by-step description of how prefetch integrates with a training job.

### 2. Conceptual flow

1. **Job planning**
   - An upstream component produces a simple **access manifest** (e.g. JSON or CSV) describing candidate objects and their access signals (recent access count, last accessed timestamp, feature importance score, etc.).
   - The manifest is written to object storage or a shared filesystem location.

2. **Hot file selection**
   - The prefetch engine reads the manifest.
   - It scores entries based on a simple policy (e.g. "top N by recent access count" or "top N by a weighted score").
   - It emits a **hot set** – a list of object URIs that should be prefetched before the ML job starts.

3. **Prefetch & staging**
   - For each selected object, the engine:
     - Resolves the remote URI (e.g. `s3://`, `minio://`, or local path).
     - Fetches the object (for the demo we simulate this with local file copies and sleep-based latency).
     - Writes it into a **local cache directory** (e.g. `/tmp/streamforge/prefetch-cache`).

4. **ML job consumption**
   - The ML job is launched with:
     - A pointer to the cache directory.
     - The list of hot files that were prefetched.
   - The job first looks into the local cache, then falls back to remote storage if an object is missing.

### 3. Demo implementation

The initial implementation lives in `prefetch-engine/prefetch.py` and provides:

- **`select_hot_files(manifest, top_n)`**: Selects the top-N hot files from an in-memory manifest based on a simple score.
- **`prefetch_files(hot_files, cache_dir)`**: Simulates prefetch by copying local files into a cache directory and sleeping to emulate network delay.
- **`run_simulated_ml_job(cache_dir, hot_files)`**: Represents an ML workload that would consume the prefetched objects.

All three functions are wired together in a small CLI-style `main()` so you can run the prefetch demo locally.

### 4. Intended optimization flow

1. **Without prefetch**
   - Training starts cold.
   - Each object read incurs full remote storage latency.
   - Overall job time is dominated by storage IO cold-starts.

2. **With prefetch**
   - A short prefetch phase runs **before** the ML job.
   - The majority of hot objects are already in the local cache when the job starts.
   - Remote IO is reduced, lowering both **time-to-first-batch** and overall training time.

3. **Future evolution**
   - Replace the local-file simulation with actual S3/MinIO clients.
   - Drive hotness signals from streamed usage metrics instead of static manifests.
   - Add observability (metrics on prefetch hit-rate and prefetch time vs training time).

### 5. How to run the demo

For now, you can run the demo using Python 3.10+:

```bash
cd prefetch-engine
python prefetch.py
```

This will:

1. Build a small in-memory manifest.
2. Select hot files.
3. Simulate prefetch into a local cache directory.
4. Run a stub ML job that reports which files were found in cache.

