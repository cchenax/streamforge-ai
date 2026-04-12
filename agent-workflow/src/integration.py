import os
import sys
from pathlib import Path
from typing import Dict, Any, List
import logging

# Add prefetch-engine to path
sys.path.append(str(Path(__file__).parent.parent.parent / "prefetch-engine"))

from prefetch import (
    select_hot_files, 
    prefetch_files, 
    run_simulated_ml_job, 
    build_processed_records, 
    upload_processed_records_to_minio,
    FileStat
)

class IntegrationManager:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def run_prefetch_demo(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Run the prefetch demo from prefetch-engine"""
        try:
            base = Path(config.get("demo_dir", "/tmp/streamforge-demo"))
            manifest_dir = base / "manifest"
            cache_dir = base / "prefetch-cache"
            top_n = config.get("top_n", 3)
            
            self.logger.info(f"Running prefetch demo with base directory: {base}")
            
            # Build demo manifest
            candidates = self._build_demo_manifest(manifest_dir)
            hot_files = select_hot_files(candidates, top_n=top_n)
            
            self.logger.info("Selected hot files:")
            for f in hot_files:
                self.logger.info(f"  - {f.uri} (recent_access_count={f.recent_access_count})")
            
            # Prefetch files
            self.logger.info(f"Prefetching into cache: {cache_dir}")
            prefetch_files(hot_files, cache_dir)
            
            # Run simulated ML job
            self.logger.info("Running simulated ML job")
            run_simulated_ml_job(cache_dir, hot_files)
            
            # Build and upload processed records
            job_id = config.get("job_id", self._utc_run_id())
            records = build_processed_records(job_id=job_id, cache_dir=cache_dir, hot_files=hot_files)
            object_key = upload_processed_records_to_minio(records)
            
            result = {
                "status": "completed",
                "hot_files_count": len(hot_files),
                "job_id": job_id,
                "minio_object_key": object_key
            }
            
            self.logger.info(f"Prefetch demo completed with result: {result}")
            return result
        except Exception as e:
            self.logger.error(f"Error running prefetch demo: {e}")
            return {"status": "error", "message": str(e)}
    
    def _build_demo_manifest(self, tmp_dir: Path) -> List[FileStat]:
        """Build a demo manifest for testing"""
        import time
        from prefetch import FileStat
        
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
    
    def _utc_run_id(self) -> str:
        """Generate a UTC run ID"""
        from datetime import datetime, timezone
        return datetime.now(timezone.utc).strftime("run-%Y%m%dT%H%M%SZ")
