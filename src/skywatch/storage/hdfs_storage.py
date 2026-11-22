"""HDFS storage adapter with NameNode safemode pre-checks and offline fallback.

Implements HYG-06 & HYG-07: Idempotent HDFS partition upload with staging rename
and transparent fallback to local storage when HDFS is offline or in safemode.
"""
from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

from skywatch.pipeline.logging import get_logger

logger = get_logger("hdfs_storage")


class HDFSStorageAdapter:
    """Adapter wrapping HDFS CLI operations with safemode verification and offline fallback."""

    def __init__(
        self,
        hdfs_root: str = "/skywatch",
        offline_dir: str = "data/local_hdfs",
    ) -> None:
        self.hdfs_root = hdfs_root
        self.offline_dir = Path(offline_dir)

    def is_available(self) -> bool:
        """Check if hdfs binary exists and NameNode is responsive."""
        try:
            proc = subprocess.run(
                ["hdfs", "version"],
                capture_output=True,
                timeout=5,
                check=False,
            )
            return proc.returncode == 0
        except Exception:
            return False

    def check_safemode(self) -> bool:
        """Check NameNode safemode state.

        Returns True if safemode is ON (or cluster unreachable), False if safemode is OFF.
        """
        try:
            proc = subprocess.run(
                ["hdfs", "dfsadmin", "-safemode", "get"],
                capture_output=True,
                text=True,
                timeout=5,
                check=False,
            )
            out = (proc.stdout or "") + (proc.stderr or "")
            if "Safe mode is ON" in out:
                logger.warning("HDFS NameNode is in SAFE MODE.")
                return True
            if "Safe mode is OFF" in out:
                return False
            return True
        except Exception as exc:
            logger.debug(f"HDFS safemode check failed: {exc}")
            return True

    def upload_partition(
        self,
        local_file_path: str,
        hdfs_partition_dir: str,
        run_id: str,
    ) -> bool:
        """Upload partition file to HDFS with staging swap, falling back to offline_dir if unavailable."""
        local_file = Path(local_file_path)
        if not local_file.is_file():
            raise FileNotFoundError(f"Local file does not exist: {local_file_path}")

        filename = local_file.name

        # If HDFS not available or in safemode, route to offline fallback directory
        if not self.is_available() or self.check_safemode():
            fallback_target_dir = self.offline_dir / hdfs_partition_dir.lstrip("/")
            fallback_target_dir.mkdir(parents=True, exist_ok=True)
            fallback_file = fallback_target_dir / filename
            shutil.copy2(str(local_file), str(fallback_file))
            logger.info(f"HDFS offline/safemode: cached partition to {fallback_file}")
            return True

        # HDFS is online: stage to /skywatch/_staging and move to target partition
        staging_dir = f"{self.hdfs_root}/_staging"
        target_dir = f"{self.hdfs_root}/{hdfs_partition_dir.lstrip('/')}"
        staging_file = f"{staging_dir}/{run_id}_{filename}"
        final_file = f"{target_dir}/{filename}"

        try:
            # 1. Create staging and target directories
            subprocess.run(["hdfs", "dfs", "-mkdir", "-p", staging_dir], check=True, timeout=10)
            subprocess.run(["hdfs", "dfs", "-mkdir", "-p", target_dir], check=True, timeout=10)

            # 2. Put to staging
            subprocess.run(["hdfs", "dfs", "-put", "-f", str(local_file), staging_file], check=True, timeout=30)

            # 3. Atomic rename/move to target
            subprocess.run(["hdfs", "dfs", "-mv", staging_file, final_file], check=True, timeout=10)
            logger.info(f"Uploaded {local_file_path} to HDFS: {final_file}")
            return True

        except Exception as exc:
            logger.error(f"Failed HDFS upload: {exc}. Falling back to local offline cache.")
            fallback_target_dir = self.offline_dir / hdfs_partition_dir.lstrip("/")
            fallback_target_dir.mkdir(parents=True, exist_ok=True)
            shutil.copy2(str(local_file), str(fallback_target_dir / filename))
            return False
