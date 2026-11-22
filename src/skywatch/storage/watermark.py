"""Per-sensor watermark state management with atomic two-phase commit.

Implements ING-05: Incremental collection tracking per-sensor last_measurement_utc
committed atomically with file renames to guarantee idempotent resumption.
"""
from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from skywatch.pipeline.logging import get_logger

logger = get_logger("watermark")


class WatermarkManager:
    """Manages incremental per-sensor high-water marks stored in data/.watermarks.json."""

    def __init__(self, watermark_path: str = "data/.watermarks.json") -> None:
        self.watermark_path = Path(watermark_path)
        self.state: Dict[str, Any] = {"updated_at_utc": None, "sensors": {}}
        self._load()

    def _load(self) -> None:
        """Load watermark state from disk, gracefully initializing empty state if absent or corrupt."""
        if not self.watermark_path.is_file():
            return

        try:
            with open(self.watermark_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict) and "sensors" in data:
                    self.state = data
        except Exception as exc:
            logger.warning(f"Error loading watermarks from {self.watermark_path}: {exc}. Starting fresh.")
            self.state = {"updated_at_utc": None, "sensors": {}}

    def get_watermark(self, sensor_id: str) -> Optional[str]:
        """Retrieve the last successfully processed UTC measurement timestamp for a sensor."""
        sensor_info = self.state.get("sensors", {}).get(str(sensor_id))
        if sensor_info and isinstance(sensor_info, dict):
            return sensor_info.get("last_measurement_utc")
        return None

    def update_watermark(
        self,
        sensor_id: str,
        station_id: str,
        parameter: str,
        last_measurement_utc: str,
        records_ingested: int,
        run_id: str,
    ) -> None:
        """Update in-memory watermark for a sensor without writing to disk yet."""
        if "sensors" not in self.state:
            self.state["sensors"] = {}

        self.state["sensors"][str(sensor_id)] = {
            "parameter": parameter,
            "station_id": str(station_id),
            "last_measurement_utc": last_measurement_utc,
            "last_run_id": run_id,
            "records_ingested": records_ingested,
        }

    def commit(self, run_id: str) -> None:
        """Two-phase commit: atomically persist updated watermark state using temporary file swap."""
        now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        self.state["updated_at_utc"] = now_utc

        self.watermark_path.parent.mkdir(parents=True, exist_ok=True)
        content = json.dumps(self.state, indent=2)

        # Write to temporary file in the same directory with cleanup on failure (WR-22)
        tf = tempfile.NamedTemporaryFile(
            "w",
            dir=str(self.watermark_path.parent),
            prefix=f"{self.watermark_path.name}.",
            suffix=f".tmp.{run_id}",
            delete=False,
            encoding="utf-8",
        )
        temp_name = tf.name
        try:
            tf.write(content)
            tf.flush()
            os.fsync(tf.fileno())
            tf.close()
            os.replace(temp_name, str(self.watermark_path))
        except Exception:
            if os.path.exists(temp_name):
                os.unlink(temp_name)
            raise

        logger.info(f"Committed watermarks for {len(self.state.get('sensors', {}))} sensors (run_id: {run_id})")
