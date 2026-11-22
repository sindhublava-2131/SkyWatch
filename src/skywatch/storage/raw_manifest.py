"""Preservation of unmodified raw API responses with audit manifest ledger.

Implements ING-04: Store untouched API responses with audit metadata (run_id,
SHA-256 digest, record count, endpoint, status code) before parsing or cleaning.
"""
from __future__ import annotations

import csv
import hashlib
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Union

from skywatch.pipeline.logging import get_logger

logger = get_logger("raw_manifest")

MANIFEST_HEADERS = [
    "run_id",
    "file_path",
    "source",
    "endpoint_url",
    "http_status",
    "sha256",
    "record_count",
    "collected_at_utc",
]


def save_raw_response(
    raw_data: Union[Dict[str, Any], str],
    source: str,
    endpoint_url: str,
    http_status: int,
    run_id: str,
    base_dir: str = "data/raw",
    ingest_dt: Optional[str] = None,
) -> Dict[str, Any]:
    """Atomically save raw API response to disk and append an entry to _manifest.csv."""
    now_utc = datetime.now(timezone.utc)
    collected_at_utc = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    partition_dt = ingest_dt or now_utc.strftime("%Y-%m-%d")

    # Serialize content if dictionary
    if isinstance(raw_data, (dict, list)):
        content_str = json.dumps(raw_data, indent=2)
    else:
        content_str = str(raw_data)

    content_bytes = content_str.encode("utf-8")
    sha256_hash = hashlib.sha256(content_bytes).hexdigest()

    # Determine record count
    record_count = 1
    if isinstance(raw_data, dict):
        if "results" in raw_data and isinstance(raw_data["results"], list):
            record_count = len(raw_data["results"])
        elif "hourly" in raw_data and isinstance(raw_data["hourly"], dict):
            times = raw_data["hourly"].get("time", [])
            record_count = len(times)

    # Format directory: data/raw/{source}/ingest_dt=YYYY-MM-DD/
    partition_dir = Path(base_dir) / source / f"ingest_dt={partition_dt}"
    partition_dir.mkdir(parents=True, exist_ok=True)

    # Unique per-payload filename preventing multi-station overwrites within the same run (CR-02)
    target_file = partition_dir / f"{source}_{run_id}_{sha256_hash[:8]}.json"

    # Atomic write to temporary file with cleanup on failure (WR-03)
    tf = tempfile.NamedTemporaryFile("wb", dir=str(partition_dir), delete=False)
    temp_path = tf.name
    try:
        tf.write(content_bytes)
        tf.flush()
        os.fsync(tf.fileno())
        tf.close()
        os.replace(temp_path, str(target_file))
    except Exception:
        if os.path.exists(temp_path):
            os.unlink(temp_path)
        raise

    manifest_path = Path(base_dir) / "_manifest.csv"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)

    # Normalize relative path for manifest
    file_rel_path = str(target_file).replace("\\", "/")

    manifest_entry = {
        "run_id": run_id,
        "file_path": file_rel_path,
        "source": source,
        "endpoint_url": endpoint_url,
        "http_status": http_status,
        "sha256": sha256_hash,
        "record_count": record_count,
        "collected_at_utc": collected_at_utc,
    }

    # Append to manifest atomically with file lock / creation check
    write_header = not manifest_path.is_file()
    with open(manifest_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=MANIFEST_HEADERS)
        if write_header:
            writer.writeheader()
        writer.writerow(manifest_entry)

    logger.info(f"Preserved raw response to {file_rel_path} with SHA256={sha256_hash[:8]}...")
    return manifest_entry
