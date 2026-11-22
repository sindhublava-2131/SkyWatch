"""Atomic local storage writer with partitioned layout support.

Implements HYG-05 & HYG-06: Atomic write protocol (flush, fsync, os.replace)
and partitioned dataset directory layout: /normalized/{stream}/ingest_dt=YYYY-MM-DD/.
"""
from __future__ import annotations

import csv
import io
import os
import tempfile
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from skywatch.data.schemas import get_column_names
from skywatch.pipeline.logging import get_logger

logger = get_logger("local_storage")


def atomic_write_file(
    target_path: str,
    content: Union[str, bytes],
    run_id: Optional[str] = None,
) -> str:
    """Write string or byte content to disk using atomic temporary write and os.replace."""
    target = Path(target_path)
    target.parent.mkdir(parents=True, exist_ok=True)

    rid = run_id or uuid.uuid4().hex
    temp_suffix = f".tmp.{rid}"

    is_binary = isinstance(content, bytes)
    mode = "wb" if is_binary else "w"
    encoding = None if is_binary else "utf-8"

    # Write to temp file in the same directory to ensure same filesystem for os.replace with cleanup (WR-21)
    tf = tempfile.NamedTemporaryFile(
        mode,
        dir=str(target.parent),
        prefix=f"{target.name}.",
        suffix=temp_suffix,
        delete=False,
        encoding=encoding,
    )
    temp_name = tf.name
    try:
        tf.write(content)
        tf.flush()
        os.fsync(tf.fileno())
        tf.close()
        os.replace(temp_name, str(target))
    except Exception:
        if os.path.exists(temp_name):
            os.unlink(temp_name)
        raise

    return str(target)


def write_normalized_csv(
    records: List[Dict[str, Any]],
    stream_name: str,
    ingest_dt: str,
    run_id: str,
    base_dir: str = "data/normalized",
) -> str:
    """Serialize normalized records to partitioned CSV atomically."""
    columns = get_column_names(stream_name)

    partition_dir = Path(base_dir) / stream_name / f"ingest_dt={ingest_dt}"
    partition_dir.mkdir(parents=True, exist_ok=True)

    target_file = partition_dir / f"{stream_name}_{run_id}.csv"

    # Build CSV content in memory
    output_buffer = io.StringIO()
    writer = csv.DictWriter(output_buffer, fieldnames=columns, extrasaction="ignore")
    writer.writeheader()
    for rec in records:
        writer.writerow(rec)

    csv_content = output_buffer.getvalue()
    result_path = atomic_write_file(str(target_file), csv_content, run_id=run_id)
    logger.info(f"Wrote {len(records)} rows to {result_path}")
    return result_path
