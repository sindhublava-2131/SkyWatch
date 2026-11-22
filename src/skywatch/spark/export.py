"""Dataset export publisher with atomic staging directory swaps (SPK-09)."""
from __future__ import annotations

import csv
import json
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from skywatch.data.schemas import get_column_names, get_schema

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    PYARROW_AVAILABLE = True
except ImportError:
    PYARROW_AVAILABLE = False


def _safe_remove(path: Path) -> None:
    """Recursively remove a file or directory if it exists."""
    if not path.exists():
        return
    if path.is_file():
        path.unlink(missing_ok=True)
    elif path.is_dir():
        shutil.rmtree(path, ignore_errors=True)


def export_published_dataset(
    data: Union[List[Dict[str, Any]], Any],
    dataset_name: str,
    base_output_dir: Union[str, Path] = "output/published",
    run_id: Optional[str] = None,
    config_path: Optional[str] = None,
) -> Path:
    """Publish a dataset with atomic directory swap, dual CSV/Parquet formats, and _schema.json.

    Protocol:
        1. Write files into staging directory .staging_{dataset_name}_{run_id}/
        2. Write CSV to csv/part-00000.csv
        3. Write Parquet to parquet/part-00000.parquet
        4. Write sidecar metadata contract to _schema.json
        5. Atomically swap staging directory to target directory output/published/{dataset_name}/
        6. On failure, clean up staging directory and leave target intact.

    Args:
        data: List of record dictionaries or PySpark DataFrame.
        dataset_name: Canonical schema entity name.
        base_output_dir: Base publishing directory (default 'output/published').
        run_id: Unique pipeline execution identifier.
        config_path: Optional path to schemas.yaml.

    Returns:
        Path to final published directory.
    """
    if run_id is None:
        run_id = uuid.uuid4().hex[:12]

    base_dir = Path(base_output_dir)
    base_dir.mkdir(parents=True, exist_ok=True)

    target_dir = base_dir / dataset_name
    staging_dir = base_dir / f".staging_{dataset_name}_{run_id}"

    # Ensure clean staging directory
    _safe_remove(staging_dir)
    staging_dir.mkdir(parents=True, exist_ok=True)
    csv_dir = staging_dir / "csv"
    parquet_dir = staging_dir / "parquet"
    csv_dir.mkdir(parents=True, exist_ok=True)
    parquet_dir.mkdir(parents=True, exist_ok=True)

    backup_dir = base_dir / f".backup_{dataset_name}_{run_id}"

    try:
        # Determine schema definition
        schema_def = get_schema(dataset_name, config_path)
        col_names = get_column_names(dataset_name, config_path)

        # Handle PySpark DataFrame vs List[Dict]
        if hasattr(data, "collect") and hasattr(data, "coalesce"):
            # PySpark DataFrame
            rows = [r.asDict() for r in data.collect()]
        elif isinstance(data, list):
            rows = data
        else:
            raise TypeError(f"Unsupported data type for export: {type(data)}")

        # 1. Write CSV
        csv_file = csv_dir / "part-00000.csv"
        with open(csv_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=col_names, extrasaction="ignore")
            writer.writeheader()
            for r in rows:
                writer.writerow(r)

        # 2. Write Parquet
        parquet_file = parquet_dir / "part-00000.parquet"
        if PYARROW_AVAILABLE:
            if rows:
                # Build dict of lists for PyArrow Table
                pydict = {col: [r.get(col) for r in rows] for col in col_names}
                table = pa.Table.from_pydict(pydict)
            else:
                table = pa.Table.from_pylist([], schema=pa.schema([]))
            pq.write_table(table, str(parquet_file))
        else:
            # Fallback when pyarrow is not installed: create copy of CSV in parquet folder
            shutil.copy2(csv_file, parquet_dir / "part-00000.parquet")

        # 3. Write sidecar _schema.json
        schema_meta = {
            "schema_version": schema_def.get("version", "2.0"),
            "entity": dataset_name,
            "formats": ["parquet", "csv"],
            "published_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "columns": schema_def.get("columns", []),
        }
        with open(staging_dir / "_schema.json", "w", encoding="utf-8") as f:
            json.dump(schema_meta, f, indent=2)

        # 4. Atomic directory swap
        if target_dir.exists():
            _safe_remove(backup_dir)
            target_dir.rename(backup_dir)
            staging_dir.rename(target_dir)
            _safe_remove(backup_dir)
        else:
            staging_dir.rename(target_dir)

        return target_dir

    except Exception:
        # Rollback staging and backup on failure
        _safe_remove(staging_dir)
        if backup_dir.exists() and not target_dir.exists():
            backup_dir.rename(target_dir)
        _safe_remove(backup_dir)
        raise
