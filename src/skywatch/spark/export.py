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


def compute_station_freshness(
    stations: List[Dict[str, Any]],
    clean_records: Optional[List[Dict[str, Any]]] = None,
    reference_time_utc: Optional[Union[str, datetime]] = None,
) -> List[Dict[str, Any]]:
    """Enrich station metadata with last_measurement_time_utc and freshness_status."""
    from skywatch.analytics_core.freshness import freshness_status

    latest_by_station: Dict[str, str] = {}
    if clean_records:
        for r in clean_records:
            st_id = str(r.get("station_id", "")).strip()
            ts = r.get("measurement_time_utc")
            if st_id and ts:
                ts_str = str(ts).strip()
                if st_id not in latest_by_station or ts_str > latest_by_station[st_id]:
                    latest_by_station[st_id] = ts_str

    enriched: List[Dict[str, Any]] = []
    for st in stations:
        st_id = str(st.get("station_id", "")).strip()
        last_ts = latest_by_station.get(st_id) or st.get("last_active_utc")
        status = freshness_status(last_ts, reference_time_utc=reference_time_utc)
        params = (
            list(st.get("sensors", {}).keys())
            if isinstance(st.get("sensors"), dict)
            else ["pm25", "pm10"]
        )
        enriched.append({
            "station_id": st_id,
            "name": st.get("name", f"Station {st_id}"),
            "latitude": float(st.get("latitude", 0.0)),
            "longitude": float(st.get("longitude", 0.0)),
            "parameters": params,
            "last_measurement_time_utc": last_ts,
            "freshness_status": status,
        })
    return enriched


def export_static_api_endpoints(
    base_output_dir: Union[str, Path] = "output/published",
    run_id: Optional[str] = None,
    profile_name: str = "modern",
    engine_name: str = "STANDALONE_PYTHON_SIMULATION",
    generated_at_utc: Optional[str] = None,
    clean_records: Optional[List[Dict[str, Any]]] = None,
    stations_file: Union[str, Path] = "config/stations.json",
) -> Path:
    """Export static JSON responses for Vercel / static frontend consumption."""
    base_dir = Path(base_output_dir)
    api_dir = base_dir / "api" / "v1"
    api_dir.mkdir(parents=True, exist_ok=True)

    ts = generated_at_utc or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    rid = run_id or uuid.uuid4().hex[:12]

    # 1. meta.json
    meta = {
        "run_id": rid,
        "generated_at_utc": ts,
        "profile": profile_name,
        "engine": engine_name,
        "is_simulation": engine_name == "STANDALONE_PYTHON_SIMULATION",
    }
    with open(api_dir / "meta.json", "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2)

    # 2. stations.json
    st_path = Path(stations_file)
    stations: List[Dict[str, Any]] = []
    if st_path.is_file():
        try:
            with open(st_path, "r", encoding="utf-8") as f:
                stations = json.load(f)
        except Exception:
            stations = []

    enriched_stations = compute_station_freshness(stations, clean_records, reference_time_utc=ts)
    with open(api_dir / "stations.json", "w", encoding="utf-8") as f:
        json.dump(enriched_stations, f, indent=2)

    # 3. Export analytical tables from published directories
    datasets = [
        "cpcb_index",
        "station_statistics",
        "trends_hourly",
        "trends_daily",
        "weather_correlation",
        "alerts",
        "quality_report",
    ]

    for ds in datasets:
        csv_file = base_dir / ds / "csv" / "part-00000.csv"
        rows: List[Dict[str, Any]] = []
        if csv_file.is_file():
            with open(csv_file, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for r in reader:
                    typed_row: Dict[str, Any] = {}
                    for k, v in r.items():
                        if v is None or v == "":
                            typed_row[k] = None
                        elif v.lower() == "true":
                            typed_row[k] = True
                        elif v.lower() == "false":
                            typed_row[k] = False
                        else:
                            try:
                                if "." in v:
                                    typed_row[k] = float(v)
                                else:
                                    typed_row[k] = int(v)
                            except ValueError:
                                typed_row[k] = v

                    # Enhance trends_hourly with IST time range
                    if ds == "trends_hourly" and "hour_of_day" in typed_row:
                        h_utc = int(typed_row["hour_of_day"])
                        # IST is UTC + 5h30m
                        start_min = (h_utc * 60 + 330) % 1440
                        end_min = (h_utc * 60 + 390) % 1440
                        ist_start = f"{start_min // 60:02d}:{start_min % 60:02d}"
                        ist_end = f"{end_min // 60:02d}:{end_min % 60:02d}"
                        typed_row["hour_of_day_utc"] = h_utc
                        typed_row["ist_time_range"] = f"{ist_start} - {ist_end} IST"
                        typed_row["ist_label"] = f"{ist_start} IST"

                    rows.append(typed_row)

        with open(api_dir / f"{ds}.json", "w", encoding="utf-8") as f:
            json.dump(rows, f, indent=2)

    return api_dir
