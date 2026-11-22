"""Unit tests for atomic export publisher with staging swaps and rollback safety (SPK-09)."""
from __future__ import annotations

import csv
import json
from pathlib import Path
from unittest.mock import patch

import pytest

from skywatch.spark.export import export_published_dataset


def test_export_published_dataset_success(tmp_path: Path):
    sample_data = [
        {
            "station_id": "2178",
            "station_name": "BTM Layout",
            "parameter": "pm25",
            "reading_count": 100,
            "average_value": 35.5,
            "minimum_value": 15.0,
            "maximum_value": 75.0,
            "pollution_rank": 1,
            "generated_at_utc": "2026-11-23T00:00:00Z",
        },
        {
            "station_id": "2179",
            "station_name": "Silk Board",
            "parameter": "pm25",
            "reading_count": 100,
            "average_value": 45.0,
            "minimum_value": 20.0,
            "maximum_value": 90.0,
            "pollution_rank": 2,
            "generated_at_utc": "2026-11-23T00:00:00Z",
        },
    ]

    target_dir = export_published_dataset(
        data=sample_data,
        dataset_name="station_statistics",
        base_output_dir=tmp_path,
        run_id="test_run_01",
    )

    assert target_dir.is_dir()
    csv_file = target_dir / "csv" / "part-00000.csv"
    parquet_file = target_dir / "parquet" / "part-00000.parquet"
    schema_file = target_dir / "_schema.json"

    assert csv_file.is_file()
    assert parquet_file.is_file()
    assert schema_file.is_file()

    # Verify CSV content
    with open(csv_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        assert len(rows) == 2
        assert rows[0]["station_id"] == "2178"
        assert rows[0]["pollution_rank"] == "1"

    # Verify _schema.json content
    with open(schema_file, "r", encoding="utf-8") as f:
        schema_meta = json.load(f)
        assert schema_meta["entity"] == "station_statistics"
        assert "parquet" in schema_meta["formats"]
        assert "csv" in schema_meta["formats"]
        assert len(schema_meta["columns"]) == 9


def test_export_published_dataset_atomic_overwrite(tmp_path: Path):
    initial_data = [
        {
            "station_id": "2178",
            "station_name": "BTM Layout",
            "parameter": "pm25",
            "reading_count": 10,
            "average_value": 30.0,
            "minimum_value": 20.0,
            "maximum_value": 40.0,
            "pollution_rank": 1,
            "generated_at_utc": "2026-11-23T00:00:00Z",
        }
    ]
    target_dir = export_published_dataset(
        data=initial_data,
        dataset_name="station_statistics",
        base_output_dir=tmp_path,
        run_id="run_v1",
    )
    assert target_dir.is_dir()

    # Overwrite with new data
    updated_data = [
        {
            "station_id": "2178",
            "station_name": "BTM Layout",
            "parameter": "pm25",
            "reading_count": 20,
            "average_value": 35.0,
            "minimum_value": 20.0,
            "maximum_value": 50.0,
            "pollution_rank": 1,
            "generated_at_utc": "2026-11-23T01:00:00Z",
        }
    ]
    target_dir2 = export_published_dataset(
        data=updated_data,
        dataset_name="station_statistics",
        base_output_dir=tmp_path,
        run_id="run_v2",
    )

    with open(target_dir2 / "csv" / "part-00000.csv", "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        assert len(rows) == 1
        assert rows[0]["reading_count"] == "20"


def test_export_published_dataset_rollback_on_error(tmp_path: Path):
    initial_data = [
        {
            "station_id": "2178",
            "station_name": "BTM Layout",
            "parameter": "pm25",
            "reading_count": 10,
            "average_value": 30.0,
            "minimum_value": 20.0,
            "maximum_value": 40.0,
            "pollution_rank": 1,
            "generated_at_utc": "2026-11-23T00:00:00Z",
        }
    ]
    target_dir = export_published_dataset(
        data=initial_data,
        dataset_name="station_statistics",
        base_output_dir=tmp_path,
        run_id="run_initial",
    )
    assert target_dir.is_dir()

    # Simulate failure during parquet writing
    bad_data = [{"corrupted": "bad"}]
    with patch("pyarrow.parquet.write_table", side_effect=RuntimeError("Disk write error")):
        with pytest.raises(RuntimeError, match="Disk write error"):
            export_published_dataset(
                data=bad_data,
                dataset_name="station_statistics",
                base_output_dir=tmp_path,
                run_id="run_failed",
            )

    # Confirm original target directory is intact and staging dir was cleaned up
    assert target_dir.is_dir()
    staging_dirs = list(tmp_path.glob(".staging_*"))
    assert len(staging_dirs) == 0

    with open(target_dir / "csv" / "part-00000.csv", "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        assert len(rows) == 1
        assert rows[0]["reading_count"] == "10"
