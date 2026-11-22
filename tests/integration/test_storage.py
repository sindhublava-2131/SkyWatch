"""Integration tests for local storage and HDFS storage adapter."""
from __future__ import annotations

import csv
from pathlib import Path
from unittest import mock

from skywatch.storage.hdfs_storage import HDFSStorageAdapter
from skywatch.storage.local_storage import atomic_write_file, write_normalized_csv


def test_atomic_write_file(tmp_path):
    target = tmp_path / "test.txt"
    written = atomic_write_file(str(target), "Hello World", run_id="r1")
    assert Path(written).is_file()
    assert Path(written).read_text(encoding="utf-8") == "Hello World"


def test_write_normalized_csv(tmp_path):
    records = [
        {
            "record_key": "k1",
            "station_id": "2178",
            "station_name": "BTM Layout",
            "latitude": 12.91,
            "longitude": 77.61,
            "sensor_id": "3917",
            "parameter": "pm25",
            "value": 45.0,
            "unit": "ug/m3",
            "measurement_time_utc": "2026-11-22T08:00:00Z",
            "ingestion_time_utc": "2026-11-22T08:05:00Z",
            "source": "openaq",
            "is_valid": True,
            "reject_reason": "",
        }
    ]
    base_dir = tmp_path / "normalized"
    csv_path = write_normalized_csv(
        records=records,
        stream_name="air_quality",
        ingest_dt="2026-11-22",
        run_id="run-123",
        base_dir=str(base_dir),
    )

    file_obj = Path(csv_path)
    assert file_obj.is_file()
    assert "ingest_dt=2026-11-22" in str(file_obj)

    with open(file_obj, "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 1
    assert rows[0]["record_key"] == "k1"
    assert rows[0]["parameter"] == "pm25"


def test_hdfs_adapter_offline_fallback(tmp_path):
    src_file = tmp_path / "sample.csv"
    src_file.write_text("data,value\n1,2", encoding="utf-8")

    offline_dir = tmp_path / "local_hdfs"
    adapter = HDFSStorageAdapter(hdfs_root="/skywatch", offline_dir=str(offline_dir))

    # Mock HDFS unavailable
    with mock.patch.object(adapter, "is_available", return_value=False):
        success = adapter.upload_partition(str(src_file), "normalized/air_quality/ingest_dt=2026-11-22", "run-1")

    assert success is True
    cached_file = offline_dir / "normalized/air_quality/ingest_dt=2026-11-22/sample.csv"
    assert cached_file.is_file()
    assert cached_file.read_text(encoding="utf-8") == "data,value\n1,2"
