"""Unit tests for raw payload preservation and manifest ledger."""
from __future__ import annotations

import csv
import hashlib
from pathlib import Path

from skywatch.storage.raw_manifest import save_raw_response


def test_save_raw_response(tmp_path):
    raw_data = {
        "results": [
            {"id": 1, "value": 45.0},
            {"id": 2, "value": 50.0},
        ]
    }
    base_dir = tmp_path / "raw"
    entry = save_raw_response(
        raw_data=raw_data,
        source="openaq",
        endpoint_url="https://api.openaq.org/v3/locations/2178/latest",
        http_status=200,
        run_id="run-test-abc",
        base_dir=str(base_dir),
        ingest_dt="2026-11-22",
    )

    # Verify return dictionary
    assert entry["run_id"] == "run-test-abc"
    assert entry["source"] == "openaq"
    assert entry["http_status"] == 200
    assert entry["record_count"] == 2

    # Verify physical file existence
    saved_file = Path(entry["file_path"])
    assert saved_file.is_file()
    content = saved_file.read_bytes()
    expected_sha = hashlib.sha256(content).hexdigest()
    assert entry["sha256"] == expected_sha

    # Verify manifest entry
    manifest_file = base_dir / "_manifest.csv"
    assert manifest_file.is_file()

    with open(manifest_file, "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 1
    assert rows[0]["run_id"] == "run-test-abc"
    assert rows[0]["sha256"] == expected_sha
    assert rows[0]["record_count"] == "2"
