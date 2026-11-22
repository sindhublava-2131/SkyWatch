"""Integration tests for per-sensor watermark state management."""
from __future__ import annotations

import json

from skywatch.storage.watermark import WatermarkManager


def test_watermark_lifecycle_and_two_phase_commit(tmp_path):
    wm_file = tmp_path / ".watermarks.json"
    wm = WatermarkManager(str(wm_file))

    assert wm.get_watermark("3917") is None

    wm.update_watermark(
        sensor_id="3917",
        station_id="2178",
        parameter="pm25",
        last_measurement_utc="2026-11-22T08:00:00Z",
        records_ingested=50,
        run_id="run-1",
    )

    # In-memory updated, but disk file not yet created prior to commit
    assert wm_file.is_file() is False

    # Commit phase
    wm.commit(run_id="run-1")
    assert wm_file.is_file() is True

    # Reload from disk in a fresh instance
    wm2 = WatermarkManager(str(wm_file))
    assert wm2.get_watermark("3917") == "2026-11-22T08:00:00Z"


def test_corrupted_watermark_recovery(tmp_path):
    wm_file = tmp_path / ".watermarks.json"
    wm_file.write_text("{corrupt-json...", encoding="utf-8")

    wm = WatermarkManager(str(wm_file))
    assert wm.get_watermark("any") is None

    # Should commit cleanly overwriting corrupted file
    wm.update_watermark("1", "2", "pm25", "2026-11-22T09:00:00Z", 10, "r2")
    wm.commit("r2")

    with open(wm_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    assert "sensors" in data
    assert "1" in data["sensors"]
