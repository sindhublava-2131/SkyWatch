"""Integration tests for end-to-end ingestion orchestrator and backfill."""
from __future__ import annotations

import csv
from pathlib import Path

from skywatch.collector.collector import IngestionOrchestrator
from skywatch.collector.openaq_client import OpenAQClient
from skywatch.collector.weather_client import WeatherClient
from skywatch.storage.hdfs_storage import HDFSStorageAdapter
from skywatch.storage.watermark import WatermarkManager


def test_collector_e2e_offline(tmp_path):
    raw_dir = tmp_path / "raw"
    normalized_dir = tmp_path / "normalized"
    local_hdfs = tmp_path / "local_hdfs"
    wm_file = tmp_path / ".watermarks.json"
    cp_file = tmp_path / ".backfill_checkpoint.json"
    stations_file = tmp_path / "stations.json"

    openaq = OpenAQClient(offline=True)
    weather = WeatherClient(offline=True)
    wm = WatermarkManager(str(wm_file))
    hdfs = HDFSStorageAdapter(hdfs_root="/skywatch", offline_dir=str(local_hdfs))

    orchestrator = IngestionOrchestrator(
        openaq_client=openaq,
        weather_client=weather,
        watermark_manager=wm,
        hdfs_adapter=hdfs,
        stations_path=str(stations_file),
        raw_dir=str(raw_dir),
        normalized_dir=str(normalized_dir),
        checkpoint_path=str(cp_file),
        offline=True,
    )

    # 1. Run latest collection
    summary = orchestrator.run_incremental_collection(run_id="run-e2e-1", mode="latest")

    assert summary["stations_polled"] >= 5
    assert summary["air_quality_records"] > 0
    assert summary["weather_records"] > 0

    # Verify raw manifest exists and has entries
    manifest_file = raw_dir / "_manifest.csv"
    assert manifest_file.is_file()
    with open(manifest_file, "r", encoding="utf-8") as f:
        manifest_rows = list(csv.DictReader(f))
    assert len(manifest_rows) >= 2  # openaq + weather

    # Verify normalized files exist on disk
    aq_file = Path(summary["air_quality_csv"])
    assert aq_file.is_file()
    with open(aq_file, "r", encoding="utf-8") as f:
        aq_rows = list(csv.DictReader(f))
    assert len(aq_rows) == summary["air_quality_records"]

    # Verify watermarks committed
    assert wm_file.is_file()

    # 2. Run chunked backfill
    bf_summary = orchestrator.run_chunked_backfill(
        start_date="2026-11-01",
        end_date="2026-11-15",
        chunk_days=7,
        run_id="run-bf-1",
    )
    assert bf_summary["chunks_processed"] == 2
    assert cp_file.is_file()

    # Re-running backfill should skip already completed chunks
    bf_summary2 = orchestrator.run_chunked_backfill(
        start_date="2026-11-01",
        end_date="2026-11-15",
        chunk_days=7,
        run_id="run-bf-2",
    )
    assert bf_summary2["chunks_processed"] == 0
