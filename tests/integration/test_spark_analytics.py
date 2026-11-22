"""Integration tests for Spark analytics pipeline and golden numerical verification."""
from __future__ import annotations

import csv
from pathlib import Path

import pytest

from skywatch.pipeline.spark_runner import is_spark_available, run_spark_analytics


def _assert_csv_matches_golden(actual_csv: Path, golden_csv: Path, tolerance: float = 0.02) -> None:
    assert actual_csv.is_file(), f"Output file does not exist: {actual_csv}"
    assert golden_csv.is_file(), f"Golden file does not exist: {golden_csv}"

    with open(actual_csv, "r", encoding="utf-8") as f_act, open(golden_csv, "r", encoding="utf-8") as f_gold:
        act_reader = csv.reader(f_act)
        gold_reader = csv.reader(f_gold)

        act_rows = list(act_reader)
        gold_rows = list(gold_reader)

    assert len(act_rows) == len(gold_rows), (
        f"Row count mismatch for {actual_csv.name}: actual={len(act_rows)}, golden={len(gold_rows)}"
    )

    if not act_rows:
        return

    # Compare header
    assert act_rows[0] == gold_rows[0], f"Header mismatch in {actual_csv.name}"

    header = act_rows[0]
    for r_idx, (act_row, gold_row) in enumerate(zip(act_rows[1:], gold_rows[1:]), start=2):
        assert len(act_row) == len(gold_row), f"Column count mismatch at row {r_idx}"
        for c_idx, (act_val, gold_val) in enumerate(zip(act_row, gold_row)):
            col_name = header[c_idx]
            if col_name == "alert_id":
                assert act_val, f"alert_id must not be empty in row {r_idx}"
                continue
            if act_val == gold_val:
                continue
            # Try float comparison
            try:
                act_f = float(act_val)
                gold_f = float(gold_val)
                diff = abs(act_f - gold_f)
                assert diff < tolerance, (
                    f"Numeric mismatch in {actual_csv.name} row {r_idx} col {c_idx}: "
                    f"actual={act_f}, golden={gold_f}, diff={diff}"
                )
            except (ValueError, TypeError):
                # If values differ and are not floats, fail
                pytest.fail(
                    f"String mismatch in {actual_csv.name} row {r_idx} col {c_idx}: "
                    f"actual='{act_val}', golden='{gold_val}'"
                )


def test_spark_analytics_e2e_matches_golden(tmp_path: Path):
    clean_path = Path("tests/fixtures/sample_air_quality.csv")
    weather_path = Path("tests/fixtures/sample_weather_normalized.csv")
    out_dir = tmp_path / "published"

    results = run_spark_analytics(
        input_clean_dir=clean_path,
        input_weather_dir=weather_path,
        output_published_dir=out_dir,
        force_simulation=True,
        run_id="golden_run_01",
        generated_at_utc="2026-11-23T00:00:00Z",
    )

    assert len(results) == 7
    assert all(r.success for r in results)

    golden_dir = Path("tests/fixtures/golden")
    job_names = [
        "station_statistics",
        "trends_hourly",
        "trends_daily",
        "weather_correlation",
        "cpcb_index",
        "alerts",
        "quality_report",
    ]

    for job in job_names:
        pub_job_dir = out_dir / job
        assert pub_job_dir.is_dir()

        csv_file = pub_job_dir / "csv" / "part-00000.csv"
        parquet_file = pub_job_dir / "parquet" / "part-00000.parquet"
        schema_file = pub_job_dir / "_schema.json"

        assert csv_file.is_file(), f"Missing CSV for {job}"
        assert parquet_file.is_file(), f"Missing Parquet for {job}"
        assert schema_file.is_file(), f"Missing _schema.json for {job}"

        golden_csv = golden_dir / f"golden_{job}.csv"
        _assert_csv_matches_golden(csv_file, golden_csv)


def test_is_spark_available_detection():
    # PySpark is installed, should return boolean without exception
    res = is_spark_available()
    assert isinstance(res, bool)
