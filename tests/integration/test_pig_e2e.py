"""End-to-end integration tests for Apache Pig ETL pipelines and golden alignment."""
from __future__ import annotations

import csv
import json
import time
from pathlib import Path

from skywatch.pipeline.pig_runner import run_pig_script


def _read_csv_rows(path: Path):
    with open(path, "r", encoding="utf-8", newline="") as f:
        return list(csv.reader(f))


def test_clean_air_quality_e2e_matches_golden(tmp_path):
    """Verify clean_air_quality ETL produces byte-identical or numeric-identical results to golden dataset."""
    sample_input = Path("tests/fixtures/sample_air_quality.csv")
    out_dir = tmp_path / "clean_air_quality"

    result = run_pig_script(
        script_path="pig/clean_air_quality.pig",
        params={"INPUT": str(sample_input), "OUTPUT": str(out_dir)},
        offline_fallback=True,
    )

    assert result.success is True
    assert (out_dir / "part-m-00000").is_file()
    assert (out_dir / "_schema.json").is_file()

    actual_rows = _read_csv_rows(out_dir / "part-m-00000")
    golden_rows = _read_csv_rows(Path("tests/fixtures/golden/golden_clean_air_quality.csv"))

    assert len(actual_rows) == len(golden_rows)
    assert sorted(actual_rows) == sorted(golden_rows)

    # Verify sidecar schema matches golden sidecar
    with open(out_dir / "_schema.json", "r", encoding="utf-8") as f:
        actual_schema = json.load(f)
    with open("tests/fixtures/golden/golden_clean_air_quality_schema.json", "r", encoding="utf-8") as f:
        golden_schema = json.load(f)

    assert actual_schema == golden_schema


def test_station_stats_e2e_matches_golden(tmp_path):
    """Verify station_stats ETL produces exact matching aggregations against golden dataset."""
    sample_input = Path("tests/fixtures/sample_air_quality.csv")
    clean_dir = tmp_path / "clean_air_quality"
    stats_dir = tmp_path / "station_stats"

    run_pig_script("pig/clean_air_quality.pig", {"INPUT": str(sample_input), "OUTPUT": str(clean_dir)})

    result = run_pig_script(
        script_path="pig/station_stats.pig",
        params={"INPUT": str(clean_dir), "OUTPUT": str(stats_dir)},
        offline_fallback=True,
    )

    assert result.success is True
    assert (stats_dir / "part-m-00000").is_file()

    actual_rows = _read_csv_rows(stats_dir / "part-m-00000")
    golden_rows = _read_csv_rows(Path("tests/fixtures/golden/golden_station_stats.csv"))

    assert len(actual_rows) == len(golden_rows)
    for act, gld in zip(actual_rows, golden_rows):
        # station_id, parameter, date, count, avg, min, max
        assert act[0] == gld[0]  # station_id
        assert act[1] == gld[1]  # parameter
        assert act[2] == gld[2]  # date
        assert int(act[3]) == int(gld[3])  # reading_count
        assert abs(float(act[4]) - float(gld[4])) < 1e-4  # average_value
        assert abs(float(act[5]) - float(gld[5])) < 1e-4  # minimum_value
        assert abs(float(act[6]) - float(gld[6])) < 1e-4  # maximum_value


def test_hourly_rollup_e2e_matches_golden(tmp_path):
    """Verify hourly_rollup ETL produces exact regularized hourly buckets against golden dataset."""
    sample_input = Path("tests/fixtures/sample_air_quality.csv")
    clean_dir = tmp_path / "clean_air_quality"
    rollup_dir = tmp_path / "hourly_rollup"

    run_pig_script("pig/clean_air_quality.pig", {"INPUT": str(sample_input), "OUTPUT": str(clean_dir)})

    result = run_pig_script(
        script_path="pig/hourly_rollup.pig",
        params={"INPUT": str(clean_dir), "OUTPUT": str(rollup_dir)},
        offline_fallback=True,
    )

    assert result.success is True
    assert (rollup_dir / "part-m-00000").is_file()

    actual_rows = _read_csv_rows(rollup_dir / "part-m-00000")
    golden_rows = _read_csv_rows(Path("tests/fixtures/golden/golden_hourly_rollup.csv"))

    assert len(actual_rows) == len(golden_rows)
    for act, gld in zip(actual_rows, golden_rows):
        assert act[0] == gld[0]  # station_id
        assert act[1] == gld[1]  # parameter
        assert act[2] == gld[2]  # bucket_start_utc
        assert int(act[3]) == int(gld[3])  # reading_count
        assert abs(float(act[4]) - float(gld[4])) < 1e-4  # average_value
        assert abs(float(act[5]) - float(gld[5])) < 1e-4  # minimum_value
        assert abs(float(act[6]) - float(gld[6])) < 1e-4  # maximum_value


def test_pig_e2e_performance_budget(tmp_path):
    """Verify execution of all three ETL scripts completes well under the 90-second SLA (NFR-04)."""
    sample_input = Path("tests/fixtures/sample_air_quality.csv")
    clean_dir = tmp_path / "perf_clean"
    stats_dir = tmp_path / "perf_stats"
    rollup_dir = tmp_path / "perf_rollup"

    start_total = time.monotonic()

    r1 = run_pig_script("pig/clean_air_quality.pig", {"INPUT": str(sample_input), "OUTPUT": str(clean_dir)})
    assert r1.success is True

    r2 = run_pig_script("pig/station_stats.pig", {"INPUT": str(clean_dir), "OUTPUT": str(stats_dir)})
    assert r2.success is True

    r3 = run_pig_script("pig/hourly_rollup.pig", {"INPUT": str(clean_dir), "OUTPUT": str(rollup_dir)})
    assert r3.success is True

    total_duration = time.monotonic() - start_total
    assert total_duration < 90.0, f"Total execution time {total_duration:.2f}s exceeded 90.0s SLA"
    print(f"\nTotal ETL pipeline runtime: {total_duration:.3f}s (well within 90s SLA)")
