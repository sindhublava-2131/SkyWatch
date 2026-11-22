"""Integration tests for synthetic data generation and scale benchmarking (SPK-10)."""
from __future__ import annotations

import csv
from pathlib import Path

from benchmarks.run_scale_benchmark import benchmark_scale, format_markdown_report
from scripts.make_synthetic import generate_synthetic_dataset
from skywatch.data.schemas import get_column_names, validate_record_dict


def test_make_synthetic_generator(tmp_path: Path):
    target_rows = 5000
    syn_csv = generate_synthetic_dataset(
        scale="1x",
        target_row_count=target_rows,
        output_dir=tmp_path,
        seed=42,
    )

    assert syn_csv.is_file()

    expected_cols = get_column_names("air_quality")
    with open(syn_csv, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        assert reader.fieldnames == expected_cols

        count = 0
        for row in reader:
            count += 1
            assert row["source"] == "synthetic"
            assert row["record_key"].startswith("syn_1x_")
            assert row["is_valid"] == "true"

            # Validate against canonical contract
            row_typed = dict(row)
            row_typed["latitude"] = float(row["latitude"])
            row_typed["longitude"] = float(row["longitude"])
            row_typed["value"] = float(row["value"])
            row_typed["is_valid"] = True
            valid, err = validate_record_dict(row_typed, "air_quality")
            assert valid, f"Record failed schema validation: {err}"

        assert count == target_rows


def test_scale_benchmark_quick(tmp_path: Path):
    work_dir = tmp_path / "bench_work"
    metrics = benchmark_scale(
        scale="test_quick",
        row_count=1000,
        work_dir=work_dir,
    )

    assert metrics["scale"] == "test_quick"
    assert metrics["row_count"] == 1000
    assert metrics["duration_seconds"] > 0
    assert metrics["throughput_rows_per_sec"] > 0
    assert metrics["peak_memory_mb"] > 0
    assert metrics["status"] == "PASS"


def test_format_markdown_report(tmp_path: Path):
    report_file = tmp_path / "results.md"
    sample_metrics = [
        {
            "scale": "1x",
            "row_count": 50000,
            "duration_seconds": 2.45,
            "throughput_rows_per_sec": 20408.2,
            "peak_memory_mb": 145.2,
            "status": "PASS",
        }
    ]

    report = format_markdown_report(sample_metrics, report_file)
    assert report_file.is_file()
    assert "SkyWatch Scale Benchmark Results" in report
    assert "20,408.2" in report
    assert "Status: VERIFIED" in report
