"""Unit tests for Python Apache Pig runner and standalone simulation engine."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from skywatch.pipeline.pig_runner import (
    _pre_clear_output,
    _run_standalone_fallback,
    generate_sidecar_schema,
    is_pig_available,
    run_pig_script,
)


def test_is_pig_available(monkeypatch, tmp_path):
    """Test pig binary detection across environment configurations."""
    # When PIG_HOME contains pig binary
    mock_bin_dir = tmp_path / "bin"
    mock_bin_dir.mkdir(parents=True)
    mock_pig = mock_bin_dir / "pig"
    mock_pig.touch()

    monkeypatch.setenv("PIG_HOME", str(tmp_path))
    assert is_pig_available(str(tmp_path)) is True

    # When PIG_HOME does not contain pig binary and PATH is cleared
    empty_home = tmp_path / "empty"
    empty_home.mkdir()
    monkeypatch.setenv("PIG_HOME", str(empty_home))
    monkeypatch.setenv("PATH", "")
    assert is_pig_available(str(empty_home)) is False


def test_generate_sidecar_schema(tmp_path):
    """Test generation of _schema.json metadata file."""
    out_dir = tmp_path / "output_test"
    schema_path = generate_sidecar_schema("clean_air_quality", str(out_dir))

    assert Path(schema_path).is_file()
    with open(schema_path, "r", encoding="utf-8") as f:
        meta = json.load(f)

    assert meta["schema_version"] == "2.0"
    assert meta["entity"] == "clean_air_quality"
    assert meta["format"] == "csv"
    assert meta["has_header"] is False
    assert meta["delimiter"] == ","
    assert len(meta["columns"]) == 14
    assert meta["columns"][0] == {"name": "record_key", "type": "string"}
    assert meta["columns"][7] == {"name": "value", "type": "double"}


def test_pre_clear_output(tmp_path):
    """Test that existing directory is completely pre-cleared before execution."""
    target_dir = tmp_path / "to_clear"
    target_dir.mkdir()
    (target_dir / "old_part.csv").write_text("old_data", encoding="utf-8")

    assert target_dir.exists()
    assert (target_dir / "old_part.csv").exists()

    _pre_clear_output(str(target_dir))
    assert not target_dir.exists()

    # Should not raise error if directory does not exist
    _pre_clear_output(str(target_dir))


def test_run_pig_script_validation(tmp_path):
    """Test security and validation guards in run_pig_script."""
    # Path traversal detection in OUTPUT
    with pytest.raises(ValueError, match="Path traversal detected"):
        run_pig_script(
            script_path="pig/clean_air_quality.pig",
            params={"INPUT": "data/sample/sample_air_quality.csv", "OUTPUT": "../unsafe_dir"},
        )

    # Missing script file detection
    with pytest.raises(FileNotFoundError, match="Pig script does not exist"):
        run_pig_script(
            script_path="pig/non_existent.pig",
            params={"INPUT": "data/sample/sample_air_quality.csv", "OUTPUT": str(tmp_path / "out")},
        )

    # Missing OUTPUT parameter
    with pytest.raises(ValueError, match="Parameter 'OUTPUT' is required"):
        run_pig_script(
            script_path="pig/clean_air_quality.pig",
            params={"INPUT": "data/sample/sample_air_quality.csv"},
        )


def test_run_standalone_fallback_clean_air_quality(tmp_path):
    """Test standalone simulation fallback for clean_air_quality.pig."""
    out_dir = tmp_path / "clean_out"
    sample_csv = Path("tests/fixtures/sample_air_quality.csv")

    result = _run_standalone_fallback(
        script_path="pig/clean_air_quality.pig",
        params={"INPUT": str(sample_csv), "OUTPUT": str(out_dir)},
    )

    assert result.success is True
    assert result.returncode == 0
    assert result.records_processed == 720
    assert (out_dir / "part-m-00000").is_file()
    assert (out_dir / "_SUCCESS").is_file()
    assert (out_dir / "_schema.json").is_file()

    # Verify no header in output
    with open(out_dir / "part-m-00000", "r", encoding="utf-8") as f:
        first_line = f.readline().strip()
        assert not first_line.startswith("record_key,")


def test_run_standalone_fallback_station_stats(tmp_path):
    """Test standalone simulation fallback for station_stats.pig."""
    # First generate clean records
    clean_dir = tmp_path / "clean_out"
    sample_csv = Path("tests/fixtures/sample_air_quality.csv")
    _run_standalone_fallback("pig/clean_air_quality.pig", {"INPUT": str(sample_csv), "OUTPUT": str(clean_dir)})

    # Run station_stats
    stats_dir = tmp_path / "stats_out"
    result = _run_standalone_fallback(
        script_path="pig/station_stats.pig",
        params={"INPUT": str(clean_dir), "OUTPUT": str(stats_dir)},
    )

    assert result.success is True
    assert result.records_processed == 30  # 5 stations * 2 pollutants * 3 days
    assert (stats_dir / "part-m-00000").is_file()
    assert (stats_dir / "_schema.json").is_file()


def test_run_standalone_fallback_hourly_rollup(tmp_path):
    """Test standalone simulation fallback for hourly_rollup.pig."""
    # First generate clean records
    clean_dir = tmp_path / "clean_out"
    sample_csv = Path("tests/fixtures/sample_air_quality.csv")
    _run_standalone_fallback("pig/clean_air_quality.pig", {"INPUT": str(sample_csv), "OUTPUT": str(clean_dir)})

    # Run hourly_rollup
    rollup_dir = tmp_path / "rollup_out"
    result = _run_standalone_fallback(
        script_path="pig/hourly_rollup.pig",
        params={"INPUT": str(clean_dir), "OUTPUT": str(rollup_dir)},
    )

    assert result.success is True
    assert result.records_processed == 720  # 5 stations * 2 pollutants * 3 days * 24 hours
    assert (rollup_dir / "part-m-00000").is_file()
    assert (rollup_dir / "_schema.json").is_file()
