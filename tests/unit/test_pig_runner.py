"""Unit tests for Python Apache Pig runner and standalone simulation engine."""
from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from skywatch.pipeline.pig_runner import (
    _attach_sidecar,
    _pre_clear_output,
    _resolve_input_files,
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


def test_pre_clear_output_safety():
    """Test safety guards in _pre_clear_output refusing dangerous paths."""
    with pytest.raises(ValueError, match="Cannot pre-clear empty"):
        _pre_clear_output("")
    with pytest.raises(ValueError, match="Cannot pre-clear empty"):
        _pre_clear_output("   ")
    with pytest.raises(ValueError, match="Refusing to pre-clear dangerous directory"):
        _pre_clear_output(".")
    with pytest.raises(ValueError, match="Refusing to pre-clear dangerous directory"):
        _pre_clear_output(str(Path.cwd()))
    with pytest.raises(ValueError, match="Refusing to pre-clear dangerous directory"):
        _pre_clear_output(str(Path.cwd().anchor))


def test_run_pig_script_absolute_path_traversal():
    """Test that arbitrary external absolute paths are rejected."""
    external_path = "C:\\Windows\\System32" if os.name == "nt" else "/etc/data"
    with pytest.raises(ValueError, match="OUTPUT path must resolve within repository root or temp directory"):
        run_pig_script(
            script_path="pig/clean_air_quality.pig",
            params={"INPUT": "data/sample/sample_air_quality.csv", "OUTPUT": external_path},
        )


def test_resolve_input_files_glob_directories(tmp_path):
    """Test that _resolve_input_files properly handles partition subdirectories matched by glob."""
    part1 = tmp_path / "part_1"
    part1.mkdir()
    (part1 / "data1.csv").write_text("a,b,c", encoding="utf-8")
    part2 = tmp_path / "part_2"
    part2.mkdir()
    (part2 / "data2.csv").write_text("d,e,f", encoding="utf-8")

    resolved = _resolve_input_files(str(tmp_path / "part_*"))
    assert len(resolved) == 2
    filenames = {p.name for p in resolved}
    assert filenames == {"data1.csv", "data2.csv"}


def test_attach_sidecar_variations(tmp_path):
    """Test substring matching and entity override in _attach_sidecar."""
    out_dir = tmp_path / "custom_out"
    out_dir.mkdir()

    # Substring matching
    schema_path = _attach_sidecar("custom_clean_air_quality_v2.pig", str(out_dir))
    assert schema_path is not None
    assert Path(schema_path).is_file()

    # Explicit entity override
    schema_path_override = _attach_sidecar("unknown_script.pig", str(out_dir), entity_override="station_stats")
    assert schema_path_override is not None
    with open(schema_path_override, "r", encoding="utf-8") as f:
        meta = json.load(f)
    assert meta["entity"] == "station_stats"

    # Unknown script without override returns None
    assert _attach_sidecar("unknown_script.pig", str(out_dir)) is None


def test_pig_runner_require_real_engine_raises(tmp_path, monkeypatch):
    """Test that pig_runner raises RuntimeError when real engine is required but Pig is absent."""
    empty_home = tmp_path / "empty_pig"
    empty_home.mkdir()
    monkeypatch.setenv("PIG_HOME", str(empty_home))
    monkeypatch.setenv("PATH", "")

    input_file = Path("tests/fixtures/sample_air_quality.csv")
    out_dir = tmp_path / "clean_fail"

    with pytest.raises(RuntimeError, match="Apache Pig executable not found"):
        run_pig_script(
            script_path="pig/clean_air_quality.pig",
            params={"INPUT": str(input_file), "OUTPUT": str(out_dir)},
            require_real_engine=True,
        )


def test_pig_runner_engine_tag_in_result(tmp_path):
    """Test that pig_runner tags result with engine type."""
    input_file = Path("tests/fixtures/sample_air_quality.csv")
    out_dir = tmp_path / "clean_tag"

    result = run_pig_script(
        script_path="pig/clean_air_quality.pig",
        params={"INPUT": str(input_file), "OUTPUT": str(out_dir)},
        require_real_engine=False,
    )

    assert result.engine in ("APACHE_PIG", "STANDALONE_PYTHON_SIMULATION")
    assert result.success is True


