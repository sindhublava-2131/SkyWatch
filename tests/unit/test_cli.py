"""Unit tests for unified CLI (OPS-05)."""
from __future__ import annotations

from pathlib import Path

from skywatch.cli import build_parser, main


def test_build_parser_subcommands():
    parser = build_parser()
    subparsers_actions = [
        action for action in parser._actions if action.dest == "subcommand"
    ]
    assert len(subparsers_actions) == 1
    choices = subparsers_actions[0].choices
    for expected in ["discover", "collect", "backfill", "pipeline", "export", "smoke"]:
        assert expected in choices, f"Missing subcommand {expected}"


def test_cli_help(capsys):
    import pytest
    with pytest.raises(SystemExit) as exc_info:
        main(["--help"])
    assert exc_info.value.code == 0
    captured = capsys.readouterr()
    assert "SkyWatch: Big Data Environmental Analytics Platform for Bengaluru" in captured.out


def test_cli_pipeline_offline(tmp_path: Path):
    clean_dir = tmp_path / "clean"
    published_dir = tmp_path / "published"
    ret = main([
        "pipeline",
        "--offline",
        "--simulate",
        "--clean-dir", str(clean_dir),
        "--published-dir", str(published_dir),
        "--sample-air-input", "tests/fixtures/sample_air_quality.csv",
        "--sample-weather-input", "tests/fixtures/sample_weather_normalized.csv",
        "--run-id", "cli_test_run",
    ])
    assert ret == 0
    assert (clean_dir / "part-m-00000").is_file()
    assert (published_dir / "api" / "v1" / "meta.json").is_file()
    assert (published_dir / "cpcb_index" / "csv" / "part-00000.csv").is_file()


def test_cli_smoke_offline(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("SKYWATCH_ALLOW_SIMULATION", "1")
    ret = main(["smoke", "--simulate", "--offline"])
    assert ret == 0


def test_cli_export(tmp_path: Path):
    # Pre-populate dummy published dataset
    cpcb_csv = tmp_path / "cpcb_index" / "csv"
    cpcb_csv.mkdir(parents=True, exist_ok=True)
    (cpcb_csv / "part-00000.csv").write_text("station_id,aqi\nsite_1,150\n", encoding="utf-8")

    ret = main([
        "export",
        "--base-output-dir", str(tmp_path),
        "--run-id", "exp_test_01",
    ])
    assert ret == 0
    assert (tmp_path / "api" / "v1" / "meta.json").is_file()
    assert (tmp_path / "api" / "v1" / "cpcb_index.json").is_file()
