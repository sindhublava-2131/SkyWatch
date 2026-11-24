"""Unit tests for pipeline orchestrator (OPS-05, OPS-07)."""
from __future__ import annotations

from pathlib import Path

from skywatch.pipeline.pipeline_runner import run_pipeline


def test_run_pipeline_offline_simulation(tmp_path: Path):
    clean_dir = tmp_path / "clean"
    pub_dir = tmp_path / "published"
    air_input = "tests/fixtures/sample_air_quality.csv"
    weather_input = "tests/fixtures/sample_weather_normalized.csv"

    summary = run_pipeline(
        profile_name="modern",
        offline=True,
        simulate=True,
        require_real_engine=False,
        clean_dir=clean_dir,
        published_dir=pub_dir,
        sample_air_input=air_input,
        sample_weather_input=weather_input,
        run_id="pipe_test_01",
    )

    assert summary.success is True
    assert summary.run_id == "pipe_test_01"
    assert summary.profile == "modern"
    assert len(summary.stages) == 4

    stage_names = [s.stage_name for s in summary.stages]
    assert stage_names == [
        "ingestion_normalization",
        "pig_data_cleansing",
        "spark_analytics_indexing",
        "static_api_publishing",
    ]

    assert all(s.success for s in summary.stages)

    # Check stage 2 output
    assert (clean_dir / "part-m-00000").is_file()
    assert (clean_dir / "_schema.json").is_file()

    # Check stage 3 output
    assert (pub_dir / "cpcb_index" / "csv" / "part-00000.csv").is_file()
    assert (pub_dir / "weather_correlation" / "csv" / "part-00000.csv").is_file()

    # Check stage 4 output
    api_dir = pub_dir / "api" / "v1"
    assert (api_dir / "meta.json").is_file()
    assert (api_dir / "stations.json").is_file()
    assert (api_dir / "cpcb_index.json").is_file()

    summary_dict = summary.to_dict()
    assert "total_duration_seconds" in summary_dict
    assert summary_dict["success"] is True


def test_run_pipeline_require_real_engine_fails_without_native_pig(tmp_path: Path):
    clean_dir = tmp_path / "clean"
    pub_dir = tmp_path / "published"

    summary = run_pipeline(
        profile_name="modern",
        offline=True,
        simulate=False,
        require_real_engine=True,
        clean_dir=clean_dir,
        published_dir=pub_dir,
        sample_air_input="tests/fixtures/sample_air_quality.csv",
        sample_weather_input="tests/fixtures/sample_weather_normalized.csv",
        run_id="pipe_test_fail",
    )

    # In environment without real Pig binary installed at /opt/bigdata/pig-0.18.0,
    # stage 2 must fail when require_real_engine is True.
    assert summary.success is False
    assert any(s.success is False for s in summary.stages)
