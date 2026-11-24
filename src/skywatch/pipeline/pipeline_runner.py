"""End-to-end multi-stage pipeline orchestrator for SkyWatch.

Coordinates batch processing across all four architectural stages:
  Stage 1: Telemetry Ingestion and Normalization (IngestionOrchestrator)
  Stage 2: Apache Pig Data Cleansing (pig/clean_air_quality.pig)
  Stage 3: Apache Spark Analytics and CPCB Indexing (run_spark_analytics)
  Stage 4: Static API Lake Publishing (export_static_api_endpoints)

Propagates a unified run_id across all stages and emits structured execution summaries.
"""
from __future__ import annotations

import shutil
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from skywatch.collector.collector import IngestionOrchestrator
from skywatch.pipeline.logging import generate_run_id, get_logger, run_context
from skywatch.pipeline.pig_runner import run_pig_script
from skywatch.pipeline.profile import load_profile
from skywatch.pipeline.spark_runner import run_spark_analytics
from skywatch.spark.export import export_static_api_endpoints

logger = get_logger("pipeline_runner")


@dataclass
class StageResult:
    """Execution metrics for an individual pipeline stage."""
    stage_name: str
    success: bool
    duration_seconds: float
    records_in: int = 0
    records_out: int = 0
    engine: str = "UNKNOWN"
    details: Dict[str, Any] = field(default_factory=dict)
    error_message: Optional[str] = None


@dataclass
class PipelineRunSummary:
    """Consolidated summary of an end-to-end pipeline execution."""
    run_id: str
    profile: str
    start_time_utc: str
    end_time_utc: str
    total_duration_seconds: float
    success: bool
    stages: List[StageResult] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert pipeline summary to dictionary for JSON serialization."""
        return asdict(self)


def run_pipeline(
    profile_name: Optional[str] = None,
    offline: bool = False,
    simulate: bool = False,
    require_real_engine: bool = False,
    raw_dir: Union[str, Path] = "data/raw",
    normalized_dir: Union[str, Path] = "data/normalized",
    clean_dir: Union[str, Path] = "data/clean/air_quality",
    published_dir: Union[str, Path] = "output/published",
    run_id: Optional[str] = None,
    sample_air_input: Optional[Union[str, Path]] = None,
    sample_weather_input: Optional[Union[str, Path]] = None,
    stations_file: Union[str, Path] = "config/stations.json",
) -> PipelineRunSummary:
    """Execute all four stages of the SkyWatch pipeline in sequence with a unified run_id.

    Args:
        profile_name: Execution profile ('modern' or 'lab').
        offline: Run collection in offline mode using mock fixtures.
        simulate: Allow/force pure-Python simulation for Pig and Spark engines.
        require_real_engine: Fail if native Apache Pig or Apache Spark are unavailable.
        raw_dir: Directory containing raw telemetry data.
        normalized_dir: Directory containing normalized partition data.
        clean_dir: Destination directory for cleaned Pig data.
        published_dir: Base directory for published analytical tables and static API.
        run_id: Optional user-supplied unique execution identifier.
        sample_air_input: Optional path to sample air quality input CSV.
        sample_weather_input: Optional path to sample weather normalized input CSV.
        stations_file: Path to stations registry file.

    Returns:
        PipelineRunSummary with timings, record counts, and status per stage.
    """
    active_profile = load_profile(profile_name)
    active_run_id = run_id or generate_run_id()
    start_dt = datetime.now(timezone.utc)
    start_ts = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    raw_path = Path(raw_dir)
    norm_path = Path(normalized_dir)
    clean_path = Path(clean_dir)
    pub_path = Path(published_dir)

    stages: List[StageResult] = []
    overall_success = True
    start_mono = time.monotonic()

    logger.info(
        f"[PIPELINE START] run_id={active_run_id} profile={active_profile.profile_name} "
        f"offline={offline} simulate={simulate} require_real={require_real_engine}"
    )

    with run_context(active_run_id):
        # ------------------------------------------------------------------
        # Stage 1: Ingestion & Normalization
        # ------------------------------------------------------------------
        stage1_start = time.monotonic()
        stage1_success = False
        stage1_records_in = 0
        stage1_records_out = 0
        stage1_error = None
        stage1_details: Dict[str, Any] = {}

        try:
            logger.info("[STAGE 1/4] Starting Ingestion and Normalization...")
            if sample_air_input and Path(sample_air_input).is_file():
                # Direct sample input provided
                logger.info(f"Using provided sample air input: {sample_air_input}")
                staged_air_dir = norm_path / "air_quality"
                staged_air_dir.mkdir(parents=True, exist_ok=True)
                target_sample = staged_air_dir / "sample_ingested.csv"
                shutil.copyfile(str(sample_air_input), str(target_sample))
                stage1_records_out = 1
                stage1_details["source"] = str(sample_air_input)
                stage1_success = True
            else:
                orchestrator = IngestionOrchestrator(
                    raw_dir=str(raw_path),
                    normalized_dir=str(norm_path),
                    stations_path=str(stations_file),
                    offline=offline,
                )
                collect_summary = orchestrator.run_incremental_collection(run_id=active_run_id)
                stage1_records_in = collect_summary.get("stations_polled", 0)
                stage1_records_out = collect_summary.get("total_records_ingested", 0)
                stage1_details = collect_summary
                stage1_success = True

        except Exception as exc:
            logger.error(f"Stage 1 failed: {exc}", exc_info=True)
            stage1_error = str(exc)
            stage1_success = False
            overall_success = False

        stage1_duration = time.monotonic() - stage1_start
        stages.append(
            StageResult(
                stage_name="ingestion_normalization",
                success=stage1_success,
                duration_seconds=round(stage1_duration, 4),
                records_in=stage1_records_in,
                records_out=stage1_records_out,
                engine="PYTHON_COLLECTOR",
                details=stage1_details,
                error_message=stage1_error,
            )
        )

        if not stage1_success and require_real_engine:
            end_dt = datetime.now(timezone.utc)
            return PipelineRunSummary(
                run_id=active_run_id,
                profile=active_profile.profile_name,
                start_time_utc=start_ts,
                end_time_utc=end_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                total_duration_seconds=round(time.monotonic() - start_mono, 4),
                success=False,
                stages=stages,
            )

        # ------------------------------------------------------------------
        # Stage 2: Apache Pig Data Cleansing
        # ------------------------------------------------------------------
        stage2_start = time.monotonic()
        stage2_success = False
        stage2_records_in = 0
        stage2_records_out = 0
        stage2_engine = "APACHE_PIG"
        stage2_error = None
        stage2_details = {}

        try:
            logger.info("[STAGE 2/4] Starting Apache Pig Data Cleansing...")
            # Resolve Pig input file
            pig_input: Optional[Path] = None
            if sample_air_input and Path(sample_air_input).is_file():
                pig_input = Path(sample_air_input)
            else:
                air_norm_dir = norm_path / "air_quality"
                if air_norm_dir.is_dir():
                    candidates = list(air_norm_dir.glob("**/*.csv"))
                    if candidates:
                        pig_input = candidates[0]

            if pig_input is None:
                # Fallback to test fixture if available
                fixture_air = Path("tests/fixtures/sample_air_quality.csv")
                if fixture_air.is_file():
                    pig_input = fixture_air
                else:
                    raise FileNotFoundError("No input air quality CSV data found for Pig ETL.")

            clean_path.parent.mkdir(parents=True, exist_ok=True)
            pig_result = run_pig_script(
                script_path="pig/clean_air_quality.pig",
                params={"INPUT": str(pig_input), "OUTPUT": str(clean_path)},
                profile=active_profile,
                offline_fallback=not require_real_engine,
                run_id=active_run_id,
                require_real_engine=require_real_engine,
            )

            stage2_success = pig_result.success
            stage2_records_in = pig_result.records_processed
            stage2_records_out = pig_result.records_processed
            stage2_engine = pig_result.engine
            stage2_details = {
                "script": "pig/clean_air_quality.pig",
                "input": str(pig_input),
                "output": str(clean_path),
                "pig_output_dir": pig_result.output_dir,
            }

        except Exception as exc:
            logger.error(f"Stage 2 failed: {exc}", exc_info=True)
            stage2_error = str(exc)
            stage2_success = False
            overall_success = False

        stage2_duration = time.monotonic() - stage2_start
        stages.append(
            StageResult(
                stage_name="pig_data_cleansing",
                success=stage2_success,
                duration_seconds=round(stage2_duration, 4),
                records_in=stage2_records_in,
                records_out=stage2_records_out,
                engine=stage2_engine,
                details=stage2_details,
                error_message=stage2_error,
            )
        )

        if not stage2_success and require_real_engine:
            end_dt = datetime.now(timezone.utc)
            return PipelineRunSummary(
                run_id=active_run_id,
                profile=active_profile.profile_name,
                start_time_utc=start_ts,
                end_time_utc=end_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                total_duration_seconds=round(time.monotonic() - start_mono, 4),
                success=False,
                stages=stages,
            )

        # ------------------------------------------------------------------
        # Stage 3: Apache Spark Analytics & CPCB Indexing
        # ------------------------------------------------------------------
        stage3_start = time.monotonic()
        stage3_success = False
        stage3_records_in = 0
        stage3_records_out = 0
        stage3_engine = "APACHE_SPARK"
        stage3_error = None
        stage3_details = {}

        try:
            logger.info("[STAGE 3/4] Starting Apache Spark Analytics and CPCB Indexing...")
            # Resolve weather input
            weather_input: Optional[Path] = None
            if sample_weather_input and Path(sample_weather_input).is_file():
                weather_input = Path(sample_weather_input)
            else:
                weather_norm_dir = norm_path / "weather"
                if weather_norm_dir.is_dir():
                    candidates = list(weather_norm_dir.glob("**/*.csv"))
                    if candidates:
                        weather_input = candidates[0]

            if weather_input is None:
                fixture_weather = Path("tests/fixtures/sample_weather_normalized.csv")
                if fixture_weather.is_file():
                    weather_input = fixture_weather
                else:
                    weather_input = Path("data/normalized/weather")

            spark_results = run_spark_analytics(
                input_clean_dir=clean_path,
                input_weather_dir=weather_input,
                output_published_dir=pub_path,
                profile=active_profile,
                force_simulation=simulate,
                run_id=active_run_id,
                require_real_engine=require_real_engine,
            )

            stage3_success = all(r.success for r in spark_results)
            total_recs = sum(r.records_processed for r in spark_results)
            stage3_records_out = total_recs
            engines_used = set(r.engine for r in spark_results)
            stage3_engine = ", ".join(sorted(engines_used)) if engines_used else "APACHE_SPARK"
            stage3_details = {
                "jobs": [
                    {
                        "name": r.job_name,
                        "success": r.success,
                        "records": r.records_processed,
                        "duration_s": r.duration_seconds,
                        "engine": r.engine,
                    }
                    for r in spark_results
                ]
            }

        except Exception as exc:
            logger.error(f"Stage 3 failed: {exc}", exc_info=True)
            stage3_error = str(exc)
            stage3_success = False
            overall_success = False

        stage3_duration = time.monotonic() - stage3_start
        stages.append(
            StageResult(
                stage_name="spark_analytics_indexing",
                success=stage3_success,
                duration_seconds=round(stage3_duration, 4),
                records_in=stage3_records_in,
                records_out=stage3_records_out,
                engine=stage3_engine,
                details=stage3_details,
                error_message=stage3_error,
            )
        )

        # ------------------------------------------------------------------
        # Stage 4: Static API Lake Publishing
        # ------------------------------------------------------------------
        stage4_start = time.monotonic()
        stage4_success = False
        stage4_records_out = 0
        stage4_error = None
        stage4_details = {}

        try:
            logger.info("[STAGE 4/4] Starting Static API Lake Export...")
            api_dir = export_static_api_endpoints(
                base_output_dir=pub_path,
                run_id=active_run_id,
                profile_name=active_profile.profile_name,
                engine_name=stage3_engine if "STANDALONE" in stage3_engine else "APACHE_SPARK",
                generated_at_utc=start_ts,
                stations_file=stations_file,
            )

            # Count generated json endpoints
            json_files = list(api_dir.glob("*.json"))
            stage4_records_out = len(json_files)
            stage4_success = stage4_records_out >= 7
            stage4_details = {
                "api_dir": str(api_dir),
                "endpoints_exported": [p.name for p in sorted(json_files)],
            }

        except Exception as exc:
            logger.error(f"Stage 4 failed: {exc}", exc_info=True)
            stage4_error = str(exc)
            stage4_success = False
            overall_success = False

        stage4_duration = time.monotonic() - stage4_start
        stages.append(
            StageResult(
                stage_name="static_api_publishing",
                success=stage4_success,
                duration_seconds=round(stage4_duration, 4),
                records_in=0,
                records_out=stage4_records_out,
                engine="PYTHON_JSON_EXPORTER",
                details=stage4_details,
                error_message=stage4_error,
            )
        )

    end_dt = datetime.now(timezone.utc)
    total_duration = time.monotonic() - start_mono

    summary = PipelineRunSummary(
        run_id=active_run_id,
        profile=active_profile.profile_name,
        start_time_utc=start_ts,
        end_time_utc=end_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        total_duration_seconds=round(total_duration, 4),
        success=overall_success,
        stages=stages,
    )

    logger.info(
        f"[PIPELINE END] run_id={active_run_id} success={overall_success} "
        f"duration={summary.total_duration_seconds}s"
    )
    return summary
