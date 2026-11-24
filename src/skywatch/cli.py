"""Unified Command-Line Interface for SkyWatch.

Implements OPS-05:
  Subcommands: discover, collect, backfill, pipeline, export, smoke.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import List, Optional

from skywatch.collector.collector import IngestionOrchestrator
from skywatch.collector.discovery import discover_stations
from skywatch.collector.openaq_client import OpenAQClient
from skywatch.pipeline.logging import get_logger
from skywatch.pipeline.pipeline_runner import run_pipeline
from skywatch.pipeline.profile import load_profile, validate_environment
from skywatch.spark.export import export_static_api_endpoints

logger = get_logger("cli")


def build_parser() -> argparse.ArgumentParser:
    """Build and configure argparse CLI command parser."""
    parser = argparse.ArgumentParser(
        prog="skywatch",
        description="SkyWatch: Big Data Environmental Analytics Platform for Bengaluru",
    )
    parser.add_argument(
        "--profile",
        choices=["modern", "lab"],
        default="modern",
        help="Target execution profile (default: modern)",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Console logging level",
    )

    subparsers = parser.add_subparsers(dest="subcommand", help="Available subcommands")

    # 1. discover
    discover_p = subparsers.add_parser("discover", help="Auto-discover active monitoring stations")
    discover_p.add_argument("--max-age-hours", type=int, default=72, help="Max inactivity threshold in hours")
    discover_p.add_argument("--output", type=str, default="config/stations.json", help="Path to output stations file")
    discover_p.add_argument("--offline", action="store_true", help="Run in offline mode using mock fixtures")

    # 2. collect
    collect_p = subparsers.add_parser("collect", help="Execute collection cycle for air quality and weather")
    collect_p.add_argument(
        "--mode",
        choices=["incremental", "latest"],
        default="incremental",
        help="Collection mode (default: incremental)",
    )
    collect_p.add_argument("--offline", action="store_true", help="Run in offline mode using mock fixtures")

    # 3. backfill
    backfill_p = subparsers.add_parser("backfill", help="Execute historical chunked backfill")
    backfill_p.add_argument("--start-date", type=str, required=True, help="Start date (YYYY-MM-DD)")
    backfill_p.add_argument("--end-date", type=str, required=True, help="End date (YYYY-MM-DD)")
    backfill_p.add_argument("--chunk-days", type=int, default=7, help="Window size per chunk in days (default: 7)")
    backfill_p.add_argument("--offline", action="store_true", help="Run in offline mode using mock fixtures")

    # 4. pipeline
    pipeline_p = subparsers.add_parser(
        "pipeline",
        help="Execute end-to-end pipeline (Ingestion -> Pig Cleansing -> Spark Analytics -> API Export)",
    )
    pipeline_p.add_argument("--profile", choices=["modern", "lab"], default="modern", help="Target execution profile")
    pipeline_p.add_argument("--offline", action="store_true", help="Run collection in offline mode using mock fixtures")
    pipeline_p.add_argument("--simulate", action="store_true", help="Allow standalone simulation fallback for engines")
    pipeline_p.add_argument("--require-real-engine", action="store_true", help="Fail if native Pig or Spark missing")
    pipeline_p.add_argument("--clean-dir", type=str, default="data/clean/air_quality", help="Clean Pig output dir")
    pipeline_p.add_argument("--published-dir", type=str, default="output/published", help="Published analytics dir")
    pipeline_p.add_argument("--sample-air-input", type=str, default=None, help="Path to sample air CSV")
    pipeline_p.add_argument("--sample-weather-input", type=str, default=None, help="Path to sample weather CSV")
    pipeline_p.add_argument("--run-id", type=str, default=None, help="Pipeline execution run_id")

    # 5. export
    export_p = subparsers.add_parser("export", help="Export static JSON endpoints for web dashboard")
    export_p.add_argument("--base-output-dir", type=str, default="output/published", help="Base directory with published datasets")
    export_p.add_argument("--stations-file", type=str, default="config/stations.json", help="Path to stations registry")
    export_p.add_argument("--run-id", type=str, default=None, help="Execution identifier")

    # 6. smoke
    smoke_p = subparsers.add_parser("smoke", help="Run end-to-end acceptance smoke test")
    smoke_p.add_argument("--profile", choices=["modern", "lab"], default="modern", help="Target execution profile")
    smoke_p.add_argument("--simulate", action="store_true", help="Allow simulation fallback in smoke run")
    smoke_p.add_argument("--require-real-engine", action="store_true", help="Enforce real native big data engines")
    smoke_p.add_argument("--offline", action="store_true", default=True, help="Use offline test fixtures")

    return parser


def main(args: Optional[List[str]] = None) -> int:
    """Main CLI entrypoint."""
    parser = build_parser()
    parsed = parser.parse_args(args)

    if not parsed.subcommand:
        parser.print_help()
        return 0

    try:
        profile_name = getattr(parsed, "profile", "modern")
        if profile_name:
            os.environ["SKYWATCH_PROFILE"] = profile_name

        if parsed.subcommand == "discover":
            client = OpenAQClient(offline=parsed.offline)
            stations = discover_stations(
                client=client,
                max_age_hours=parsed.max_age_hours,
                output_file=parsed.output,
            )
            print(f"Discovered {len(stations)} active stations. Output saved to {parsed.output}")
            return 0

        elif parsed.subcommand == "collect":
            orchestrator = IngestionOrchestrator(offline=parsed.offline)
            summary = orchestrator.run_incremental_collection(mode=parsed.mode)
            print(json.dumps(summary, indent=2))
            return 0

        elif parsed.subcommand == "backfill":
            orchestrator = IngestionOrchestrator(offline=parsed.offline)
            summary = orchestrator.run_chunked_backfill(
                start_date=parsed.start_date,
                end_date=parsed.end_date,
                chunk_days=parsed.chunk_days,
            )
            print(json.dumps(summary, indent=2))
            return 0

        elif parsed.subcommand == "pipeline":
            # If sample files not explicitly passed, check standard test fixtures for convenience
            air_input = parsed.sample_air_input
            weather_input = parsed.sample_weather_input
            if parsed.offline and not air_input and Path("tests/fixtures/sample_air_quality.csv").is_file():
                air_input = "tests/fixtures/sample_air_quality.csv"
            if parsed.offline and not weather_input and Path("tests/fixtures/sample_weather_normalized.csv").is_file():
                weather_input = "tests/fixtures/sample_weather_normalized.csv"

            summary = run_pipeline(
                profile_name=profile_name,
                offline=parsed.offline,
                simulate=parsed.simulate,
                require_real_engine=parsed.require_real_engine,
                clean_dir=parsed.clean_dir,
                published_dir=parsed.published_dir,
                sample_air_input=air_input,
                sample_weather_input=weather_input,
                run_id=parsed.run_id,
            )
            print(json.dumps(summary.to_dict(), indent=2))
            return 0 if summary.success else 1

        elif parsed.subcommand == "export":
            api_dir = export_static_api_endpoints(
                base_output_dir=parsed.base_output_dir,
                run_id=parsed.run_id,
                profile_name=profile_name,
                stations_file=parsed.stations_file,
            )
            print(f"Static REST API lake successfully exported to {api_dir}")
            return 0

        elif parsed.subcommand == "smoke":
            # Execute smoke check through python runner
            cfg = load_profile(profile_name)
            env_status = validate_environment(cfg)
            print(f"Validating profile '{profile_name}': {env_status}")

            sample_air = "tests/fixtures/sample_air_quality.csv"
            sample_weather = "tests/fixtures/sample_weather_normalized.csv"

            smoke_summary = run_pipeline(
                profile_name=profile_name,
                offline=True,
                simulate=parsed.simulate or (not parsed.require_real_engine),
                require_real_engine=parsed.require_real_engine,
                clean_dir="data/smoke/clean",
                published_dir="data/smoke/published",
                sample_air_input=sample_air,
                sample_weather_input=sample_weather,
            )
            print(json.dumps(smoke_summary.to_dict(), indent=2))
            return 0 if smoke_summary.success else 1

    except Exception as exc:
        logger.error(f"Command '{parsed.subcommand}' failed: {exc}", exc_info=True)
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
