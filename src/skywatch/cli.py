"""Unified Command-Line Interface for SkyWatch.

Implements OPS-05 (Phase 1 subcommands: discover, collect, backfill).
"""
from __future__ import annotations

import argparse
import json
import sys

from skywatch.collector.collector import IngestionOrchestrator
from skywatch.collector.discovery import discover_stations
from skywatch.collector.openaq_client import OpenAQClient
from skywatch.pipeline.logging import get_logger

logger = get_logger("cli")


def build_parser() -> argparse.ArgumentParser:
    """Build and configure argparse CLI command parser."""
    parser = argparse.ArgumentParser(
        prog="skywatch",
        description="SkyWatch: Big Data Environmental Analytics Platform for Bengaluru",
    )
    subparsers = parser.add_subparsers(dest="subcommand", help="Available subcommands")

    # discover
    discover_p = subparsers.add_parser("discover", help="Auto-discover active monitoring stations")
    discover_p.add_argument("--max-age-hours", type=int, default=72, help="Max inactivity threshold in hours")
    discover_p.add_argument("--output", type=str, default="config/stations.json", help="Path to output stations file")
    discover_p.add_argument("--offline", action="store_true", help="Run in offline mode using mock fixtures")

    # collect
    collect_p = subparsers.add_parser("collect", help="Execute collection cycle for air quality and weather")
    collect_p.add_argument(
        "--mode",
        choices=["incremental", "latest"],
        default="incremental",
        help="Collection mode (default: incremental)",
    )
    collect_p.add_argument("--offline", action="store_true", help="Run in offline mode using mock fixtures")

    # backfill
    backfill_p = subparsers.add_parser("backfill", help="Execute historical chunked backfill")
    backfill_p.add_argument("--start-date", type=str, required=True, help="Start date (YYYY-MM-DD)")
    backfill_p.add_argument("--end-date", type=str, required=True, help="End date (YYYY-MM-DD)")
    backfill_p.add_argument("--chunk-days", type=int, default=7, help="Window size per chunk in days (default: 7)")
    backfill_p.add_argument("--offline", action="store_true", help="Run in offline mode using mock fixtures")

    return parser


def main(args: list[str] | None = None) -> int:
    """Main CLI entrypoint."""
    parser = build_parser()
    parsed = parser.parse_args(args)

    if not parsed.subcommand:
        parser.print_help()
        return 0

    try:
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

    except Exception as exc:
        logger.error(f"Command '{parsed.subcommand}' failed: {exc}", exc_info=True)
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
