"""Apache Pig runner orchestrator with standalone Python simulation fallback.

Implements PIG-01, PIG-02, PIG-03 ETL orchestration, parameter injection,
output pre-clearing, sidecar _schema.json generation, and transparent offline
execution fallback when Apache Pig / Hadoop winutils are unavailable.
"""
from __future__ import annotations

import argparse
import csv
import glob
import json
import os
import shutil
import subprocess
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from skywatch.data.schemas import get_schema
from skywatch.pipeline.logging import get_logger, run_context
from skywatch.pipeline.profile import ProfileConfig, load_profile

logger = get_logger("pig_runner")


@dataclass
class RunResult:
    """Execution result container for Pig or fallback ETL runs."""

    script: str
    returncode: int
    stdout: str
    stderr: str
    duration_seconds: float
    output_dir: Optional[str] = None
    records_processed: int = 0
    success: bool = True
    engine: str = "APACHE_PIG"


def is_pig_available(pig_home: Optional[str] = None) -> bool:
    """Inspect environment to detect whether the Apache Pig binary is executable."""
    home = pig_home or os.environ.get("PIG_HOME")
    if home:
        candidates = [
            Path(home) / "bin" / "pig",
            Path(home) / "bin" / "pig.cmd",
            Path(home) / "bin" / "pig.exe",
        ]
        for candidate in candidates:
            if candidate.is_file():
                return True

    # Check system PATH
    for cmd in ["pig", "pig.cmd", "pig.exe"]:
        if shutil.which(cmd):
            return True

    return False


def generate_sidecar_schema(
    entity_name: str,
    output_dir: str,
    config_path: Optional[str] = None,
) -> str:
    """Generate and write sidecar _schema.json metadata file in the target directory."""
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    schema_file = out_dir / "_schema.json"

    schema = get_schema(entity_name, config_path)
    columns = [
        {"name": col["name"], "type": col["type"]}
        for col in schema.get("columns", [])
    ]

    sidecar_data = {
        "schema_version": "2.0",
        "entity": entity_name,
        "format": "csv",
        "delimiter": schema.get("delimiter", ","),
        "has_header": schema.get("has_header", False),
        "columns": columns,
    }

    with open(schema_file, "w", encoding="utf-8") as f:
        json.dump(sidecar_data, f, indent=2)

    logger.info("Generated sidecar schema metadata", extra={"schema_file": str(schema_file), "entity": entity_name})
    return str(schema_file)


def _pre_clear_output(output_dir: str) -> None:
    """Pre-clear target output directory to prevent MapReduce FileAlreadyExistsException."""
    if not output_dir or not output_dir.strip():
        raise ValueError("Cannot pre-clear empty output directory path.")

    path = Path(output_dir).resolve()
    cwd = Path.cwd().resolve()
    temp_root = Path(tempfile.gettempdir()).resolve()

    if path == cwd or path == Path(path.anchor) or path == temp_root:
        raise ValueError(f"Refusing to pre-clear dangerous directory: '{output_dir}'")

    if path.exists():
        if path.is_dir():
            shutil.rmtree(path)
        else:
            os.remove(path)
        logger.info("Pre-cleared existing output directory", extra={"output_dir": str(path)})


def _attach_sidecar(
    script_name: str,
    output_dir: str,
    config_path: Optional[str] = None,
    entity_override: Optional[str] = None,
) -> Optional[str]:
    """Resolve entity from script name or override and attach sidecar _schema.json metadata."""
    if entity_override:
        return generate_sidecar_schema(entity_override, output_dir, config_path)

    script_base = Path(script_name).name
    script_to_entity = {
        "clean_air_quality": "clean_air_quality",
        "station_stats": "station_stats",
        "hourly_rollup": "hourly_rollup",
    }
    for k, v in script_to_entity.items():
        if k in script_base:
            return generate_sidecar_schema(v, output_dir, config_path)

    logger.warning(
        "No canonical entity matching script name; skipping sidecar _schema.json",
        extra={"script": script_name},
    )
    return None


def _resolve_input_files(input_param: str) -> List[Path]:
    """Resolve input path, directory, or pattern to an ordered list of concrete files."""
    input_path = Path(input_param)
    if input_path.is_file():
        return [input_path]

    if input_path.is_dir():
        files: List[Path] = []
        for p in sorted(input_path.iterdir()):
            if p.is_file() and not p.name.startswith((".", "_")):
                files.append(p)
        if files:
            return files

    # Fallback to glob expansion supporting file and directory matches
    expanded: List[Path] = []
    for p in sorted(glob.glob(input_param)):
        pp = Path(p)
        if pp.is_file() and not pp.name.startswith((".", "_")):
            expanded.append(pp)
        elif pp.is_dir():
            for sub in sorted(pp.iterdir()):
                if sub.is_file() and not sub.name.startswith((".", "_")):
                    expanded.append(sub)
    if expanded:
        return expanded

    raise FileNotFoundError(f"Input path or directory not found: '{input_param}'")


def _run_standalone_fallback(
    script_path: str,
    params: Dict[str, str],
    config_path: Optional[str] = None,
    entity_override: Optional[str] = None,
) -> RunResult:
    """Execute relational Pig ETL operations in pure Python producing golden-compatible output."""
    start_time = time.monotonic()
    script_name = Path(script_path).name
    input_param = params.get("INPUT")
    output_param = params.get("OUTPUT")

    if not input_param or not output_param:
        raise ValueError("Both 'INPUT' and 'OUTPUT' parameters must be specified.")

    input_files = _resolve_input_files(input_param)
    raw_rows: List[List[str]] = []
    for f in input_files:
        with open(f, "r", encoding="utf-8", newline="") as fp:
            reader = csv.reader(fp)
            for row in reader:
                if row:
                    raw_rows.append(row)

    out_dir = Path(output_param)
    out_dir.mkdir(parents=True, exist_ok=True)
    part_file = out_dir / "part-m-00000"

    processed_count = 0

    if "clean_air_quality" in script_name:
        # Filter: record_key != 'record_key' AND is_valid == true AND value IS NOT NULL AND 0.0 <= value <= 1000.0
        # Distinct deduplication
        seen: Set[Tuple[str, ...]] = set()
        clean_rows: List[List[str]] = []
        for row in raw_rows:
            if len(row) < 14:
                continue
            if row[0] == "record_key":
                continue
            is_valid_str = row[12].strip().lower()
            if is_valid_str not in ("true", "1"):
                continue
            val_str = row[7].strip()
            if not val_str:
                continue
            try:
                val = float(val_str)
            except ValueError:
                continue
            if val < 0.0 or val > 1000.0:
                continue

            tup = tuple(row)
            if tup not in seen:
                seen.add(tup)
                clean_rows.append(row)

        clean_rows.sort(key=lambda r: (r[1], r[6], r[9]))  # station_id, parameter, measurement_time_utc
        with open(part_file, "w", encoding="utf-8", newline="") as fp:
            writer = csv.writer(fp)
            writer.writerows(clean_rows)
        processed_count = len(clean_rows)

    elif "station_stats" in script_name:
        # Group clean records by (station_id, parameter, date_dt) where date_dt = measurement_time_utc[:10]
        # Aggregations: count, round(avg, 4), min, max
        groups: Dict[Tuple[str, str, str], List[float]] = {}
        for row in raw_rows:
            if len(row) < 14 or row[0] == "record_key":
                continue
            st_id = row[1]
            param = row[6]
            try:
                val = float(row[7])
            except ValueError:
                continue
            date_dt = row[9][:10]
            key = (st_id, param, date_dt)
            if key not in groups:
                groups[key] = []
            groups[key].append(val)

        stats_rows: List[List[str]] = []
        for key in sorted(groups.keys()):
            st_id, param, date_dt = key
            vals = groups[key]
            count = len(vals)
            avg_val = round(sum(vals) / count, 4)
            min_val = min(vals)
            max_val = max(vals)
            stats_rows.append([st_id, param, date_dt, str(count), str(avg_val), str(min_val), str(max_val)])

        with open(part_file, "w", encoding="utf-8", newline="") as fp:
            writer = csv.writer(fp)
            writer.writerows(stats_rows)
        processed_count = len(stats_rows)

    elif "hourly_rollup" in script_name:
        # Group clean records by (station_id, parameter, bucket_start_utc) where bucket_start_utc = measurement_time_utc[:13] + ":00:00Z"
        # Aggregations: count, round(avg, 4), min, max
        groups_h: Dict[Tuple[str, str, str], List[float]] = {}
        for row in raw_rows:
            if len(row) < 14 or row[0] == "record_key":
                continue
            st_id = row[1]
            param = row[6]
            try:
                val = float(row[7])
            except ValueError:
                continue
            bucket = row[9][:13] + ":00:00Z"
            key = (st_id, param, bucket)
            if key not in groups_h:
                groups_h[key] = []
            groups_h[key].append(val)

        rollup_rows: List[List[str]] = []
        for key in sorted(groups_h.keys()):
            st_id, param, bucket = key
            vals = groups_h[key]
            count = len(vals)
            avg_val = round(sum(vals) / count, 4)
            min_val = min(vals)
            max_val = max(vals)
            rollup_rows.append([st_id, param, bucket, str(count), str(avg_val), str(min_val), str(max_val)])

        with open(part_file, "w", encoding="utf-8", newline="") as fp:
            writer = csv.writer(fp)
            writer.writerows(rollup_rows)
        processed_count = len(rollup_rows)
    else:
        raise ValueError(f"Unsupported Pig script for standalone fallback: '{script_name}'")

    # Write MapReduce success marker and sidecar schema
    (out_dir / "_SUCCESS").touch()
    _attach_sidecar(script_name, output_param, config_path, entity_override=entity_override)

    duration = time.monotonic() - start_time
    logger.info(
        "Standalone Pig simulation ETL completed",
        extra={
            "script": script_name,
            "duration_seconds": round(duration, 4),
            "records_processed": processed_count,
            "output_dir": output_param,
        },
    )

    return RunResult(
        script=script_name,
        returncode=0,
        stdout=f"Successfully processed {processed_count} records via standalone fallback",
        stderr="",
        duration_seconds=duration,
        output_dir=output_param,
        records_processed=processed_count,
        success=True,
        engine="STANDALONE_PYTHON_SIMULATION",
    )


def run_pig_script(
    script_path: str,
    params: Dict[str, str],
    profile: Optional[ProfileConfig] = None,
    mode: str = "local",
    offline_fallback: bool = True,
    run_id: Optional[str] = None,
    config_path: Optional[str] = None,
    entity: Optional[str] = None,
    require_real_engine: bool = False,
) -> RunResult:
    """Execute an Apache Pig script with parameter binding and standalone simulation fallback."""
    with run_context(run_id) as active_run_id:
        script = Path(script_path)
        if not script.is_file():
            raise FileNotFoundError(f"Pig script does not exist: '{script_path}'")

        output_dir = params.get("OUTPUT")
        if not output_dir:
            raise ValueError("Parameter 'OUTPUT' is required.")

        # Guard against path traversal in OUTPUT parameter (CR-02 & WR-18 / security gate)
        if ".." in Path(output_dir).parts:
            raise ValueError(f"Path traversal detected in OUTPUT parameter: '{output_dir}'")

        resolved_out = Path(output_dir).resolve()
        repo_root = Path.cwd().resolve()
        temp_root = Path(tempfile.gettempdir()).resolve()
        is_sub_repo = False
        is_sub_temp = False
        try:
            resolved_out.relative_to(repo_root)
            is_sub_repo = True
        except ValueError:
            pass
        try:
            resolved_out.relative_to(temp_root)
            is_sub_temp = True
        except ValueError:
            pass

        if not (is_sub_repo or is_sub_temp):
            raise ValueError(f"OUTPUT path must resolve within repository root or temp directory: '{output_dir}'")

        _pre_clear_output(output_dir)

        # Check Pig availability
        active_profile = profile or load_profile()
        pig_bin_available = is_pig_available(active_profile.pig_home)

        require_real = (
            require_real_engine
            or os.environ.get("SKYWATCH_REQUIRE_REAL_ENGINE", "").lower() in ("1", "true", "yes")
        )

        if not pig_bin_available:
            if require_real or not offline_fallback:
                logger.error(
                    "[ENGINE: APACHE_PIG] Pig executable not found and real engine is required",
                    extra={"pig_home": active_profile.pig_home, "script": script.name},
                )
                raise RuntimeError(
                    f"Apache Pig executable not found (PIG_HOME='{active_profile.pig_home}') and real engine is required."
                )

            logger.warning(
                "[ENGINE: STANDALONE_PYTHON_SIMULATION] Pig executable not found on host; executing via standalone Python fallback",
                extra={"engine": "STANDALONE_PYTHON_SIMULATION", "script": script.name, "run_id": active_run_id},
            )
            print(f"[ENGINE: STANDALONE_PYTHON_SIMULATION] Executing {script.name} via pure-Python fallback (Pig not found)")
            res = _run_standalone_fallback(script_path, params, config_path, entity_override=entity)
            res.engine = "STANDALONE_PYTHON_SIMULATION"
            return res

        # Build command array without shell=True for execution safety
        pig_bin = "pig.cmd" if os.name == "nt" else "pig"
        if active_profile.pig_home:
            candidates = ["pig.cmd", "pig.exe", "pig"] if os.name == "nt" else ["pig"]
            for candidate in candidates:
                bin_path = Path(active_profile.pig_home) / "bin" / candidate
                if bin_path.is_file():
                    pig_bin = str(bin_path)
                    break

        cmd: List[str] = [pig_bin, "-x", mode]
        for key in sorted(params.keys()):
            cmd.extend(["-param", f"{key}={params[key]}"])
        cmd.extend(["-f", str(script)])

        env = os.environ.copy()
        if active_profile.java_home_pig:
            env["JAVA_HOME"] = active_profile.java_home_pig
        if active_profile.pig_home:
            env["PIG_HOME"] = active_profile.pig_home
        if active_profile.hadoop_home:
            env["HADOOP_HOME"] = active_profile.hadoop_home

        start_time = time.monotonic()
        logger.info("[ENGINE: APACHE_PIG] Launching Apache Pig subprocess", extra={"engine": "APACHE_PIG", "cmd": cmd, "run_id": active_run_id})
        print(f"[ENGINE: APACHE_PIG] Launching Apache Pig subprocess: {script.name}")

        try:
            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                env=env,
                timeout=90,
                check=False,
            )
        except subprocess.TimeoutExpired:
            duration = time.monotonic() - start_time
            logger.error("Pig execution timed out after 90 seconds", extra={"script": script.name})
            return RunResult(
                script=script.name,
                returncode=124,
                stdout="",
                stderr="Execution timed out after 90 seconds",
                duration_seconds=duration,
                output_dir=output_dir,
                records_processed=0,
                success=False,
                engine="APACHE_PIG",
            )

        duration = time.monotonic() - start_time
        if proc.returncode == 0:
            _attach_sidecar(script.name, output_dir, config_path, entity_override=entity)
            # Count records processed if part files exist
            rec_count = 0
            for part in Path(output_dir).glob("part*"):
                if part.is_file():
                    with open(part, "r", encoding="utf-8") as fp:
                        rec_count += sum(1 for line in fp if line.strip())

            logger.info("Apache Pig execution succeeded", extra={"script": script.name, "duration_seconds": duration, "engine": "APACHE_PIG"})
            return RunResult(
                script=script.name,
                returncode=0,
                stdout=proc.stdout,
                stderr=proc.stderr,
                duration_seconds=duration,
                output_dir=output_dir,
                records_processed=rec_count,
                success=True,
                engine="APACHE_PIG",
            )

        logger.error("Apache Pig execution failed", extra={"script": script.name, "returncode": proc.returncode, "stderr": proc.stderr, "engine": "APACHE_PIG"})
        return RunResult(
            script=script.name,
            returncode=proc.returncode,
            stdout=proc.stdout,
            stderr=proc.stderr,
            duration_seconds=duration,
            output_dir=output_dir,
            records_processed=0,
            success=False,
            engine="APACHE_PIG",
        )


def main() -> None:
    """CLI entrypoint for running Pig ETL scripts."""
    parser = argparse.ArgumentParser(description="SkyWatch Apache Pig ETL script runner")
    parser.add_argument("--script", required=True, help="Path to .pig script")
    parser.add_argument("--input", required=True, help="Input CSV path or directory")
    parser.add_argument("--output", required=True, help="Target output directory")
    parser.add_argument("--run-date", default=time.strftime("%Y-%m-%d"), help="Execution date partition")
    parser.add_argument("--profile", default=os.environ.get("SKYWATCH_PROFILE", "modern"), help="Profile name")
    parser.add_argument("--mode", default="local", help="Execution mode (local or mapreduce)")
    parser.add_argument("--entity", default=None, help="Explicit canonical entity name for sidecar schema")
    parser.add_argument("--no-fallback", action="store_true", help="Disable offline Python fallback")
    parser.add_argument("--require-real-engine", action="store_true", help="Fail if Apache Pig binary is unavailable")

    args = parser.parse_args()

    prof = load_profile(args.profile)
    params = {
        "INPUT": args.input,
        "OUTPUT": args.output,
        "RUN_DATE": args.run_date,
    }

    result = run_pig_script(
        script_path=args.script,
        params=params,
        profile=prof,
        mode=args.mode,
        offline_fallback=not args.no_fallback,
        entity=args.entity,
        require_real_engine=args.require_real_engine,
    )

    if not result.success:
        print(f"ERROR: Pig runner failed with returncode {result.returncode}:\n{result.stderr}")
        raise SystemExit(result.returncode)

    print(f"SUCCESS [{result.engine}]: {result.script} completed in {result.duration_seconds:.2f}s ({result.records_processed} records)")


if __name__ == "__main__":
    main()
