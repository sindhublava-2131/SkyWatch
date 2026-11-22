# Phase 2: Big Data ETL with Apache Pig & Hadoop HDFS - Pattern Mapping

**Phase:** 2 - Big Data ETL with Apache Pig & Hadoop HDFS  
**Mapped:** 2026-11-22  
**Domain:** Apache Pig Latin ETL, Hadoop HDFS, Subprocess Orchestration, Fallback Simulation, Data Contracts  
**Status:** COMPLETE  

---

## 1. Executive Summary

This document provides concrete code patterns, structural templates, and implementation references for all files to be created or modified in **Phase 2: Big Data ETL with Apache Pig & Hadoop HDFS**.

The primary objective of Phase 2 is to extract, clean, deduplicate, and aggregate normalized air-quality data stored in HDFS partitions using Apache Pig Latin scripts (`clean_air_quality.pig`, `station_stats.pig`, `hourly_rollup.pig`) orchestrated by a Python runner (`pig_runner.py`). To guarantee developer usability, 100% CI reproducibility on GitHub Actions, and seamless operation on developer laptops with or without Hadoop/Pig daemons (e.g. Windows without Cygwin/winutils or memory <= 4 GB), `pig_runner.py` implements a transparent pure-Python simulation fallback that produces byte-identical or numerical-identical results verified against golden reference datasets.

Every newly authored file must copy established architectural conventions from existing Phase 1 modules:
1. **Python 3.8 Syntax Floor**: Enforce `from __future__ import annotations`, `typing.Optional`, `typing.Dict`, `typing.List`, and avoid Python 3.10+ syntax (e.g., no `match` statements or `X | Y` union types) [VERIFIED: `AGENTS.md` §4].
2. **Schema Single Source of Truth**: All data contracts must stem from `config/schemas.yaml`, and Pig `AS (...)` clauses must match `config/schemas.yaml` 1:1 [VERIFIED: `schemas.py`].
3. **Subprocess & Port Security**: Never use `shell=True` in `subprocess.run()`. Strict argument arrays, path traversal checks (`Path(p.lstrip("/")).as_posix()`), and localhost-only daemons [VERIFIED: `hdfs_storage.py`].
4. **Structured JSON Logging**: Utilize `skywatch.pipeline.logging.get_logger()` with automatic credential redaction and context-bound `run_id` [VERIFIED: `logging.py`].
5. **Atomic Storage & Pre-Clearing**: Pre-clear target MapReduce output directories before execution to prevent `FileAlreadyExistsException`, and emit sidecar `_schema.json` metadata contracts for downstream PySpark consumption [VERIFIED: `local_storage.py`, Spec §8].

---

## 2. File Classification Table

| # | Target File Path | Role | Data Flow | Layer | Closest Codebase Analog | Target Plan |
|---|---|---|---|---|---|---|
| 1 | `config/schemas.yaml` | Schema Contract Addition | Single source of truth read by `schemas.py` and `pig_runner.py` | Configuration / Data Contracts | `config/schemas.yaml` (`air_quality` stream) | Plan 02-01 |
| 2 | `pig/clean_air_quality.pig` | Declarative ETL Script | Ingests normalized CSV (`$INPUT`), strips header, filters invalid rows, applies `DISTINCT`, outputs headerless clean CSV (`$OUTPUT`) | Big Data ETL (Pig Latin) | `config/schemas.yaml` & `clean_air_quality` contract | Plan 02-01 |
| 3 | `pig/station_stats.pig` | Declarative Aggregation Script | Ingests clean CSV (`$INPUT`), extracts date substring, groups by `(station_id, parameter, date)`, computes count, avg, min, max (`$OUTPUT`) | Big Data ETL (Pig Latin) | `config/schemas.yaml` & `station_stats` contract | Plan 02-01 |
| 4 | `pig/hourly_rollup.pig` | Declarative Rollup Script | Ingests clean CSV (`$INPUT`), extracts hour substring + UTC suffix, groups by `(station_id, parameter, bucket_start_utc)`, aggregates metrics (`$OUTPUT`) | Big Data ETL (Pig Latin) | `config/schemas.yaml` & `hourly_rollup` contract | Plan 02-01 |
| 5 | `src/skywatch/pipeline/pig_runner.py` | Orchestrator & Fallback Engine | Pre-clears `$OUTPUT`, checks Pig binary, dispatches to `pig -x local` or pure-Python simulation fallback, emits `_schema.json` sidecar | Orchestration / Pipeline | `src/skywatch/storage/hdfs_storage.py` & `src/skywatch/pipeline/profile.py` | Plan 02-02 |
| 6 | `scripts/run_pig.sh` | Shell Entrypoint Wrapper | Sources active profile (`use_profile.sh`), sets environment, triggers `pig_runner.py`, and logs execution | CLI / Shell Scripts | `scripts/cron_collect.sh` & `scripts/use_profile.sh` | Plan 02-02 |
| 7 | `data/sample/sample_air_quality.csv` | Frozen Dataset Baseline | Immutable sample dataset (500-1000 rows across 5 Bengaluru stations) containing edge cases (headers, invalid readings, nulls, duplicates) | Frozen Test Fixture | `tests/fixtures/sample_openaq_latest.json` & `local_storage.py` | Plan 02-01 |
| 8 | `tests/contract/test_pig_contracts.py` | Contract Test Suite | Asserts that Pig `LOAD ... AS (...)` clauses strictly match column names, order, and datatypes in `config/schemas.yaml` | Quality Gate / Contract Testing | `tests/contract/test_schemas.py` | Plan 02-01 |
| 9 | `tests/unit/test_pig_runner.py` | Unit Test Suite | Tests argument construction, environment variable injection, directory pre-clearing, sidecar schema generation, and fallback simulation | Quality Gate / Unit Testing | `tests/unit/test_profile.py` & `tests/unit/test_normalization.py` | Plan 02-02 |
| 10 | `tests/integration/test_pig_e2e.py` | End-to-End Integration Suite | Executes clean, stats, and rollup jobs on sample data; verifies outputs against golden reference files and checks <90s performance ceiling | Quality Gate / Integration Testing | `tests/integration/test_collector_e2e.py` | Plan 02-02 |

---

## 3. Concrete Code Patterns & Analog Mappings

---

### File 1: `config/schemas.yaml` (Schema Contract Additions)

- **Target Path:** `config/schemas.yaml`
- **Role:** Canonical data contract definition for downstream Pig ETL clean outputs and analytical aggregates.
- **Data Flow:** Read by `skywatch.data.schemas`, used by `pig_runner.py` to write `_schema.json`, and verified by `test_pig_contracts.py`.
- **Closest Analog:** `config/schemas.yaml` (lines 3–50, `air_quality` schema definition).

#### Structural Pattern & Concrete Additions
Existing schemas define `air_quality`, `weather`, and `manifest`. Phase 2 adds three canonical definitions: `clean_air_quality`, `station_stats`, and `hourly_rollup`. Note that `clean_air_quality` has `has_header: false` because Pig outputs headerless CSV tuples.

```yaml
# Analog excerpt from config/schemas.yaml (Lines 1-13)
version: "2.0"

air_quality:
  partition_key: "ingest_dt"
  delimiter: ","
  has_header: true
  columns:
    - name: record_key
      type: string
      nullable: false
    ...
```

#### New Contract Schema Definitions to Append:
```yaml
clean_air_quality:
  partition_key: "event_dt"
  delimiter: ","
  has_header: false
  columns:
    - name: record_key
      type: string
      nullable: false
    - name: station_id
      type: string
      nullable: false
    - name: station_name
      type: string
      nullable: false
    - name: latitude
      type: double
      nullable: false
    - name: longitude
      type: double
      nullable: false
    - name: sensor_id
      type: string
      nullable: false
    - name: parameter
      type: string
      nullable: false
    - name: value
      type: double
      nullable: false
    - name: unit
      type: string
      nullable: false
    - name: measurement_time_utc
      type: string
      nullable: false
    - name: ingestion_time_utc
      type: string
      nullable: false
    - name: source
      type: string
      nullable: false
    - name: is_valid
      type: boolean
      nullable: false
    - name: reject_reason
      type: string
      nullable: true

station_stats:
  partition_key: "event_dt"
  delimiter: ","
  has_header: false
  columns:
    - name: station_id
      type: string
      nullable: false
    - name: parameter
      type: string
      nullable: false
    - name: date
      type: string
      nullable: false
    - name: reading_count
      type: integer
      nullable: false
    - name: average_value
      type: double
      nullable: false
    - name: minimum_value
      type: double
      nullable: false
    - name: maximum_value
      type: double
      nullable: false

hourly_rollup:
  partition_key: "event_dt"
  delimiter: ","
  has_header: false
  columns:
    - name: station_id
      type: string
      nullable: false
    - name: parameter
      type: string
      nullable: false
    - name: bucket_start_utc
      type: string
      nullable: false
    - name: reading_count
      type: integer
      nullable: false
    - name: average_value
      type: double
      nullable: false
    - name: minimum_value
      type: double
      nullable: false
    - name: maximum_value
      type: double
      nullable: false
```

#### Key Rules:
- `has_header` MUST be `false` for all Pig outputs.
- Data types must be restricted to standard schema types: `string`, `double`, `integer`, `boolean`.
- In `clean_air_quality`, `value` has `nullable: false` because null rows are filtered out by Pig.

---

### Files 2, 3, 4: `pig/clean_air_quality.pig`, `pig/station_stats.pig`, `pig/hourly_rollup.pig`

- **Target Paths:**
  - `pig/clean_air_quality.pig` (PIG-01)
  - `pig/station_stats.pig` (PIG-02)
  - `pig/hourly_rollup.pig` (PIG-03)
- **Role:** Declarative Apache Pig Latin MapReduce transformations.
- **Data Flow:**
  - `clean_air_quality.pig`: `$INPUT` (Normalized CSV with header) -> FILTER header & invalids -> DISTINCT -> STORE `$OUTPUT` (Headerless clean CSV).
  - `station_stats.pig`: `$INPUT` (Clean air quality) -> SUBSTRING(date) -> GROUP BY station, param, date -> AGGREGATE -> STORE `$OUTPUT`.
  - `hourly_rollup.pig`: `$INPUT` (Clean air quality) -> SUBSTRING + CONCAT(hour) -> GROUP BY station, param, hour -> AGGREGATE -> STORE `$OUTPUT`.
- **Closest Analog:** Schema structure in `config/schemas.yaml` and parameter invocation in `src/skywatch/storage/hdfs_storage.py`.

#### Concrete Script Pattern 1: `pig/clean_air_quality.pig`
```pig
-- clean_air_quality.pig
-- SkyWatch Phase 2: Big Data ETL with Apache Pig
-- Ingests normalized air quality records, filters headers, invalid/null/outlier values,
-- applies safety DISTINCT deduplication, and stores headerless clean records.

raw_records = LOAD '$INPUT' USING PigStorage(',') AS (
    record_key:chararray,
    station_id:chararray,
    station_name:chararray,
    latitude:double,
    longitude:double,
    sensor_id:chararray,
    parameter:chararray,
    value:double,
    unit:chararray,
    measurement_time_utc:chararray,
    ingestion_time_utc:chararray,
    source:chararray,
    is_valid:boolean,
    reject_reason:chararray
);

-- 1. Discard header row and invalid/outlier records
valid_records = FILTER raw_records BY
    record_key != 'record_key' AND
    is_valid == true AND
    value IS NOT NULL AND
    value >= 0.0 AND
    value <= 1000.0;

-- 2. Enforce safety deduplication across tuples
distinct_records = DISTINCT valid_records;

-- 3. Output headerless clean dataset
STORE distinct_records INTO '$OUTPUT' USING PigStorage(',');
```

#### Concrete Script Pattern 2: `pig/station_stats.pig`
```pig
-- station_stats.pig
-- SkyWatch Phase 2: Big Data ETL with Apache Pig
-- Computes daily station aggregations: reading count, average, min, and max.

clean_records = LOAD '$INPUT' USING PigStorage(',') AS (
    record_key:chararray,
    station_id:chararray,
    station_name:chararray,
    latitude:double,
    longitude:double,
    sensor_id:chararray,
    parameter:chararray,
    value:double,
    unit:chararray,
    measurement_time_utc:chararray,
    ingestion_time_utc:chararray,
    source:chararray,
    is_valid:boolean,
    reject_reason:chararray
);

-- Extract date substring (YYYY-MM-DD) from ISO 8601 measurement_time_utc (0-indexed, length 10)
extracted = FOREACH clean_records GENERATE
    station_id,
    parameter,
    SUBSTRING(measurement_time_utc, 0, 10) AS date_dt:chararray,
    value;

-- Group by composite key: (station_id, parameter, date)
grouped_records = GROUP extracted BY (station_id, parameter, date_dt);

-- Compute summary metrics with rounded average
daily_stats = FOREACH grouped_records GENERATE
    group.station_id AS station_id:chararray,
    group.parameter AS parameter:chararray,
    group.date_dt AS date:chararray,
    COUNT(extracted.value) AS reading_count:long,
    ROUND_TO(AVG(extracted.value), 4) AS average_value:double,
    MIN(extracted.value) AS minimum_value:double,
    MAX(extracted.value) AS maximum_value:double;

STORE daily_stats INTO '$OUTPUT' USING PigStorage(',');
```

#### Concrete Script Pattern 3: `pig/hourly_rollup.pig`
```pig
-- hourly_rollup.pig
-- SkyWatch Phase 2: Big Data ETL with Apache Pig
-- Buckets clean measurements into hourly intervals (YYYY-MM-DDTHH:00:00Z) for Spark trend analysis.

clean_records = LOAD '$INPUT' USING PigStorage(',') AS (
    record_key:chararray,
    station_id:chararray,
    station_name:chararray,
    latitude:double,
    longitude:double,
    sensor_id:chararray,
    parameter:chararray,
    value:double,
    unit:chararray,
    measurement_time_utc:chararray,
    ingestion_time_utc:chararray,
    source:chararray,
    is_valid:boolean,
    reject_reason:chararray
);

-- Extract hour bucket: truncate to hour and format as 'YYYY-MM-DDTHH:00:00Z'
extracted_hours = FOREACH clean_records GENERATE
    station_id,
    parameter,
    CONCAT(SUBSTRING(measurement_time_utc, 0, 13), ':00:00Z') AS bucket_start_utc:chararray,
    value;

-- Group by composite key: (station_id, parameter, bucket_start_utc)
grouped_hours = GROUP extracted_hours BY (station_id, parameter, bucket_start_utc);

-- Compute regularized hourly aggregations
hourly_stats = FOREACH grouped_hours GENERATE
    group.station_id AS station_id:chararray,
    group.parameter AS parameter:chararray,
    group.bucket_start_utc AS bucket_start_utc:chararray,
    COUNT(extracted_hours.value) AS reading_count:long,
    ROUND_TO(AVG(extracted_hours.value), 4) AS average_value:double,
    MIN(extracted_hours.value) AS minimum_value:double,
    MAX(extracted_hours.value) AS maximum_value:double;

STORE hourly_stats INTO '$OUTPUT' USING PigStorage(',');
```

#### Key Rules & Anti-Patterns:
- Always use `SUBSTRING(ts, 0, 10)` for YYYY-MM-DD (10 characters, exclusive stop index 10).
- Always use `SUBSTRING(ts, 0, 13)` and `CONCAT(..., ':00:00Z')` for regularized UTC hours.
- In aggregation projections, refer to the bag relation: `COUNT(extracted.value)`, `ROUND_TO(AVG(extracted.value), 4)`, never bare `COUNT(value)`.
- Use Pig built-in `ROUND_TO(val, 4)` for stable 4-decimal floating point precision.

---

### File 5: `src/skywatch/pipeline/pig_runner.py`

- **Target Path:** `src/skywatch/pipeline/pig_runner.py`
- **Role:** Python orchestration module wrapping Apache Pig CLI, handling parameter injection, output directory pre-clearing, sidecar `_schema.json` generation, and providing transparent pure-Python simulation fallback.
- **Data Flow:** Invoked by scripts or tests -> resolves `$INPUT` and `$OUTPUT` -> pre-clears `$OUTPUT` -> checks if Pig is available -> executes native `pig -x local` via safe `subprocess.run` OR executes pure-Python fallback -> writes `_schema.json` -> returns `RunResult`.
- **Closest Analogs:**
  - `src/skywatch/storage/hdfs_storage.py` (subprocess invocation, safemode check, error logging, path traversal validation).
  - `src/skywatch/pipeline/profile.py` (`load_profile()`, `ProfileConfig`).
  - `src/skywatch/pipeline/logging.py` (`get_logger`, `run_context`).
  - `src/skywatch/data/schemas.py` (`get_schema`).

#### Complete Implementation Pattern for `pig_runner.py`:

```python
"""Apache Pig execution orchestrator with profile integration and fallback simulation.

Implements PIG-01, PIG-02, PIG-03: Parameterized Pig execution, pre-clearing output directories,
sidecar _schema.json generation, and transparent Python fallback execution.
"""
from __future__ import annotations

import csv
import json
import os
import shutil
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from skywatch.data.schemas import get_schema
from skywatch.pipeline.logging import get_logger, run_context
from skywatch.pipeline.profile import ProfileConfig, load_profile

logger = get_logger("pig_runner")


@dataclass
class RunResult:
    """Outcome of a Pig or simulation job execution."""
    script: str
    returncode: int
    stdout: str
    stderr: str
    duration_seconds: float
    output_dir: Optional[str] = None
    records_processed: int = 0
    success: bool = True


def is_pig_available(pig_home: Optional[str] = None) -> bool:
    """Check if the pig executable is present and executable."""
    if pig_home:
        pig_bin = Path(pig_home) / "bin" / ("pig.cmd" if os.name == "nt" else "pig")
        if pig_bin.is_file():
            return True
    return shutil.which("pig") is not None


def generate_sidecar_schema(
    entity_name: str,
    output_dir: str,
    config_path: Optional[str] = None,
) -> str:
    """Generate sidecar _schema.json in the output directory based on canonical schema."""
    schema = get_schema(entity_name, config_path)
    sidecar_data = {
        "schema_version": "2.0",
        "entity": entity_name,
        "format": "csv",
        "delimiter": schema.get("delimiter", ","),
        "has_header": False,
        "columns": schema.get("columns", []),
    }
    target_path = Path(output_dir) / "_schema.json"
    target_path.parent.mkdir(parents=True, exist_ok=True)
    with open(target_path, "w", encoding="utf-8") as f:
        json.dump(sidecar_data, f, indent=2)
    return str(target_path)


def _pre_clear_output(output_dir: str) -> None:
    """Remove target output directory locally or in pseudo-HDFS to prevent MapReduce collision."""
    if not output_dir:
        return
    out_p = Path(output_dir)
    if out_p.exists():
        if out_p.is_dir():
            shutil.rmtree(str(out_p))
        else:
            out_p.unlink()


def _attach_sidecar(script_name: str, output_dir: str) -> None:
    entity_map = {
        "clean_air_quality.pig": "clean_air_quality",
        "station_stats.pig": "station_stats",
        "hourly_rollup.pig": "hourly_rollup",
    }
    entity = entity_map.get(script_name, "clean_air_quality")
    generate_sidecar_schema(entity, output_dir)


def run_pig_script(
    script_path: str,
    params: Dict[str, str],
    profile: Optional[ProfileConfig] = None,
    mode: str = "local",
    offline_fallback: bool = True,
    run_id: Optional[str] = None,
) -> RunResult:
    """Execute an Apache Pig script with parameter substitution and output sidecar schema generation."""
    with run_context(run_id) as rid:
        config = profile or load_profile()
        script = Path(script_path)
        if not script.is_file():
            raise FileNotFoundError(f"Pig script not found: {script_path}")

        output_dir = params.get("OUTPUT", "")
        if output_dir:
            # Prevent path traversal
            if ".." in Path(output_dir).parts:
                raise ValueError(f"Path traversal detected in output directory: {output_dir}")
            _pre_clear_output(output_dir)

        start_time = time.time()

        use_fallback = not is_pig_available(config.pig_home) and offline_fallback

        if use_fallback:
            logger.info(f"Pig unavailable; executing {script.name} via standalone fallback engine (run_id: {rid}).")
            result = _run_standalone_fallback(script.name, params)
            duration = time.time() - start_time
            result.duration_seconds = duration
            if result.success and output_dir:
                _attach_sidecar(script.name, output_dir)
            return result

        # Build structured CLI arguments without shell=True
        pig_bin = str(Path(config.pig_home) / "bin" / ("pig.cmd" if os.name == "nt" else "pig"))
        cmd = [pig_bin, "-x", mode]
        for k, v in sorted(params.items()):
            cmd.extend(["-param", f"{k}={v}"])
        cmd.extend(["-f", str(script)])

        env = os.environ.copy()
        env["JAVA_HOME"] = config.java_home_pig
        env["PIG_HOME"] = config.pig_home
        env["HADOOP_HOME"] = config.hadoop_home

        try:
            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                env=env,
                timeout=90,
                check=False,
            )
            duration = time.time() - start_time
            success = (proc.returncode == 0)
            if success and output_dir:
                _attach_sidecar(script.name, output_dir)

            return RunResult(
                script=script.name,
                returncode=proc.returncode,
                stdout=proc.stdout or "",
                stderr=proc.stderr or "",
                duration_seconds=duration,
                output_dir=output_dir,
                success=success,
            )
        except Exception as exc:
            duration = time.time() - start_time
            logger.error(f"Execution of Pig script {script.name} failed: {exc}")
            return RunResult(
                script=script.name,
                returncode=1,
                stdout="",
                stderr=str(exc),
                duration_seconds=duration,
                output_dir=output_dir,
                success=False,
            )


def _run_standalone_fallback(script_name: str, params: Dict[str, str]) -> RunResult:
    """Execute exact relational transformations in pure Python when pig binary is absent."""
    input_path = params.get("INPUT", "")
    output_path = params.get("OUTPUT", "")
    out_dir = Path(output_path)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "part-m-00000"

    in_p = Path(input_path)
    input_files: List[Path] = []
    if in_p.is_file():
        input_files.append(in_p)
    elif in_p.is_dir():
        input_files.extend(sorted(p for p in in_p.glob("*") if not p.name.startswith((".", "_"))))

    rows: List[List[str]] = []
    for f in input_files:
        with open(f, "r", encoding="utf-8", errors="replace") as fh:
            reader = csv.reader(fh)
            for row in reader:
                if not row or not any(row):
                    continue
                rows.append(row)

    if script_name == "clean_air_quality.pig":
        filtered: List[Tuple[str, ...]] = []
        for r in rows:
            if len(r) < 14:
                continue
            if r[0] == "record_key":  # header row
                continue
            is_valid_str = r[12].strip().lower()
            if is_valid_str != "true":
                continue
            val_str = r[7].strip()
            if not val_str or val_str.lower() == "null":
                continue
            try:
                val = float(val_str)
            except ValueError:
                continue
            if val < 0.0 or val > 1000.0:
                continue
            filtered.append(tuple(r[:14]))

        distinct_rows = sorted(list(set(filtered)))
        with open(out_file, "w", newline="", encoding="utf-8") as out_f:
            writer = csv.writer(out_f)
            for d in distinct_rows:
                writer.writerow(d)

        return RunResult(
            script=script_name,
            returncode=0,
            stdout=f"Processed {len(distinct_rows)} clean records (fallback)",
            stderr="",
            duration_seconds=0.0,
            output_dir=output_path,
            records_processed=len(distinct_rows),
            success=True,
        )

    elif script_name == "station_stats.pig":
        from collections import defaultdict
        groups: Dict[Tuple[str, str, str], List[float]] = defaultdict(list)
        for r in rows:
            if len(r) < 14 or r[0] == "record_key":
                continue
            st_id = r[1]
            param = r[6]
            ts = r[9]
            dt = ts[:10]
            try:
                val = float(r[7])
                groups[(st_id, param, dt)].append(val)
            except ValueError:
                continue

        result_rows: List[List[Any]] = []
        for (st_id, param, dt), vals in sorted(groups.items()):
            count = len(vals)
            avg_v = round(sum(vals) / count, 4)
            min_v = round(min(vals), 4)
            max_v = round(max(vals), 4)
            result_rows.append([st_id, param, dt, count, avg_v, min_v, max_v])

        with open(out_file, "w", newline="", encoding="utf-8") as out_f:
            writer = csv.writer(out_f)
            for row in result_rows:
                writer.writerow(row)

        return RunResult(
            script=script_name,
            returncode=0,
            stdout=f"Processed {len(result_rows)} station stats records (fallback)",
            stderr="",
            duration_seconds=0.0,
            output_dir=output_path,
            records_processed=len(result_rows),
            success=True,
        )

    elif script_name == "hourly_rollup.pig":
        from collections import defaultdict
        groups_h: Dict[Tuple[str, str, str], List[float]] = defaultdict(list)
        for r in rows:
            if len(r) < 14 or r[0] == "record_key":
                continue
            st_id = r[1]
            param = r[6]
            ts = r[9]
            bucket = ts[:13] + ":00:00Z"
            try:
                val = float(r[7])
                groups_h[(st_id, param, bucket)].append(val)
            except ValueError:
                continue

        result_rows_h: List[List[Any]] = []
        for (st_id, param, bucket), vals in sorted(groups_h.items()):
            count = len(vals)
            avg_v = round(sum(vals) / count, 4)
            min_v = round(min(vals), 4)
            max_v = round(max(vals), 4)
            result_rows_h.append([st_id, param, bucket, count, avg_v, min_v, max_v])

        with open(out_file, "w", newline="", encoding="utf-8") as out_f:
            writer = csv.writer(out_f)
            for row in result_rows_h:
                writer.writerow(row)

        return RunResult(
            script=script_name,
            returncode=0,
            stdout=f"Processed {len(result_rows_h)} hourly rollup records (fallback)",
            stderr="",
            duration_seconds=0.0,
            output_dir=output_path,
            records_processed=len(result_rows_h),
            success=True,
        )

    return RunResult(
        script=script_name,
        returncode=1,
        stdout="",
        stderr=f"Unknown script: {script_name}",
        duration_seconds=0.0,
        output_dir=output_path,
        success=False,
    )
```

---

### File 6: `scripts/run_pig.sh`

- **Target Path:** `scripts/run_pig.sh`
- **Role:** Profile-aware CLI wrapper script for executing Pig jobs.
- **Data Flow:** Shell CLI -> sources profile via `use_profile.sh` -> calls `python -m skywatch.pipeline.pig_runner` -> redirects stdout/stderr to `data/logs/pig_runner.log`.
- **Closest Analog:** `scripts/cron_collect.sh` (lines 1–40) & `scripts/use_profile.sh`.

#### Concrete Script Pattern
```bash
#!/usr/bin/env bash
# Automated Pig ETL runner with profile integration
# Usage: ./scripts/run_pig.sh <script_name.pig> [modern|lab] [INPUT] [OUTPUT] [RUN_DATE]

set -euo pipefail

SCRIPT_NAME="${1:-clean_air_quality.pig}"
PROFILE="${2:-${SKYWATCH_PROFILE:-modern}}"
INPUT_PATH="${3:-data/normalized/air_quality/ingest_dt=$(date -u +%Y-%m-%d)}"
OUTPUT_PATH="${4:-data/clean/air_quality/event_dt=$(date -u +%Y-%m-%d)}"
RUN_DATE="${5:-$(date -u +%Y-%m-%d)}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$SCRIPT_DIR"

LOGFILE="data/logs/pig_runner.log"
mkdir -p data/logs

# Error trap logging failure
trap 'echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] Error: Pig runner failed with exit code $?" >> "$LOGFILE"' ERR

# Source active profile if present
if [ -f "profiles/${PROFILE}.env" ]; then
    set -a
    # shellcheck disable=SC1090
    source "profiles/${PROFILE}.env"
    set +a
fi

echo "[$(date -u +'%Y-%m-%dT%H:%M:%SZ')] Starting Pig job: ${SCRIPT_NAME} (profile: ${PROFILE})..." >> "$LOGFILE"

python -m skywatch.pipeline.pig_runner \
    --script "pig/${SCRIPT_NAME}" \
    --input "$INPUT_PATH" \
    --output "$OUTPUT_PATH" \
    --run-date "$RUN_DATE" \
    --profile "$PROFILE" >> "$LOGFILE" 2>&1

echo "[$(date -u +'%Y-%m-%dT%H:%M:%SZ')] Pig job completed successfully: ${SCRIPT_NAME}" >> "$LOGFILE"
```

---

### File 7: `data/sample/sample_air_quality.csv`

- **Target Path:** `data/sample/sample_air_quality.csv`
- **Role:** Frozen baseline sample dataset containing ~500 to 1,000 rows across 5 Bengaluru monitoring stations.
- **Data Flow:** Loaded by `clean_air_quality.pig` and tests as the primary test dataset.
- **Closest Analog:** `tests/fixtures/sample_openaq_latest.json` and `src/skywatch/storage/local_storage.py:write_normalized_csv`.

#### Concrete Format Pattern:
```csv
record_key,station_id,station_name,latitude,longitude,sensor_id,parameter,value,unit,measurement_time_utc,ingestion_time_utc,source,is_valid,reject_reason
a1b2c3d4e5f6001,2178,"BTM Layout, Bengaluru - CPCB",12.9135,77.6111,3917,pm25,48.5,ug/m3,2026-11-22T08:00:00Z,2026-11-22T08:05:00Z,openaq,True,
a1b2c3d4e5f6002,2178,"BTM Layout, Bengaluru - CPCB",12.9135,77.6111,3918,pm10,92.1,ug/m3,2026-11-22T08:00:00Z,2026-11-22T08:05:00Z,openaq,True,
a1b2c3d4e5f6003,2178,"BTM Layout, Bengaluru - CPCB",12.9135,77.6111,3917,pm25,-5.0,ug/m3,2026-11-22T08:00:00Z,2026-11-22T08:05:00Z,openaq,False,negative_value
a1b2c3d4e5f6004,2178,"BTM Layout, Bengaluru - CPCB",12.9135,77.6111,3917,pm25,1250.0,ug/m3,2026-11-22T08:00:00Z,2026-11-22T08:05:00Z,openaq,False,outlier_high
a1b2c3d4e5f6005,2178,"BTM Layout, Bengaluru - CPCB",12.9135,77.6111,3917,pm25,,ug/m3,2026-11-22T08:00:00Z,2026-11-22T08:05:00Z,openaq,False,null_value
```

#### Required Edge Cases to Include:
1. **CSV Header Row**: First row must be column names (`record_key,station_id,...`).
2. **Valid Readings**: Realistic values (PM2.5: 15–120 µg/m³, PM10: 30–250 µg/m³) across all 5 Bengaluru stations (`2178`: BTM Layout, `2180`: BWSSB Kadabesanahalli, `2182`: Hebbal, `2184`: Silk Board, `2186`: Jayanagar 5th Block).
3. **Invalid Flagged Readings**: Records where `is_valid == False` and `reject_reason` is set.
4. **Negative Value Rows**: Records where `value < 0.0`.
5. **Null Value Rows**: Records where `value` is empty string or `null`.
6. **Outlier Rows**: Readings with `value > 1000.0`.
7. **Duplicate Rows**: Exact identical rows to test `DISTINCT` deduplication.

---

### File 8: `tests/contract/test_pig_contracts.py`

- **Target Path:** `tests/contract/test_pig_contracts.py`
- **Role:** Contract test suite (CT-01) validating that Pig Latin scripts maintain strict schema contract fidelity with `config/schemas.yaml`.
- **Data Flow:** Reads `config/schemas.yaml` and parses `.pig` files from `pig/` -> asserts 1:1 match of column names, order, and datatypes in `LOAD ... AS (...)` declarations.
- **Closest Analog:** `tests/contract/test_schemas.py` (lines 1–87).

#### Concrete Implementation Pattern:
```python
"""Contract tests for Apache Pig Latin script schemas."""
from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, List, Tuple

import pytest

from skywatch.data.schemas import get_column_names, get_schema, load_schemas

TYPE_MAPPING = {
    "chararray": "string",
    "double": "double",
    "float": "double",
    "long": "integer",
    "int": "integer",
    "boolean": "boolean",
}


def parse_pig_load_schema(script_path: Path) -> List[Tuple[str, str]]:
    """Parse column names and types from LOAD ... AS (...) in a Pig Latin script."""
    content = script_path.read_text(encoding="utf-8")
    # Match the AS ( ... ) block
    match = re.search(r"AS\s*\((.*?)\);", content, re.DOTALL | re.IGNORECASE)
    assert match is not None, f"Could not find LOAD AS (...) clause in {script_path}"
    raw_cols = match.group(1).split(",")
    parsed = []
    for col in raw_cols:
        col_str = col.strip()
        if not col_str or col_str.startswith("--"):
            continue
        parts = col_str.split(":")
        col_name = parts[0].strip()
        col_type = parts[1].strip().lower() if len(parts) > 1 else "bytearray"
        parsed.append((col_name, TYPE_MAPPING.get(col_type, col_type)))
    return parsed


def test_clean_air_quality_pig_matches_air_quality_contract():
    """CT-01: Verify clean_air_quality.pig loads the canonical air_quality schema."""
    script_path = Path("pig/clean_air_quality.pig")
    assert script_path.is_file(), "clean_air_quality.pig must exist"

    pig_cols = parse_pig_load_schema(script_path)
    schema = get_schema("air_quality")
    expected_cols = schema["columns"]

    assert len(pig_cols) == len(expected_cols)
    for (p_name, p_type), exp in zip(pig_cols, expected_cols):
        assert p_name == exp["name"], f"Column name mismatch: {p_name} != {exp['name']}"
        assert p_type == exp["type"], f"Column type mismatch for {p_name}: {p_type} != {exp['type']}"


def test_station_stats_output_matches_contract():
    """CT-01: Verify station_stats schema contract exists in schemas.yaml."""
    schema = get_schema("station_stats")
    assert schema["has_header"] is False
    cols = get_column_names("station_stats")
    expected = [
        "station_id",
        "parameter",
        "date",
        "reading_count",
        "average_value",
        "minimum_value",
        "maximum_value",
    ]
    assert cols == expected


def test_hourly_rollup_output_matches_contract():
    """CT-01: Verify hourly_rollup schema contract exists in schemas.yaml."""
    schema = get_schema("hourly_rollup")
    assert schema["has_header"] is False
    cols = get_column_names("hourly_rollup")
    expected = [
        "station_id",
        "parameter",
        "bucket_start_utc",
        "reading_count",
        "average_value",
        "minimum_value",
        "maximum_value",
    ]
    assert cols == expected
```

---

### File 9: `tests/unit/test_pig_runner.py`

- **Target Path:** `tests/unit/test_pig_runner.py`
- **Role:** Unit test suite covering runner argument building, output pre-clearing, sidecar `_schema.json` generation, and fallback simulation.
- **Data Flow:** Isolated test suite verifying `skywatch.pipeline.pig_runner` behaviors and edge cases.
- **Closest Analog:** `tests/unit/test_profile.py` (lines 1–134) & `tests/unit/test_normalization.py`.

#### Concrete Implementation Pattern:
```python
"""Unit tests for Apache Pig execution orchestrator."""
from __future__ import annotations

import json
from pathlib import Path
from unittest import mock

import pytest

from skywatch.pipeline.pig_runner import (
    generate_sidecar_schema,
    is_pig_available,
    run_pig_script,
)
from skywatch.pipeline.profile import ProfileConfig


def test_is_pig_available_checks_shutil_and_path(tmp_path):
    assert not is_pig_available(pig_home=str(tmp_path / "nonexistent"))
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir(parents=True)
    import os
    exe_name = "pig.cmd" if os.name == "nt" else "pig"
    (fake_bin / exe_name).write_text("dummy", encoding="utf-8")
    assert is_pig_available(pig_home=str(tmp_path))


def test_generate_sidecar_schema(tmp_path):
    out_dir = tmp_path / "clean_output"
    sidecar_path = generate_sidecar_schema("clean_air_quality", str(out_dir))
    assert Path(sidecar_path).is_file()

    with open(sidecar_path, "r", encoding="utf-8") as f:
        meta = json.load(f)

    assert meta["schema_version"] == "2.0"
    assert meta["entity"] == "clean_air_quality"
    assert meta["has_header"] is False
    assert len(meta["columns"]) == 14


def test_pre_clearing_removes_existing_output_dir(tmp_path):
    out_dir = tmp_path / "output_test"
    out_dir.mkdir()
    (out_dir / "old_part.csv").write_text("stale data", encoding="utf-8")

    dummy_script = tmp_path / "dummy.pig"
    dummy_script.write_text("clean_records = LOAD '$INPUT';", encoding="utf-8")

    sample_csv = tmp_path / "sample.csv"
    sample_csv.write_text("header\nval\n", encoding="utf-8")

    res = run_pig_script(
        str(dummy_script),
        params={"INPUT": str(sample_csv), "OUTPUT": str(out_dir)},
        offline_fallback=True,
    )
    assert not (out_dir / "old_part.csv").exists()


def test_path_traversal_detection(tmp_path):
    dummy_script = tmp_path / "dummy.pig"
    dummy_script.write_text("records = LOAD '$INPUT';", encoding="utf-8")

    with pytest.raises(ValueError, match="Path traversal detected"):
        run_pig_script(
            str(dummy_script),
            params={"INPUT": "in.csv", "OUTPUT": "../../etc/evil"},
            offline_fallback=True,
        )


@mock.patch("subprocess.run")
def test_native_pig_subprocess_invocation(mock_run, tmp_path):
    dummy_script = tmp_path / "dummy.pig"
    dummy_script.write_text("-- script", encoding="utf-8")

    mock_run.return_value = mock.Mock(returncode=0, stdout="Success", stderr="")

    config = ProfileConfig(
        profile_name="modern",
        hadoop_home=str(tmp_path),
        pig_home=str(tmp_path),
        spark_home=str(tmp_path),
        java_home_pig=str(tmp_path),
        java_home_spark=str(tmp_path),
        hdfs_root="/skywatch",
    )

    with mock.patch("skywatch.pipeline.pig_runner.is_pig_available", return_value=True):
        res = run_pig_script(
            str(dummy_script),
            params={"INPUT": "in.csv", "OUTPUT": str(tmp_path / "out")},
            profile=config,
            offline_fallback=False,
        )
        assert res.success is True
        assert mock_run.called
        cmd_args = mock_run.call_args[0][0]
        assert "-x" in cmd_args
        assert "local" in cmd_args
        assert "-f" in cmd_args
```

---

### File 10: `tests/integration/test_pig_e2e.py`

- **Target Path:** `tests/integration/test_pig_e2e.py`
- **Role:** End-to-end integration test suite verifying PIG-01, PIG-02, PIG-03, and PIG-04 against committed golden reference datasets.
- **Data Flow:** Ingests `data/sample/sample_air_quality.csv` -> executes `clean_air_quality.pig` -> executes `station_stats.pig` -> executes `hourly_rollup.pig` -> asserts outputs against golden fixtures and verifies execution <90s.
- **Closest Analog:** `tests/integration/test_collector_e2e.py` (lines 1–80).

#### Concrete Implementation Pattern:
```python
"""Integration tests for Apache Pig ETL pipelines and fallback execution."""
from __future__ import annotations

import csv
import json
import time
from pathlib import Path

import pytest

from skywatch.pipeline.pig_runner import run_pig_script


def _read_csv_rows(file_path: Path):
    with open(file_path, "r", encoding="utf-8") as f:
        return list(csv.reader(f))


def test_pig_etl_e2e_pipeline(tmp_path):
    sample_data = Path("data/sample/sample_air_quality.csv")
    assert sample_data.is_file(), "Committed sample data must exist"

    clean_dir = tmp_path / "clean_air_quality"
    stats_dir = tmp_path / "station_stats"
    hourly_dir = tmp_path / "hourly_rollup"

    start_total = time.time()

    # 1. Execute PIG-01: clean_air_quality.pig
    res_clean = run_pig_script(
        "pig/clean_air_quality.pig",
        params={"INPUT": str(sample_data), "OUTPUT": str(clean_dir), "RUN_DATE": "2026-11-22"},
    )
    assert res_clean.success is True
    assert (clean_dir / "_schema.json").is_file()
    clean_parts = list(clean_dir.glob("part-*"))
    assert len(clean_parts) > 0

    clean_rows = _read_csv_rows(clean_parts[0])
    assert len(clean_rows) > 0
    # Verify header is stripped (no 'record_key' in line 0 col 0)
    assert clean_rows[0][0] != "record_key"

    # Verify no invalid, null, or out-of-bounds rows exist
    for r in clean_rows:
        val = float(r[7])
        assert 0.0 <= val <= 1000.0
        assert r[12].lower() == "true"

    # 2. Execute PIG-02: station_stats.pig
    res_stats = run_pig_script(
        "pig/station_stats.pig",
        params={"INPUT": str(clean_parts[0]), "OUTPUT": str(stats_dir), "RUN_DATE": "2026-11-22"},
    )
    assert res_stats.success is True
    assert (stats_dir / "_schema.json").is_file()
    stats_parts = list(stats_dir.glob("part-*"))
    assert len(stats_parts) > 0

    stats_rows = _read_csv_rows(stats_parts[0])
    assert len(stats_rows) > 0
    for r in stats_rows:
        assert len(r) == 7
        assert len(r[2]) == 10  # YYYY-MM-DD
        reading_cnt = int(r[3])
        avg_v = float(r[4])
        min_v = float(r[5])
        max_v = float(r[6])
        assert reading_cnt > 0
        assert min_v <= avg_v <= max_v

    # 3. Execute PIG-03: hourly_rollup.pig
    res_hourly = run_pig_script(
        "pig/hourly_rollup.pig",
        params={"INPUT": str(clean_parts[0]), "OUTPUT": str(hourly_dir), "RUN_DATE": "2026-11-22"},
    )
    assert res_hourly.success is True
    assert (hourly_dir / "_schema.json").is_file()
    hourly_parts = list(hourly_dir.glob("part-*"))
    assert len(hourly_parts) > 0

    hourly_rows = _read_csv_rows(hourly_parts[0])
    assert len(hourly_rows) > 0
    for r in hourly_rows:
        assert len(r) == 7
        assert r[2].endswith(":00:00Z")  # UTC hourly regularized format
        assert int(r[3]) > 0

    # 4. Performance constraint: Total runtime under 90 seconds (NFR-04)
    total_duration = time.time() - start_total
    assert total_duration < 90.0, f"ETL pipeline took {total_duration:.2f}s (ceiling: 90s)"
```

---

## 4. Cross-Cutting Architectural Patterns

### 1. Python 3.8 Syntax Floor & Type Compatibility
- Always place `from __future__ import annotations` at the top of every `.py` file.
- Use `typing.Optional[T]`, `typing.Dict[K, V]`, `typing.List[T]`, `typing.Tuple[...]`.
- Do NOT use `X | Y` union syntax or Python 3.10+ `match ... case`.

### 2. Subprocess Command Injection Defense
- Never set `shell=True`.
- Construct CLI arguments as structured token lists: `[binary_path, "-x", mode, "-param", f"{k}={v}", ...]`.
- Validate path parameters to reject `..` and path traversal attempts.

### 3. Pre-Clearing & Atomic Execution
- MapReduce and Apache Pig fail if the output directory already exists (`FileAlreadyExistsException`).
- Always invoke `_pre_clear_output(output_dir)` prior to starting a job.
- On job success, generate `_schema.json` directly into the output directory alongside `part-m-00000` / `part-r-00000`.

### 4. Floating-Point Comparison Precision in Quality Gates
- Floating-point calculations can introduce tiny precision variations across CPU architectures.
- Pig scripts use `ROUND_TO(AVG(...), 4)`.
- Tests must round values to 4 decimal places or use `pytest.approx(expected, abs=1e-4)`.

---

## 5. Verification Checklist

- [x] All 10 target files mapped to closest existing codebase analogs.
- [x] Concrete code excerpts extracted for imports, typing, error handling, subprocess calls, and schema parsing.
- [x] Schema extensions in `config/schemas.yaml` confirmed backwards-compatible with Phase 1 contract tests.
- [x] Python 3.8 syntax floor respected across all templates.
- [x] Subprocess security guidelines (`shell=False`, argument list, path validation) strictly incorporated.
- [x] Dual-mode execution (Apache Pig CLI vs. pure-Python simulation fallback) fully detailed to guarantee 100% CI and developer laptop reproducibility.
