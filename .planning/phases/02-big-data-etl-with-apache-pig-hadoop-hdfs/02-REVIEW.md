---
phase: 02-big-data-etl-with-apache-pig-hadoop-hdfs
reviewed: 2026-11-22
depth: standard
files_reviewed: 11
files_reviewed_list:
  - config/schemas.yaml
  - pig/clean_air_quality.pig
  - pig/hourly_rollup.pig
  - pig/station_stats.pig
  - scripts/generate_sample_fixtures.py
  - scripts/run_pig.sh
  - src/skywatch/pipeline/pig_runner.py
  - tests/contract/test_pig_contracts.py
  - tests/contract/test_schemas.py
  - tests/integration/test_pig_e2e.py
  - tests/unit/test_pig_runner.py
findings:
  critical: 3
  warning: 5
  info: 2
  total: 10
  necessity: fix_before_proceeding
status: issues_found
---

# Adversarial Code Review: Phase 2 — Big Data ETL with Apache Pig & Hadoop HDFS

## Executive Summary

Phase 2 implementation—comprising Apache Pig Latin ETL scripts, the Python Pig runner orchestrator, standalone simulation engine, shell wrappers, schema extensions, and unit/contract/integration test suites—underwent standard depth adversarial review. The implementation demonstrates robust relational modeling and thorough test coverage (63 passing tests). However, **3 Critical defects**, **5 Warning defects**, and **2 Info defects** were identified across file safety, path traversal security, native Apache Pig compatibility, shell parameter defaulting, and test determinism.

The most critical findings:
1. **Destructive Unchecked Directory Deletion (CR-01):** `_pre_clear_output` in `pig_runner.py` executes `shutil.rmtree` without verifying that the target path is not the repository root, filesystem root, or current working directory.
2. **Path Traversal via Absolute Paths (CR-02):** The security check in `run_pig_script` only inspects relative `..` path tokens, permitting arbitrary absolute filesystem paths to be wiped and written to.
3. **Vanilla Pig Syntax Incompatibility (CR-03):** `ROUND_TO` is invoked in `station_stats.pig` and `hourly_rollup.pig` without defining or registering Piggybank UDFs, which fails on vanilla Apache Pig installations.

---

## Critical Findings (CR-01 - CR-03)

### CR-01: Destructive Unchecked Directory Deletion in `_pre_clear_output`
- **File:** `src/skywatch/pipeline/pig_runner.py:95-103`
- **Issue:** `_pre_clear_output(output_dir)` receives a directory string and immediately calls `shutil.rmtree(path)`. If `OUTPUT` is `.` (current working directory), empty string `""`, root `/`, `C:\`, or an untrusted parameter, `shutil.rmtree` recursively wipes the entire workspace or system directory without confirming that the path is not the cwd, repo root, or filesystem root.
- **Fix:** Validate that `output_dir` is not empty, resolve it, and verify it is not the filesystem root or current working directory:
```python
def _pre_clear_output(output_dir: str) -> None:
    """Pre-clear target output directory to prevent MapReduce FileAlreadyExistsException."""
    if not output_dir or not output_dir.strip():
        raise ValueError("Cannot pre-clear empty output directory path.")

    path = Path(output_dir).resolve()
    cwd = Path.cwd().resolve()

    if path == cwd or path == Path(path.anchor):
        raise ValueError(f"Refusing to pre-clear dangerous directory: '{output_dir}'")

    if path.exists():
        if path.is_dir():
            shutil.rmtree(path)
        else:
            os.remove(path)
        logger.info("Pre-cleared existing output directory", extra={"output_dir": str(path)})
```

---

### CR-02: Path Traversal via Arbitrary Absolute Paths in `run_pig_script`
- **File:** `src/skywatch/pipeline/pig_runner.py:326-328`
- **Issue:** `run_pig_script` performs a path traversal check using `if ".." in Path(output_dir).parts: raise ValueError(...)`. While this catches relative `..` sequences, it permits arbitrary absolute paths (e.g., `/etc/data` or `C:\Windows\System32`). When supplied, `_pre_clear_output` will destroy the external directory, and `_run_standalone_fallback` or Pig will write files into it.
- **Fix:** Enforce that `output_dir` resolves strictly within the repository workspace (or an explicitly configured staging base):
```python
resolved_out = Path(output_dir).resolve()
repo_root = Path.cwd().resolve()
try:
    resolved_out.relative_to(repo_root)
except ValueError:
    raise ValueError(f"OUTPUT path must resolve within repository root: '{output_dir}'")
```

---

### CR-03: Vanilla Apache Pig Incompatibility: Calling `ROUND_TO` Without Piggybank Registration
- **File:** `pig/station_stats.pig:34`, `pig/hourly_rollup.pig:34`
- **Issue:** In standard Apache Pig (0.17+ / MapReduce), `ROUND_TO` is not a core built-in keyword. Pig's core built-in is `ROUND(double)` (which returns integer `long`). `ROUND_TO` resides in the optional Piggybank UDF library (`org.apache.pig.piggybank.evaluation.math.DoubleRoundTo`). When executed on an Apache Pig installation without `REGISTER piggybank.jar` and `DEFINE ROUND_TO ...`, Pig compilation halts with `Could not resolve ROUND_TO function`.
- **Fix:** Use standard arithmetic rounding `ROUND(x * 10000.0) / 10000.0` or provide explicit `DEFINE ROUND_TO` / `REGISTER` declarations in both Pig Latin scripts:
```pig
ROUND(AVG(extracted.value) * 10000.0) / 10000.0 AS average_value:double
```

---

## Warning Findings (WR-01 - WR-05)

### WR-01: Default Path Misconfiguration in `scripts/run_pig.sh` for Non-Clean Scripts
- **File:** `scripts/run_pig.sh:18-19`
- **Issue:** `INPUT_PATH` defaults to `data/normalized/air_quality/ingest_dt=${RUN_DATE}` and `OUTPUT_PATH` defaults to `data/clean/air_quality/event_dt=${RUN_DATE}` regardless of `SCRIPT_NAME`. Running `./scripts/run_pig.sh station_stats.pig` or `hourly_rollup.pig` with default parameters reads normalized data instead of clean data, and overwrites `data/clean/air_quality/` with aggregated statistics, wiping out the clean air quality dataset!
- **Fix:** Contextually resolve defaults based on `SCRIPT_NAME`:
```bash
case "$SCRIPT_NAME" in
    *station_stats*)
        INPUT_PATH="${3:-data/clean/air_quality/event_dt=${RUN_DATE}}"
        OUTPUT_PATH="${4:-data/analytics/station_stats/event_dt=${RUN_DATE}}"
        ;;
    *hourly_rollup*)
        INPUT_PATH="${3:-data/clean/air_quality/event_dt=${RUN_DATE}}"
        OUTPUT_PATH="${4:-data/analytics/hourly_rollup/event_dt=${RUN_DATE}}"
        ;;
    *)
        INPUT_PATH="${3:-data/normalized/air_quality/ingest_dt=${RUN_DATE}}"
        OUTPUT_PATH="${4:-data/clean/air_quality/event_dt=${RUN_DATE}}"
        ;;
esac
```

---

### WR-02: Fragile Unsorted Row Equality in Integration Test `test_clean_air_quality_e2e_matches_golden`
- **File:** `tests/integration/test_pig_e2e.py:31`
- **Issue:** `clean_air_quality.pig` runs `distinct_records = DISTINCT valid_records;` without an `ORDER BY` clause. In Hadoop MapReduce, reducer outputs are partitioned across part files without global row order guarantees. In `test_clean_air_quality_e2e_matches_golden`, `assert actual_rows == golden_rows` checks exact sequence order, which will fail intermittently when executed on a real Hadoop cluster with multiple reduce tasks.
- **Fix:** Sort both lists before asserting equality:
```python
assert sorted(actual_rows) == sorted(golden_rows)
```

---

### WR-03: Incomplete Windows Executable Resolution in `run_pig_script`
- **File:** `src/skywatch/pipeline/pig_runner.py:347-352`
- **Issue:** `is_pig_available()` checks `pig`, `pig.cmd`, and `pig.exe`. However, `run_pig_script()` hardcodes `pig_bin = "pig.cmd" if os.name == "nt" else "pig"`. If `PIG_HOME/bin` has `pig` (a shell wrapper) or `pig.exe`, `bin_path.is_file()` evaluates to false, causing execution to fail by falling back to the unverified literal `"pig.cmd"`.
- **Fix:** Iterate candidate binary names in `PIG_HOME/bin/`:
```python
pig_bin = "pig.cmd" if os.name == "nt" else "pig"
if active_profile.pig_home:
    for candidate in (["pig.cmd", "pig.exe", "pig"] if os.name == "nt" else ["pig"]):
        c_path = Path(active_profile.pig_home) / "bin" / candidate
        if c_path.is_file():
            pig_bin = str(c_path)
            break
```

---

### WR-04: Glob Input Resolution Silently Discards Partition Directories in `_resolve_input_files`
- **File:** `src/skywatch/pipeline/pig_runner.py:139-141`
- **Issue:** If `input_param` is a wildcard matching partitioned folders (e.g. `data/normalized/air_quality/*`), `glob.glob` returns directory paths. Line 140 filters `if Path(p).is_file()`, discarding all partition folders and raising `FileNotFoundError`.
- **Fix:** Recursively include data files when glob targets match directories:
```python
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
```

---

### WR-05: Silent Schema Attachment Failure on Script Path Variations in `_attach_sidecar`
- **File:** `src/skywatch/pipeline/pig_runner.py:106-121`
- **Issue:** `_attach_sidecar` exact-matches `"clean_air_quality.pig"`, `"station_stats.pig"`, and `"hourly_rollup.pig"`. If a script is invoked with prefixes/suffixes (e.g., `pig/clean_air_quality_v1.pig`), `_attach_sidecar` returns `None` without warning, causing downstream Spark jobs to fail schema validation.
- **Fix:** Use substring matching fallback and emit a warning log when no schema mapping is found:
```python
entity = None
for k, v in script_to_entity.items():
    if k in script_base:
        entity = v
        break
if entity:
    return generate_sidecar_schema(entity, output_dir, config_path)
logger.warning("No canonical entity matching script name; skipping sidecar _schema.json", extra={"script": script_name})
return None
```

---

## Info Findings (IN-01 - IN-02)

### IN-01: Missing `--entity` CLI Parameter in `pig_runner.py`
- **File:** `src/skywatch/pipeline/pig_runner.py:429-438`
- **Issue:** `main()` CLI entrypoint supports `--script`, `--input`, `--output`, `--run-date`, `--profile`, and `--mode`, but lacks an optional `--entity` parameter to allow users to override sidecar schema generation.
- **Fix:** Add `--entity` in `argparse` and pass to `run_pig_script`.

---

### IN-02: Hardcoded Schema Metadata in `scripts/generate_sample_fixtures.py`
- **File:** `scripts/generate_sample_fixtures.py:171`
- **Issue:** `generate_sample_fixtures.py` hardcodes a literal `golden_schema` dictionary instead of calling `generate_sidecar_schema("clean_air_quality", ...)`.
- **Fix:** Import and invoke `generate_sidecar_schema` directly to ensure DRY schema contracts.
