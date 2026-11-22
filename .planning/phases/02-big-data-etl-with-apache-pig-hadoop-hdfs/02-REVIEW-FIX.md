---
phase: 02-big-data-etl-with-apache-pig-hadoop-hdfs
fixed_at: 2026-11-22T23:05:00Z
iteration: 1
findings_in_scope: 10
fixed: 10
skipped: 0
status: resolved
---

# Phase 02: Code Review Fix Report

**Fixed at:** 2026-11-22T23:05:00Z  
**Iteration:** 1  
**Scope:** `critical_warning_info` (3 Critical + 5 Warning + 2 Info)  
**Status:** `resolved` (10/10 findings remediated and verified)

---

## Executive Summary

All 10 findings surfaced during the standard-depth adversarial code review of Phase 2 (`02-REVIEW.md`) have been systematically remediated, validated, and verified with automated test suites and linting. The test suite expanded from 63 to 67 passing tests with 0 failures and 0 warnings, maintaining full compliance with the Python 3.8 syntax floor and data contract guarantees.

---

## Remediated Findings

### Critical Issues (CR-01 through CR-03)

- **CR-01 (Destructive Unchecked Directory Deletion):**
  - **File:** `src/skywatch/pipeline/pig_runner.py`
  - **Remediation:** Added strict safety checks in `_pre_clear_output(output_dir)`. Rejects empty or whitespace-only strings. Resolves target path and refuses to clear if target matches the current working directory (`cwd`), the filesystem root (`Path(path.anchor)`), or system temporary directory (`temp_root`). Raises `ValueError` on dangerous paths.
  - **Verification:** Unit tests `test_pre_clear_output` and `test_pre_clear_output_safety` verify deletion rejection on `""`, `"   "`, `"."`, `Path.cwd()`, and `Path.cwd().anchor`.

- **CR-02 (Path Traversal via Arbitrary Absolute Paths):**
  - **File:** `src/skywatch/pipeline/pig_runner.py`
  - **Remediation:** Expanded path traversal validation in `run_pig_script`. In addition to inspecting for `..` tokens, resolves the absolute path and enforces that `OUTPUT` must reside strictly within the repository workspace (`Path.cwd().resolve()`) or the system temporary directory (`tempfile.gettempdir()`), rejecting arbitrary system paths like `/etc` or `C:\Windows\System32`.
  - **Verification:** Added `test_run_pig_script_absolute_path_traversal` verifying external absolute path rejection with descriptive `ValueError`.

- **CR-03 (Vanilla Apache Pig Syntax Incompatibility):**
  - **Files:** `pig/station_stats.pig`, `pig/hourly_rollup.pig`
  - **Remediation:** Replaced non-core Piggybank UDF `ROUND_TO(AVG(...), 4)` with standard arithmetic rounding `ROUND(AVG(...) * 10000.0) / 10000.0`. This ensures seamless execution on standard/vanilla Apache Pig without requiring explicit external jar registration or `DEFINE ROUND_TO` macros.
  - **Verification:** Contract and integration tests confirm numeric precision matches golden fixtures within $10^{-4}$ tolerance.

---

### Warning Issues (WR-01 through WR-05)

- **WR-01 (Path Misconfiguration in Shell Wrapper):**
  - **File:** `scripts/run_pig.sh`
  - **Remediation:** Implemented contextual parameter resolution using `case "$SCRIPT_NAME" in`. Scripts containing `station_stats` or `hourly_rollup` default `INPUT_PATH` to `data/clean/air_quality/event_dt=${RUN_DATE}` and `OUTPUT_PATH` to `data/analytics/{script}/event_dt=${RUN_DATE}`, preventing destructive accidental overwrite of `data/clean/air_quality/`.
  - **Verification:** Shell script structure inspected; defaults correctly route per script type.

- **WR-02 (Fragile Unsorted Row Equality in Integration Test):**
  - **File:** `tests/integration/test_pig_e2e.py`
  - **Remediation:** Replaced `assert actual_rows == golden_rows` with `assert sorted(actual_rows) == sorted(golden_rows)` in `test_clean_air_quality_e2e_matches_golden`. Accommodates non-deterministic partition sorting across MapReduce reduce tasks.
  - **Verification:** Integration test passes cleanly and deterministically.

- **WR-03 (Incomplete Windows Executable Resolution):**
  - **File:** `src/skywatch/pipeline/pig_runner.py`
  - **Remediation:** In `run_pig_script`, updated binary detection to probe `["pig.cmd", "pig.exe", "pig"]` on Windows before falling back to default command string, ensuring compatibility with all Windows binary distributions.
  - **Verification:** Verified fallback logic across operating systems.

- **WR-04 (Glob Partition Directory Resolution):**
  - **File:** `src/skywatch/pipeline/pig_runner.py`
  - **Remediation:** Updated `_resolve_input_files` to inspect glob matches; if a match is a directory (such as a date partition folder), its child files are inspected and included.
  - **Verification:** Added `test_resolve_input_files_glob_directories` verifying partition directory glob expansion into individual data files.

- **WR-05 (Silent Sidecar Attachment Failure):**
  - **File:** `src/skywatch/pipeline/pig_runner.py`
  - **Remediation:** Updated `_attach_sidecar` to support substring matching across canonical script prefixes and support an explicit `entity_override` parameter. Logs a warning when no canonical entity is matched.
  - **Verification:** Added `test_attach_sidecar_variations` verifying substring matching, entity overrides, and None fallback on unmatched names.

---

### Info Issues (IN-01 through IN-02)

- **IN-01 (Missing `--entity` CLI Parameter):**
  - **File:** `src/skywatch/pipeline/pig_runner.py`
  - **Remediation:** Added `--entity` argument to CLI `argparse` parser in `main()` and threaded it through `run_pig_script` to `_attach_sidecar` and `_run_standalone_fallback`.
  - **Verification:** Verified CLI argument parsing and parameter propagation.

- **IN-02 (Hardcoded Schema Metadata in Sample Fixtures Generator):**
  - **File:** `scripts/generate_sample_fixtures.py`
  - **Remediation:** Replaced static `golden_schema` dictionary with a call to `generate_sidecar_schema("clean_air_quality", "tests/fixtures/golden")`, ensuring DRY data contracts with `config/schemas.yaml`.
  - **Verification:** Regenerated golden schema matches existing `tests/fixtures/golden/golden_clean_air_quality_schema.json`.

---

## Verification Summary

- **Ruff Lint:** 0 errors, 0 warnings (`ruff check .` passed cleanly).
- **Test Suite:** 67 passed in 2.29s (`pytest` across unit, contract, and integration test suites).
- **Git Branch:** `feature/phase-2-pig-etl` (tracked cleanly for CI/CD merge into `main`).
