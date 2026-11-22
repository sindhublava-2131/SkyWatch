---
phase: 01-foundation-data-ingestion-storage-architecture
fixed_at: 2026-11-22T11:36:00Z
iteration: 1
findings_in_scope: 32
fixed: 32
skipped: 0
status: resolved
---

# Phase 01: Code Review Fix Report

**Fixed at:** 2026-11-22T11:36:00Z  
**Iteration:** 1  
**Scope:** `critical_warning` (6 Critical + 26 Warning)  
**Status:** `resolved` (32 in-scope findings fixed; 6 Info items addressed in tandem)

---

## Executive Summary

All 6 Critical defects (CR-01 through CR-06) and 26 Warning defects (WR-01 through WR-26) surfaced during the adversarial code review of Phase 1 have been systematically fixed and verified across the codebase. Accompanying unit, contract, and integration tests were added, bringing the test suite to 44/44 passing in under 2 seconds with zero `ruff` lint warnings.

---

## Remediated Findings

### Critical Issues

- **CR-01 (Security - Bearer Token Scrubbing):** Fixed regex pattern in `src/skywatch/pipeline/logging.py` to capture `(bearer\s+)` as group 1 and substitute `r"\1***REDACTED***"`, preventing plaintext Bearer token leakage.
- **CR-02 (Data Loss - Multi-Station Raw Payload Preservation):** Updated raw filename in `src/skywatch/storage/raw_manifest.py` to `f"{source}_{run_id}_{sha256_hash[:8]}.json"`, eliminating filename collisions and preserving all raw station responses.
- **CR-03 (Audit Integrity - Historical Backfill Raw Ledger):** Added `save_raw_response` calls for both OpenAQ measurements and Open-Meteo archive weather inside `IngestionOrchestrator.run_chunked_backfill`, fulfilling ING-04 audit compliance.
- **CR-04 (Runtime Crash - Naive Timestamp Subtraction):** Updated `check_timestamp` in `src/skywatch/data/validation.py` to ensure naive parsed datetimes default to UTC timezone, preventing `TypeError` on subtraction with aware `now`.
- **CR-05 (Config Hierarchy - Explicit Profile Loading):** Updated `load_profile` in `src/skywatch/pipeline/profile.py` so that explicit `profile_name` loading prioritizes profile file values over ambient environment variables, enabling reliable rollback profile testing (OPS-06).
- **CR-06 (CI Failure - Manifest File Ignore Conflict):** Un-ignored `!data/raw/_manifest.csv` in `.gitignore` and added `-f` to `git add` in `.github/workflows/scheduled-collect.yml`, allowing the CI workflow to track audit manifests without git errors.

---

### Warning & Quality Issues

- **WR-01 & WR-04 (Python 3.8 Syntax Floor):** Replaced PEP 604 `| None` union syntax with `typing.Optional` and `typing.List` in `src/skywatch/storage/raw_manifest.py` and `src/skywatch/cli.py`.
- **WR-02 (Manifest Concurrency):** Enforced atomic file write and header presence checks in `src/skywatch/storage/raw_manifest.py`.
- **WR-03, WR-10, WR-12, WR-21, WR-22 (Resource Leaks):** Wrapped all `tempfile.NamedTemporaryFile` instances across storage, collector, and discovery modules in `try...finally` blocks with `os.unlink` cleanup on failure.
- **WR-05 (Station ID Handling):** Added `_safe_id` helper in `collector.py` to safely handle numeric and string station and sensor identifiers.
- **WR-06 & WR-08 (Watermark Validity & Staging):** Guarded watermark advancement on `norm.get("is_valid")` and non-empty result sets in `collector.py`.
- **WR-07 (Timestamp Ordering):** Standardized timestamp formatting to ISO UTC strings and safe timestamp comparisons.
- **WR-09 (Fault Tolerance):** Wrapped individual station collection in `try...except` so single-station network timeouts or errors do not abort the entire batch.
- **WR-11 (Discovery ID Safety):** Skipped stations or sensors with null or missing IDs in `src/skywatch/collector/discovery.py`.
- **WR-13 & WR-14 (API Client Error Handling):** Added `json.JSONDecodeError` handling and widened transport retry exceptions in `OpenAQClient` and `WeatherClient`.
- **WR-15 (Weather Validation):** Implemented `validate_weather_record` in `src/skywatch/data/validation.py` with physical range checks for temperature, humidity, pressure, wind, and precipitation.
- **WR-16 (UTC Normalization):** Updated `_format_utc_timestamp` in `src/skywatch/data/normalization.py` to parse and convert non-UTC offsets to explicit UTC `Z` format.
- **WR-17 (Schema Strictness):** Explicitly rejected boolean types from integer schema columns in `src/skywatch/data/schemas.py`.
- **WR-18 & WR-20 (Path Traversal Protection):** Sanitized profile names and verified HDFS offline fallback paths against directory traversal.
- **WR-19 (HDFS Overwrite):** Added pre-removal of existing target partitions and staging file cleanup in `HDFSStorageAdapter`.
- **WR-23 & WR-24 (Shell Portability):** Resolved paths relative to `SCRIPT_DIR` in `cron_collect.sh` and `use_profile.sh`.
- **WR-25 & WR-26 (CI Workflow Resilience):** Added `timeout-minutes: 15`, `git pull --rebase` before push, and explicit manifest staging.

---

### Verification Summary

- **Ruff Lint:** 0 warnings (`ruff check .` passed).
- **Pytest Suite:** 44/44 passed in 1.89s (added tests for Bearer token redaction, multi-station raw preservation, naive timestamps, weather record validation, profile ambient precedence, and schema boolean rejection).
- **CLI End-to-End:** Tested `skywatch discover`, `collect --mode latest`, `collect --mode incremental`, and `backfill` in offline mode; all succeeded with zero errors.
