---
phase: 01-foundation-data-ingestion-storage-architecture
reviewed: 2026-11-22
depth: standard
files_reviewed: 28
files_reviewed_list:
  - config/schemas.yaml
  - config/stations.json
  - profiles/lab.env
  - profiles/modern.env
  - pyproject.toml
  - scripts/cron_collect.sh
  - scripts/use_profile.sh
  - .github/workflows/scheduled-collect.yml
  - src/skywatch/__init__.py
  - src/skywatch/cli.py
  - src/skywatch/collector/__init__.py
  - src/skywatch/collector/collector.py
  - src/skywatch/collector/discovery.py
  - src/skywatch/collector/openaq_client.py
  - src/skywatch/collector/weather_client.py
  - src/skywatch/data/__init__.py
  - src/skywatch/data/deduplication.py
  - src/skywatch/data/normalization.py
  - src/skywatch/data/schemas.py
  - src/skywatch/data/validation.py
  - src/skywatch/pipeline/__init__.py
  - src/skywatch/pipeline/logging.py
  - src/skywatch/pipeline/profile.py
  - src/skywatch/storage/__init__.py
  - src/skywatch/storage/hdfs_storage.py
  - src/skywatch/storage/local_storage.py
  - src/skywatch/storage/raw_manifest.py
  - src/skywatch/storage/watermark.py
findings:
  critical: 6
  warning: 26
  info: 6
  total: 38
  necessity: fix_before_proceeding
status: issues_found
---

# Adversarial Code Review: Phase 1 — Foundation, Data Ingestion & Storage Architecture

## Executive Summary

Phase 1 source files, configuration schemas, shell runners, and CI workflows underwent a standard depth adversarial review. While the implementation demonstrates strong conceptual alignment with requirements (such as two-phase commit watermarks, atomic staging swaps, and SHA-256 deduplication), **6 Critical defects**, **26 Warning defects**, and **6 Info defects** were identified across Python compatibility, security, data integrity, error resilience, and deployment automation.

Most urgent among the findings:
1. **Security / Credential Scrubbing Defect (CR-01):** The regex redaction logic in `src/skywatch/pipeline/logging.py` captures the secret token instead of the prefix, emitting the raw Bearer token directly into log files.
2. **Data Loss / Payload Destruction (CR-02):** In `src/skywatch/storage/raw_manifest.py`, raw payloads are named `f"{source}_{run_id}.json"`. When multiple stations or sensors are polled within the same run, each query overwrites the previous station's raw payload, destroying all raw API responses except the last one.
3. **Audit Ledger Bypass in Backfill (CR-03):** `IngestionOrchestrator.run_chunked_backfill` bypasses `save_raw_response` entirely, violating requirement ING-04 by skipping raw preservation and manifest ledgering during historical backfills.
4. **Validation Crash on Naive Timestamps (CR-04):** `check_timestamp` in `src/skywatch/data/validation.py` subtracts timezone-aware `now` from timezone-naive parsed timestamps, raising an uncaught `TypeError` that halts execution.
5. **Profile Rollback Override (CR-05):** In `src/skywatch/pipeline/profile.py`, existing ambient environment variables take precedence over profile file values, preventing profile switching from loading `lab.env`.
6. **CI Push Failure via Git Ignore (CR-06):** In `.github/workflows/scheduled-collect.yml`, `git add data/raw/_manifest.csv` is blocked by `.gitignore`'s rule `data/raw/*`, causing scheduled workflow executions to fail on git add.

---

## Critical Findings (CR-01 - CR-06)

### CR-01: Plaintext Bearer Token Leakage in Logging Credential Redactor
- **File:** `src/skywatch/pipeline/logging.py:25-26, 60-63`
- **Issue:** The regex pattern `re.compile(r'(?i)bearer\s+([a-zA-Z0-9_\-\.]+)')` captures the secret token itself as group 1 (`\1`). In `redact_sensitive()`, `pattern.sub(r"\1=***REDACTED***", redacted)` substitutes `\1` back into the string, producing `<secret_token>=***REDACTED***`. This directly logs the sensitive token in plaintext instead of masking it.
- **Fix:** Capture the keyword and trailing space as group 1, and substitute with `r"\1***REDACTED***"`:
```python
_SENSITIVE_PATTERNS = [
    re.compile(r'(?i)(api[_-]?key|token|secret|password|auth)\s*[:=]\s*["\']?([^"\'\s&,]+)["\']?'),
    re.compile(r'(?i)(bearer\s+)[a-zA-Z0-9_\-\.]+'),
]

def redact_sensitive(text: str) -> str:
    if not isinstance(text, str):
        return text
    redacted = _SENSITIVE_PATTERNS[0].sub(r"\1=***REDACTED***", text)
    redacted = _SENSITIVE_PATTERNS[1].sub(r"\1***REDACTED***", redacted)
    return redacted
```

---

### CR-02: Single-Run Raw Payload Overwrites Causing Permanent Data Loss
- **File:** `src/skywatch/storage/raw_manifest.py:69, 78`
- **Issue:** The raw storage filename is generated as `target_file = partition_dir / f"{source}_{run_id}.json"`. When polling multiple stations or sensors during a single collection run (`run_id`), every subsequent station's payload is written to the exact same path, executing `os.replace` over the previous file. Consequently, raw data from stations 1 through N-1 is permanently destroyed, while `_manifest.csv` records orphaned SHA-256 digests referencing non-existent previous states.
- **Fix:** Include a unique per-request component (such as the payload's SHA-256 prefix, sensor/station ID, or request sequence counter) in the filename:
```python
target_file = partition_dir / f"{source}_{run_id}_{sha256_hash[:8]}.json"
```

---

### CR-03: Raw Payload Preservation and Manifest Logging Skipped During Backfill
- **File:** `src/skywatch/collector/collector.py:276-296`
- **Issue:** In `run_chunked_backfill`, historical air quality and archive weather responses are retrieved from OpenAQ and Open-Meteo, but `save_raw_response` is never invoked. This violates requirement ING-04 (mandatory raw payload preservation with SHA-256 manifest tracking), leaving historical datasets without raw provenance.
- **Fix:** Add calls to `save_raw_response` inside the backfill chunk loop for both OpenAQ and Open-Meteo queries:
```python
save_raw_response(
    raw_data=payload,
    source="openaq",
    endpoint_url=f"https://api.openaq.org/v3/sensors/{sensor_id}/measurements",
    http_status=200,
    run_id=rid,
    base_dir=self.raw_dir,
    ingest_dt=chunk_ingest_dt,
)
```

---

### CR-04: Uncaught `TypeError` in Timestamp Validation on Naive ISO Strings
- **File:** `src/skywatch/data/validation.py:79-85`
- **Issue:** When a timestamp string does not contain an explicit offset or trailing `Z` (e.g. `"2026-11-22T08:00:00"`), `datetime.fromisoformat` returns an offset-naive `datetime`. When evaluated against `now = reference_time or datetime.now(timezone.utc)`, the operation `(dt - now)` raises `TypeError: can't subtract offset-naive and offset-aware datetimes`, crashing the validation engine and the entire pipeline.
- **Fix:** Ensure `dt` is assigned UTC tzinfo when parsed without an offset:
```python
try:
    dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
except (ValueError, TypeError):
    return ValidationResult(False, "invalid_timestamp_format")
```

---

### CR-05: Ambient Environment Variables Override Explicit Profile Switching
- **File:** `src/skywatch/pipeline/profile.py:49-51`
- **Issue:** The helper `get_val(key, default)` evaluates `os.environ.get(key) or file_values.get(key) or default`. If environment variables like `HADOOP_HOME` or `SPARK_HOME` are already set in the ambient environment (e.g. from `.env` or system environment), calling `load_profile("lab")` ignores the values in `profiles/lab.env` and retains the modern ambient values. This prevents switching to the rollback profile in active environments.
- **Fix:** Prioritize `file_values` when a specific profile name is explicitly requested, or reconcile when `SKYWATCH_PROFILE` matches:
```python
def get_val(key: str, default: str) -> str:
    if profile_name is not None and key in file_values:
        return file_values[key]
    return os.environ.get(key) or file_values.get(key) or default
```

---

### CR-06: GitHub Actions Scheduled Ingestion Fails on Ignored Manifest File
- **File:** `.github/workflows/scheduled-collect.yml:40` and `.gitignore:49-50`
- **Issue:** The CI workflow executes `git add data/raw/_manifest.csv config/stations.json`. However, `.gitignore` specifies:
  ```gitignore
  data/raw/*
  !data/raw/.gitkeep
  ```
  Because `data/raw/*` ignores all files in `data/raw/`, `git add data/raw/_manifest.csv` aborts with `fatal: Pathspec 'data/raw/_manifest.csv' is in submodule or ignored by .gitignore`, failing the scheduled workflow step.
- **Fix:** Un-ignore `_manifest.csv` explicitly in `.gitignore`:
```gitignore
data/raw/*
!data/raw/.gitkeep
!data/raw/_manifest.csv
```
Or use `git add -f data/raw/_manifest.csv` in the workflow.

---

## Warning Findings (WR-01 - WR-26)

### WR-01: Python 3.8 Syntax Floor Violation in `raw_manifest.py`
- **File:** `src/skywatch/storage/raw_manifest.py:40`
- **Issue:** Function signature uses PEP 604 union syntax: `ingest_dt: str | None = None`. This violates AGENTS.md Rule 4 requiring strict Python 3.8 syntax floor compatibility.
- **Fix:** Replace with `Optional[str] = None` using `from typing import Optional`.

---

### WR-02: Missing File Lock on Manifest Ledger Append
- **File:** `src/skywatch/storage/raw_manifest.py:97-104`
- **Issue:** The docstring states `# Append to manifest atomically with file lock`, but `open(manifest_path, "a")` performs an uncoordinated file append. Under concurrent collection processes or worker threads, header writes and record appends can interleave or corrupt CSV rows.
- **Fix:** Implement file locking (e.g. via `fcntl.flock` or a dedicated lockfile) around the manifest file open/write block.

---

### WR-03: Temporary File Leak on Exception in `save_raw_response`
- **File:** `src/skywatch/storage/raw_manifest.py:72-78`
- **Issue:** `tempfile.NamedTemporaryFile("wb", delete=False)` is used without a `try...finally` block. If `write()`, `fsync()`, or `os.replace()` raises an error, the temporary file remains uncleaned on disk.
- **Fix:** Wrap the tempfile operation in `try...finally`:
```python
tf = tempfile.NamedTemporaryFile("wb", dir=str(partition_dir), delete=False)
try:
    tf.write(content_bytes)
    tf.flush()
    os.fsync(tf.fileno())
    temp_path = tf.name
    tf.close()
    os.replace(temp_path, str(target_file))
except Exception:
    if os.path.exists(tf.name):
        os.unlink(tf.name)
    raise
```

---

### WR-04: Python 3.8 Syntax Floor Violation in `cli.py`
- **File:** `src/skywatch/cli.py:53`
- **Issue:** `def main(args: list[str] | None = None) -> int:` uses PEP 585 generic builtins and PEP 604 union syntax without runtime safety in Python 3.8.
- **Fix:** Use `Optional[List[str]] = None` with imports from `typing`.

---

### WR-05: Alphanumeric Station/Sensor ID Crash on Blind `int()` Cast
- **File:** `src/skywatch/collector/collector.py:92, 137`
- **Issue:** Calling `self.openaq.get_latest(int(station_id))` and `int(sensor_id)` crashes with `ValueError` if OpenAQ returns non-integer or alphanumeric station/sensor identifiers.
- **Fix:** Pass IDs as strings or use `int(station_id) if str(station_id).isdigit() else station_id`.

---

### WR-06: Watermark Progression Ignores Record Hygiene Validity
- **File:** `src/skywatch/collector/collector.py:113-114, 158-160`
- **Issue:** Both latest and incremental modes advance the sensor high-water mark timestamp based solely on `meas_time > current_wm` without checking `norm["is_valid"]`. An invalid measurement (e.g. invalid unit, corrupt value, or spike) can advance the high watermark, permanently locking out future valid readings.
- **Fix:** Only advance `newest_ts` if `norm.get("is_valid") is True`:
```python
if norm.get("is_valid") and (not newest_ts or meas_ts > newest_ts):
    newest_ts = meas_ts
```

---

### WR-07: Lexical Comparison of ISO Timestamps with Varying Precision
- **File:** `src/skywatch/collector/collector.py:113, 158`
- **Issue:** Comparing timestamps with `>` string comparison fails when precision differs. For example, `"2026-11-22T08:00:00Z" > "2026-11-22T08:00:00.000Z"` evaluates to `True` in ASCII because `'Z' > '.'`, causing millisecond timestamps to incorrectly fail watermark progression.
- **Fix:** Standardize timestamps to microsecond-free UTC ISO strings or parse to datetimes before comparing.

---

### WR-08: Redundant Watermark Staging on Empty API Results
- **File:** `src/skywatch/collector/collector.py:153, 161-169`
- **Issue:** In incremental mode, `newest_ts` is initialized to `wm`. If the API returns zero measurements, `newest_ts` retains `wm`, and `if newest_ts:` evaluates to `True`. This stages an unnecessary watermark update with `records_ingested=0`.
- **Fix:** Initialize `newest_ts = None`, and only stage when `newest_ts is not None and newest_ts > wm`.

---

### WR-09: Unhandled Station Query Failure Aborts Full Collection Cycle
- **File:** `src/skywatch/collector/collector.py:87-170`
- **Issue:** There is no `try...except` block wrapping individual station queries in `run_incremental_collection`. If one station encounters a transient network timeout or 404, the entire loop aborts, discarding data for previously queried stations and failing the entire cycle.
- **Fix:** Wrap individual station processing in a `try...except` block, log a warning, and proceed to the next station.

---

### WR-10: Temporary File Leak in Backfill Checkpoint Persistence
- **File:** `src/skywatch/collector/collector.py:311-314`
- **Issue:** `tempfile.NamedTemporaryFile` with `delete=False` lacks exception cleanup. If `json.dump` or `os.replace` fails, temporary files accumulate in `data/`.
- **Fix:** Add `try...finally` with `os.unlink` cleanup on error.

---

### WR-11: Station Discovery Converts Missing Station/Sensor ID to `"None"`
- **File:** `src/skywatch/collector/discovery.py:38, 53`
- **Issue:** `loc_id = str(loc.get("id"))` and `sensor_id = str(sensor.get("id"))` evaluate to `"None"` if the key is missing or null, which generates malformed station dictionaries.
- **Fix:** Skip locations and sensors with missing IDs:
```python
if loc.get("id") is None:
    continue
```

---

### WR-12: Temporary File Leak in `discover_stations`
- **File:** `src/skywatch/collector/discovery.py:94-97`
- **Issue:** `NamedTemporaryFile(..., delete=False)` lacks `try...finally` cleanup if JSON serialization fails.
- **Fix:** Add `try...finally` with `os.unlink` fallback.

---

### WR-13: Uncaught `JSONDecodeError` on HTML Gateway Responses
- **File:** `src/skywatch/collector/openaq_client.py:99` and `src/skywatch/collector/weather_client.py:64`
- **Issue:** `return response.json()` is invoked without catching `json.JSONDecodeError` / `ValueError`. When upstream CDNs or proxies return HTML error pages with HTTP 200/502/504, the client crashes without retry.
- **Fix:** Wrap `response.json()` in a `try...except` block and trigger a retry.

---

### WR-14: Incomplete Exception Catching in HTTP Retry Loop
- **File:** `src/skywatch/collector/openaq_client.py:101` and `src/skywatch/collector/weather_client.py:66`
- **Issue:** Catching only `(requests.ConnectionError, requests.Timeout)` omits `requests.exceptions.ChunkedEncodingError` and socket errors, preventing retries on common transport interruptions.
- **Fix:** Catch `(requests.ConnectionError, requests.Timeout, requests.exceptions.ChunkedEncodingError)`.

---

### WR-15: Missing Weather Record Hygiene Validation Rules
- **File:** `src/skywatch/data/validation.py:90-123`
- **Issue:** `validation.py` only implements `validate_air_quality_record`. Weather observation streams have no corresponding `validate_weather_record` function to verify humidity ranges (0-100%), temperature bounds, and non-negative wind/rain values.
- **Fix:** Add a `validate_weather_record` function implementing sanity checks for meteorological metrics.

---

### WR-16: `_format_utc_timestamp` Corrupts Non-UTC Timezone Strings
- **File:** `src/skywatch/data/normalization.py:21-24`
- **Issue:** If an ISO string has an offset like `+05:30` (e.g. `"2026-11-22T13:30:00+05:30"` from local OpenAQ fields), `_format_utc_timestamp` appends `"Z"`, creating `"2026-11-22T13:30:00+05:30Z"`. This is an invalid ISO string and fails to convert to UTC.
- **Fix:** Parse using `datetime.fromisoformat`, convert with `.astimezone(timezone.utc)`, and format as UTC ISO string.

---

### WR-17: Integer Schema Validator Accepts Boolean Types
- **File:** `src/skywatch/data/schemas.py:78-82`
- **Issue:** In `validate_record_dict`, `if not isinstance(val, int) or isinstance(val, bool): try: int(val)` executes `int(True)`, which evaluates to `1`. Consequently, booleans are accepted as integers.
- **Fix:** Explicitly reject booleans before integer casting:
```python
elif col_type == "integer":
    if isinstance(val, bool):
        return False, f"Column '{col_name}' must be integer, got bool"
```

---

### WR-18: Directory Traversal Risk in Profile Name Loader
- **File:** `src/skywatch/pipeline/profile.py:42-44`
- **Issue:** `profile_file = base_dir / f"{chosen_profile}.env"` allows path traversal if `chosen_profile` contains `../`.
- **Fix:** Enforce alphanumeric and underscore validation on `chosen_profile`.

---

### WR-19: HDFS Upload Fails on Existing Partition and Leaves Orphan Staging Files
- **File:** `src/skywatch/storage/hdfs_storage.py:101-103`
- **Issue:** In `upload_partition()`, `hdfs dfs -mv staging_file final_file` fails if `final_file` already exists. On re-runs, this triggers an error, falls back to offline storage, and leaves `staging_file` in `hdfs://<root>/_staging/` permanently.
- **Fix:** Add `-f` or pre-remove the existing target file before renaming, and ensure staging files are unlinked in a `finally` block.

---

### WR-20: Path Traversal in HDFS Offline Fallback Path
- **File:** `src/skywatch/storage/hdfs_storage.py:80`
- **Issue:** `fallback_target_dir = self.offline_dir / hdfs_partition_dir.lstrip("/")` can escape `self.offline_dir` if `hdfs_partition_dir` contains `..`.
- **Fix:** Verify `fallback_target_dir.resolve().is_relative_to(self.offline_dir.resolve())`.

---

### WR-21: Temporary File Leak in Local Storage Atomic Writer
- **File:** `src/skywatch/storage/local_storage.py:39-52`
- **Issue:** `atomic_write_file` creates a `NamedTemporaryFile(..., delete=False)` without a `try...finally` block to remove the temporary file if `os.replace` fails.
- **Fix:** Wrap in `try...finally` with `os.unlink` fallback.

---

### WR-22: Temporary File Leak in Watermark Commit
- **File:** `src/skywatch/storage/watermark.py:79-92`
- **Issue:** `WatermarkManager.commit` creates temporary files with `delete=False` without `try...finally` cleanup.
- **Fix:** Add `try...finally` with `os.unlink` on exception.

---

### WR-23: Working Directory Dependency and Hardcoded Profile in Cron Script
- **File:** `scripts/cron_collect.sh:8-10, 22-26`
- **Issue:** `cron_collect.sh` references relative paths (`data/logs`, `profiles/modern.env`). In standard cron environments where working directories vary, relative path operations fail. Additionally, `profiles/modern.env` is hardcoded, ignoring `SKYWATCH_PROFILE`.
- **Fix:** Change directory to repository root at script start:
```bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$SCRIPT_DIR"
PROFILE="${SKYWATCH_PROFILE:-modern}"
```

---

### WR-24: Relative Path Failure in Profile Switcher Script
- **File:** `scripts/use_profile.sh:6`
- **Issue:** `PROFILE_FILE="profiles/${PROFILE}.env"` assumes current working directory is the repo root. Sourcing the script from any other directory results in file not found.
- **Fix:** Resolve `PROFILE_FILE` relative to `${BASH_SOURCE[0]}`:
```bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROFILE_FILE="${SCRIPT_DIR}/profiles/${PROFILE}.env"
```

---

### WR-25: Unprotected `git push` Subject to Non-Fast-Forward Rejection
- **File:** `.github/workflows/scheduled-collect.yml:41`
- **Issue:** `git push` fails if commits have landed on `main` since workflow checkout.
- **Fix:** Run `git pull --rebase origin main` immediately before `git push`.

---

### WR-26: Workflow Missing Watermark and Partition Artifact Persistence
- **File:** `.github/workflows/scheduled-collect.yml:40`
- **Issue:** The workflow only commits `_manifest.csv` and `config/stations.json`. It neither caches nor commits `data/.watermarks.json`, preventing incremental collection across workflow runs.
- **Fix:** Use GitHub Actions cache (`actions/cache`) for `data/.watermarks.json` or commit watermarks.

---

## Info Findings (IN-01 - IN-06)

### IN-01: Hardcoded Meteorological Reference Coordinates
- **File:** `src/skywatch/collector/collector.py:172-173, 286-287`
- **Issue:** Coordinates `12.91, 77.61` are hardcoded in method bodies.
- **Fix:** Define `BENGALURU_CENTRAL_COORDINATES = (12.91, 77.61)` in a centralized configuration.

### IN-02: Misnamed Offline Environment Variable in `WeatherClient`
- **File:** `src/skywatch/collector/weather_client.py:33`
- **Issue:** `WeatherClient` checks `OPENAQ_OFFLINE` instead of `SKYWATCH_OFFLINE` or `WEATHER_OFFLINE`.
- **Fix:** Check `os.environ.get("WEATHER_OFFLINE") or os.environ.get("SKYWATCH_OFFLINE")`.

### IN-03: Lack of HTTP Connection Pooling
- **File:** `src/skywatch/collector/openaq_client.py:66` and `src/skywatch/collector/weather_client.py:49`
- **Issue:** Every HTTP request invokes `requests.get` directly, creating a new TCP connection per call.
- **Fix:** Use `requests.Session()` within client instances to enable connection pooling.

### IN-04: Missing Sensor ID Fallback from Station Metadata
- **File:** `src/skywatch/data/normalization.py:67`
- **Issue:** If `sensorsId` is absent from the raw record, `sensor_id` defaults to `""` instead of looking up `station_meta["sensors"]`.
- **Fix:** Look up sensor ID from `station_meta.get("sensors", {}).get(param_name, {}).get("sensor_id", "")`.

### IN-05: Missing Error Trapping in Cron Script
- **File:** `scripts/cron_collect.sh:33`
- **Issue:** If `python -m skywatch.cli` fails, `set -e` causes an immediate exit without an explicit failure entry in `$LOGFILE`.
- **Fix:** Add a shell trap: `trap 'echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] Error: collection failed" >> "$LOGFILE"' ERR`.

### IN-06: Profile Input Validation in Profile Switcher
- **File:** `scripts/use_profile.sh:5`
- **Issue:** `PROFILE` is not validated against allowed profiles (`modern`, `lab`).
- **Fix:** Check `if [[ "$PROFILE" != "modern" && "$PROFILE" != "lab" ]]; then echo "Invalid profile"; return 1; fi`.

---

## Conclusion & Recommendations

The reviewed Phase 1 codebase implements solid core architectures including cryptographic deduplication, atomic staging file swaps, and two-phase watermark commits. However, the 6 Critical issues—particularly raw payload file destruction within runs (CR-02), Bearer token leakage (CR-01), naive timestamp crash (CR-04), and broken profile switching (CR-05)—represent high-severity functional, security, and data integrity defects.

**Recommendation:** Address the 6 Critical defects and key Warning defects prior to advancing to Phase 2 (Pig/Hadoop ETL).
