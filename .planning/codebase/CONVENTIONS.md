# Coding Conventions

**Analysis Date:** 2026-11-22

## Naming Patterns

**Files:**
- `snake_case.py` modules, one responsibility per file: `openaq_client.py`, `weather_client.py`, `raw_manifest.py`, `local_storage.py`, `hdfs_storage.py`, `deduplication.py`.
- Package `__init__.py` files are docstring-only stubs — no re-exports. Example `src/skywatch/data/__init__.py`:
  ```python
  """SkyWatch data package."""
  from __future__ import annotations
  ```
- Tests are `test_<module>.py` matching the module under test: `tests/unit/test_normalization.py` ↔ `src/skywatch/data/normalization.py`.
- Shell scripts in `scripts/` use `snake_case.sh` (`cron_collect.sh`, `use_profile.sh`).
- Profile files use `<profile>.env` under `profiles/` (`modern.env`, `lab.env`). Treat contents as environment configuration; never commit secrets.

**Functions:**
- `snake_case` verbs for public functions: `discover_stations`, `normalize_openaq_record`, `validate_air_quality_record`, `deduplicate_batch`, `save_raw_response`, `write_normalized_csv`.
- Leading underscore marks module-private helpers: `_safe_id` (`src/skywatch/collector/collector.py:35`), `_format_utc_timestamp` (`src/skywatch/data/normalization.py:15`), `_load_fixture` (`src/skywatch/collector/openaq_client.py:53`), `_check_java_version` (`src/skywatch/pipeline/profile.py:75`), `_redact_dict` (`src/skywatch/pipeline/logging.py:64`).
- Boolean-returning predicates read as questions/assertions: `is_available`, `check_safemode`, `check_unit`, `check_timestamp`.
- Validators are named `check_*` for a single rule and `validate_*` for a composite pipeline (`src/skywatch/data/validation.py`).
- `run_*` for orchestration entry points: `run_incremental_collection`, `run_chunked_backfill` (`src/skywatch/collector/collector.py`).

**Variables:**
- `snake_case`; units/UTC suffixes are part of the name: `measurement_time_utc`, `ingestion_time_utc`, `collected_at_utc`, `last_measurement_utc`, `max_age_hours`, `chunk_days`.
- Timestamp string variables end in `_utc` or `_str`: `now_utc`, `formatted_ts`, `dt_from_str`, `last_utc_str`.
- Module-level constants are `UPPER_SNAKE_CASE`: `BENGALURU_BBOX` (`src/skywatch/collector/discovery.py:17`), `BENGALURU_CENTRAL_COORDINATES` (`src/skywatch/collector/collector.py:32`), `MANIFEST_HEADERS` (`src/skywatch/storage/raw_manifest.py:21`), `BASE_URL`/`FORECAST_URL`/`ARCHIVE_URL` as class constants.
- Single-letter temporaries are tolerated only in tight numeric loops (`i`, `n`, `k`, `v` in `src/skywatch/data/normalization.py:141` and `src/skywatch/pipeline/logging.py:67`).

**Types:**
- `PascalCase` for classes and dataclasses: `OpenAQClient`, `WeatherClient`, `IngestionOrchestrator`, `WatermarkManager`, `HDFSStorageAdapter`, `ValidationResult`, `ProfileConfig`, `JsonPipelineFormatter`, `AuthenticationError`.
- Custom exceptions are `PascalCase` and end in `Error` (`AuthenticationError` in `src/skywatch/collector/openaq_client.py:19`).
- Typing uses Python 3.8-compatible generics only — `List[...]`, `Dict[...]`, `Optional[...]`, `Tuple[...]`, `Set[...]`. Never `list[...]`, `dict[...]`, or `X | Y`.

## Code Style

**Formatting:**
- Ruff with `target-version = "py38"`, `line-length = 100` (`pyproject.toml`).
- `E501` (line length) is ignored, so lines may exceed 100 characters. Longest observed lines: 145 chars in `src/skywatch/collector/openaq_client.py`, 123 in `src/skywatch/collector/collector.py`. Still prefer wrapping at ~100–110 chars for new code.
- Double quotes for strings throughout (`"utf-8"`, `"ug/m3"`). Single quotes are reserved for embedded quoting inside strings: `f"Starting collection cycle in mode '{mode}'"` (`src/skywatch/collector/collector.py:83`).
- 4-space indentation; no tabs.
- Multi-line call arguments use one argument per line with a trailing comma:
  ```python
  self.watermark_mgr.update_watermark(
      sensor_id=sensor_id,
      station_id=station_id,
      parameter=norm["parameter"],
      last_measurement_utc=meas_time,
      records_ingested=1,
      run_id=rid,
  )
  ```
  (`src/skywatch/collector/collector.py:123-130`)
- Two blank lines between top-level definitions; one blank line between methods.
- Every module starts with a one-line summary docstring, optionally followed by a blank line and an "Implements ..." paragraph naming requirement IDs:
  ```python
  """Atomic local storage writer with partitioned layout support.

  Implements HYG-05 & HYG-06: Atomic write protocol (flush, fsync, os.replace)
  and partitioned dataset directory layout: /normalized/{stream}/ingest_dt=YYYY-MM-DD/.
  """
  ```
  (`src/skywatch/storage/local_storage.py:1-5`)

**Linting:**
- `ruff check .` with `select = ["E", "F", "I", "W"]`, `ignore = ["E501"]`. Current tree passes clean (verified: "All checks passed!").
- Import sorting is enforced by `I` (isort rules). Keep the three-group ordering below.
- CI runs `ruff check .` before `pytest` (`.github/workflows/ci.yml`).

## Python 3.8 Floor (Mandatory)

`requires-python = ">=3.8"` and `target-version = "py38"`. Observed rules:
- `from __future__ import annotations` is the first statement after the module docstring in **every** source and test file. Example `src/skywatch/cli.py:1-5`.
- Use `typing.List/Dict/Optional/Tuple/Any/Union` instead of builtin generics.
- No `match` statements, no walrus-dependent style, no `X | Y` unions, no `str.removeprefix`/`removesuffix`.
- `datetime.fromisoformat` is used with a manual `"Z"` → `"+00:00"` replacement because Python 3.8 cannot parse `Z` (`src/skywatch/data/validation.py:79`).

## Import Organization

**Order:**
1. Module docstring, then `from __future__ import annotations`.
2. Standard library: `argparse`, `json`, `os`, `sys`, `tempfile`, `csv`, `hashlib`, `datetime`, `pathlib`, `typing`, `subprocess`, `contextvars`, `dataclasses`, `contextlib`.
3. Third-party: `requests`, `yaml`, `dotenv`.
4. First-party absolute imports rooted at `skywatch.`: `from skywatch.collector.openaq_client import OpenAQClient`.

**Path Aliases:**
- None. There is no `conftest.py` path hack and no alias table. Imports are absolute from the `skywatch` package; pytest resolves `src/` via `pythonpath = ["src"]` in `pyproject.toml`.

**Patterns:**
- Never relative imports. `from .foo import bar` does not appear anywhere in `src/skywatch/`.
- Import the concrete submodule, not the package: `from skywatch.storage.watermark import WatermarkManager`, never `from skywatch.storage import WatermarkManager`.
- Multi-name imports from one module are parenthesized one-per-line:
  ```python
  from skywatch.data.normalization import (
      normalize_openaq_record,
      normalize_weather_response,
  )
  ```
  (`src/skywatch/collector/collector.py:20-23`)

## Error Handling

**Strategy:** Two-tier. *Expected data problems* are values, not exceptions. *Operational failures* raise, and orchestration loops isolate them per unit of work.

**Patterns:**
- **Validation returns a result object.** `ValidationResult(is_valid, reject_reason)` is a dataclass in `src/skywatch/data/validation.py:13-16`. Rules return it instead of raising; composite validators short-circuit on the first failure and return the specific reason code (`missing_field:<name>`, `negative_value`, `implausible_spike`, `invalid_unit:<unit>`, `future_timestamp`).
- **Reason codes are machine-readable and prefixed.** Follow the `snake_case` reason-code convention and include the offending value when useful: `f"invalid_humidity_range:{humidity}"`, `f"negative_precipitation:{precip}"` (`src/skywatch/data/validation.py:109-118`).
- **Custom exceptions only for unrecoverable contract violations.** `AuthenticationError` for HTTP 401/403 (`src/skywatch/collector/openaq_client.py:19,74-77`). Built-ins (`FileNotFoundError`, `KeyError`, `ValueError`) are used elsewhere: `raise FileNotFoundError(f"Offline fixture not found: {fixture_path}")`, `raise KeyError(f"Unknown schema name '{schema_name}'...")`.
- **Retry with bounded exponential backoff plus jitter.** `_request_with_retry` loops `for attempt in range(self.max_retries)`, honors `Retry-After` on 429, and raises after exhaustion:
  ```python
  sleep_time = min(60.0, 1.5 * (2**attempt)) + random.uniform(0.1, 1.0)
  ```
  (`src/skywatch/collector/openaq_client.py:79-89`; mirrored in `weather_client.py:57-61`)
- **Per-item isolation in orchestrators.** Catch broad exceptions around each station/sensor, log a warning, continue the run, and re-raise only when the whole run cannot proceed:
  ```python
  except Exception as exc:
      logger.warning(f"Error querying station {station_id}: {exc}. Continuing with next station.")
  ```
  (`src/skywatch/collector/collector.py:179-180`)
- **Atomic-write cleanup contract.** Every temp-file write follows the same shape — write, `flush()`, `os.fsync(fileno())`, `close()`, `os.replace()`, and on exception `os.unlink` the temp file then `raise`:
  ```python
  except Exception:
      if os.path.exists(temp_name):
          os.unlink(temp_name)
      raise
  ```
  (`src/skywatch/storage/local_storage.py:54-57`, `raw_manifest.py:81-84`, `watermark.py:94-97`, `discovery.py:106-109`, `collector.py:352-355`). Reuse this exact shape for any new durable write; do not invent a second variant.
- **Corrupt-state recovery is non-fatal.** Loaders log and reset instead of crashing: watermark load (`src/skywatch/storage/watermark.py:38-40`) and stations load (`src/skywatch/collector/collector.py:72-73`).
- **CLI is the only catch-all.** `main()` wraps the subcommand dispatch in `try/except Exception`, logs with `exc_info=True`, prints `Error: ...` to stderr, and returns exit code `1` (`src/skywatch/cli.py:90-93`). Do not add top-level catches inside library modules.
- **Broad `except Exception` is acceptable only** for (a) temp-file cleanup-and-reraise, (b) corrupt-state recovery with a logged warning, (c) per-item isolation in a batch loop, (d) best-effort HDFS cleanup (`src/skywatch/storage/hdfs_storage.py:120-123`). Narrow to `(ValueError, TypeError)` for parsing/coercion: `except (ValueError, TypeError): val = None` (`src/skywatch/data/normalization.py:86-87`).

## Logging

**Framework:** stdlib `logging` wrapped by `src/skywatch/pipeline/logging.py`.

**Patterns:**
- One module-level logger per file, named for the component: `logger = get_logger("orchestrator")` (`collector.py:30`), `get_logger("openaq_client")`, `get_logger("watermark")`, `get_logger("raw_manifest")`, `get_logger("cli")`.
- Output is JSON via `JsonPipelineFormatter` with keys `timestamp_utc`, `run_id`, `level`, `component`, `message` (`src/skywatch/pipeline/logging.py:79-104`).
- Messages are f-strings that include the identifiers needed to debug: run id, station/sensor id, endpoint, attempt count, computed sleep. Example:
  ```python
  logger.warning(f"Rate limit HTTP 429 received from {endpoint}. Retrying in {sleep_time:.2f}s (attempt {attempt + 1}/{self.max_retries})")
  ```
  (`src/skywatch/collector/openaq_client.py:85-87`)
- Scope a run with `with run_context(run_id) as rid:` so every log line carries the `run_id` (`src/skywatch/pipeline/logging.py:43-51`, used at `collector.py:82` and `:250`).
- Never log secrets. `redact_sensitive()` scrubs `api_key|token|secret|password|auth` and `Bearer ...` patterns, and `_redact_dict` masks sensitive keys (`src/skywatch/pipeline/logging.py:23-76`). `OpenAQClient.__repr__` deliberately omits the key (`openaq_client.py:43-45`).
- Level policy: `info` for lifecycle milestones, `warning` for recoverable/retried failures, `error` for terminal failures, `debug` for best-effort probes (`hdfs_storage.py:62`). Avoid `print()` outside `src/skywatch/cli.py`.

## Comments

**When to Comment:**
- Comments explain *why* and cite the requirement/invariant they protect, using short uppercase IDs (`HYG-05`, `WR-03`, `CR-02`, `IN-04`):
  ```python
  # Write to temp file in the same directory to ensure same filesystem for os.replace with cleanup (WR-21)
  ```
  (`src/skywatch/storage/local_storage.py:38`)
- Section comments number the orchestrator's stages: `# 1. Collect Air Quality Data`, `# 2. Collect Weather Data ...`, `# 3. Deduplication`, `# 4. Write Normalized Partitions`, `# 5. HDFS Upload / Cache`, `# 6. Commit Watermarks (Two-Phase Commit)` (`src/skywatch/collector/collector.py:94-227`).
- Inline comments mark deliberate non-obvious choices: `# Prevent API key leakage in string representations`, `# Case insensitive parameter normalization`.
- Do not restate the code. No commented-out code is present.

**JSDoc/TSDoc:**
- Not applicable. Use PEP 257 docstrings: one-line imperative summary for every public function, class, and method. Multi-line docstrings only when documenting a non-obvious contract or a `Returns:` section:
  ```python
  """Filter duplicate records from an in-memory batch while preserving original order.

  Returns:
      Tuple of (deduplicated_records_list, duplicate_count)
  """
  ```
  (`src/skywatch/data/deduplication.py:40-44`)
- `check_safemode` documents the inverted-boolean contract explicitly because it is easy to misuse (`src/skywatch/storage/hdfs_storage.py:42-45`).

## Function Design

**Size:** Small, single-purpose functions; most are 10–40 lines. The exception is `IngestionOrchestrator.run_incremental_collection` (~165 lines, `collector.py:76-240`) which is a linear numbered pipeline — acceptable because each stage is delegated.

**Parameters:**
- Keyword arguments for anything beyond 2–3 positional parameters, and always for boolean flags: `offline: bool = False`, `base_dir: str = "data/raw"`, `run_id: str`.
- Injectable collaborators with `Optional[...] = None` defaults, resolved in `__init__`:
  ```python
  self.openaq = openaq_client or OpenAQClient(offline=offline)
  ```
  (`src/skywatch/collector/collector.py:57-60`) — this is the primary seam used by integration tests.
- Deterministic-time seam: validators accept `reference_time: Optional[datetime] = None` and fall back to `datetime.now(timezone.utc)` (`src/skywatch/data/validation.py:72,85`). Always thread `reference_time` through new validators so tests stay clock-independent.
- All timestamps crossing a boundary are `str` in `"%Y-%m-%dT%H:%M:%SZ"` form, never `datetime`, never naive.

**Return Values:**
- Validators → `ValidationResult`. Normalizers → `Dict[str, Any]` (single record) or `List[Dict[str, Any]]` (unrolled rows). Orchestrators → a summary `Dict[str, Any]` with counts and output paths (`collector.py:229-238`).
- Storage writers return the written path as `str` (`write_normalized_csv`, `atomic_write_file`). `save_raw_response` returns the manifest entry dict.
- Absence is `None` (`Optional`), never `False` or `""` overloaded.

## Module Design

**Exports:**
- No `__all__` declarations and no barrel re-exports. Consumers import the concrete submodule path.
- Each package `__init__.py` holds only a docstring and `from __future__ import annotations`.

**Layering (respect when adding code):**
- `collector/` — network I/O only (`openaq_client`, `weather_client`, `discovery`) plus orchestration (`collector`).
- `data/` — pure transforms: `normalization`, `validation`, `deduplication`, `schemas`. No network, no filesystem writes (except reading `config/schemas.yaml`).
- `storage/` — durable persistence: `local_storage`, `hdfs_storage`, `raw_manifest`, `watermark`.
- `pipeline/` — cross-cutting infrastructure: `logging`, `profile`.
- `cli.py` — thin argparse dispatch; no business logic.
- Dependencies point downward only: `collector → data/storage/pipeline`, `storage → data/pipeline`. `data/` must not import `collector/` or `storage/`.

**Global state:**
- Only one module-level mutable cache: `_CACHED_SCHEMAS` in `src/skywatch/data/schemas.py:9`, guarded by the `config_path is None` check. Follow that pattern (cache only the default path) if caching is needed; do not add unbounded module caches.
- `run_id` is ambient via `contextvars.ContextVar` (`src/skywatch/pipeline/logging.py:18-20`), not a global variable.

**Data contracts:**
- `config/schemas.yaml` is the single source of truth for column names, order, and types. Read columns through `get_column_names()` / `validate_record_dict()`; never hardcode a CSV header list in production code (`src/skywatch/data/schemas.py`, consumed at `src/skywatch/storage/local_storage.py:70`).
- Adding a column means editing `config/schemas.yaml`, then updating `tests/contract/test_schemas.py::test_air_quality_schema_contract` (or the weather/manifest equivalent) in the same change.

---

*Convention analysis: 2026-11-22*
