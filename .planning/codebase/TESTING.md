# Testing Patterns

**Analysis Date:** 2026-11-22

## Test Framework

**Runner:**
- pytest `>=7.0.0` (`pyproject.toml`, `[project.optional-dependencies] dev`)
- Config: `pyproject.toml` → `[tool.pytest.ini_options]`
  ```toml
  testpaths = ["tests"]
  python_files = ["test_*.py"]
  pythonpath = ["src"]
  ```
  `pythonpath = ["src"]` means tests run against `src/` **without** installing the package and without a `conftest.py`. There is no `conftest.py` anywhere in the repo.

**Assertion Library:**
- Plain `assert` statements only. No `pytest.approx`, no custom assertion helpers, no `unittest.TestCase`.
- `pytest.raises` is used for exception paths: `tests/unit/test_clients.py:52`.

**Supporting libraries:**
- `responses >= 0.23.0` — HTTP interception via the `@responses.activate` decorator (`tests/unit/test_clients.py:42,57`).
- `pytest-cov >= 4.0.0` — declared but no coverage gate is configured.
- `unittest.mock` from the stdlib for everything non-HTTP.

**Run Commands:**
```bash
pytest                                   # Run all tests (testpaths = tests)
pytest -q                                # Quiet run (44 tests, ~1.8s)
pytest tests/unit                        # Unit suite only
pytest tests/integration                 # Integration suite only
pytest tests/contract                    # Contract suite only
pytest tests/unit/test_validation.py -k timestamp   # Single test / -k filter
pytest --cov=skywatch --cov-report=term-missing     # Coverage (no threshold enforced)
ruff check .                             # Lint gate (runs before pytest in CI)
```

**Current status (verified 2026-11-22):** `44 passed in 1.83s`; `ruff check .` → "All checks passed!".

## Test File Organization

**Location:**
- Separate `tests/` tree, never co-located with source.
- Three tiers by directory, all committed with `__init__.py` so they are importable packages:
  - `tests/unit/` — single-module logic with mocked I/O.
  - `tests/integration/` — real filesystem (`tmp_path`), real storage writers, real orchestrator.
  - `tests/contract/` — schema/YAML contract assertions against `config/schemas.yaml`.
  - `tests/fixtures/` — JSON payload fixtures + `__init__.py`.

**Naming:**
- `test_<module_under_test>.py`, mirroring the source module: `tests/unit/test_normalization.py` ↔ `src/skywatch/data/normalization.py`; `tests/integration/test_watermark.py` ↔ `src/skywatch/storage/watermark.py`.
- Test functions are `def test_<behavior>_<condition>()` in `snake_case`, describing the invariant rather than the method:
  - `test_openaq_authentication_failure`
  - `test_openaq_rate_limiting_retry`
  - `test_watermark_lifecycle_and_two_phase_commit`
  - `test_corrupted_watermark_recovery`
  - `test_save_raw_response_multi_station_no_overwrite`
  - `test_check_timestamp_naive_iso_does_not_crash`
  - `test_load_profile_prioritizes_file_over_ambient_env`
  - `test_hdfs_adapter_offline_fallback`

**Structure:**
```
tests/
├── __init__.py
├── fixtures/
│   ├── __init__.py
│   ├── sample_openaq_locations.json
│   ├── sample_openaq_latest.json
│   ├── sample_openaq_measurements.json
│   ├── sample_weather_forecast.json
│   └── sample_weather_archive.json
├── unit/          (8 files, 33 tests)
├── integration/   (3 files, 6 tests)
└── contract/      (1 file, 5 tests)
```

**Test counts by file:**

| File | Tests |
|------|-------|
| `tests/unit/test_validation.py` | 8 |
| `tests/unit/test_clients.py` | 5 |
| `tests/unit/test_logging.py` | 5 |
| `tests/unit/test_profile.py` | 5 |
| `tests/unit/test_deduplication.py` | 3 |
| `tests/unit/test_normalization.py` | 3 |
| `tests/unit/test_discovery.py` | 2 |
| `tests/unit/test_raw_manifest.py` | 2 |
| `tests/integration/test_storage.py` | 3 |
| `tests/integration/test_watermark.py` | 2 |
| `tests/integration/test_collector_e2e.py` | 1 |
| `tests/contract/test_schemas.py` | 5 |

## Test Structure

**Suite Organization:**
- One module docstring per file, then `from __future__ import annotations`, then imports, then flat top-level `def test_*` functions. No test classes, no `setup_method`, no `@pytest.fixture` definitions anywhere.

```python
"""Unit tests for data hygiene validation engine."""
from __future__ import annotations

from datetime import datetime, timezone

from skywatch.data.validation import (
    check_numeric_range,
    check_plausible_ceiling,
    check_required_fields,
    check_timestamp,
    check_unit,
    validate_air_quality_record,
)


def test_check_required_fields():
    rec = {"station_id": "2178", "value": 45.0}
    assert check_required_fields(rec, ["station_id", "value"]).is_valid is True
    assert check_required_fields(rec, ["station_id", "missing"]).is_valid is False
```
(`tests/unit/test_validation.py:1-21`)

**Patterns:**
- **Arrange–Act–Assert with inline literals.** No fixture factories; each test builds the exact record it needs. Related cases are grouped in one function as a sequence of asserts with comments, e.g. `tests/unit/test_validation.py:72-94` (valid, bad unit, negative, spike).
- **Assertions use identity for booleans** — `is True` / `is False`, never `== True`. Reason codes are compared with `==` (`assert res_neg.reject_reason == "negative_value"`).
- **No setup/teardown.** `tmp_path` provides per-test isolation; the repo's own atomic writers do the rest. There is no shared mutable state between tests.
- **Deterministic time via injection, not freezing.** Validators are called with `reference_time=datetime(2026, 11, 22, 12, 0, 0, tzinfo=timezone.utc)` instead of patching the clock (`tests/unit/test_validation.py:57,73,99,106`). Timestamps in fixtures are fixed at `2026-11-*` so assertions are stable.
- **Deterministic run ids via injection.** `run_id="run-e2e-1"`, `run_id="run-bf-1"` are passed explicitly so filenames and manifests are predictable (`tests/integration/test_collector_e2e.py:40,68`).
- **Path assertions on the partition layout**, not just file existence: `assert "ingest_dt=2026-11-22" in str(file_obj)` (`tests/integration/test_storage.py:49`).
- **Round-trip verification.** CSV output is re-read with `csv.DictReader` and row counts/values checked against the summary (`tests/integration/test_collector_e2e.py:54-58`); manifest SHA-256 is recomputed from bytes (`tests/unit/test_raw_manifest.py:38-40`).
- **Idempotency is asserted explicitly** by re-running the backfill and expecting zero processed chunks (`tests/integration/test_collector_e2e.py:74-80`).
- **Contract cross-checks inside unit tests.** Normalization tests validate the produced dict against the YAML schema: `valid, err = validate_record_dict(norm, "air_quality"); assert valid is True, f"Contract mismatch: {err}"` (`tests/unit/test_normalization.py:40-41`).

## Mocking

**Framework:** stdlib `unittest.mock` + `responses` for HTTP.

**Patterns:**

*HTTP interception with `responses`* — preferred over patching `requests` directly:
```python
@responses.activate
def test_openaq_rate_limiting_retry():
    client = OpenAQClient(max_retries=3)
    responses.add(
        responses.GET,
        "https://api.openaq.org/v3/locations",
        headers={"Retry-After": "0"},
        status=429,
    )
    responses.add(
        responses.GET,
        "https://api.openaq.org/v3/locations",
        json={"results": [{"id": 1, "name": "Test Station"}]},
        status=200,
    )
    result = client.get_locations("77.45,12.83,77.75,13.12")
    assert len(result["results"]) == 1
```
(`tests/unit/test_clients.py:57-77`) — `Retry-After: 0` keeps the retry path fast; never let a test sleep for real backoff.

*Method patching on a real instance* — for storage adapters:
```python
with mock.patch.object(adapter, "is_available", return_value=False):
    success = adapter.upload_partition(str(src_file), "normalized/air_quality/ingest_dt=2026-11-22", "run-1")
```
(`tests/integration/test_storage.py:66-67`)

*Function patching by dotted path* — as a decorator for collaborators in another module:
```python
@mock.patch("skywatch.pipeline.profile._check_java_version")
def test_validate_environment_mocked_java(mock_check_java, tmp_path):
    ...
    mock_check_java.side_effect = side_effect
```
(`tests/unit/test_profile.py:97-127`)

*Spec'd `MagicMock` for injected collaborators*:
```python
mock_client = mock.MagicMock(spec=OpenAQClient)
mock_client.offline = False
mock_client.get_locations.return_value = {"results": [...]}
stations = discover_stations(mock_client, output_file=None)
```
(`tests/unit/test_discovery.py:35-54`) — always pass `spec=` so a renamed method fails loudly.

*Environment isolation*:
```python
with mock.patch.dict(os.environ, {}, clear=True):
    config = load_profile("modern", profiles_dir=str(profiles_dir))
```
(`tests/unit/test_profile.py:30-31`) — use `clear=True` when asserting profile-file precedence; use a targeted dict when asserting ambient-env behavior (`tests/unit/test_profile.py:74`).

*Log capture*: pytest's built-in `capsys`, then parse the JSON line:
```python
captured = capsys.readouterr()
parsed = json.loads(captured.err or captured.out)
```
(`tests/unit/test_logging.py:73-79`)

**What to Mock:**
- Network egress: `responses` for `requests`-based clients, or the client's `offline=True` fixture mode.
- External binaries: `hdfs` (via `mock.patch.object(adapter, "is_available", ...)`), `java` (via `_check_java_version`).
- Injected collaborators: pass a `MagicMock(spec=...)` into `IngestionOrchestrator(..., openaq_client=..., weather_client=...)`.
- `os.environ` whenever profile/env-precedence behavior is under test.

**What NOT to Mock:**
- The filesystem. Use `tmp_path` and the real `atomic_write_file`, `write_normalized_csv`, `WatermarkManager`, `save_raw_response`. All integration tests do this, which is what actually exercises the fsync/`os.replace` contract.
- `config/schemas.yaml`. Contract tests read the real file (`tests/contract/test_schemas.py:15`).
- Time in validators — inject `reference_time` instead.
- `deduplicate_batch`, `normalize_*`, `check_*` — these are pure and fast; call them directly.

## Fixtures and Factories

**Test Data:**
- Static JSON payloads under `tests/fixtures/`, shaped like real API responses (`results` arrays for OpenAQ; `hourly.time`/`hourly.temperature_2m` parallel arrays for Open-Meteo).
- Tests load them by constructing a `Path`:
  ```python
  fixtures_dir = Path("tests/fixtures")
  with open(fixtures_dir / "sample_openaq_latest.json", "r", encoding="utf-8") as f:
      latest = json.load(f)
  ```
  (`tests/unit/test_normalization.py:15-17`; also `test_normalization.py:63-65`)
- The offline clients resolve the same directory by default (`Path("tests/fixtures")` in `src/skywatch/collector/openaq_client.py:40` and `weather_client.py:39`), so `OpenAQClient(offline=True)` and `WeatherClient(offline=True)` are themselves fixtures-by-construction (`tests/unit/test_clients.py:18-39`, `tests/integration/test_collector_e2e.py:22-23`).

**Location:**
- `tests/fixtures/`. Add a new JSON file there when a test needs a new upstream payload shape; do not inline large payloads in test bodies.

**Known fragility:** fixture paths are resolved relative to the **process working directory** (`Path("tests/fixtures")`). Tests must be run from the repo root (which `pytest`'s `testpaths` assumes). There is no `conftest.py` anchoring the path. Prefer passing `fixtures_dir=str(...)` or deriving from `__file__` in new tests.

**No factories or builders exist.** If a test needs a valid record, it writes the dict inline (e.g. `valid_rec` in `tests/unit/test_validation.py:74-81`). Keep that style: explicit literals make the assertion self-documenting.

## Coverage

**Requirements:** None enforced. `pytest-cov` is a declared dev dependency, but `pyproject.toml` has no `--cov` addopts, no `fail_under`, and `.github/workflows/ci.yml` runs plain `pytest` with no coverage step or threshold.

**View Coverage:**
```bash
pytest --cov=skywatch --cov-report=term-missing
pytest --cov=skywatch --cov-report=html   # then open htmlcov/index.html
```

**Observed coverage shape (by inspection, not measured):**
- Well covered: `data/validation.py`, `data/deduplication.py`, `data/normalization.py`, `data/schemas.py`, `storage/local_storage.py`, `storage/watermark.py`, `storage/raw_manifest.py`, `pipeline/logging.py`, `pipeline/profile.py`, `collector/openaq_client.py` (offline + 401 + 429 paths), `collector/discovery.py`, `collector/collector.py` (both modes via the e2e test).
- Uncovered: `cli.py` (no test invokes `build_parser`/`main`), `collector/weather_client.py` retry/error branches (only offline mode is tested), `storage/hdfs_storage.py` online path and `check_safemode` parsing, `data/validation.py::check_plausible_ceiling` non-PM branch is covered but weather unit-mismatch branches are thin.

## Test Types

**Unit Tests (`tests/unit/`, 33 tests):**
- Scope: one module, pure functions, or a client with HTTP mocked or offline fixtures.
- Approach: inline literals, `is True/is False` assertions, reason-code equality, no filesystem except `tmp_path` for storage helpers (`test_raw_manifest.py`).
- Examples: `test_check_unit` (unit spelling variants `ug/m3`, `µg/m³`, `ug/m^3`; rejects `ppm`), `test_redact_sensitive_strings` (secret never appears in output), `test_compute_air_quality_key` (case-insensitive parameter, 64-char hex).

**Integration Tests (`tests/integration/`, 6 tests):**
- Scope: real filesystem + real collaborators, external world still offline.
- Approach: build every path under `tmp_path`, inject `offline=True` clients, real `WatermarkManager`/`HDFSStorageAdapter` with `offline_dir` pointed at `tmp_path`, then assert on disk artifacts (manifest rows, CSV rows, checkpoint file, watermark JSON).
- The end-to-end test drives both `run_incremental_collection(mode="latest")` and `run_chunked_backfill(...)` in one function, including the re-run idempotency check (`tests/integration/test_collector_e2e.py`).

**Contract Tests (`tests/contract/`, 5 tests):**
- Scope: the `config/schemas.yaml` data contract and the validator's behavior against it.
- Approach: assert exact ordered column lists, `partition_key`, `has_header`, and the `version`; then feed good/bad dicts to `validate_record_dict` and assert on the returned reason string (`tests/contract/test_schemas.py:24-143`).
- These are the guardrail for schema changes: any column reorder/rename must update this file.

**E2E Tests:**
- Not used against live APIs. Live-network behavior is covered only by the scheduled workflow `.github/workflows/scheduled-collect.yml`, which runs `python -m skywatch.cli collect --mode latest` every 30 minutes with `OPENAQ_API_KEY` from secrets. That workflow is **not** a test and has no assertions.

## CI Pipeline

`.github/workflows/ci.yml` — "CI / Test Suite", triggers on push to `main`/`feature/**`/`chore/**` and PRs to `main`:
1. Python 3.11 (`setup-python@v5`) — note: CI runs 3.11 while `requires-python = ">=3.8"`; the 3.8 floor is enforced only by ruff's `target-version = "py38"`, not by a 3.8 test job.
2. `pip install ruff pytest responses` then `pip install -e .`.
3. `ruff check .`
4. `pytest` (no coverage flag, no markers, no matrix).

`.github/workflows/scheduled-collect.yml` — separate ingestion cron, not part of the test gate.

## Common Patterns

**Async Testing:**
- Not applicable. The codebase is synchronous (`requests`, blocking `time.sleep`). No `asyncio`, `pytest-asyncio`, or `anyio`.

**Error Testing:**
```python
with pytest.raises(AuthenticationError) as exc_info:
    client.get_locations("77.45,12.83,77.75,13.12")
assert "HTTP 401" in str(exc_info.value)
```
(`tests/unit/test_clients.py:52-54`)

**Negative-path coverage via result objects** (most common form here) — call the function and assert the reason code rather than expecting an exception:
```python
res_future = check_timestamp("2026-11-22T15:00:00Z", reference_time=now)
assert res_future.is_valid is False
assert res_future.reject_reason == "future_timestamp"
```
(`tests/unit/test_validation.py:64-66`)

**Corrupt-input resilience:**
```python
wm_file.write_text("{corrupt-json...", encoding="utf-8")
wm = WatermarkManager(str(wm_file))
assert wm.get_watermark("any") is None
```
(`tests/integration/test_watermark.py:36-41`)

**Two-phase commit assertion** — prove nothing is on disk before `commit()`:
```python
assert wm_file.is_file() is False
wm.commit(run_id="run-1")
assert wm_file.is_file() is True
```
(`tests/integration/test_watermark.py:25-29`)

## Gaps and Guidance for New Tests

- **No `parametrize` usage anywhere.** For new table-driven cases (unit variants, reason codes, timestamp formats), prefer `@pytest.mark.parametrize` over repeated assert blocks; it is compatible with the existing style and reduces duplication in files like `test_validation.py`.
- **No `conftest.py`.** If you need a shared fixture (e.g. a repo-root anchor for `tests/fixtures`, or a reusable valid-record builder), add `tests/conftest.py` rather than duplicating path logic. Do not add `sys.path` manipulation — `pythonpath = ["src"]` already handles imports.
- **No CLI tests.** `src/skywatch/cli.py` is untested. Add `tests/unit/test_cli.py` exercising `build_parser()` (subcommand/arg defaults) and `main([...])` with offline clients, asserting the `0`/`1` exit codes.
- **Weather retry paths untested.** `WeatherClient._request_with_retry` 429/5xx branches (`src/skywatch/collector/weather_client.py:52-85`) have no `responses`-based test; mirror `test_openaq_rate_limiting_retry`.
- **No coverage gate.** Consider `--cov=skywatch --cov-fail-under=<n>` in `[tool.pytest.ini_options] addopts` once a baseline is measured, so coverage cannot silently regress.
- **No Python 3.8 CI job.** Add a matrix entry (`3.8`, `3.11`) to `.github/workflows/ci.yml` to actually enforce the documented syntax floor.
- **Regression-test rule (from `AGENTS.md`):** every behavior change ships with a targeted regression test. The existing convention is to append a focused `test_<behavior>` to the matching tier file, not to create new files per fix.
- **Test naming references requirement IDs in comments**, not in test names: `# CR-04: naive ISO format without Z or offset should be safely handled as UTC` (`tests/unit/test_validation.py:98`). Follow that pattern to keep the test name behavioral while preserving traceability.

---

*Testing analysis: 2026-11-22*
