# Codebase Structure

**Analysis Date:** 2026-11-22

## Directory Layout

```text
skywatch/
├── .github/
│   └── workflows/          # CI and scheduled ingestion automation
├── .planning/              # GSD project state, roadmap, phase plans/summaries
├── config/                 # Static and discovered data contracts
│   ├── schemas.yaml        # Canonical column contracts for all streams
│   └── stations.json       # Discovered Bengaluru station/sensor catalog
├── data/                   # Runtime data lake root (mostly git-ignored)
│   ├── raw/                # Untouched API responses + audit manifest
│   ├── normalized/         # Schema-conformant CSV partitions
│   ├── local_hdfs/         # Mirror of HDFS partitions when HDFS is offline
│   ├── .watermarks.json    # Per-sensor incremental state
│   └── .backfill_checkpoint.json
├── docs/                   # Project specification
├── profiles/               # modern/lab environment definitions
├── scripts/                # Cron runner and profile switcher
├── src/
│   ├── skywatch/           # Installable Python package
│   │   ├── cli.py
│   │   ├── collector/
│   │   ├── data/
│   │   ├── pipeline/
│   │   └── storage/
│   └── skywatch.egg-info/  # Generated packaging metadata (do not edit)
├── tests/
│   ├── unit/               # Pure-function and client tests
│   ├── contract/           # Schema contract tests
│   ├── integration/        # End-to-end orchestrator/storage/watermark tests
│   └── fixtures/           # Offline JSON API responses
├── pyproject.toml          # Package metadata, entry point, ruff/pytest config
└── README.md
```

## Directory Purposes

**`.github/workflows/`:**
- Purpose: Automation outside the Python package.
- Contains: `ci.yml` (ruff + pytest on push/PR), `scheduled-collect.yml` (30-minute `collect --mode latest` with secret injection and data commit).
- Key files: `.github/workflows/ci.yml`, `.github/workflows/scheduled-collect.yml`

**`.planning/`:**
- Purpose: GSD source of truth for scope, phases, and verification.
- Contains: `PROJECT.md`, `REQUIREMENTS.md`, `ROADMAP.md`, `STATE.md`, phase plans/summaries, review artifacts.
- Key files: `.planning/ROADMAP.md` (phases 2–4 are planned, not implemented), `.planning/REQUIREMENTS.md` (requirement IDs referenced throughout code comments).

**`config/`:**
- Purpose: Config-driven data contracts and station catalog.
- Contains: `schemas.yaml` (air_quality, weather, manifest column definitions), `stations.json` (8 stations, each with `station_id`, coordinates, and `pm25`/`pm10` sensor IDs).
- Key files: `config/schemas.yaml`, `config/stations.json`
- Note: `config/breakpoints.yaml` and `config/settings.yaml` appear in the target specification but are **not present**; they belong to planned AQI/analytics work (`.planning/ROADMAP.md:58-72`).

**`data/`:**
- Purpose: Filesystem system of record for ingestion output.
- Contains:
  - `data/raw/{source}/ingest_dt=YYYY-MM-DD/{source}_{run_id}_{sha8}.json` — untouched payloads.
  - `data/raw/_manifest.csv` — audit ledger (`run_id`, `file_path`, `source`, `endpoint_url`, `http_status`, `sha256`, `record_count`, `collected_at_utc`).
  - `data/normalized/{air_quality,weather}/ingest_dt=YYYY-MM-DD/{stream}_{run_id}.csv` — header-bearing canonical CSVs.
  - `data/local_hdfs/normalized/...` — fallback copy when HDFS is unavailable.
  - `data/.watermarks.json`, `data/.backfill_checkpoint.json` — incremental state.
- Key files: `data/.watermarks.json`, `data/.backfill_checkpoint.json`, generated partition CSVs.
- Generated: yes; committed selectively (manifest and directory placeholders only per `.gitignore`).

**`docs/`:**
- Purpose: Human-facing specification.
- Contains: `SkyWatch - Project Specification v2.0.md` (requirements, profiles, HDFS layout, target project tree, test cases, expected outputs).
- Key files: `docs/SkyWatch - Project Specification v2.0.md`

**`profiles/`:**
- Purpose: Keep toolchain versions and runtime thresholds out of code.
- Contains: `modern.env` (Hadoop 3.4.3, Pig 0.18.0, Spark 4.3.0, JDK 17 for Spark), `lab.env` (Hadoop 3.2.2, Spark 3.2.2, JDK 8 for Spark).
- Key files: `profiles/modern.env`, `profiles/lab.env`

**`scripts/`:**
- Purpose: Shell-level operations.
- Contains: `cron_collect.sh` (flock-guarded scheduled incremental collection), `use_profile.sh` (exports a profile's variables into the current shell).
- Key files: `scripts/cron_collect.sh`, `scripts/use_profile.sh`

**`src/skywatch/`:**
- Purpose: Installable application package (`skywatch`).
- Contains: CLI, collector clients/discovery/orchestrator, data contracts/normalization/validation/dedup, storage adapters, pipeline logging/profile utilities.
- Key files: `src/skywatch/cli.py`, `src/skywatch/collector/collector.py`, `src/skywatch/data/normalization.py`, `src/skywatch/storage/local_storage.py`

**`src/skywatch/collector/`:**
- Purpose: External source adapters and ingestion orchestration.
- Contains: `openaq_client.py`, `weather_client.py`, `discovery.py`, `collector.py`.
- Key files: `src/skywatch/collector/collector.py` (the integration point for every ingestion stage).

**`src/skywatch/data/`:**
- Purpose: Pure domain logic for canonical data.
- Contains: `schemas.py` (YAML loader + type checks), `normalization.py` (source payload → canonical row), `validation.py` (hygiene rules), `deduplication.py` (SHA-256 keys + batch dedupe).
- Key files: `src/skywatch/data/schemas.py`, `src/skywatch/data/validation.py`, `src/skywatch/data/normalization.py`, `src/skywatch/data/deduplication.py`

**`src/skywatch/pipeline/`:**
- Purpose: Cross-cutting operations and future orchestration entry points.
- Contains: `logging.py` (JSON logger, `run_id` context, redaction), `profile.py` (profile loading and environment validation).
- Key files: `src/skywatch/pipeline/logging.py`, `src/skywatch/pipeline/profile.py`
- Note: Planned `pig_runner.py`, `spark_runner.py`, and `pipeline_runner.py` are **not present** (`.planning/ROADMAP.md:88-90`).

**`src/skywatch/storage/`:**
- Purpose: Persistence primitives.
- Contains: `local_storage.py` (atomic write + partitioned CSV), `hdfs_storage.py` (safemode-aware upload + fallback), `raw_manifest.py` (raw JSON + manifest), `watermark.py` (per-sensor state).
- Key files: `src/skywatch/storage/local_storage.py`, `src/skywatch/storage/hdfs_storage.py`, `src/skywatch/storage/watermark.py`, `src/skywatch/storage/raw_manifest.py`

**`tests/`:**
- Purpose: Verification by test kind.
- Contains:
  - `tests/unit/` — clients, discovery, normalization, validation, dedupe, logging, profile, raw manifest.
  - `tests/contract/` — `schemas.yaml` contract checks.
  - `tests/integration/` — storage atomicity, watermark commit, end-to-end orchestrator + backfill.
  - `tests/fixtures/` — `sample_openaq_locations.json`, `sample_openaq_latest.json`, `sample_openaq_measurements.json`, `sample_weather_forecast.json`, `sample_weather_archive.json`.
- Key files: `tests/integration/test_collector_e2e.py`, `tests/contract/test_schemas.py`, `tests/fixtures/sample_openaq_measurements.json`

**Planned but absent top-level directories:**
- `pig/` — Phase 2 Pig scripts (`.planning/ROADMAP.md:18,42-54`).
- `src/skywatch/spark/`, `src/skywatch/analytics_core/` — Phase 3 Spark jobs and CPCB reference logic (`.planning/ROADMAP.md:19,58-72`).
- `src/skywatch/dashboard/`, `output/published/`, `deploy/` — Phase 4 Streamlit and export publishing (`.planning/ROADMAP.md:20,76-90`).
- `benchmarks/`, `Makefile` — Phase 3/4 scale benchmark and smoke/acceptance targets. Do not create these until their phase is planned.

## Key File Locations

**Entry Points:**
- `src/skywatch/cli.py`: argparse parser (`build_parser`, line 20) and dispatch (`main`, line 54).
- `pyproject.toml`: console script `skywatch = "skywatch.cli:main"` (lines 26-27).
- `scripts/cron_collect.sh`: local scheduled runner.
- `.github/workflows/scheduled-collect.yml`: cloud scheduled runner.

**Configuration:**
- `config/schemas.yaml`: canonical stream schemas (`air_quality`, `weather`, `manifest`).
- `config/stations.json`: discovered station catalog consumed by the orchestrator.
- `profiles/modern.env`, `profiles/lab.env`: runtime paths and thresholds.
- `pyproject.toml`: dependencies, Python floor, ruff and pytest settings.
- `.gitignore`: data-lake ignore rules; keeps `_manifest.csv` and directory placeholders.

**Core Logic:**
- `src/skywatch/collector/collector.py`: `IngestionOrchestrator` and all sequencing.
- `src/skywatch/collector/discovery.py`: station filtering and atomic catalog write.
- `src/skywatch/data/normalization.py`: canonical row construction.
- `src/skywatch/data/validation.py`: hygiene rules and reject codes.
- `src/skywatch/data/deduplication.py`: record-key generation and dedupe.
- `src/skywatch/storage/local_storage.py`: atomic writes and CSV partitioning.
- `src/skywatch/storage/hdfs_storage.py`: HDFS upload and offline fallback.
- `src/skywatch/storage/watermark.py`: incremental state.
- `src/skywatch/storage/raw_manifest.py`: raw audit trail.
- `src/skywatch/pipeline/logging.py`: structured logging and run identity.
- `src/skywatch/pipeline/profile.py`: dual-profile configuration.

**Testing:**
- `tests/unit/`: isolated behavior tests.
- `tests/contract/test_schemas.py`: schema shape/type contract.
- `tests/integration/test_storage.py`: atomic write and HDFS fallback behavior.
- `tests/integration/test_watermark.py`: commit/resume semantics.
- `tests/integration/test_collector_e2e.py`: offline end-to-end collection and backfill checkpoint behavior.
- `tests/fixtures/`: offline API payloads.

## Naming Conventions

**Files:**
- Python modules are `snake_case` and named for the responsibility: `openaq_client.py`, `local_storage.py`, `raw_manifest.py`.
- Test files are `test_<subject>.py` and grouped by test kind (`unit`, `contract`, `integration`).
- Documentation files in `.planning/codebase/` use uppercase names (`ARCHITECTURE.md`, `STRUCTURE.md`).
- Configuration files use lowercase names with extensions that match their format (`schemas.yaml`, `stations.json`, `modern.env`).

**Directories:**
- Package directories are lowercase singular nouns (`collector`, `storage`, `pipeline`).
- Data partitions follow Hive-style `key=value` directories: `ingest_dt=YYYY-MM-DD`.
- Test directories describe the test scope (`unit`, `contract`, `integration`, `fixtures`).

**Python Symbols:**
- Classes: `PascalCase` — `IngestionOrchestrator`, `OpenAQClient`, `WeatherClient`, `HDFSStorageAdapter`, `WatermarkManager`, `ProfileConfig`, `ValidationResult`.
- Functions/methods: `snake_case` verbs — `run_incremental_collection`, `write_normalized_csv`, `save_raw_response`, `validate_air_quality_record`, `discover_stations`.
- Private helpers: leading underscore — `_ensure_stations`, `_safe_id`, `_format_utc_timestamp`, `_request_with_retry`, `_load_fixture`.
- Constants: `UPPER_SNAKE_CASE` — `BENGALURU_BBOX`, `BENGALURU_CENTRAL_COORDINATES`, `MANIFEST_HEADERS`, `BASE_URL`, `FORECAST_URL`.
- Type hints: `typing.Dict`, `typing.List`, `typing.Optional` with `from __future__ import annotations` (Python 3.8 compatibility).

**Data Artifacts:**
- Raw JSON: `{source}_{run_id}_{sha256[:8]}.json`.
- Normalized CSV: `{stream}_{run_id}.csv`.
- Partition: `ingest_dt=YYYY-MM-DD`.
- Timestamps: ISO-8601 UTC with `Z` suffix (`%Y-%m-%dT%H:%M:%SZ`).
- Record identity: lowercase hex SHA-256 in `record_key`.

**Environment Variables:**
- SkyWatch-owned: `SKYWATCH_PROFILE`, `SKYWATCH_HTTP_TIMEOUT`, `SKYWATCH_MAX_RETRIES`, `SKYWATCH_FRESH_HOURS`, `SKYWATCH_STALE_HOURS`, `SKYWATCH_OFFLINE`.
- Toolchain: `HADOOP_HOME`, `PIG_HOME`, `SPARK_HOME`, `JAVA_HOME_PIG`, `JAVA_HOME_SPARK`, `JAVA_HOME`, `HDFS_ROOT`.
- Source-specific: `OPENAQ_API_KEY`, `OPENAQ_OFFLINE`, `WEATHER_OFFLINE`.

## Where to Add New Code

**New External Data Source:**
- Client: add `src/skywatch/collector/<source>_client.py` following the `OpenAQClient`/`WeatherClient` shape (session, retries, offline fixture branch, redacted `__repr__`).
- Fixtures: add `tests/fixtures/sample_<source>_*.json`.
- Integration: call it from `IngestionOrchestrator.run_incremental_collection` and, if it produces a new stream, add a schema and storage write in `src/skywatch/collector/collector.py`.
- Tests: `tests/unit/test_clients.py` or a new `tests/unit/test_<source>_client.py`.

**New Normalized Stream or Column:**
- Contract first: edit `config/schemas.yaml` (column name, type, nullable, order).
- Normalization: add a function in `src/skywatch/data/normalization.py` that emits the exact schema fields.
- Persistence: call `write_normalized_csv(..., stream_name="<new_stream>")` from the orchestrator; the writer derives columns from the schema.
- Tests: add a contract assertion in `tests/contract/test_schemas.py` and a normalization test in `tests/unit/test_normalization.py`.

**New Validation Rule:**
- Implement a `check_*` function returning `ValidationResult` in `src/skywatch/data/validation.py`.
- Chain it inside `validate_air_quality_record` (or a new `validate_<stream>_record`) in the same short-circuit order.
- Add a focused test in `tests/unit/test_validation.py` and ensure the reject reason is stable (downstream reports will group by it).

**New Storage Backend:**
- Add `src/skywatch/storage/<backend>_storage.py` with the same surface as `HDFSStorageAdapter`: availability check, `upload_partition(local_file_path, partition_dir, run_id)`, and explicit fallback/error behavior.
- Wire it into `IngestionOrchestrator.__init__` and the two upload call sites in `src/skywatch/collector/collector.py` (lines 222-224 and 338-339).
- Tests: `tests/integration/test_storage.py`.

**New Pipeline Stage or Runner (planned Pig/Spark/pipeline):**
- Put runner/orchestration code in `src/skywatch/pipeline/` (`pig_runner.py`, `spark_runner.py`, `pipeline_runner.py` per the target specification).
- Put Pig scripts in a new top-level `pig/` directory; put Spark jobs in `src/skywatch/spark/`.
- Keep `pipeline/` free of analytics; analytics belongs in `spark/` or `analytics_core/`.
- Do not add these until Phase 2/3 is planned (`.planning/ROADMAP.md:42-72`).

**New CLI Subcommand:**
- Add a subparser in `build_parser` (`src/skywatch/cli.py:20-51`) and a dispatch branch in `main` (`:63-93`).
- Reuse the JSON-summary + exit-code pattern already used by `collect` and `backfill`.
- Update `pyproject.toml` only if a new console entry point is needed; the existing `skywatch` script routes through `main`.

**Dashboard (planned):**
- Add `src/skywatch/dashboard/` with `app.py`, `data_access.py`, `components.py`, and `pages/` once Phase 4 begins (`.planning/ROADMAP.md:76-90`).
- Dashboard code should read exported `output/published/` files, not HDFS directly.

**Utilities:**
- Cross-cutting helpers (paths, time, hashing) belong in the owning layer: time formatting in `src/skywatch/data/normalization.py` or a shared module under `src/skywatch/data/`; logging/run identity in `src/skywatch/pipeline/logging.py`; filesystem atomicity in `src/skywatch/storage/local_storage.py`.
- Avoid a catch-all `utils.py`; the current code keeps helpers near their domain.

## Special Directories

**`data/`:**
- Purpose: Runtime data lake and incremental state.
- Generated: yes.
- Committed: mostly no. `.gitignore` preserves directory structure and `data/raw/_manifest.csv` only; normalized partitions, raw JSON, local HDFS mirror, watermarks, checkpoints, and logs are ignored.

**`src/skywatch.egg-info/`:**
- Purpose: Setuptools metadata from editable install.
- Generated: yes.
- Committed: no (ignored via `*.egg-info/`).
- Do not edit by hand.

**`.pytest_cache/`, `.ruff_cache/`, `__pycache__/`:**
- Purpose: Tool caches.
- Generated: yes.
- Committed: no.

**`.github/workflows/`:**
- Purpose: CI and scheduled collection definitions.
- Generated: no.
- Committed: yes.
- Note: `scheduled-collect.yml` writes data back to the repository with `git add -f data/raw/_manifest.csv config/stations.json`; changes there are automation-owned.

**`.planning/`:**
- Purpose: GSD planning and codebase-mapping artifacts.
- Generated: partly (phase outputs, this mapping).
- Committed: yes, by project convention.

---

*Structure analysis: 2026-11-22*
