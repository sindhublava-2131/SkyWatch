<!-- refreshed: 2026-11-22 -->
# Architecture

**Analysis Date:** 2026-11-22

## System Overview

SkyWatch is a single-process, batch-oriented ingestion pipeline. Phase 1 (the only implemented phase) collects OpenAQ v3 air-quality readings and Open-Meteo weather data, normalizes them to config-defined schemas, validates and deduplicates records, writes atomic partitioned CSV files, mirrors partitions to HDFS (or a local fallback), and advances per-sensor watermarks.

```text
┌──────────────────────────────────────────────────────────────────────┐
│ Entry points                                                          │
│ `skywatch` console script · `python -m skywatch.cli`                  │
│ `scripts/cron_collect.sh` · `.github/workflows/scheduled-collect.yml` │
└────────────────────────────────┬─────────────────────────────────────┘
                                 ▼
┌──────────────────────────────────────────────────────────────────────┐
│ Orchestration — `src/skywatch/collector/collector.py`                 │
│ `IngestionOrchestrator`: incremental collection + chunked backfill    │
└───────┬──────────────────┬───────────────────┬───────────────────────┘
        ▼                  ▼                   ▼
┌───────────────┐  ┌──────────────────┐  ┌─────────────────────────────┐
│ Collector     │  │ Data hygiene     │  │ Storage                     │
│ OpenAQ client │  │ normalize →      │  │ raw manifest · local CSV    │
│ Weather client│  │ validate →       │  │ HDFS adapter · watermarks   │
│ Discovery     │  │ deduplicate      │  │ checkpoint                  │
└───────┬───────┘  └────────┬─────────┘  └──────────────┬──────────────┘
        ▼                   ▼                           ▼
  OpenAQ v3 API       `config/schemas.yaml`       `data/raw/`,
  Open-Meteo API      validation rules             `data/normalized/`,
  or `tests/fixtures`                              `data/local_hdfs/`,
                                                   `data/.watermarks.json`
```

Planned downstream layers (not implemented):

```text
normalized CSV
  → Phase 2: Apache Pig clean / distinct / rollup on HDFS   (ROADMAP.md:18,42-54)
  → Phase 3: Spark analytics, weather join, CPCB PM index  (ROADMAP.md:19,58-72)
  → `output/published/` CSV + Parquet
  → Phase 4: Streamlit dashboard                           (ROADMAP.md:20,76-90)
```

## Component Responsibilities

| Component | Responsibility | File |
|-----------|----------------|------|
| CLI | Parse subcommands, construct clients/orchestrator, print JSON summaries, map failures to exit code 1 | `src/skywatch/cli.py` |
| Ingestion orchestrator | Coordinate discovery, API calls, raw preservation, normalization, validation, deduplication, storage, watermarks, backfill checkpoints | `src/skywatch/collector/collector.py` |
| OpenAQ client | OpenAQ v3 HTTP calls, retry/backoff, 429 handling, auth abort, offline fixtures | `src/skywatch/collector/openaq_client.py` |
| Weather client | Open-Meteo forecast/archive calls, retry/backoff, offline fixtures | `src/skywatch/collector/weather_client.py` |
| Station discovery | Filter Bengaluru bbox locations to active stations with PM2.5/PM10 sensors, atomically write `config/stations.json` | `src/skywatch/collector/discovery.py` |
| Schema loader | Load/cache `config/schemas.yaml`, expose ordered columns and record type checks | `src/skywatch/data/schemas.py` |
| Normalization | Map OpenAQ records and Open-Meteo parallel arrays to canonical row dicts with UTC timestamps | `src/skywatch/data/normalization.py` |
| Validation | Required-field, numeric, ceiling, unit, and timestamp hygiene checks returning structured reject codes | `src/skywatch/data/validation.py` |
| Deduplication | Deterministic SHA-256 record keys and in-batch dedupe | `src/skywatch/data/deduplication.py` |
| Raw manifest | Atomically persist untouched JSON responses and append audit metadata to `_manifest.csv` | `src/skywatch/storage/raw_manifest.py` |
| Local storage | Atomic file replacement and schema-ordered partitioned CSV writing | `src/skywatch/storage/local_storage.py` |
| HDFS adapter | Safemode check, staging-then-rename upload, offline fallback cache | `src/skywatch/storage/hdfs_storage.py` |
| Watermark manager | Per-sensor high-water marks with in-memory updates and atomic commit | `src/skywatch/storage/watermark.py` |
| Profile loader | Read `profiles/{modern,lab}.env`, expose runtime paths/thresholds, validate installed runtimes | `src/skywatch/pipeline/profile.py` |
| Logging | JSON logs, `run_id` context propagation, credential redaction | `src/skywatch/pipeline/logging.py` |

## Pattern Overview

**Overall:** Layered ingestion pipeline with adapter boundaries and config-driven data contracts.

**Key Characteristics:**
- **Synchronous and single-process.** Stations and sensors are polled sequentially in one Python process; there is no queue, worker pool, or async I/O.
- **Filesystem as system of record.** There is no database. Raw JSON, normalized CSV, watermarks, and backfill checkpoints are all files under `data/`.
- **Config-as-contract.** `config/schemas.yaml` defines column order and types for CSV output and validation; `config/stations.json` defines the station/sensor universe; `profiles/*.env` defines runtime paths and freshness thresholds.
- **Offline-first testability.** Every client accepts an `offline` flag and loads JSON fixtures from `tests/fixtures/`; HDFS upload transparently falls back to `data/local_hdfs/`.
- **Atomic replacement everywhere state matters.** Raw files, normalized CSVs, station discovery output, watermarks, and backfill checkpoints are written to a temp file in the destination directory, flushed/fsynced, then `os.replace`d.
- **Deterministic identity.** Every normalized row carries a SHA-256 `record_key` derived from natural keys, making dedupe independent of input order.

## Layers

**Entry / Interface Layer:**
- Purpose: Translate user or scheduler intent into orchestrator calls.
- Location: `src/skywatch/cli.py`, `scripts/cron_collect.sh`, `.github/workflows/scheduled-collect.yml`
- Contains: argparse parser, subcommand dispatch, top-level exception handling, JSON summary printing.
- Depends on: `skywatch.collector.*`, `skywatch.pipeline.logging`.
- Used by: operators, cron, GitHub Actions.

**Orchestration Layer:**
- Purpose: Own the end-to-end sequence and side-effect ordering.
- Location: `src/skywatch/collector/collector.py`
- Contains: `IngestionOrchestrator`, station loading fallback, incremental loop, latest-mode loop, weather fetch, dedupe, partition writes, HDFS upload, watermark commit, chunked backfill loop.
- Depends on: collector clients, data modules, storage modules, logging.
- Used by: `cli.py`, integration tests.

**Collection / Source Adapter Layer:**
- Purpose: Isolate external API behavior, retries, and fixture fallbacks.
- Location: `src/skywatch/collector/openaq_client.py`, `src/skywatch/collector/weather_client.py`, `src/skywatch/collector/discovery.py`
- Contains: HTTP session management, exponential backoff, `AuthenticationError`, `get_locations`, `get_latest`, `get_measurements`, `get_hours`, `get_forecast`, `get_archive`, `discover_stations`.
- Depends on: `requests`, `skywatch.pipeline.logging`.
- Used by: orchestrator and CLI `discover`.

**Data Hygiene / Domain Layer:**
- Purpose: Convert heterogeneous payloads into canonical rows and decide validity/identity.
- Location: `src/skywatch/data/`
- Contains: schema loading and record checks (`schemas.py`), feed normalization (`normalization.py`), hygiene rules (`validation.py`), SHA-256 keys and batch dedupe (`deduplication.py`).
- Depends on: `pyyaml`, standard library.
- Used by: orchestrator, local storage (schema column order), tests.

**Storage / Persistence Layer:**
- Purpose: Persist raw payloads, normalized partitions, HDFS copies, and incremental state.
- Location: `src/skywatch/storage/`
- Contains: `save_raw_response`, `atomic_write_file`, `write_normalized_csv`, `HDFSStorageAdapter`, `WatermarkManager`.
- Depends on: `skywatch.data.schemas`, `skywatch.pipeline.logging`, `hdfs` CLI, filesystem.
- Used by: orchestrator.

**Cross-Cutting / Operations Layer:**
- Purpose: Provide profile configuration and structured observability.
- Location: `src/skywatch/pipeline/`
- Contains: `load_profile`, `validate_environment`, `ProfileConfig`, `get_logger`, `run_context`, `redact_sensitive`, `JsonPipelineFormatter`.
- Depends on: `python-dotenv`, standard library.
- Used by: every other layer.

**Planned Layers (absent):**
- Pig ETL (`pig/`, Pig runner) — Phase 2, not present. See `.planning/ROADMAP.md:18` and `:42-54`.
- Spark analytics (`src/skywatch/spark/`, `analytics_core/`) — Phase 3, not present. See `.planning/ROADMAP.md:19` and `:58-72`.
- Streamlit dashboard (`src/skywatch/dashboard/`, `output/published/`) — Phase 4, not present. See `.planning/ROADMAP.md:20` and `:76-90`.

## Data Flow

### Incremental Collection (default)

1. `cli.main` parses `collect --mode incremental` and constructs `IngestionOrchestrator` (`src/skywatch/cli.py:74-78`).
2. `run_incremental_collection` opens a `run_context`, generating a UUID4 `run_id` used in logs and filenames (`src/skywatch/collector/collector.py:76-88`; `src/skywatch/pipeline/logging.py:43-51`).
3. `_ensure_stations` loads `config/stations.json`, or calls `discover_stations` if the file is missing/unreadable (`src/skywatch/collector/collector.py:66-74`).
4. For each station and each sensor, the orchestrator reads the sensor watermark. If absent, it defaults the lower bound to `now - 24h` (`src/skywatch/collector/collector.py:132-142`).
5. `OpenAQClient.get_measurements` calls `/v3/sensors/{id}/measurements` with retry/backoff, or returns `tests/fixtures/sample_openaq_measurements.json` when offline (`src/skywatch/collector/openaq_client.py:131-148`).
6. The untouched response is written atomically to `data/raw/openaq/ingest_dt=YYYY-MM-DD/openaq_{run_id}_{sha8}.json` and appended to `data/raw/_manifest.csv` (`src/skywatch/collector/collector.py:150-158`; `src/skywatch/storage/raw_manifest.py:33-112`).
7. Each result is normalized into the canonical air-quality row, validated, and given a SHA-256 `record_key` (`src/skywatch/data/normalization.py:37-118`; `src/skywatch/data/validation.py:130-163`; `src/skywatch/data/deduplication.py:11-21`).
8. The newest valid timestamp per sensor updates the in-memory watermark; nothing is persisted yet (`src/skywatch/collector/collector.py:162-178`; `src/skywatch/storage/watermark.py:49-68`).
9. Weather is fetched for the fixed Bengaluru central point `(12.91, 77.61)` via `WeatherClient.get_forecast`, raw-preserved, and unrolled from parallel arrays into weather rows (`src/skywatch/collector/collector.py:182-199`; `src/skywatch/data/normalization.py:121-167`).
10. Both streams are deduplicated in memory by `record_key` (`src/skywatch/collector/collector.py:201-203`; `src/skywatch/data/deduplication.py:36-62`).
11. `write_normalized_csv` orders columns from `config/schemas.yaml`, writes to `data/normalized/{stream}/ingest_dt=YYYY-MM-DD/{stream}_{run_id}.csv`, and atomically replaces the target (`src/skywatch/storage/local_storage.py:62-87`).
12. `HDFSStorageAdapter.upload_partition` checks `hdfs version` and NameNode safemode. If unavailable, it copies to `data/local_hdfs/...`; otherwise it stages under `/skywatch/_staging` and renames into `/skywatch/normalized/...` (`src/skywatch/storage/hdfs_storage.py:65-123`).
13. `WatermarkManager.commit` atomically replaces `data/.watermarks.json` only after partition writes and HDFS attempts complete (`src/skywatch/collector/collector.py:226-227`; `src/skywatch/storage/watermark.py:70-99`).
14. A summary dict (run id, stations polled, record/duplicate counts, CSV paths) is printed as JSON (`src/skywatch/collector/collector.py:229-240`; `src/skywatch/cli.py:76-78`).

### Latest Mode

1. `collect --mode latest` calls `/v3/locations/{station_id}/latest` per station instead of per-sensor measurement windows (`src/skywatch/collector/collector.py:98-115`).
2. Each result is normalized, validated, and immediately compared to the existing watermark; only newer valid records update the in-memory watermark (`src/skywatch/collector/collector.py:113-130`).
3. Weather collection, dedupe, partition writes, HDFS upload, and watermark commit follow the same path as incremental mode.

### Chunked Backfill

1. `cli.main` parses `backfill --start-date --end-date --chunk-days` and calls `run_chunked_backfill` (`src/skywatch/cli.py:80-88`).
2. The orchestrator loads `data/.backfill_checkpoint.json` and skips chunk keys already listed in `completed_chunks` (`src/skywatch/collector/collector.py:255-276`).
3. For each chunk, it fetches per-sensor measurements and Open-Meteo archive weather, preserving both raw payloads (`src/skywatch/collector/collector.py:282-329`).
4. Records are normalized and deduplicated per chunk, then written to `ingest_dt={chunk_end}` partitions and uploaded to HDFS (`src/skywatch/collector/collector.py:331-339`).
5. The chunk key is appended to the checkpoint, which is atomically replaced before advancing (`src/skywatch/collector/collector.py:341-358`).
6. Backfill does **not** update sensor watermarks; it advances only the chunk checkpoint.

### Station Discovery

1. `discover` constructs `OpenAQClient` and calls `discover_stations` (`src/skywatch/cli.py:64-72`).
2. The client queries `/v3/locations` with bbox `77.45,12.83,77.75,13.12` (`src/skywatch/collector/discovery.py:16-32`; `src/skywatch/collector/openaq_client.py:118-123`).
3. Locations without coordinates or PM2.5/PM10 sensors are dropped; recency filtering uses `datetimeLast` unless offline (`src/skywatch/collector/discovery.py:37-90`).
4. The station list is atomically written to `config/stations.json` (`src/skywatch/collector/discovery.py:94-112`).

**State Management:**
- **Watermarks:** `data/.watermarks.json`, keyed by `sensor_id`, holding `parameter`, `station_id`, `last_measurement_utc`, `last_run_id`, `records_ingested` (`src/skywatch/storage/watermark.py:20-99`).
- **Backfill checkpoint:** `data/.backfill_checkpoint.json`, holding `completed_chunks` and `last_updated` (`src/skywatch/collector/collector.py:255-264`, `:341-355`).
- **Schema cache:** process-global `_CACHED_SCHEMAS` in `src/skywatch/data/schemas.py:9-27`.
- **Run identity:** `contextvars.ContextVar` in `src/skywatch/pipeline/logging.py:17-20`, scoped by `run_context`.
- **No database, queue, or distributed state store.**

## Key Abstractions

**`record_key` (deterministic identity):**
- Purpose: Stable dedupe key independent of payload order.
- Formula: air quality = SHA-256 of `"{sensor_id}|{parameter.lower()}|{measurement_time_utc}"`; weather = SHA-256 of `"{location_id}|{measurement_time_utc}"`.
- Files: `src/skywatch/data/deduplication.py:11-33`
- Pattern: natural composite key → hash → row field `record_key` → in-batch `seen` set.

**Schema contract (`config/schemas.yaml`):**
- Purpose: Single source of truth for CSV column order, names, types, nullability, delimiter, and partition key.
- Files: `config/schemas.yaml`; consumed by `src/skywatch/data/schemas.py:12-92` and `src/skywatch/storage/local_storage.py:70-79`.
- Pattern: YAML → cached dict → `get_column_names` / `validate_record_dict`.

**Atomic write protocol:**
- Purpose: Prevent partial files from being observed.
- Files: `src/skywatch/storage/local_storage.py:22-59`; also implemented inline in `src/skywatch/storage/raw_manifest.py:72-84`, `src/skywatch/storage/watermark.py:78-97`, `src/skywatch/collector/discovery.py:97-109`, `src/skywatch/collector/collector.py:344-355`.
- Pattern: same-directory `NamedTemporaryFile` → `write` → `flush` → `fsync` → `os.replace` → cleanup on exception.

**Per-sensor watermark:**
- Purpose: Incremental resume without re-reading history.
- Files: `src/skywatch/storage/watermark.py:20-99`
- Pattern: load JSON at construction → mutate in memory during run → single atomic `commit` at the end.

**HDFS adapter with offline fallback:**
- Purpose: Keep ingestion working on laptops without a live Hadoop cluster.
- Files: `src/skywatch/storage/hdfs_storage.py:17-123`
- Pattern: `is_available()` + `check_safemode()` gate → stage to `/skywatch/_staging` → `hdfs dfs -mv` → fallback copy to `data/local_hdfs/`.

**`ProfileConfig` / `load_profile`:**
- Purpose: Keep Hadoop/Pig/Spark versions and thresholds out of application logic.
- Files: `src/skywatch/pipeline/profile.py:16-72`
- Pattern: profile name → sanitized filename → dotenv values → dataclass with defaults; `validate_environment` checks directories and Java versions.

**`ValidationResult`:**
- Purpose: Uniform pass/fail plus machine-readable reject reason.
- Files: `src/skywatch/data/validation.py:13-16`
- Pattern: dataclass `(is_valid, reject_reason)` returned by each rule; `validate_air_quality_record` short-circuits on first failure.

## Entry Points

**`skywatch` console script:**
- Location: `pyproject.toml:26-27` → `src/skywatch/cli.py:54`
- Triggers: shell invocation after `pip install -e .`.
- Responsibilities: parse `discover`, `collect`, `backfill`; construct dependencies; print summaries; return exit code.

**`python -m skywatch.cli`:**
- Location: `src/skywatch/cli.py:98-99`
- Triggers: cron script, GitHub Actions, manual execution.
- Responsibilities: same as console script.

**`scripts/cron_collect.sh`:**
- Location: `scripts/cron_collect.sh`
- Triggers: local cron.
- Responsibilities: flock guard, profile sourcing, `python -m skywatch.cli collect --mode incremental`, append to `data/logs/cron_collect.log`.

**`.github/workflows/scheduled-collect.yml`:**
- Location: `.github/workflows/scheduled-collect.yml`
- Triggers: every 30 minutes and `workflow_dispatch`.
- Responsibilities: install package, run `collect --mode latest` with `OPENAQ_API_KEY`, commit `data/raw/_manifest.csv` and `config/stations.json` back to the repo.

**`.github/workflows/ci.yml`:**
- Location: `.github/workflows/ci.yml`
- Triggers: push/PR.
- Responsibilities: `ruff check .` and `pytest`.

## Architectural Constraints

- **Python 3.8 floor:** All modules use `from __future__ import annotations`, `typing.List/Dict/Optional`, and avoid `match`/`X | Y` (`pyproject.toml:11`; `AGENTS.md`).
- **Working-directory coupling:** Default paths are relative (`config/stations.json`, `data/raw`, `data/normalized`, `data/.watermarks.json`, `tests/fixtures`, `profiles`). Running from anywhere other than the repo root changes behavior (`src/skywatch/collector/collector.py:50-54`; `src/skywatch/storage/watermark.py:23`; `src/skywatch/pipeline/profile.py:47`).
- **Single-threaded execution:** The orchestrator loops synchronously; no concurrency, locking, or parallel station polling. `run_id` is propagated via `contextvars`, not threads.
- **HDFS access is shell-based:** `HDFSStorageAdapter` shells out to the `hdfs` binary with short timeouts. There is no WebHDFS/REST client and no retry loop around upload.
- **No database or migration layer:** State is JSON/CSV on disk; schemas are versioned in `config/schemas.yaml` (`version: "2.0"`).
- **Profile version isolation:** Runtime versions are injected through `profiles/modern.env` and `profiles/lab.env`; application code does not hardcode versions.
- **UTC and determinism:** All timestamps are formatted as `%Y-%m-%dT%H:%M:%SZ`; dedupe keys and CSV column order are deterministic.
- **Planned distributed layers are not wired:** There is no `pig/`, `src/skywatch/spark/`, `src/skywatch/analytics_core/`, `src/skywatch/dashboard/`, `output/`, or `Makefile` in the repository. See `.planning/ROADMAP.md:18-20`.

## Anti-Patterns

### Weather hygiene is defined but not invoked

**What happens:** `validate_weather_record` exists in `src/skywatch/data/validation.py:92-127`, but `normalize_weather_response` never calls it. Normalized weather rows use `measurement_time_utc`/`temperature_c`/`humidity_pct`, while the validator expects `observed_time_utc`/`temperature_2m`/`relative_humidity_2m`, so the two contracts do not match.
**Why it's wrong:** Weather rows bypass range, timestamp, humidity, and precipitation checks entirely; bad weather data flows straight into `data/normalized/weather/` and will reach the planned Spark correlation jobs.
**Do this instead:** Make normalization call a weather validator with the normalized field names, or change the validator to accept the canonical schema and return a `ValidationResult` that is stored on each row.

### Default paths assume the repo root

**What happens:** Every module constructs paths relative to the current working directory (`config/stations.json`, `data/raw`, `data/normalized`, `profiles/`, `tests/fixtures`). There is no project-root resolution.
**Why it's wrong:** Running the CLI from a cron directory, a notebook, or an installed wheel writes data into the wrong tree or fails to find fixtures/config. The GitHub workflow works only because it runs at repo root.
**Do this instead:** Resolve defaults from a single project-root helper (for example `Path(__file__).resolve().parents[2]`) and let environment variables override it.

### Raw manifest append is not atomic or locked

**What happens:** `save_raw_response` atomically writes the JSON payload, but appends to `data/raw/_manifest.csv` with a plain `open(..., "a")` (`src/skywatch/storage/raw_manifest.py:103-109`). There is no lock, temp-swap, or fsync for the manifest.
**Why it's wrong:** Concurrent collectors (cron plus GitHub Actions, or two manual runs) can interleave or truncate manifest rows, breaking the audit ledger that is supposed to prove raw-response preservation.
**Do this instead:** Serialize manifest updates through a lock file, or write one manifest fragment per run and compact them; at minimum flush/fsync and document the single-writer assumption.

### Backfill uses ingestion-date partitions for historical events

**What happens:** `run_chunked_backfill` writes every record in a chunk to `ingest_dt={chunk_end}` (`src/skywatch/collector/collector.py:279`, `:335-339`), even though the records' `measurement_time_utc` spans the whole chunk. It also does not update watermarks.
**Why it's wrong:** The partition key no longer represents when data arrived, and a multi-day chunk collapses into one partition. Planned `event_dt`-partitioned `clean/` and `analytics/` layers (`.planning/REQUIREMENTS.md:25`) will need to re-derive event dates, and backfill and incremental state can diverge.
**Do this instead:** Partition historical output by `event_dt` derived from `measurement_time_utc` (or explicitly document `ingest_dt` as arrival date), and decide whether backfill should advance watermarks or remain checkpoint-only.

### Global schema cache hides config edits

**What happens:** `load_schemas` stores the parsed YAML in module-global `_CACHED_SCHEMAS` (`src/skywatch/data/schemas.py:9-27`). Once loaded, later calls with `config_path=None` never re-read the file.
**Why it's wrong:** Long-running processes and tests that edit `config/schemas.yaml` see stale columns; there is no cache invalidation hook.
**Do this instead:** Cache by resolved path plus mtime, or expose an explicit `reload_schemas()` for callers that need fresh contracts.

## Error Handling

**Strategy:** Fail soft per station, fail hard per command, degrade storage to local fallback.

**Patterns:**
- **Per-station isolation:** Exceptions while polling a station are logged as warnings and the loop continues (`src/skywatch/collector/collector.py:179-180`). The summary does not include a failure count, so partial success looks like success.
- **Top-level CLI catch:** Any uncaught exception is logged with traceback and converted to exit code 1 (`src/skywatch/cli.py:90-93`).
- **HTTP retries:** OpenAQ retries 429/5xx/connection errors up to 5 times with jittered exponential backoff; 401/403 raises `AuthenticationError` immediately (`src/skywatch/collector/openaq_client.py:60-116`). Weather has the same retry shape without auth handling (`src/skywatch/collector/weather_client.py:52-85`).
- **Storage fallback:** HDFS unavailability or safemode copies the partition to `data/local_hdfs/` and returns success (`src/skywatch/storage/hdfs_storage.py:83-89`); upload exceptions also fall back and return `False` (`:113-117`).
- **Corrupt state recovery:** Missing or invalid watermark JSON resets to empty state with a warning (`src/skywatch/storage/watermark.py:28-40`); corrupt backfill checkpoint resets `completed_chunks` to `[]` (`src/skywatch/collector/collector.py:257-263`).
- **Reject codes instead of exceptions:** Invalid records remain in the normalized CSV with `is_valid=False` and a `reject_reason`; validation never raises (`src/skywatch/data/validation.py:19-163`).

## Cross-Cutting Concerns

**Logging:** JSON lines via `JsonPipelineFormatter`, including `timestamp_utc`, `run_id`, `level`, `component`, and sanitized `message` (`src/skywatch/pipeline/logging.py:79-116`). `run_context` scopes a `run_id` across an orchestrator run; `redact_sensitive` and `_redact_dict` scrub API keys/tokens/passwords.

**Validation:** Two related but separate mechanisms:
- Row hygiene in `src/skywatch/data/validation.py` (required fields, non-negative values, PM ceiling 1000 µg/m³, accepted units, future timestamps).
- Schema/type checks in `src/skywatch/data/schemas.py:45-92` (used by contract tests and potentially downstream consumers, not by the orchestrator write path).

**Authentication:** OpenAQ uses `X-API-Key` from `OPENAQ_API_KEY` (`src/skywatch/collector/openaq_client.py:36-51`). Open-Meteo needs no key. `__repr__` methods omit credentials. GitHub Actions supplies `OPENAQ_API_KEY` from repository secrets.

**Configuration:** `config/schemas.yaml` (data contract), `config/stations.json` (discovered station/sensor list), `profiles/modern.env` and `profiles/lab.env` (runtime paths and thresholds), plus environment variables (`OPENAQ_API_KEY`, `OPENAQ_OFFLINE`, `WEATHER_OFFLINE`, `SKYWATCH_OFFLINE`, `SKYWATCH_PROFILE`).

**Idempotency:** `record_key` dedupe, deterministic CSV column order, `hdfs dfs -rm -f` before rename, and atomic state replacement together make repeated runs safe at the file level. Incremental resume depends on watermarks; backfill resume depends on the checkpoint.

---

*Architecture analysis: 2026-11-22*
