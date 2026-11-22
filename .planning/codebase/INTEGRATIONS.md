# External Integrations

**Analysis Date:** 2026-11-22

## APIs & External Services

**Air Quality:**
- OpenAQ API v3 — station discovery, latest readings, and historical sensor measurements.
  - Base URL: `https://api.openaq.org/v3` (`src/skywatch/collector/openaq_client.py`)
  - SDK/Client: custom synchronous client on `requests.Session`; no official OpenAQ SDK is used.
  - Auth: `X-API-Key` header populated from `OPENAQ_API_KEY` (`src/skywatch/collector/openaq_client.py`). HTTP 401/403 raises `AuthenticationError` and is not retried.
  - Endpoints called:
    - `GET /locations?bbox=77.45,12.83,77.75,13.12&limit=100&page=1` — `get_locations()` used by `discover_stations()` in `src/skywatch/collector/discovery.py`.
    - `GET /locations/{id}/latest` — `get_latest()` used for `--mode latest` in `src/skywatch/collector/collector.py`.
    - `GET /sensors/{id}/measurements?datetime_from=...&datetime_to=...&limit=100&page=1` — `get_measurements()` used for incremental collection and backfill.
    - `GET /sensors/{id}/hours` — `get_hours()` is defined but not called anywhere in `src/` or `tests/`.
  - Retry behavior: exponential backoff with jitter up to `max_retries` (default 5); HTTP 429 honors `Retry-After` or waits `min(60, 1.5 * 2^attempt)`; HTTP 5xx waits `min(30, 2^attempt)`; connection/timeout and invalid-JSON responses are retried.
  - Timeout: 20 seconds by default (`OpenAQClient(timeout=20)`).
  - Offline mode: `offline=True` or a non-empty `OPENAQ_OFFLINE` environment variable reads `tests/fixtures/sample_openaq_locations.json`, `sample_openaq_latest.json`, and `sample_openaq_measurements.json`.
  - Attribution: OpenAQ aggregates provider data; the spec requires OpenAQ and originating-provider attribution.

**Weather:**
- Open-Meteo Forecast API — current/recent hourly weather for Bengaluru.
  - URL: `https://api.open-meteo.com/v1/forecast` (`src/skywatch/collector/weather_client.py`)
  - Auth: none (free non-commercial tier; fair-use limits).
  - Parameters: `latitude`, `longitude`, `hourly=temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation`, `timezone=UTC`.
  - Called once per collection cycle for the fixed Bengaluru central point `(12.91, 77.61)` in `src/skywatch/collector/collector.py`; not per station.
- Open-Meteo Archive API — historical hourly weather for chunked backfill.
  - URL: `https://archive-api.open-meteo.com/v1/archive` (`src/skywatch/collector/weather_client.py`)
  - Parameters: `latitude`, `longitude`, `start_date`, `end_date`, `hourly`, `timezone=UTC`.
  - Called per backfill chunk in `src/skywatch/collector/collector.py`.
  - Retry behavior: 429 and 5xx retries with exponential backoff and jitter; connection/timeout and invalid-JSON retries; no `Retry-After` handling.
  - Offline mode: `offline=True`, `WEATHER_OFFLINE`, `SKYWATCH_OFFLINE`, or `OPENAQ_OFFLINE` reads `tests/fixtures/sample_weather_forecast.json` / `sample_weather_archive.json`.
  - Attribution: "Weather data by Open-Meteo.com" (CC BY 4.0) required by the spec; data is gridded model output, not a station measurement.

**Planned but not implemented:**
- No CPCB API integration; CPCB breakpoints and PM-based AQI are computed locally in planned Phase 3.
- No Streamlit Cloud or other dashboard service integration exists; planned in Phase 4.
- No mapping tile provider or geocoding service exists; a station map is planned in Phase 4.

## Data Storage

**Databases:**
- None. No relational database, NoSQL store, ORM, or SQL driver exists in `pyproject.toml` or `src/`.

**File Storage:**
- Local filesystem — raw responses and normalized partitions.
  - Raw: `data/raw/{source}/ingest_dt=YYYY-MM-DD/{source}_{run_id}_{sha256[:8]}.json` written atomically by `src/skywatch/storage/raw_manifest.py`.
  - Audit ledger: `data/raw/_manifest.csv` with `run_id`, `file_path`, `source`, `endpoint_url`, `http_status`, `sha256`, `record_count`, `collected_at_utc` (`config/schemas.yaml` `manifest`).
  - Normalized: `data/normalized/{air_quality|weather}/ingest_dt=YYYY-MM-DD/{stream}_{run_id}.csv` written atomically (temp file + `fsync` + `os.replace`) by `src/skywatch/storage/local_storage.py`.
- HDFS — optional partitioned storage through the `hdfs` CLI (`src/skywatch/storage/hdfs_storage.py`).
  - Checks `hdfs version` availability and `hdfs dfsadmin -safemode get`; writes to `/skywatch/_staging/{run_id}_{filename}` then `hdfs dfs -mv` to `/skywatch/normalized/{stream}/ingest_dt=YYYY-MM-DD/`.
  - Falls back to `data/local_hdfs/normalized/{stream}/ingest_dt=.../` when the binary is missing, the NameNode is unreachable, safemode is ON, or an upload fails.
  - No WebHDFS, Kerberos, or HDFS Python library is configured.
- State files (JSON, atomic replace):
  - `data/.watermarks.json` — per-sensor `last_measurement_utc`, `station_id`, `parameter`, `last_run_id`, `records_ingested` (`src/skywatch/storage/watermark.py`).
  - `data/.backfill_checkpoint.json` — completed chunk keys and `last_updated` (`src/skywatch/collector/collector.py`).
- Runtime data is git-ignored except `data/raw/_manifest.csv`; `data/local_hdfs/` currently contains cached partitions from offline runs.

**Caching:**
- No cache service. `tests/fixtures/` act as offline fixtures, not a runtime cache. There is no Redis, Memcached, or HTTP cache layer.

## Authentication & Identity

**Auth Provider:**
- None for the application itself; it is a single-tenant, local/CLI pipeline with no user accounts, sessions, or OAuth.
- The only credential is the OpenAQ API key:
  - Local: `OPENAQ_API_KEY` environment variable (`.env` is git-ignored; no `.env` or `.env.example` is present in the repository).
  - CI: repository secret `OPENAQ_API_KEY` injected in `.github/workflows/scheduled-collect.yml`.
  - Logging: `src/skywatch/pipeline/logging.py` redacts `api_key`, `token`, `secret`, `password`, and `auth` patterns, and `OpenAQClient.__repr__` omits the key.
- HDFS/YARN security: the spec requires binding daemons to localhost; no Kerberos or TLS configuration exists.

## Monitoring & Observability

**Error Tracking:**
- None. No Sentry, OpenTelemetry, Datadog, or equivalent integration.

**Logs:**
- Structured JSON logs to standard error via `logging.StreamHandler` in `src/skywatch/pipeline/logging.py`.
- Fields: `timestamp_utc`, `run_id`, `level`, `component`, `message`, optional sanitized `extra`.
- `run_id` is a UUID4 hex string propagated through `contextvars` by `run_context()`.
- Local cron writes stdout/stderr to `data/logs/cron_collect.log` (`scripts/cron_collect.sh`); GitHub Actions logs are the CI observability surface.
- No log aggregation, metrics endpoint, or dashboard exists.

## CI/CD & Deployment

**Hosting:**
- No application hosting or deployment configuration exists in the repository.
- Target per `docs/SkyWatch - Project Specification v2.0.md` §17: Hadoop/Pig/Spark on a local Linux machine, collector on GitHub Actions plus local cron, and dashboard on Streamlit Community Cloud (planned Phase 4).

**CI Pipeline:**
- `.github/workflows/ci.yml` — on push to `main`, `feature/**`, `chore/**` and pull requests to `main`; `ubuntu-latest`, Python 3.11, `pip install ruff pytest responses`, `pip install -e .`, then `ruff check .` and `pytest`.
- `.github/workflows/scheduled-collect.yml` — cron `*/30 * * * *` plus `workflow_dispatch`; Python 3.11, `pip install .`, runs `python -m skywatch.cli collect --mode latest` with `OPENAQ_API_KEY` from secrets, then commits `data/raw/_manifest.csv` and `config/stations.json` with `github-actions[bot]` and pushes to `main`. Declares `permissions: contents: write`.
- No `publish-data.yml`, `live-contract.yml`, or `release.yml` exists, although the spec plans them.
- No Docker image build, no CD deployment step, and no artifact publishing.

## Environment Configuration

**Required env vars:**
- `OPENAQ_API_KEY` — required for live OpenAQ calls; optional when using offline fixtures.
- `SKYWATCH_PROFILE` — `modern` or `lab`; defaults to `modern`.
- `HADOOP_HOME`, `PIG_HOME`, `SPARK_HOME`, `JAVA_HOME_PIG`, `JAVA_HOME_SPARK`, `HDFS_ROOT` — planned stack paths, defined in `profiles/*.env`.
- `SKYWATCH_HTTP_TIMEOUT`, `SKYWATCH_MAX_RETRIES`, `SKYWATCH_FRESH_HOURS`, `SKYWATCH_STALE_HOURS` — profile tuning values.
- `OPENAQ_OFFLINE`, `WEATHER_OFFLINE`, `SKYWATCH_OFFLINE` — optional offline switches.

**Secrets location:**
- Local: process environment / `.env` (git-ignored; file absent).
- CI: GitHub Actions repository secret `OPENAQ_API_KEY`.
- No secrets are committed; `.gitignore` excludes `.env` and runtime data.

## Webhooks & Callbacks

**Incoming:**
- None. There is no HTTP server, webhook endpoint, or callback route.
- GitHub Actions `schedule` and `workflow_dispatch` trigger collection; these are scheduler events, not webhooks.

**Outgoing:**
- None. The pipeline makes outbound HTTPS requests only to OpenAQ and Open-Meteo; it does not post to Slack, email, or other notification services.
- Planned alerts are written to local/exported files in Phase 3, not delivered through webhooks.

## Planned Integrations (Phases 2–4)

- Apache Pig on HDFS — reads normalized CSV partitions and writes clean/aggregate outputs with sidecar `_schema.json` (`.planning/ROADMAP.md` Phase 2). No Pig scripts or runner exist.
- Apache Spark — reads Pig/HDFS outputs, joins weather and pollution by UTC hour, computes CPCB index and Pearson correlations, and exports Parquet/CSV to `output/published/` (`.planning/ROADMAP.md` Phase 3). No Spark code exists.
- Streamlit dashboard — reads published CSV/Parquet files; planned for Streamlit Community Cloud using a `published-data` branch (`.planning/ROADMAP.md` Phase 4; spec §17). No dashboard code or deployment exists.
- Additional GitHub Actions workflows (`publish-data.yml`, `live-contract.yml`, `release.yml`) and Dependabot are specified in `docs/SkyWatch - Project Specification v2.0.md` §16 but are not present.

---

*Integration audit: 2026-11-22*
