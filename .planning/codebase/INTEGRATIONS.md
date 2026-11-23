---
last_mapped_commit: 831a73a6e8025d4970ff7b506be5de6d304b4c61
last_mapped_at: 2026-11-23
---
# External Integrations

**Analysis Date:** 2026-11-23

## APIs & External Services

**Air Quality Telemetry:**
- OpenAQ API v3 — Station discovery, real-time measurements, and historical sensor readings for Bengaluru.
  - Base URL: `https://api.openaq.org/v3` (`src/skywatch/collector/openaq_client.py:24`)
  - Client: Custom HTTP client wrapping `requests.Session` with retry loops and exponential backoff (`src/skywatch/collector/openaq_client.py:27`).
  - Authentication: Custom `X-API-Key` header populated from `OPENAQ_API_KEY` environment variable. HTTP 401/403 triggers non-retryable `AuthenticationError`.
  - Endpoints:
    - `GET /locations?bbox=77.45,12.83,77.75,13.12&limit=100&page=1` — Discover CAAQMS monitoring stations in the Bengaluru metropolitan bounding box (`src/skywatch/collector/discovery.py:17`).
    - `GET /locations/{id}/latest` — Fetch latest snapshot readings per location (`src/skywatch/collector/openaq_client.py:72`).
    - `GET /sensors/{id}/measurements?datetime_from=...&datetime_to=...&limit=100&page=1` — Incremental collection and chunked historical backfill per sensor (`src/skywatch/collector/openaq_client.py:86`).
  - Retry Strategy: Exponential backoff with random jitter up to `max_retries` (default 5). Honors HTTP 429 `Retry-After` headers; retries HTTP 5xx errors and network timeouts (`src/skywatch/collector/openaq_client.py:134`).
  - Offline Mode: Setting `offline=True` or `OPENAQ_OFFLINE=1` diverts requests to frozen test fixtures in `tests/fixtures/sample_openaq_*.json` (`src/skywatch/collector/openaq_client.py:38`).

**Meteorological Data:**
- Open-Meteo APIs — Free-tier meteorological forecasts and historical weather archives for Bengaluru.
  - Forecast Endpoint: `https://api.open-meteo.com/v1/forecast` (`src/skywatch/collector/weather_client.py:21`)
  - Archive Endpoint: `https://archive-api.open-meteo.com/v1/archive` (`src/skywatch/collector/weather_client.py:22`)
  - Authentication: None required.
  - Parameters: Centered at Bengaluru coordinates `(12.91, 77.61)` requesting `hourly=temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation` in `UTC` timezone (`src/skywatch/collector/weather_client.py:46`).
  - Offline Mode: Supported via `tests/fixtures/sample_weather_forecast.json` and `sample_weather_archive.json`.

## Data Storage

**Local Partitioned Filesystem:**
- Directory tree: `data/`
  - `data/raw/` — Verbatim JSON API responses accompanied by `_manifest.csv` audit trails (`src/skywatch/storage/raw_manifest.py`).
  - `data/normalized/` — Ingestion-date partitioned CSVs (`ingest_dt=YYYY-MM-DD`) validated against `config/schemas.yaml`.
  - `data/clean/` — Event-date partitioned headerless CSVs (`event_dt=YYYY-MM-DD`) produced by Pig ETL.
  - `data/analytics/` — Pig daily and hourly rollup datasets (`data/analytics/station_stats/`, `data/analytics/hourly_rollup/`).
  - `output/published/` — Dual Parquet and CSV analytical datasets produced by Spark jobs (`src/skywatch/spark/export.py`).
  - `data/synthetic/` — Deterministic synthetic benchmarking datasets for 1x, 10x, and 100x scale stress testing (`scripts/make_synthetic.py`).

**Hadoop Distributed File System (HDFS):**
- Module: `src/skywatch/storage/hdfs_storage.py` (`HDFSStorageAdapter`)
- Integration: Interacts with Apache Hadoop HDFS via `hdfs dfs` subprocess execution, with automatic environment detection.
- Fallback: When HDFS is unconfigured or unavailable, automatically routes files to a local simulation directory `data/local_hdfs/`.

**State & Watermark Tracking:**
- High-Water Marks: `data/.watermarks.json` managed by `WatermarkManager` (`src/skywatch/storage/watermark.py`). Tracks ISO-8601 UTC timestamps per sensor to ensure idempotent incremental ingestion.
- Backfill Checkpoints: `data/.backfill_checkpoint.json` records completed date chunks to support resumption after failure.

## Big Data Engines

**Apache Pig (ETL & Aggregation):**
- Module: `src/skywatch/pipeline/pig_runner.py` and `scripts/run_pig.sh`.
- Pig Scripts: `pig/clean_air_quality.pig`, `pig/station_stats.pig`, `pig/hourly_rollup.pig`.
- Contract: Outputs headerless CSV files and automatically generates sidecar `_schema.json` files to guarantee schema contracts for downstream Spark jobs.
- Execution Mode: `pig -x local` or MapReduce on YARN; automatically falls back to an in-memory pure-Python fallback when Pig/Hadoop is absent.

**Apache Spark / PySpark (Analytical Engine):**
- Modules: `src/skywatch/spark/` and `src/skywatch/pipeline/spark_runner.py`.
- Execution: Managed via `SparkSession` builder (`src/skywatch/spark/common.py`), submitting analytics jobs or using local PySpark.
- Published Datasets: 7 distinct analytical products written to `output/published/` via atomic staging swaps (`src/skywatch/spark/export.py`).
- Fallback: Pure-Python standalone fallback (`_run_standalone_spark_fallback` in `spark_runner.py`) enables full testing in zero-JVM environments.

## Authentication & Identity

- `OPENAQ_API_KEY`: Injected into the HTTP header `X-API-Key`.
- Profile Isolation: `profiles/modern.env` and `profiles/lab.env` manage environment variables (`JAVA_HOME`, `HADOOP_HOME`, `SPARK_HOME`, `PIG_HOME`).

## Monitoring & Observability

- Structured Logging: `src/skywatch/pipeline/logging.py` provides JSON-formatted output with run identifiers (`run_id`), timestamps, and component tags.
- Quality Reporting: `src/skywatch/spark/quality_report.py` generates diagnostic counts of accepted, rejected, and duplicate records across pipeline stages.
- Alerting Engine: `src/skywatch/spark/alert_engine.py` evaluates health thresholds (CPCB AQI > 200) and telemetry anomalies (stale or offline stations, rolling median spikes).

## CI/CD & Deployment

- GitHub Actions Workflows:
  - `.github/workflows/ci.yml` — Runs Ruff linting and pytest test suites on Python 3.11 for all pushes and PRs.
  - `.github/workflows/scheduled-collect.yml` — Runs hourly automated ingestion with artifact uploads.
- Cron Script: `scripts/cron_collect.sh` enables server-side scheduling with file locks (`flock`) and rotating log output.
