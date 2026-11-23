---
last_mapped_commit: 831a73a6e8025d4970ff7b506be5de6d304b4c61
last_mapped_at: 2026-11-23
---
<!-- refreshed: 2026-11-23 -->

# Architecture

**Analysis Date:** 2026-11-23

## System Overview

SkyWatch is an end-to-end Big Data analytics and environmental intelligence platform for Bengaluru's air quality and meteorology. The system spans ingestion, distributed batch ETL, analytics, and publication tiers with dual-profile execution (`modern` and `lab` rollback) and a concrete scale benchmark.

```text
┌────────────────────────────────────────────────────────────────────────────────────────┐
│ Ingestion Tier (Phase 1)                                                                │
│ OpenAQ v3 API + Open-Meteo API ──► IngestionOrchestrator ──► Normalization & Validation│
│                                                                │                       │
│                                                      Raw Audit Manifest &              │
│                                                      Partitioned CSV (ingest_dt)       │
└────────────────────────────────────────┬───────────────────────────────────────────────┘
                                         ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│ Big Data ETL Tier - Apache Pig & HDFS (Phase 2)                                         │
│ pig/clean_air_quality.pig ──► Headerless clean CSV (event_dt) + _schema.json           │
│ pig/station_stats.pig     ──► Daily Station Aggregations (event_dt)                    │
│ pig/hourly_rollup.pig     ──► Hourly Bucketed Rollups (event_dt)                       │
└────────────────────────────────────────┬───────────────────────────────────────────────┘
                                         ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│ Analytics & Publishing Tier - Apache Spark (Phase 3)                                    │
│ CPCB 24-hr Rolling Index  ──► cpcb_index (AQI sub-index & suppression reason codes)    │
│ Weather Correlation Engine ──► weather_correlation (Hourly joins, Pearson r)            │
│ Station & Trend Analytics ──► station_statistics, trends_hourly, trends_daily          │
│ Health & Telemetry Alerts ──► alerts (CPCB threshold breaches & rolling median spikes) │
│ Quality Diagnostics       ──► quality_report (Stage-by-stage auditing)                 │
│                                        │                                               │
│                                        ▼                                               │
│                       output/published/ (Atomic Staging Swap: Parquet + CSV + Schema)   │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

## Component Responsibilities

1. **Collector & Discovery (`src/skywatch/collector/`):**
   - `openaq_client.py`: Synchronous HTTP client for OpenAQ API v3 with backoff, jitter, and offline test mocks.
   - `weather_client.py`: Fetches hourly meteorological data (temperature, humidity, wind, precipitation) from Open-Meteo.
   - `discovery.py`: Discovers active CAAQMS monitoring stations within Bengaluru's bounding box `[77.45, 12.83, 77.75, 13.12]`.
   - `collector.py`: `IngestionOrchestrator` coordinates incremental collection, latest snapshots, and chunked historical backfills.

2. **Data Hygiene & Contracts (`src/skywatch/data/`):**
   - `schemas.py`: Loads and verifies datasets against `config/schemas.yaml`.
   - `normalization.py`: Maps external JSON structures into canonical flat dictionaries with strict types.
   - `validation.py`: Enforces numeric physical ranges (e.g. PM2.5 between 0 and 1000 µg/m³) and marks invalid records.
   - `deduplication.py`: Removes duplicate records within a batch based on composite primary keys (`record_key`).

3. **Storage Tier (`src/skywatch/storage/`):**
   - `local_storage.py`: Writes atomic CSV partitions with header management and temporary file swapping.
   - `raw_manifest.py`: Appends raw payload audit metadata (SHA-256, HTTP status, record count) to `data/raw/_manifest.csv`.
   - `watermark.py`: `WatermarkManager` stores high-water marks in `data/.watermarks.json` to prevent duplicate ingestion.
   - `hdfs_storage.py`: `HDFSStorageAdapter` mirrors partitions to HDFS or local fallback `data/local_hdfs/`.

4. **Batch ETL with Apache Pig (`pig/` and `src/skywatch/pipeline/pig_runner.py`):**
   - `pig/clean_air_quality.pig`: Filters `is_valid == true`, deduplicates, and splits normalized data into event-date partitions (`event_dt=YYYY-MM-DD`).
   - `pig/station_stats.pig`: Groups clean readings by `(station_id, parameter, date)` to compute count, min, max, and avg.
   - `pig/hourly_rollup.pig`: Groups clean readings by `(station_id, parameter, bucket_start_utc)`.
   - `pig_runner.py`: Profile-aware orchestrator that verifies Pig/Hadoop availability, pre-clears target directories, executes Pig Latin, attaches sidecar `_schema.json` files, and provides a pure-Python fallback.

5. **Analytics Core (`src/skywatch/analytics_core/`):**
   - `cpcb.py`: Implements the official Central Pollution Control Board (CPCB) 24-hour linear interpolation algorithm for PM2.5 and PM10 sub-indices, 16-hour minimum valid data rule, health classification (Good, Satisfactory, Moderate, Poor, Very Poor, Severe), and suppression reason codes (`INSUFFICIENT_HOURLY_DATA`, `MISSING_POLLUTANT`).
   - `freshness.py`: Classifies station reporting telemetry as `FRESH` (< 3h), `STALE` (3-24h), or `OFFLINE` (> 24h).

6. **Analytics Engine with Apache Spark (`src/skywatch/spark/` and `src/skywatch/pipeline/spark_runner.py`):**
   - `common.py`: Configures `SparkSession` with modern or lab tuning properties and loads headerless datasets via sidecar schemas.
   - `index_job.py`: Computes 24-hour rolling CPCB AQI sub-indices across sliding windows.
   - `weather_analysis.py`: Joins air quality rollups with weather metrics and computes the Pearson correlation matrix ($r$).
   - `station_statistics.py`: Computes aggregate station statistics and rankings across Bengaluru.
   - `trend_analysis.py`: Computes diurnal (hourly) and day-of-week trends.
   - `alert_engine.py`: Generates emergency/warning health alerts (AQI > 200) and telemetry alerts (stale/offline stations, 3.0x rolling median spikes).
   - `quality_report.py`: Aggregates audit metrics (accepted, rejected, duplicates, reason breakdown) across pipeline runs.
   - `export.py`: Publishes datasets via atomic directory staging swap protocol, writing dual Parquet, CSV, and `_schema.json`.
   - `spark_runner.py`: End-to-end analytics orchestrator with standalone Python fallback for zero-JVM testing.

7. **Pipeline Orchestration & Profiles (`src/skywatch/pipeline/`):**
   - `profile.py`: Loads environment configurations (`modern.env` vs `lab.env`), validates Java/Hadoop/Spark runtimes, and returns structured configuration.
   - `logging.py`: Structured JSON logging with unique `run_id` tracking.

## Pattern Overview

- **Dual-Profile Runtime:** Decouples pipeline logic from environment specifics; switchable via `scripts/use_profile.sh modern` or `lab` without code changes.
- **Sidecar Schema Contracts:** Solves Pig's headerless CSV output limitation by generating a sidecar `_schema.json` alongside data files, allowing downstream PySpark/Arrow jobs to ingest clean data with explicit column typing.
- **Atomic Directory Staging Swap:** Prevents consumers from reading partial or in-progress batch writes. Jobs write to a temporary staging folder (`<output>.__staging_<uuid>`) and execute an atomic directory rename into place.
- **Idempotent Ingestion & Resumption:** High-water mark tracking (`data/.watermarks.json`) ensures re-running collection does not duplicate records; chunked backfill tracks progress via `.backfill_checkpoint.json`.
- **Pure-Python Standalone Fallbacks:** Enables complete unit and integration test verification on developer machines without requiring active Hadoop or Spark clusters.

## Layers

```text
┌────────────────────────────────────────────────────────┐
│ Presentation & CLI Layer                               │
│ - skywatch cli (discover, collect, backfill)           │
│ - Planned: Phase 4 Streamlit Dashboard & unified CLI   │
├────────────────────────────────────────────────────────┤
│ Orchestration & Pipeline Runners                       │
│ - pig_runner.py, spark_runner.py, scripts/*.sh         │
│ - logging.py (JSONFormatter, run_id context)           │
├────────────────────────────────────────────────────────┤
│ Processing & Analytics Engine                          │
│ - Apache Pig scripts (ETL, deduplication, rollups)     │
│ - Apache Spark jobs (CPCB AQI, Pearson r, alerts)      │
│ - analytics_core (pure mathematical logic)             │
├────────────────────────────────────────────────────────┤
│ Storage & Normalization Tier                           │
│ - local_storage.py, hdfs_storage.py, raw_manifest.py   │
│ - schemas.py, validation.py, normalization.py          │
├────────────────────────────────────────────────────────┤
│ Ingestion & Transport Tier                             │
│ - openaq_client.py, weather_client.py, discovery.py    │
└────────────────────────────────────────────────────────┘
```

## Data Flow

### 1. Ingestion Pipeline

1. `skywatch collect --mode incremental` queries active stations from `config/stations.json`.
2. For each station and sensor, `OpenAQClient` queries measurements since the stored watermark in `data/.watermarks.json`.
3. Verbatim JSON responses are archived under `data/raw/` and logged to `data/raw/_manifest.csv`.
4. Raw payloads are normalized into canonical flat records, validated against numeric bounds, deduplicated, and appended to `data/normalized/air_quality/ingest_dt=YYYY-MM-DD/`.
5. Simultaneously, `WeatherClient` collects Bengaluru weather, normalizes it, and saves it to `data/normalized/weather/ingest_dt=YYYY-MM-DD/`.
6. High-water marks in `data/.watermarks.json` are updated atomically.

### 2. Pig ETL Pipeline

1. `pig_runner.py` triggers `pig/clean_air_quality.pig`.
2. Reads normalized CSVs, discards invalid records (`is_valid == false`), performs distinct deduplication, and partitions into `data/clean/air_quality/event_dt=YYYY-MM-DD/` as headerless CSV.
3. Attaches `_schema.json` to the output partition.
4. Downstream Pig jobs (`station_stats.pig`, `hourly_rollup.pig`) compute initial aggregations.

### 3. Spark Analytics & Export Pipeline

1. `spark_runner.py` reads `data/clean/air_quality` using schema extracted from `_schema.json` and `data/normalized/weather`.
2. Executes 6 analytical transformations:
   - Rolling 24-hour CPCB AQI sub-index calculation with suppression reasoning.
   - Hourly weather join and Pearson correlation matrix ($r$).
   - Diurnal and daily temporal trend analysis.
   - Station statistics and pollution rankings.
   - Multi-tier alert generation (health thresholds + 3.0x rolling median spikes).
   - Data quality stage-by-stage auditing report.
3. Invokes `export_published_dataset()` to stage and atomically swap outputs into `output/published/` in dual Parquet and CSV formats with accompanying `_schema.json`.

## Key Abstractions

- `IngestionOrchestrator`: Orchestrates polling, backfilling, and raw/normalized persistence.
- `OpenAQClient` / `WeatherClient`: Encapsulate HTTP transport, rate limiting, and offline mocking.
- `WatermarkManager`: Thread-safe persistence of per-sensor high-water mark timestamps.
- `HDFSStorageAdapter`: Unified storage interface switching between HDFS CLI and local filesystem.
- `PigRunner` (`run_pig_script`): Subprocess execution of Pig Latin with directory pre-clearing and sidecar schema injection.
- `SparkRunner` (`run_spark_analytics`): PySpark session management, analytical job coordination, and dataset export.
- `export_published_dataset`: Atomic directory staging swap mechanism ensuring transactional output publishing.

## Entry Points

- `skywatch` CLI (`src/skywatch/cli.py`): Subcommands `discover`, `collect`, `backfill`.
- Pig Orchestration Runner (`src/skywatch/pipeline/pig_runner.py`): Entry point for Pig Latin execution.
- Spark Analytics Runner (`src/skywatch/pipeline/spark_runner.py`): Entry point for PySpark analytical processing.
- Shell Runners: `scripts/run_pig.sh`, `scripts/run_spark.sh`, `scripts/cron_collect.sh`, `scripts/use_profile.sh`.

## Architectural Constraints

- **Python 3.8 Syntax Floor:** Must maintain Python 3.8 compatibility across all modules (`from __future__ import annotations`, no `match/case`, no `X | Y` union syntax).
- **Dual-Profile Runtime:** Zero hardcoded paths or versions; must rely on `profiles/*.env` and `profile.py`.
- **Contract-First Design:** `config/schemas.yaml` is the single source of truth for all schemas.
- **Idempotency & Replayability:** Any pipeline step must be safely re-runnable without duplicating data or corrupting state.
- **Atomic File/Directory Operations:** All dataset writes must use temporary files/directories and atomic renames.

## Anti-Patterns Avoided

- In-place directory overwrites without staging (prevents partial reads by concurrent consumers).
- Header dependency on Pig outputs (solved by generating sidecar `_schema.json`).
- Committing secrets or credentials (API keys strictly managed via environment variables).
- Hardcoding cluster hostnames or JVM paths in application code.

## Error Handling & Cross-Cutting Concerns

- Network errors use exponential backoff with jitter.
- Authentication failures (401/403) raise explicit fatal exceptions without pointless retries.
- Pipeline logging uses structured JSON with `run_id` injection across all modules.
- Scale benchmarks isolate synthetic data to `data/synthetic/` with `source="synthetic"` to prevent polluting production data.
