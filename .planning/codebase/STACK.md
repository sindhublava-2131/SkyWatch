---
last_mapped_commit: 831a73a6e8025d4970ff7b506be5de6d304b4c61
last_mapped_at: 2026-11-23
---
# Technology Stack

**Analysis Date:** 2026-11-23

## Languages

**Primary:**
- Python 3.8+ — declared syntax floor in `pyproject.toml` (`requires-python = ">=3.8"`); every Python module across `src/skywatch/` and `tests/` uses `from __future__ import annotations` and standard `typing` annotations (`Optional`, `Dict`, `List`, `Tuple`, `Any`). Ruff enforces `target-version = "py38"`.
- Observed runtime environments: Python 3.11 pinned in GitHub Actions CI (`.github/workflows/ci.yml`), Python 3.12 on local development machines.

**Data & Query Languages:**
- Apache Pig Latin (`.pig`) — Phase 2 ETL pipeline located in `pig/`:
  - `pig/clean_air_quality.pig` — record validation, deduplication, and cleaning on HDFS.
  - `pig/station_stats.pig` — daily aggregations per station and pollutant.
  - `pig/hourly_rollup.pig` — hourly bucketed rollups for downstream temporal analytics.
- Structured Query / PySpark DataFrame API — Phase 3 analytics engine in `src/skywatch/spark/` using PySpark SQL functions, windowing, and DataFrame transformations.

**Shell & Orchestration Scripting:**
- Bash (`.sh`) — profile switching, execution wrappers, and scheduled collection:
  - `scripts/use_profile.sh` — dynamic profile switching (`modern` vs `lab`) and environment validation.
  - `scripts/run_pig.sh` — profile-aware launcher for Pig ETL jobs with logging and error traps.
  - `scripts/run_spark.sh` — profile-aware launcher for PySpark analytics jobs.
  - `scripts/cron_collect.sh` — scheduled collection wrapper using `flock` file locking.

**Data & Configuration Serialization:**
- YAML (`.yaml`) — canonical contracts and domain settings:
  - `config/schemas.yaml` — strict column, type, nullability, and header/delimiter data contract for all 10 datasets.
  - `config/settings.yaml` — operational thresholds (freshness windows, alert categories, spike detection, benchmarks).
  - `config/breakpoints.yaml` — official CPCB PM2.5 and PM10 sub-index breakpoints and calculation rules.
- JSON (`.json`) — metadata and state persistence:
  - `config/stations.json` — metadata registry for Bengaluru air quality monitoring stations.
  - `data/.watermarks.json` — high-water mark timestamps per station/sensor for incremental ingestion.
  - `data/.backfill_checkpoint.json` — stateful tracking for historical backfill resumption.
  - Sidecar `_schema.json` files — self-describing metadata accompanying headerless CSV and Parquet outputs.
- CSV / Parquet — storage formats:
  - Partitioned CSV (headered for raw/normalized, headerless for Pig outputs).
  - Parquet (columnar storage for Spark published analytical datasets).

## Runtime

**Environment:**
- CPython 3.8+ (tested against 3.11 in CI and 3.12 local).
- Big Data Execution Runtimes:
  - Apache Hadoop HDFS (`HDFSStorageAdapter` via CLI or local directory mirroring fallback in `data/local_hdfs/`).
  - Apache Pig (executed in local mode `pig -x local` or MapReduce on YARN; pure-Python standalone fallback in `src/skywatch/pipeline/pig_runner.py`).
  - Apache Spark 3.5.0 / 3.2.2 (executed via `spark-submit` or PySpark local runner; pure-Python standalone fallback in `src/skywatch/pipeline/spark_runner.py`).
- JVM / Java Requirements:
  - Modern profile: OpenJDK 17 for Spark 3.5.0, OpenJDK 8 for Pig 0.18.0.
  - Lab profile: OpenJDK 8 for Spark 3.2.2 and Pig 0.18.0.
- Operating System:
  - Linux (Ubuntu/Debian) target for production Hadoop/Spark clusters and CI.
  - Cross-platform POSIX and Windows compatibility with atomic filesystem fallbacks.

**Package Manager:**
- `pip` with `pyproject.toml` (PEP 517/518 build via `setuptools>=61.0`).
- No virtualenv committed to git (`.venv`, `env/` ignored).

## Frameworks

**Data Processing & Big Data:**
- Apache Pig Latin (MapReduce batch ETL)
- Apache Spark / PySpark (Distributed in-memory analytical processing)
- PyArrow (`pyarrow>=10.0.0`) — Parquet reading/writing and zero-copy tabular serialization.

**HTTP & Telemetry Collection:**
- `requests>=2.28.0` — synchronous HTTP transport with custom session adapters, retry loops, and exponential backoff.

**Environment & System:**
- `python-dotenv>=1.0.0` — deterministic environment loading for `profiles/*.env`.
- `psutil>=5.9.0` — system telemetry, memory usage tracking, and process inspection.
- `pyyaml>=6.0` — YAML configuration and schema parsing.

**Testing & Quality Assurance:**
- `pytest>=7.0.0` — test runner supporting unit, contract, and integration suites.
- `responses>=0.23.0` — HTTP request mocking for external API integrations.
- `ruff>=0.1.0` — linting, import sorting, and code formatting.

## Key Dependencies

**Production Dependencies (`pyproject.toml`):**
- `pyyaml>=6.0` — parsing `config/schemas.yaml`, `config/settings.yaml`, and `config/breakpoints.yaml`.
- `requests>=2.28.0` — OpenAQ v3 and Open-Meteo REST API interactions.
- `python-dotenv>=1.0.0` — profile environment loading.
- `psutil>=5.9.0` — hardware benchmarking and process metrics.
- `pyarrow>=10.0.0` — dual Parquet export in Spark publishing jobs.

**Development Dependencies (`pyproject.toml [project.optional-dependencies] dev`):**
- `pytest>=7.0.0` — test execution framework.
- `pytest-cov>=4.0.0` — coverage reporting.
- `responses>=0.23.0` — API mocking for OpenAQ and weather clients.
- `ruff>=0.1.0` — static analysis and linting.
- `pyspark` (installed in CI and production environments for PySpark execution).

## Configuration

**Profile Definitions:**
- `profiles/modern.env` — modern execution stack:
  - `HADOOP_VERSION="3.3.6"`, `PIG_VERSION="0.18.0"`, `SPARK_VERSION="3.5.0"`
  - `JAVA_HOME_SPARK="/usr/lib/jvm/java-17-openjdk-amd64"`
  - `JAVA_HOME_PIG="/usr/lib/jvm/java-8-openjdk-amd64"`
- `profiles/lab.env` — agreed lab rollback stack:
  - `HADOOP_VERSION="3.2.2"`, `PIG_VERSION="0.18.0"`, `SPARK_VERSION="3.2.2"`
  - `JAVA_HOME="/usr/lib/jvm/java-8-openjdk-amd64"` (OpenJDK 8 unified)

### Architectural Note: Spark 2.4.8 Incompatibility with Python 3.8+ Floor
PySpark 2.4.8 was released for Python 2.7 and 3.4–3.7 and fails on Python 3.8+ due to syntax and Py4J bytecode incompatibilities. Because SkyWatch enforces a Python 3.8 syntax floor (`requires-python = ">=3.8"`), Spark 2.4.8 cannot run in this codebase. The agreed rollback stack in Project Spec v2.0 (§2, §18) is Hadoop 3.2.2, Pig 0.18.0, Spark 3.2.2, and OpenJDK 8, which provides native Python 3.8+ compatibility and executes cleanly on OpenJDK 8.

**Domain & Schema Configuration:**
- `config/schemas.yaml` — schema registry for 10 datasets across raw, clean, and published tiers.
- `config/settings.yaml` — freshness limits (fresh < 3h, stale < 24h), alert rules (poor 201-300, very poor 301-400, severe 401-500), spike multipliers (3.0x), and benchmark sizes (1x: 50k, 10x: 500k, 100x: 5M).
- `config/breakpoints.yaml` — CPCB official sub-index mathematical boundaries.
- `config/stations.json` — metadata for active CAAQMS stations in Bengaluru.

## Platform Requirements

**Minimum Hardware:**
- 4 CPU cores, 8 GB RAM for local Spark/Pig execution.
- Disk: 10 GB free space for local HDFS simulation, temporary staging, and scale benchmark datasets.

**External Network:**
- HTTPS egress to `api.openaq.org` (port 443) and `api.open-meteo.com` / `archive-api.open-meteo.com` (port 443).
- Optional `OPENAQ_API_KEY` for increased OpenAQ rate limits; graceful offline fallback supported via fixture datasets.
