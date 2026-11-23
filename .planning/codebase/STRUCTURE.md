---
last_mapped_commit: 831a73a6e8025d4970ff7b506be5de6d304b4c61
last_mapped_at: 2026-11-23
---
# Codebase Structure

**Analysis Date:** 2026-11-23

## Directory Layout

```text
SkyWatch/
├── config/                          # Configuration files and schemas
│   ├── breakpoints.yaml             # CPCB AQI breakpoints and mathematical boundaries
│   ├── schemas.yaml                 # Canonical data contracts (10 datasets)
│   ├── settings.yaml                # Operational thresholds, alerts, and benchmark configs
│   └── stations.json                # Metadata for Bengaluru CAAQMS monitoring stations
├── data/                            # Local data lake partitions and persistence
│   ├── analytics/                   # Intermediate Pig analytics (station_stats, hourly_rollup)
│   ├── clean/                       # Clean event-date partitioned CSVs from Pig ETL
│   ├── local_hdfs/                  # Local filesystem HDFS simulation directory
│   ├── logs/                        # Pipeline execution log outputs
│   ├── normalized/                  # Ingestion-date partitioned CSVs (air_quality, weather)
│   ├── raw/                         # Raw verbatim API responses and _manifest.csv
│   └── synthetic/                   # Benchmark synthetic datasets (1x, 10x, 100x)
├── output/                          # Analytics publishing tier
│   └── published/                   # Staged dual Parquet, CSV, and _schema.json datasets
│       ├── alerts/
│       ├── cpcb_index/
│       ├── quality_report/
│       ├── station_statistics/
│       ├── trends_daily/
│       ├── trends_hourly/
│       └── weather_correlation/
├── pig/                             # Apache Pig Latin ETL scripts
│   ├── clean_air_quality.pig        # Ingestion cleaning, validation filtering, and deduplication
│   ├── hourly_rollup.pig            # Hourly bucketed aggregation
│   └── station_stats.pig            # Daily station summary statistics
├── profiles/                        # Environment runtime profiles
│   ├── lab.env                      # Legacy lab profile (Hadoop 2.7, Pig 0.16, Spark 2.4, Java 8)
│   └── modern.env                   # Modern profile (Hadoop 3.3, Pig 0.18, Spark 3.5, Java 17/8)
├── scripts/                         # Shell wrappers and automation scripts
│   ├── cron_collect.sh              # Scheduled cron ingestion runner with flock
│   ├── generate_sample_fixtures.py  # Generator for test fixtures
│   ├── make_sample_weather.py       # Helper script for weather samples
│   ├── make_synthetic.py            # Deterministic synthetic scale dataset generator
│   ├── run_pig.sh                   # Profile-aware Pig execution wrapper
│   ├── run_spark.sh                 # Profile-aware Spark execution wrapper
│   └── use_profile.sh               # Active profile switcher and environment validator
├── src/                             # Application source code (Python 3.8+ package)
│   └── skywatch/
│       ├── analytics_core/          # Pure analytical algorithms and health logic
│       │   ├── cpcb.py              # Official CPCB AQI sub-index calculations and rules
│       │   └── freshness.py         # Telemetry freshness status classification
│       ├── collector/               # Data collection and API clients
│       │   ├── collector.py         # IngestionOrchestrator for incremental & backfill
│       │   ├── discovery.py         # Station discovery via OpenAQ v3
│       │   ├── openaq_client.py     # OpenAQ v3 API client with retry and backoff
│       │   └── weather_client.py    # Open-Meteo forecast and archive client
│       ├── data/                    # Data transformation and validation
│       │   ├── deduplication.py     # Primary key deduplication
│       │   ├── normalization.py     # Schema-driven field extraction and casting
│       │   ├── schemas.py           # YAML schema loader and validator
│       │   └── validation.py        # Physical bounds checking
│       ├── pipeline/                # Orchestration and execution runners
│       │   ├── logging.py           # JSON logging formatter with run_id tracking
│       │   ├── pig_runner.py        # Apache Pig Latin orchestrator & fallback
│       │   ├── profile.py           # Profile loader and runtime environment validator
│       │   └── spark_runner.py      # Apache Spark analytics orchestrator & fallback
│       ├── spark/                   # PySpark analytics jobs and publishing
│       │   ├── alert_engine.py      # Health alerts and rolling median spike detection
│       │   ├── common.py            # SparkSession builder and dataset reader
│       │   ├── export.py            # Atomic staging swap publisher (Parquet + CSV)
│       │   ├── index_job.py         # Rolling 24h CPCB AQI calculation
│       │   ├── quality_report.py    # Multi-stage data quality auditing
│       │   ├── station_statistics.py # Station metrics and pollution ranking
│       │   ├── trend_analysis.py    # Diurnal hourly and day-of-week rollups
│       │   └── weather_analysis.py  # Air-weather join and Pearson correlation
│       ├── storage/                 # Storage adapters and state management
│       │   ├── hdfs_storage.py      # HDFS adapter with local fallback
│       │   ├── local_storage.py     # Atomic CSV partition writer
│       │   ├── raw_manifest.py      # Raw payload audit manifest logger
│       │   └── watermark.py         # Sensor high-water mark manager
│       └── cli.py                   # Unified CLI entrypoint
├── tests/                           # Test suite
│   ├── contract/                    # Schema contract verification tests
│   │   ├── test_pig_contracts.py    # Pig output contract and sidecar tests
│   │   ├── test_schemas.py          # YAML schema validation tests
│   │   └── test_spark_contracts.py  # Spark published schema contract tests
│   ├── fixtures/                    # Test fixtures and golden outputs
│   │   ├── golden/                  # Golden truth CSV datasets for regression checks
│   │   └── sample_*.json/csv        # Mock responses and normalized sample datasets
│   ├── integration/                 # End-to-end integration and benchmark tests
│   │   ├── test_collector_e2e.py    # End-to-end collection workflow
│   │   ├── test_pig_e2e.py          # End-to-end Pig ETL pipeline
│   │   ├── test_scale_benchmark.py  # 1x, 10x, 100x scale benchmark suite
│   │   ├── test_spark_analytics.py  # End-to-end Spark analytics suite
│   │   ├── test_storage.py          # Local and HDFS storage adapter tests
│   │   └── test_watermark.py        # Watermark persistence tests
│   └── unit/                        # Unit tests for individual components
│       ├── test_alert_engine.py
│       ├── test_clients.py
│       ├── test_cpcb.py
│       ├── test_deduplication.py
│       ├── test_discovery.py
│       ├── test_export_publisher.py
│       ├── test_freshness.py
│       ├── test_logging.py
│       ├── test_normalization.py
│       ├── test_pig_runner.py
│       ├── test_profile.py
│       ├── test_quality_report.py
│       ├── test_raw_manifest.py
│       ├── test_validation.py
│       └── test_weather_correlation.py
├── .planning/                       # GSD planning directory
│   ├── codebase/                    # Codebase intelligence maps (7 documents)
│   ├── phases/                      # Phase-by-phase specifications and plans
│   ├── PROJECT.md                   # Project overview, requirements, decisions
│   ├── REQUIREMENTS.md              # Requirement traceabilities
│   ├── ROADMAP.md                   # Milestone roadmap and execution progress
│   └── STATE.md                     # Current project state and velocity
└── pyproject.toml                   # Build system and dependency configuration
```

## Directory Purposes

- `src/skywatch/`: Python package source code containing business logic, ingestion clients, ETL runners, Spark jobs, and CLI entry points.
- `pig/`: Apache Pig Latin batch scripts executed during Phase 2 ETL on Hadoop HDFS.
- `scripts/`: Operational shell scripts for profile switching, scheduled collection, and big data job submission.
- `config/`: System configuration, schema definitions, station metadata, and official CPCB breakpoints.
- `profiles/`: Environment variable files defining tool versions, Java paths, and cluster options for `modern` and `lab` runtimes.
- `data/`: Storage tier containing partitioned raw, normalized, and clean datasets, plus logs and watermarks.
- `output/published/`: Final consumable analytical products in dual Parquet and CSV formats with schema sidecars.
- `tests/`: Comprehensive test suite divided into `unit/`, `contract/`, `integration/`, and `fixtures/`.
- `.planning/`: GSD management artifacts tracking progress, architecture, requirements, and decisions.

## Key File Locations

- CLI Entry Point: `src/skywatch/cli.py`
- Schema Contract: `config/schemas.yaml`
- Profile Switcher: `scripts/use_profile.sh`
- Pig ETL Orchestrator: `src/skywatch/pipeline/pig_runner.py`
- Spark Analytics Orchestrator: `src/skywatch/pipeline/spark_runner.py`
- CPCB Calculation Logic: `src/skywatch/analytics_core/cpcb.py`
- Atomic Export Publisher: `src/skywatch/spark/export.py`
- Scale Benchmark Test: `tests/integration/test_scale_benchmark.py`

## Naming Conventions

- Python Modules & Packages: `snake_case` (e.g. `openaq_client.py`, `spark_runner.py`).
- Classes: `PascalCase` (e.g. `IngestionOrchestrator`, `HDFSStorageAdapter`, `ProfileConfig`).
- Functions & Variables: `snake_case` (e.g. `compute_cpcb_rolling_index`, `export_published_dataset`).
- Constants: `UPPER_SNAKE_CASE` (e.g. `BENGALURU_BBOX`, `MANIFEST_HEADERS`).
- Timestamp Variables: Always suffixed with `_utc` or `_str` (e.g. `window_end_utc`, `generated_at_utc`).
- Partitions: Key-value directories (e.g. `ingest_dt=YYYY-MM-DD`, `event_dt=YYYY-MM-DD`).

## Where to Add New Code

- **New Analytical Transformations:** Add module to `src/skywatch/spark/` and register execution in `src/skywatch/pipeline/spark_runner.py`.
- **New Dataset Contracts:** Add schema definition to `config/schemas.yaml` and update contract tests in `tests/contract/`.
- **Phase 4 Interactive Dashboard:** Place Streamlit application code in `src/skywatch/dashboard/` and deploy scripts in `deploy/`.
- **Phase 4 Unified CLI Commands:** Add subcommands (`pig`, `spark`, `dashboard`, `validate`) to `src/skywatch/cli.py`.

## Special Directories

- `output/published/`: Production directory for analytical datasets published via atomic staging swaps.
- `data/local_hdfs/`: Offline simulation directory used when Hadoop HDFS cluster is unreachable.
- `tests/fixtures/golden/`: Frozen regression datasets used to verify numerical accuracy of Pig and Spark outputs.
