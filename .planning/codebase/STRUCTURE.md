---
last_mapped_commit: 6b4cc6dd01b87e70230978c94258f457e4b5fcb4
last_mapped_at: 2026-11-24
---
# Codebase Structure

**Analysis Date:** 2026-11-24

## Directory Layout

```text
SkyWatch/
├── api/                             # REST API interface for frontend and serverless hosting
│   └── index.py                     # Vercel Serverless REST API entrypoint (FastAPI / WSGI)
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
├── docs/                            # Project specifications and architecture documentation
│   ├── architecture/
│   ├── plans/
│   └── specs/
├── frontend/                        # Next.js 15 interactive dashboard application
│   ├── app/                         # App router pages, layouts, and global styles
│   │   ├── alerts/                  # Active health & telemetry alerts page
│   │   ├── analytics/               # Diurnal trends and correlation charts page
│   │   ├── methodology/             # CPCB calculation & engine methodology page
│   │   ├── stations/                # MapLibre interactive geospatial station explorer
│   │   ├── globals.css              # Tailwind CSS v4 styling rules
│   │   ├── layout.tsx               # Root application shell with nav and theme provider
│   │   └── page.tsx                 # Real-time overview dashboard
│   ├── components/                  # React UI components
│   │   ├── skywatch.tsx             # Shared dashboard widgets and simulation banner
│   │   └── ui/                      # Base UI / Radix primitive components (button, card, dialog)
│   ├── lib/                         # Client utility libraries
│   │   ├── api.ts                   # Type-safe API client with fetcher and Zod parser
│   │   ├── mocks/                   # Fallback offline mock fixtures
│   │   │   └── index.ts
│   │   ├── schemas.ts               # Zod validation schemas matching REST contracts
│   │   └── utils.ts                 # Styling utilities (clsx, twMerge)
│   ├── next.config.mjs              # Next.js compiler and build configuration
│   ├── package.json                 # Node dependencies (pnpm@12.3.4, React 19, MapLibre, Recharts)
│   └── tsconfig.json                # TypeScript compiler configuration
├── output/                          # Analytics publishing tier
│   └── published/                   # Staged dual Parquet, CSV, and _schema.json datasets
│       ├── alerts/
│       ├── api/                     # Static JSON API lake for Vercel / CDN delivery
│       │   └── v1/                  # Pre-rendered endpoints (meta, stations, aqi, trends, etc.)
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
│   ├── lab.env                      # Rollback lab profile (Hadoop 3.2.2, Pig 0.17.0, Spark 3.2.2, Java 8)
│   └── modern.env                   # Modern profile (Hadoop 3.3.6, Pig 0.18.0, Spark 3.5.0, Java 17/8)
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
│       ├── __init__.py
│       ├── analytics_core/          # Pure-Python shared business logic
│       │   ├── __init__.py
│       │   ├── cpcb.py              # CPCB linear sub-index calculation and breakpoints
│       │   └── freshness.py         # Reading age calculation and status classification
│       ├── collector/               # Data ingestion clients and discovery
│       │   ├── __init__.py
│       │   ├── collector.py         # Ingestion orchestration and backfill
│       │   ├── discovery.py         # Bounding-box CAAQMS station discovery
│       │   ├── openaq_client.py     # OpenAQ v3 HTTP client
│       │   └── weather_client.py    # Open-Meteo HTTP client
│       ├── data/                    # Normalization, validation, and contracts
│       │   ├── __init__.py
│       │   ├── deduplication.py     # Deterministic key generation and deduplication
│       │   ├── normalization.py     # Canonical field mappings and unit conversions
│       │   ├── schemas.py           # Contract enforcement and sidecar metadata
│       │   └── validation.py        # Numerical bounds and ISO timestamp validation
│       ├── pipeline/                # Profile management and runner wrappers
│       │   ├── __init__.py
│       │   ├── logging.py           # Structured JSON pipeline logger and redaction
│       │   ├── pig_runner.py        # Apache Pig launcher and standalone simulation fallback
│       │   ├── profile.py           # Runtime profile parser and environment validator
│       │   └── spark_runner.py      # Apache Spark launcher and standalone simulation fallback
│       ├── spark/                   # PySpark analytical jobs and export engine
│       │   ├── __init__.py
│       │   ├── alert_engine.py      # Threshold exceedance, dropout, and spike alerts
│       │   ├── export.py            # Atomic staging swap and static API JSON exporter
│       │   ├── index_job.py         # 24-hour rolling CPCB AQI computation
│       │   ├── quality_audit.py     # Multi-stage record quality accounting
│       │   ├── station_analytics.py # Summary metrics and pollution rankings
│       │   ├── trends_job.py        # Hourly and daily aggregation with IST mapping
│       │   └── weather_analysis.py  # Spark equijoins and Pearson r correlation
│       └── storage/                 # Storage abstractions and state tracking
│           ├── __init__.py
│           ├── hdfs_storage.py      # HDFS subprocess adapter and local fallback
│           ├── raw_manifest.py      # Raw payload writing and _manifest.csv append
│           ├── storage.py           # Atomic CSV writes and directory operations
│           └── watermark.py         # Incremental ingestion high-water mark state machine
└── tests/                           # Complete test suite (111 passing tests)
    ├── contract/                    # Schema and data contract regression tests
    ├── fixtures/                    # Frozen responses and golden reference CSVs
    ├── integration/                 # End-to-end pipeline and benchmark tests
    └── unit/                        # Isolated unit tests across all components
```

## Key Locations by Concern

- **Data Contracts:** `config/schemas.yaml` (canonical contracts for all pipeline stages).
- **CPCB Mathematical Model:** `src/skywatch/analytics_core/cpcb.py` and `config/breakpoints.yaml`.
- **Measurement Freshness:** `src/skywatch/analytics_core/freshness.py` and `src/skywatch/spark/export.py:145`.
- **API Serverless Router:** `api/index.py` (FastAPI / WSGI entrypoint).
- **Static API Lake:** `output/published/api/v1/*.json` (Zero-runtime CDN delivery).
- **Interactive Dashboard:** `frontend/` (Next.js 15, React 19, MapLibre GL, Recharts).
- **CI/CD Workflow:** `.github/workflows/ci.yml` (Linting, secrets scan, 80% coverage gate, Pig & Spark runs).
