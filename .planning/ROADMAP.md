# Roadmap: SkyWatch

## Overview

SkyWatch delivers a production-grade, reproducible big data environmental analytics platform for Bengaluru. The 4-phase execution roadmap systematically lays the foundational ingestion and storage architecture, builds robust Apache Pig ETL scripts on Hadoop HDFS, develops the distributed Apache Spark analytics and CPCB AQI engine with scale benchmarking, and finishes with an interactive Streamlit dashboard, unified CLI, and rigorous dual-profile verification.

## Phases

- [ ] **Phase 1: Foundation, Data Ingestion & Storage Architecture** - Implement project structure, dual profiles (`modern`/`lab`), schemas, OpenAQ/Open-Meteo collectors, validation, deduplication, local and HDFS storage layout, and CLI ingestion commands.
- [ ] **Phase 2: Big Data ETL with Apache Pig & Hadoop HDFS** - Implement Apache Pig data cleansing, distinct deduplication, type casting, daily aggregations, and hourly rollups on HDFS with profile parameterization.
- [ ] **Phase 3: Analytics Engine with Apache Spark & Scale Benchmarking** - Implement Spark station statistics, temporal trends, weather joins, Pearson correlations, 24h rolling CPCB PM-based AQI index, alert engine, export publishing, and 1×/10×/100× scale benchmark.
- [ ] **Phase 4: Interactive Dashboard, CLI Integration & Dual-Profile Verification** - Implement Streamlit multi-page dashboard, unified CLI (`skywatch`), smoke tests (`make smoke`), profile rollback drill, end-to-end test suite, and project documentation.

## Phase Details

### Phase 1: Foundation, Data Ingestion & Storage Architecture
**Goal**: Build a resilient data collection and storage pipeline that ingests air-quality and weather readings into validated, normalized datasets on both local disk and partitioned HDFS.
**Depends on**: Nothing (first phase)
**Requirements**: ING-01, ING-02, ING-03, ING-04, ING-05, ING-06, HYG-01, HYG-02, HYG-03, HYG-04, HYG-05, HYG-06, HYG-07
**Success Criteria** (what must be TRUE):
  1. Discovery script finds Bengaluru monitoring stations via OpenAQ v3 and outputs valid `config/stations.json`.
  2. Ingestion saves raw JSON responses untouched with audit metadata, and writes normalized, deduplicated CSV records matching `config/schemas.yaml`.
  3. Rejection rules detect missing fields, negative concentrations, invalid timestamps, and out-of-bound spikes, generating structured reject counts.
  4. Local and HDFS storage handlers support atomic writes, incremental per-sensor watermarks, and offline execution from cached data.
  5. Both `modern.env` and `lab.env` execution profile scripts load correct environment variables without errors.
**Plans**: 3 plans

Plans:
- [ ] 01-01: Project scaffolding, dual-profile environment configs, schema definitions (`schemas.yaml`), and data contracts.
- [ ] 01-02: OpenAQ v3 and Open-Meteo API clients, station discovery, validation logic, deduplication, and raw storage.
- [ ] 01-03: Local storage, HDFS partitioned storage adapter (`/skywatch/...`), incremental collection watermarking, backfill, and offline mode.

### Phase 2: Big Data ETL with Apache Pig & Hadoop HDFS
**Goal**: Implement Hadoop HDFS data ingestion and Apache Pig batch processing scripts for data hygiene, distinct filtering, and multi-granularity rollups.
**Depends on**: Phase 1
**Requirements**: PIG-01, PIG-02, PIG-03
**Success Criteria** (what must be TRUE):
  1. `clean_air_quality.pig` loads normalized air-quality CSV from HDFS, filters invalid rows, applies `DISTINCT`, casts types, and outputs headerless clean records.
  2. `station_stats.pig` groups by station, parameter, and date to output reading count, average, minimum, and maximum.
  3. `hourly_rollup.pig` buckets measurements into hourly intervals for downstream Spark analysis.
  4. Python Pig runner wrapper passes parameters (`$INPUT`, `$OUTPUT`, `$RUN_DATE`) and executes scripts cleanly in under 90 seconds on sample data.
**Plans**: 2 plans

Plans:
- [ ] 02-01: Apache Pig scripts (`clean_air_quality.pig`, `station_stats.pig`, `hourly_rollup.pig`) with explicit schema definitions.
- [ ] 02-02: Pig runner orchestration module, parameter passing, HDFS input/output verification, and unit/integration tests with sample data.

### Phase 3: Analytics Engine with Apache Spark & Scale Benchmarking
**Goal**: Build the Apache Spark distributed analytics suite, CPCB PM-based AQI calculator, alert generator, data quality reporting, and synthetic scale benchmark.
**Depends on**: Phase 2
**Requirements**: SPK-01, SPK-02, SPK-03, SPK-04, SPK-05, SPK-06, SPK-07, SPK-08, SPK-09, OPS-08
**Success Criteria** (what must be TRUE):
  1. Spark jobs calculate station rankings, hourly/daily pollution trends, and peak-hour detection.
  2. Spark hourly weather join executes and computes Pearson correlation coefficients with reported sample size `n`.
  3. 24-hour rolling CPCB index computes piecewise linear sub-indices for PM2.5 and PM10, enforcing the >=16 valid hours rule and max-subindex overall rating.
  4. Alert engine flags Poor/Severe categories, stale stations, and offline sensors, exporting published Parquet and CSV outputs to `output/published/`.
  5. Synthetic scale benchmark (`make_synthetic.py` & `run_scale_benchmark.py`) measures runtime performance across 1×, 10×, and 100× volume.
**Plans**: 3 plans

Plans:
- [ ] 03-01: Core analytical functions (`cpcb.py`, `freshness.py`) and Spark common configuration (`get_spark`, deterministic shuffle partitions, explicit schemas).
- [ ] 03-02: Spark analytics jobs: station statistics, trend analysis, weather correlation, 24h rolling CPCB index, alert engine, and data quality reporter.
- [ ] 03-03: Export publisher to `output/published/` (Parquet + CSV) and synthetic scale benchmarking suite (1×, 10×, 100×).

### Phase 4: Interactive Dashboard, CLI Integration & Dual-Profile Verification
**Goal**: Deliver the multi-page Streamlit dashboard, unified CLI, automated smoke tests, profile rollback drill, and comprehensive project documentation.
**Depends on**: Phase 3
**Requirements**: OPS-01, OPS-02, OPS-03, OPS-04, OPS-05, OPS-06, OPS-07
**Success Criteria** (what must be TRUE):
  1. Streamlit app runs and renders all 5 pages (Overview, Stations & Map, Analytics, Alerts & Reports, Methodology) with interactive charts, maps, and CSV downloads.
  2. Empty-state, stale-data banners, and explicit data attribution (OpenAQ, Open-Meteo, CPCB) display accurately.
  3. Unified CLI `skywatch` executes subcommands `discover`, `collect`, `backfill`, `pipeline`, `export`, and `smoke`.
  4. Dual-profile switching script seamlessly toggles between `modern` and `lab` environments, passing smoke tests and matching outputs.
  5. Comprehensive documentation (architecture, data dictionary, test plan, setup, methodology, AI usage, viva notes) and >=80% test coverage on core logic.
**Plans**: 3 plans

Plans:
- [ ] 04-01: Streamlit multi-page dashboard UI (data access, components, map visualization, trends, alerts, methodology).
- [ ] 04-02: Unified CLI entrypoint (`cli.py`), profile switcher script (`use_profile.sh`), and end-to-end pipeline runner (`pipeline_runner.py`).
- [ ] 04-03: Automated smoke suite (`make smoke`), profile rollback drill (`rollback_drill.sh`), unit & integration test suite, and viva/documentation guides.

## Progress

**Execution Order:**
Phases execute in numeric order: 1 → 2 → 3 → 4

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 1. Foundation, Data Ingestion & Storage Architecture | 0/3 | Not started | - |
| 2. Big Data ETL with Apache Pig & Hadoop HDFS | 0/2 | Not started | - |
| 3. Analytics Engine with Apache Spark & Scale Benchmarking | 0/3 | Not started | - |
| 4. Interactive Dashboard, CLI Integration & Dual-Profile Verification | 0/3 | Not started | - |
