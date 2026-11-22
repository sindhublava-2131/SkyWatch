# Roadmap: SkyWatch

## Overview

SkyWatch delivers a reproducible big data environmental analytics platform for Bengaluru. The 4-phase execution roadmap establishes the ingestion and storage architecture, builds Apache Pig ETL scripts on Hadoop HDFS, implements Apache Spark analytics and CPCB AQI calculation with scale benchmarking, and delivers a Streamlit dashboard, unified CLI, and dual-profile verification.

## Prerequisites & Environment Readiness

Before executing Phase 1 plans or running end-to-end pipelines:
1. **Java Runtimes**: OpenJDK 8 installed for Apache Pig across both profiles; OpenJDK 17/21 available for Spark under `modern` profile.
2. **HDFS Liveness**: Local NameNode and DataNode formatted and running with safemode off (`hdfs dfsadmin -safemode get` returns `Safe mode is OFF`).
3. **Execution Modes**: In constrained developer environments (≤ 4GB total RAM budget), Pig executes in local mode (`pig -x local`) or standalone local MapReduce runner to prevent YARN container memory exhaustion.
4. **API Credentialing**: `OPENAQ_API_KEY` defined in `.env` (or mock fixtures activated in `tests/fixtures/` for offline/air-gapped operation).

## Phases

- [ ] **Phase 1: Foundation, Data Ingestion & Storage Architecture** - Implement project structure, dual profiles (`modern`/`lab`), schemas, OpenAQ/Open-Meteo collectors with fixture fallbacks, validation, deduplication, local and HDFS storage layout, and CLI ingestion commands.
- [ ] **Phase 2: Big Data ETL with Apache Pig & Hadoop HDFS** - Implement Apache Pig data cleansing, distinct deduplication, type casting, sidecar schema generation, daily aggregations, and hourly rollups on HDFS with profile parameterization.
- [ ] **Phase 3: Analytics Engine with Apache Spark & Scale Benchmarking** - Implement Spark station statistics, temporal trends, weather joins, Pearson correlations with confidence tiers, 24h rolling CPCB PM-based AQI index, alert engine, export publishing, and 1×/10×/100× scale benchmark.
- [ ] **Phase 4: Interactive Dashboard, CLI Integration & Dual-Profile Verification** - Implement Streamlit multi-page dashboard, unified CLI (`skywatch`), smoke tests (`make smoke`), profile rollback drill, end-to-end test suite (≥80% coverage on core logic), and project documentation.

## Phase Details

### Phase 1: Foundation, Data Ingestion & Storage Architecture
**Goal**: Implement data collection and storage pipelines that ingest air-quality and weather readings into validated, normalized datasets on local disk and partitioned HDFS.  
**Depends on**: Nothing (first phase)  
**Requirements**: ING-01, ING-02, ING-03, ING-04, ING-05, ING-06, HYG-01, HYG-02, HYG-03, HYG-04, HYG-05, HYG-06, HYG-07  
**Success Criteria** (what must be TRUE):
1. Discovery script finds Bengaluru monitoring stations via OpenAQ v3 (or verified fixture) and outputs valid `config/stations.json`.
2. Ingestion saves unmodified raw JSON responses with audit metadata, and writes normalized, deduplicated CSV records matching `config/schemas.yaml`.
3. Rejection rules detect missing fields, negative concentrations, invalid timestamps, and out-of-bound spikes (>1000 μg/m³), generating structured reject counts.
4. Local and HDFS storage handlers support atomic writes, incremental per-sensor watermarks committed with file renames, and offline execution from cached data.
5. Dual execution profile scripts (`modern.env` and `lab.env`) load environment variables and validate runtime prerequisites without error.

#### Plans
- [ ] 01-01: Project scaffolding, dual-profile environment configs, schema definitions (`schemas.yaml`), logging contract (`run_id`), and data contracts. `[HYG-01, HYG-02, OPS-06, OPS-07]`
- [ ] 01-02: OpenAQ v3 and Open-Meteo API clients, mock fixtures, station discovery, validation logic, deduplication, and raw storage. `[ING-01, ING-02, ING-03, ING-04, HYG-03, HYG-04, HYG-05]`
- [ ] 01-03: Local storage, HDFS partitioned storage adapter (`/raw/` & `/normalized/` with `ingest_dt`; `/clean/` & `/analytics/` with `event_dt`), atomic watermark commits, backfill chunking, and offline mode. `[ING-05, ING-06, HYG-06, HYG-07]`

---

### Phase 2: Big Data ETL with Apache Pig & Hadoop HDFS
**Goal**: Implement Hadoop HDFS data ingestion and Apache Pig batch processing scripts for data hygiene, distinct filtering, sidecar schema contracts, and multi-granularity rollups.  
**Depends on**: Phase 1  
**Requirements**: PIG-01, PIG-02, PIG-03  
**Success Criteria** (what must be TRUE):
1. `clean_air_quality.pig` loads normalized air-quality CSV from HDFS, filters invalid rows, applies safety `DISTINCT`, casts types, emits sidecar `_schema.json`, and outputs headerless clean records.
2. `station_stats.pig` groups by station, parameter, and date to output reading count, average, minimum, and maximum.
3. `hourly_rollup.pig` buckets measurements into regularized hourly intervals feeding downstream Spark trend analysis.
4. Python Pig runner wrapper passes parameters (`$INPUT`, `$OUTPUT`, `$RUN_DATE`) and executes scripts with exit code 0 in under 90 seconds on frozen sample data.

#### Plans
- [ ] 02-01: Apache Pig scripts (`clean_air_quality.pig`, `station_stats.pig`, `hourly_rollup.pig`) with explicit schema definitions and Java 8 compatibility. `[PIG-01, PIG-02, PIG-03]`
- [ ] 02-02: Pig runner orchestration module, parameter passing, sidecar `_schema.json` generation, HDFS verification, and integration tests on sample dataset. `[PIG-01, PIG-02, PIG-03]`

---

### Phase 3: Analytics Engine with Apache Spark & Scale Benchmarking
**Goal**: Build the Apache Spark distributed analytics suite, CPCB PM-based AQI calculator, alert engine, data quality reporting, and synthetic scale benchmark.  
**Depends on**: Phase 2  
**Requirements**: SPK-01, SPK-02, SPK-03, SPK-04, SPK-05, SPK-06, SPK-07, SPK-08, SPK-09, SPK-10  
**Success Criteria** (what must be TRUE):
1. Spark jobs calculate station rankings, hourly/daily pollution trends, and peak-hour detection consuming Pig clean and rollup HDFS outputs.
2. Spark hourly weather join executes and computes Pearson correlation coefficients with sample size `n` and `correlation_confidence` tier.
3. 24-hour rolling CPCB index computes piecewise linear sub-indices for PM2.5 and PM10 over regularized hourly means, enforcing the ≥16 valid hours rule and max-subindex overall rating, emitting explicit suppression records when below threshold.
4. Alert engine flags Poor/Severe categories, stale stations, and offline sensors, exporting published Parquet and CSV outputs to `output/published/` via atomic directory staging.
5. Synthetic scale benchmark (`make_synthetic.py` & `run_scale_benchmark.py`) tests runtime performance across 1× (50k), 10× (500k), and 100× (5M) volume in local and report modes.

#### Plans
- [ ] 03-01: Core analytical functions (`cpcb.py`, `freshness.py`), sidecar schema verification, and Spark common configuration (`get_spark`, deterministic shuffle partitions). `[SPK-01, SPK-04, SPK-06]`
- [ ] 03-02: Spark analytics jobs: station statistics, trend analysis, weather correlation with confidence tiers, 24h rolling CPCB index with suppression codes, alert engine, and data quality reporter. `[SPK-02, SPK-03, SPK-05, SPK-07, SPK-08]`
- [ ] 03-03: Export publisher to `output/published/` with atomic directory staging and synthetic scale benchmarking suite (1×, 10×, 100×). `[SPK-09, SPK-10]`

---

### Phase 4: Interactive Dashboard, CLI Integration & Dual-Profile Verification
**Goal**: Deliver the multi-page Streamlit dashboard, unified CLI, automated smoke tests, profile rollback drill, and project documentation.  
**Depends on**: Phase 3  
**Requirements**: OPS-01, OPS-02, OPS-03, OPS-04, OPS-05, OPS-06, OPS-07  
**Success Criteria** (what must be TRUE):
1. Streamlit app runs and renders all 5 pages (Overview, Stations & Map, Analytics, Alerts & Reports, Methodology) with interactive charts, maps, and CSV downloads.
2. Empty-state banners, stale-data warnings, and data source attributions (OpenAQ, Open-Meteo, CPCB) render when triggered by dataset boundary conditions.
3. Unified CLI `skywatch` executes subcommands `discover`, `collect`, `backfill`, `pipeline`, `export`, and `smoke`.
4. Dual-profile switching script toggles between `modern` and `lab` environments without manual reconfiguration, passing smoke tests with matching row counts and floating point values within $\epsilon < 0.01$.
5. Complete required project documentation (architecture, data dictionary, test plan, setup guide, methodology, AI usage log, viva notes) and achieve ≥80% unit test coverage on core analytical logic.

#### Plans
- [ ] 04-01: Streamlit multi-page dashboard UI (data access, components, map visualization, trends, alerts, methodology). `[OPS-01, OPS-02, OPS-03, OPS-04]`
- [ ] 04-02: Unified CLI entrypoint (`cli.py`), profile switcher script (`use_profile.sh`), and end-to-end pipeline runner (`pipeline_runner.py`). `[OPS-05, OPS-06, OPS-07]`
- [ ] 04-03: Automated smoke suite (`make smoke`), profile rollback drill (`rollback_drill.sh`), unit & integration test suite (pytest-cov ≥80% line coverage on `src/`), and viva/documentation guides. `[OPS-06, OPS-07]`

---

## Progress

**Execution Order:**  
Phases execute in numeric order: 1 → 2 → 3 → 4

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 1. Foundation, Data Ingestion & Storage Architecture | 0/3 | Not started | - |
| 2. Big Data ETL with Apache Pig & Hadoop HDFS | 0/2 | Not started | - |
| 3. Analytics Engine with Apache Spark & Scale Benchmarking | 0/3 | Not started | - |
| 4. Interactive Dashboard, CLI Integration & Dual-Profile Verification | 0/3 | Not started | - |
