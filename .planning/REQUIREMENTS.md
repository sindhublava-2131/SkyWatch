# Requirements: SkyWatch

**Defined:** 2026-11-22  
**Specification Anchor:** Requirements map to the project functional and non-functional specifications in [`docs/SkyWatch — Project Specification v2.0.md`](file:///C:/Users/GEX/Desktop/SkyWatch/docs/SkyWatch%20—%20Project%20Specification%20v2.0.md) (`FR-01` through `FR-36`, `NFR-01` through `NFR-19`).  
**Core Value:** Deliver a reproducible, verifiable big data analytics pipeline and dashboard for Bengaluru air quality with dual-profile execution (`modern` and `lab` rollback) and synthetic scale benchmarking (1×, 10×, 100×).

## v1 Requirements

### Ingestion & Collection

- [ ] **ING-01**: Collect PM2.5 and PM10 observations from OpenAQ API v3 for Bengaluru monitoring stations (FR-01).
- [ ] **ING-02**: Collect hourly temperature, relative humidity, wind speed, and precipitation from Open-Meteo in UTC (FR-02).
- [ ] **ING-03**: Auto-discover active Bengaluru stations via `/v3/locations` and record to `config/stations.json` (FR-03).
- [ ] **ING-04**: Store unmodified raw API responses with audit metadata (timestamp, HTTP status, endpoint URL) before parsing or cleaning (FR-04).
- [ ] **ING-05**: Support incremental collection using per-sensor watermarks committed atomically with file renames, and historical backfilling with chunked checkpointing (FR-11, FR-12).
- [ ] **ING-06**: Scheduled collection support via local cron script and GitHub Actions cron workflow (FR-13).

### Data Hygiene & Storage

- [ ] **HYG-01**: Normalize ingested feeds to canonical schemas defined in `config/schemas.yaml` (FR-05).
- [ ] **HYG-02**: Maintain explicit UTC measurement and ingestion timestamps on every record (FR-06).
- [ ] **HYG-03**: Deduplicate records on composite key `(sensor_id, parameter, measurement_time_utc)` during ingestion (FR-07).
- [ ] **HYG-04**: Validate records for required fields, numeric range, unit sanity, and plausible ceilings (PM2.5/PM10 ≤ 1000 μg/m³), tracking `reject_reason` (FR-08).
- [ ] **HYG-05**: Implement exponential backoff, rate limiting (HTTP 429), and atomic writes to prevent data corruption (FR-09, FR-10).
- [ ] **HYG-06**: Manage partitioned HDFS layout: `/raw/` and `/normalized/` partitioned by `ingest_dt=YYYY-MM-DD`; `/clean/` and `/analytics/` partitioned by `event_dt=YYYY-MM-DD` with idempotent uploads (FR-14, FR-15).
- [ ] **HYG-07**: Support offline execution mode running purely from cached local or HDFS datasets and recorded mock fixtures (FR-16).

### Big Data ETL (Apache Pig)

- [ ] **PIG-01**: Load normalized air-quality data from HDFS, filter invalid rows, apply safety `DISTINCT`, cast types, write sidecar `_schema.json`, and store headerless clean records (FR-17).
- [ ] **PIG-02**: Compute daily station aggregations: reading count, average, minimum, and maximum per pollutant and station (FR-18).
- [ ] **PIG-03**: Produce hourly rollups feeding downstream Spark trend analysis and regularized timeseries jobs (FR-19).

### Analytics & Indexing (Apache Spark)

- [ ] **SPK-01**: Calculate station summary statistics, pollution rankings, and identify cleanest vs most polluted hotspots (FR-20).
- [ ] **SPK-02**: Analyze hourly and daily trends, identifying peak pollution hours for each station (FR-21).
- [ ] **SPK-03**: Join air quality with weather metrics on station hour and calculate Pearson correlation coefficients with reported sample size `n` and `correlation_confidence` tier (FR-22).
- [ ] **SPK-04**: Calculate 24-hour rolling CPCB PM-based National AQI from regularized hourly means using linear interpolation and ≥16 valid hours rule, emitting explicit suppression reasons when invalid (FR-23).
- [ ] **SPK-05**: Generate health alerts for Poor/Severe categories, and separate pipeline telemetry alerts for offline stations and stale readings (FR-24).
- [ ] **SPK-06**: Classify station freshness into Fresh (≤3h), Stale (3–24h), and Offline (>24h) (FR-25).
- [ ] **SPK-07**: Generate data quality report tracking accepted, rejected, and duplicate counts categorized by reason (FR-26).
- [ ] **SPK-08**: Compute rule-based rolling median spike detection flags (FR-27).
- [ ] **SPK-09**: Export dashboard-ready Parquet and CSV datasets to `output/published/` via atomic staging directory swaps (FR-28).
- [ ] **SPK-10**: Synthetic scale benchmarking suite testing 1× (50k rows), 10× (500k rows), and 100× (5M rows) volume performance with local and report modes (FR-35, NFR-08).

### User Interface & Operations

- [ ] **OPS-01**: Streamlit multi-page dashboard featuring Overview, Stations & Map, Analytics, Alerts & Reports, and Methodology (FR-29).
- [ ] **OPS-02**: Geospatial station map colored by CPCB category with freshness indicator badges (FR-30).
- [ ] **OPS-03**: Exportable, downloadable CSV reports directly from the UI (FR-31).
- [ ] **OPS-04**: Render source attribution (OpenAQ, Open-Meteo, CPCB) on footers and display visual warnings when feeds are empty or stale (>3h) (FR-32, FR-33).
- [ ] **OPS-05**: Unified CLI entrypoint `skywatch` supporting `discover`, `collect`, `backfill`, `pipeline`, `export`, and `smoke` (FR-34).
- [ ] **OPS-06**: Profile switching script and smoke tests between `modern` and `lab` execution profiles with environment validation (FR-35).
- [ ] **OPS-07**: Structured run logging across all pipeline stages with unique run identifiers and defined schema (FR-36).

## v2 Requirements

- [ ] **EXT-01**: Kafka streaming ingestion for second-by-second sensor readings.
- [ ] **EXT-02**: Additional CPCB criteria pollutant integration (SO2, NO2, CO, O3) when sensors become available.
- [ ] **EXT-03**: Machine learning predictive forecasting model (ARIMA/LSTM) for 24h air-quality predictions.
- [ ] **EXT-04**: Multi-city expansion beyond Bengaluru to Karnataka state monitoring networks.

## Out of Scope

| Feature | Reason |
|---------|--------|
| Kafka streaming infrastructure | Hourly batch processing cadence does not require stream brokers; filesystem and HDFS partition staging fulfills ingestion requirements. |
| Synthetic full AQI without gas sensors | CPCB 8-pollutant AQI standards require gaseous parameters (SO2, NO2, CO, O3) which are not published by active Bengaluru OpenAQ stations. Synthesizing missing gases violates CPCB computation standards. |
| Public multi-tenant auth / user login | Project is focused on open data and civic observability, not multi-tenant SaaS. |
| Mobile native application | Responsive Streamlit web application fulfills all user interface requirements. |

## Traceability

### v1 Traceability Matrix

| Requirement | Phase | Status |
|-------------|-------|--------|
| ING-01 | Phase 1 | Pending |
| ING-02 | Phase 1 | Pending |
| ING-03 | Phase 1 | Pending |
| ING-04 | Phase 1 | Pending |
| ING-05 | Phase 1 | Pending |
| ING-06 | Phase 1 | Pending |
| HYG-01 | Phase 1 | Pending |
| HYG-02 | Phase 1 | Pending |
| HYG-03 | Phase 1 | Pending |
| HYG-04 | Phase 1 | Pending |
| HYG-05 | Phase 1 | Pending |
| HYG-06 | Phase 1 | Pending |
| HYG-07 | Phase 1 | Pending |
| PIG-01 | Phase 2 | Pending |
| PIG-02 | Phase 2 | Pending |
| PIG-03 | Phase 2 | Pending |
| SPK-01 | Phase 3 | Pending |
| SPK-02 | Phase 3 | Pending |
| SPK-03 | Phase 3 | Pending |
| SPK-04 | Phase 3 | Pending |
| SPK-05 | Phase 3 | Pending |
| SPK-06 | Phase 3 | Pending |
| SPK-07 | Phase 3 | Pending |
| SPK-08 | Phase 3 | Pending |
| SPK-09 | Phase 3 | Pending |
| SPK-10 | Phase 3 | Pending |
| OPS-01 | Phase 4 | Pending |
| OPS-02 | Phase 4 | Pending |
| OPS-03 | Phase 4 | Pending |
| OPS-04 | Phase 4 | Pending |
| OPS-05 | Phase 4 | Pending |
| OPS-06 | Phase 4 | Pending |
| OPS-07 | Phase 4 | Pending |

### v2 Traceability Matrix

| Requirement | Target Milestone | Status |
|-------------|------------------|--------|
| EXT-01 | Milestone 2 / Future | Deferred |
| EXT-02 | Milestone 2 / Future | Deferred |
| EXT-03 | Milestone 2 / Future | Deferred |
| EXT-04 | Milestone 2 / Future | Deferred |

**Coverage:**
- v1 requirements: 33 total
- Mapped to phases: 33
- Unmapped: 0 ✅
- v2 requirements: 4 tracked (Deferred)

---
*Requirements defined: 2026-11-22*  
*Last updated: 2026-11-22 after EGM Stage 7 remediation*
