# SkyWatch: Urban Environmental Intelligence for Bengaluru

## What This Is

SkyWatch is a near-real-time air-quality and weather analytics platform for Bengaluru. It continuously collects PM2.5 and PM10 observations from OpenAQ v3 and hourly weather conditions from Open-Meteo, cleans and aggregates data with Apache Pig on Hadoop HDFS, computes analytical metrics and CPCB-aligned National AQI sub-indices in Apache Spark, and publishes hotspot rankings, temporal patterns, weather correlations, and health alerts through an interactive Streamlit dashboard.

## Core Value

Deliver an end-to-end, reproducible, and verifiable big data analytics pipeline and dashboard for Bengaluru air quality with dual-profile execution (`modern` and `lab` rollback) and a concrete scale benchmark.

## Business Context

- **Customer**: Citizens, urban planners, environmental researchers, and Big Data Lab evaluators.
- **Revenue model**: Open-source academic and civic platform.
- **Success metric**: 100% automated test and smoke pipeline execution under 5 minutes on frozen sample data, with verified rollback in under 5 minutes.
- **Strategy notes**: Built strictly to the engineering standards of Big Data Lab (ISL74) and CPCB National AQI methodology.

## Requirements

### Validated

(None yet — ship to validate)

### Active

- [ ] Dual-stack profile switching (`modern` vs `lab`) with zero version hardcoding
- [ ] OpenAQ v3 air-quality and Open-Meteo weather ingestion with station discovery and backfill
- [ ] Raw response preservation, schema normalization, deduplication, and data quality validation
- [ ] Partitioned HDFS storage layout with idempotent upload and offline cached fallback
- [ ] Apache Pig ETL: cleaning, distinct deduplication, type casting, and multi-granularity rollups
- [ ] Apache Spark analytics: station statistics, pollution rankings, hourly/daily trends, peak-hour detection
- [ ] Spark weather-pollution Pearson correlation analysis with reported sample sizes
- [ ] 24-hour rolling CPCB PM-based National AQI calculation (PM2.5 and PM10 sub-indices)
- [ ] Automated alerting for Poor/Severe pollution categories, offline stations, and stale readings
- [ ] Streamlit dashboard with Overview, Station Map, Trends & Correlation Analytics, Alerts, and Methodology
- [ ] Unified CLI entrypoint (`skywatch`) for discovery, collection, backfill, pipeline execution, export, and smoke testing
- [ ] Synthetic scale benchmark (1×, 10×, 100×) demonstrating Hadoop/Spark distributed computing utility

### Out of Scope

- Real-time Kafka streaming — batch and hourly scheduled collection fulfills near-real-time requirements without unnecessary streaming complexity.
- Full multi-pollutant AQI (SO2, NO2, CO, O3, NH3) — OpenAQ Bengaluru coverage is primarily PM2.5 and PM10; computing full AQI would be misleading, so alerts and indices are explicitly defined and labeled as "PM-based CPCB index".
- Machine learning pollution forecasting (e.g., LSTM/ARIMA) — focus is on big data ETL, distributed analytics (Pig, Spark, HDFS), and reproducible aggregations.
- Public multi-tenant cloud authentication — local and containerized/Streamlit deployment designed for lab evaluation and single-tenant hosting.

## Context

- **Academic Context**: Big Data Lab (ISL74) Case-Based Analytics Project.
- **Data Sources**: OpenAQ API v3 (requires API key, pagination, rate limit handling) and Open-Meteo API (free, coordinate-based hourly weather and archive).
- **Harness & Tech Stack**: Python 3.8+ compatibility floor, Apache Hadoop 3.x (HDFS, YARN), Apache Pig 0.17+, Apache Spark 3.x (PySpark), Streamlit, Ruff, Pytest.
- **Runtime Profiles**:
  - `modern`: OpenJDK 17/21, Hadoop 3.4+, Spark 3.5+, Python 3.10+
  - `lab`: OpenJDK 8, Hadoop 3.2.2, Spark 3.2.2, Python 3.8

## Constraints

- **Single-Machine Budget**: Must run smoothly on developer laptop (Spark driver max 2 GB heap, Hadoop NameNode/DataNode 1 GB heap).
- **Security**: Bound HDFS and YARN daemons to localhost; absolute zero secrets in git repository.
- **Reproducibility**: Explicit schemas for all headerless Pig outputs; deterministic Spark shuffles (`spark.sql.shuffle.partitions` fixed).
- **Methodology Honesty**: Explicit labeling of PM-based index, station freshness badges (Fresh <= 3h, Stale 3-24h, Offline > 24h), and empty-state handling.

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Dual Profile Architecture (`modern` & `lab`) | Prevents environment lock-in; enables evaluation on older university lab clusters and modern machines alike | ⏳ Pending |
| CPCB 24-hour PM-based Index | Adheres to official Indian Central Pollution Control Board breakpoints with >=16 valid hourly readings rule | ⏳ Pending |
| Apache Pig for Initial ETL on HDFS | Satisfies syllabus requirements while separating initial row-level hygiene from complex analytics | ⏳ Pending |
| Apache Spark for Aggregations & Joins | Efficient distributed processing for windowed rolling averages, weather joins, and Pearson correlation | ⏳ Pending |
| Streamlit for Multi-Page Visuals | Rapid, reactive UI rendering with built-in caching, maps, and CSV download capabilities | ⏳ Pending |

---
*Last updated: 2026-11-22 after initial project creation*
