# SkyWatch: Urban Environmental Intelligence for Bengaluru

## What This Is

SkyWatch is a near-real-time air-quality and weather analytics platform for Bengaluru. It continuously collects PM2.5 and PM10 observations from OpenAQ v3 and hourly weather conditions from Open-Meteo, cleans and aggregates data with Apache Pig on Hadoop HDFS, computes analytical metrics and CPCB-aligned National AQI sub-indices in Apache Spark, and publishes hotspot rankings, temporal patterns, weather correlations, and health alerts through an interactive Streamlit dashboard.

## Core Value

Deliver a reproducible, verifiable big data analytics pipeline and dashboard for Bengaluru air quality with dual-profile execution (`modern` and `lab` rollback) and synthetic scale benchmarking (1×, 10×, 100×).

## Business Context

- **Customer**: Citizens, urban planners, environmental researchers, and Big Data Lab evaluators.
- **Revenue model**: Open-source academic and civic platform.
- **Success metric**: Automated smoke pipeline execution under 5 minutes on frozen sample data, with verified rollback in under 5 minutes.
- **Strategy notes**: Built to the engineering standards of Big Data Lab (ISL74) and CPCB National AQI methodology.

## Requirements

### Validated

(None yet — ship to validate)

### Active

- [ ] Dual-stack profile switching (`modern` vs `lab`) with zero version hardcoding
- [ ] OpenAQ v3 air-quality and Open-Meteo weather ingestion with station discovery and backfill
- [ ] Raw response preservation, schema normalization, deduplication, and data quality validation
- [ ] Partitioned HDFS storage layout with idempotent upload and offline cached fallback
- [ ] Apache Pig ETL: cleaning, distinct deduplication, type casting, sidecar schema generation, and multi-granularity rollups
- [ ] Apache Spark analytics: station statistics, pollution rankings, hourly/daily trends, peak-hour detection
- [ ] Spark weather-pollution Pearson correlation analysis with sample size `n` and confidence tiers
- [ ] 24-hour rolling CPCB PM-based National AQI calculation over regularized hourly means
- [ ] Automated health alerts for Poor/Severe pollution categories and operational telemetry alerts for offline/stale stations
- [ ] Streamlit dashboard with Overview, Station Map, Trends & Correlation Analytics, Alerts, and Methodology
- [ ] Unified CLI entrypoint (`skywatch`) for discovery, collection, backfill, pipeline execution, export, and smoke testing
- [ ] Synthetic scale benchmark (1×, 10×, 100×) with local execution and report extrapolation modes

### Out of Scope

- Real-time Kafka streaming — hourly batch processing cadence does not require stream brokers; filesystem and HDFS partition staging fulfills ingestion requirements.
- Full multi-pollutant AQI (SO2, NO2, CO, O3, NH3) — OpenAQ Bengaluru coverage is primarily PM2.5 and PM10; computing full AQI would violate CPCB computation standards, so alerts and indices are explicitly defined and labeled as "PM-based CPCB index".
- Machine learning pollution forecasting (e.g., LSTM/ARIMA) — focus is on big data ETL, distributed analytics (Pig, Spark, HDFS), and reproducible aggregations.
- Public multi-tenant cloud authentication — local and containerized/Streamlit deployment designed for lab evaluation and single-tenant hosting.

## Context

- **Academic Context**: Big Data Lab (ISL74) Case-Based Analytics Project.
- **Data Sources**: OpenAQ API v3 (requires API key, pagination, rate limit handling) and Open-Meteo API (free, coordinate-based hourly weather and archive).
- **Harness & Tech Stack**: Python 3.8+ compatibility floor, Apache Hadoop 3.x (HDFS, YARN/Local), Apache Pig 0.17+, Apache Spark 3.x (PySpark), Next.js 15, FastAPI / WSGI, Ruff, Pytest.
- **Runtime Profiles**:
  - `modern`: OpenJDK 17/21 (Spark), OpenJDK 8 pinned for Apache Pig 0.17, Hadoop 3.4+, Spark 3.5+, Python 3.10+
  - `lab`: OpenJDK 8, Hadoop 3.2.2, Spark 3.2.2, Python 3.8

## Constraints

- **Single-Machine Budget**: Must run on developer laptop (Spark driver max 2 GB heap, Hadoop NameNode/DataNode 1 GB heap; Pig runs in `-x local` mode to prevent YARN memory exhaustion).
- **Security**: Bound HDFS and YARN daemons to localhost; absolute zero secrets in git repository.
- **Reproducibility**: Explicit schemas and sidecar `_schema.json` for all headerless Pig outputs; deterministic Spark shuffles (`spark.sql.shuffle.partitions` fixed).
- **Methodology Honesty**: Explicit labeling of PM-based index, station freshness badges (Fresh ≤ 3h, Stale 3–24h, Offline > 24h), and empty-state handling.

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Dual Profile Architecture (`modern` & `lab`) | Prevents environment lock-in; enables evaluation on older university lab clusters and modern machines alike | ⏳ Pending |
| Pinned Java 8 for Apache Pig 0.17 | Avoids fatal `InaccessibleObjectException` crashes caused by JDK 17+ strong encapsulation on unmaintained Pig internals | ⏳ Pending |
| Pig Local Execution Mode (`-x local`) | Bypasses YARN container overhead on single-machine laptop memory budgets (1–2GB heap) | ⏳ Pending |
| Two-Stage CPCB 24h Hourly Regularization | Aggregates sub-hourly readings into clock-hour means first, ensuring $\ge 16$ distinct hours are evaluated | ⏳ Pending |
| CPCB 24-hour PM-based Index | Adheres to official Indian Central Pollution Control Board breakpoints with >=16 valid hourly readings rule | ⏳ Pending |
| Apache Pig for Initial ETL on HDFS | Satisfies syllabus requirements while separating initial row-level hygiene from complex analytics | ⏳ Pending |
| Sidecar `_schema.json` at HDFS Handoff | Protects downstream Spark jobs from schema drift when reading headerless Pig outputs | ⏳ Pending |
| Apache Spark for Aggregations & Joins | Efficient distributed processing for windowed rolling averages, weather joins, and Pearson correlation | ⏳ Pending |
| Next.js Dashboard / REST API with Atomic Directory Staging | Prevents concurrent read contention during analytical export updates, delivers responsive client-side visualizations with live/mock toggle | ⏳ Pending |

---
*Last updated: 2026-11-22 after EGM Stage 7 remediation*
