# SkyWatch — Project Specification v2.0

*Urban Environmental Intelligence for Bengaluru · Big Data Lab (ISL74) · 20-mark Case-Based Analytics Project · Spec date: 22 Nov 2026*

## 0. What changed from v1

- **Two stack profiles, one command to switch.** `modern` (2026 releases) is the default; `lab` (Hadoop 3.2.2 / JDK 8 / Spark 3.2.2) is a verified rollback. Nothing in the code hardcodes a version.
- **Real AQI methodology.** Alerts use the CPCB National AQI method (24-hour average, sub-index interpolation), computed only for PM2.5 and PM10 and labelled as a PM-based index, never as the full AQI.
- **Genuinely "big" data.** v1 would have produced a few thousand rows. v2 adds historical backfill and a clearly labelled synthetic scale benchmark, so Hadoop, Pig and Spark are justified rather than decorative.
- **Data keeps accumulating while your laptop is off.** A scheduled GitHub Actions collector gathers raw readings; the local pipeline pulls them into HDFS.
- **Full engineering scope kept** (CI/CD, deployment, documentation, tests) because evaluators reward work beyond the brief.
- **Honest authorship.** A documented AI-assistance disclosure and viva-readiness notes are part of the deliverable.

## 1. Project identity

| Item | Value |
| --- | --- |
| Name | **SkyWatch** |
| Tagline | Urban Environmental Intelligence |
| Full title | SkyWatch: A Near-Real-Time Air Quality and Weather Analytics Platform for Bengaluru on Hadoop, Pig and Spark |
| Repository | `SkyWatch` |
| Repo description | Near-real-time air-quality and weather analytics for Bengaluru using OpenAQ v3, Open-Meteo, Hadoop HDFS, Apache Pig, PySpark and Streamlit. |
| Licence | MIT (code). Data carries its providers' licences and attribution. |

## 2. Purpose and problem statement

**Problem.** Bengaluru's air-quality readings are published as raw concentrations (µg/m³) by individual monitoring stations. A citizen, student or planner cannot easily tell which locations are worst, when pollution peaks, whether readings are even current, or how weather coincides with pollution. Raw values also need a recognised health classification to mean anything.

**Purpose.** SkyWatch continuously collects PM2.5 and PM10 observations from OpenAQ and hourly weather from Open-Meteo for monitored Bengaluru locations. It stores every raw response, cleans and aggregates the data with Apache Pig on HDFS, and runs analytics in Apache Spark. It then publishes hotspot rankings, temporal patterns, weather-context statistics, data-freshness monitoring and CPCB-category alerts through a dashboard.

**Objectives.**

1. Spatial insight: which stations are most polluted?
2. Temporal insight: at what hours and on which days does pollution peak?
3. Context insight: how do temperature, humidity and wind coincide with PM levels?
4. Trust insight: how recent is each reading; which stations are stale or offline?
5. Actionable insight: which stations are in Poor or worse CPCB categories?
6. Engineering objective: demonstrate a reproducible, tested, version-portable Big Data pipeline.

**Why Big Data tooling is justified.** Data is append-only and time-partitioned. It grows by (stations × pollutants × hours) every day, plus multi-year historical backfill. HDFS gives partitioned durable storage, Pig gives declarative ETL that compiles to MapReduce, and Spark gives windowed and joined analytics. A scale benchmark (§14.7) shows runtime against 1×, 10× and 100× data.

**Explicit non-claims.** SkyWatch does not forecast pollution, prove causation between weather and pollution, represent unmonitored neighbourhoods, or present Open-Meteo model output as station measurements. A polling interval is not "real time": every reading shows its own observation timestamp.

**Scope.** Bengaluru only. 5 to 10 stations (active ones, verified by the discovery tool). PM2.5 and PM10 primary; NO2 and O3 collected only where a station reports them. Rule-based alerts only.

## 3. Bottlenecks found in v1 and how v2 fixes them

| # | Bottleneck | Fix in v2 |
| --- | --- | --- |
| 1 | Version lock-in (Hadoop 3.2.2, JDK 8, Spark 3.2.2, Python 3.8) | Profile system: `profiles/lab.env`, `profiles/modern.env`, `scripts/use_profile.sh`, per-tool `JAVA_HOME` (§4) |
| 2 | pip `pyspark` can disagree with the installed Spark | Jobs run through `spark-submit` from `$SPARK_HOME`; pip PySpark is for unit tests only |
| 3 | Spark 4 turns ANSI mode on by default, so bad casts throw instead of returning null | `spark.sql.ansi.enabled=false` is set explicitly in both profiles and covered by a test |
| 4 | Latest `pandas` (3.x) conflicts with PySpark 4.3, which still wants pandas below 3.0 | Two venvs: pipeline venv pins pandas below 3; dashboard venv uses latest pandas |
| 5 | Python 3.8 syntax floor for the lab profile | Ruff `target-version = py38`; `from __future__ import annotations`; no `match` statement; use `Optional[X]` instead of the newer union syntax |
| 6 | OpenAQ "latest" returns only the newest value, so history never accumulates | Incremental watermark collection, backfill, and a scheduled GitHub Actions collector |
| 7 | AQI computed naively from PM concentration | Proper CPCB sub-index with 24 h averaging and a validity rule (§6 FR-23) |
| 8 | Pig, Spark and the collector could drift on column names | Single `config/schemas.yaml`; contract tests assert Pig `AS` clauses and Spark `StructType` match it |
| 9 | Pig output has no header, so `inferSchema` mislabels columns | Spark always uses explicit schemas; never `inferSchema` |
| 10 | Weather join mismatch (time zones, minutes vs hours) | Both sides truncated to UTC hour; Open-Meteo requested with `timezone=UTC` |
| 11 | Streamlit Cloud cannot reach local HDFS | Pipeline exports dashboard-ready files to a `published-data` branch; dashboard reads those |
| 12 | Stations in a spec may be inactive | `discover_stations` filters by recent activity before anything is frozen in config |
| 13 | Dataset too small to justify Big Data | Backfill plus labelled synthetic scale benchmark |
| 14 | Pig on a newer JDK is unverified (Pig 0.18.0 still declares only "Java 1.8 or newer") | Pig runs on JDK 11 with Hadoop 3.4.3 in `modern`; `make smoke` gates every profile |

## 4. Stack profiles and rollback design

| Component | `modern` (default) | `lab` (rollback) |
| --- | --- | --- |
| HDFS | Hadoop 3.4.3 | Hadoop 3.2.2 |
| Java for HDFS and Pig | JDK 11 | JDK 8 |
| Java for Spark | JDK 17 | JDK 8 |
| Pig | 0.18.0 | 0.18.0 (Hadoop 3 build) |
| Spark | 4.3.0 (4.1.4 as stable alternative) | 3.2.2 (Hadoop 3.2 build) |
| Python (pipeline venv) | 3.12 (3.11 minimum) | 3.8 or 3.9 |
| Python (dashboard venv) | 3.12 or 3.13 | same as modern |
| Optional `modern-max` | Hadoop 3.5.0 on JDK 17 for everything, adopted only if the Pig smoke test passes | n/a |

**Rules that keep rollback cheap**

1. Install side by side under `/opt/bigdata/` and `~/.sdkman`; never overwrite.
2. `source scripts/use_profile.sh lab|modern` exports `HADOOP_HOME`, `PIG_HOME`, `SPARK_HOME`, a per-tool `JAVA_HOME`, `SKYWATCH_PROFILE`, and activates the matching pipeline venv.
3. Pig scripts are parameterised (`$INPUT`, `$OUTPUT`, `$RUN_DATE`), use only `PigStorage(',')`, and avoid version-specific UDFs.
4. Spark code uses only DataFrame APIs available in Spark 3.2 (no Arrow-only UDFs, no declarative pipelines, no `ASOF JOIN`).
5. The dashboard venv never changes with the profile; it reads exported CSV and Parquet only.
6. Pig runs MapReduce in local mode against HDFS (`mapreduce.framework.name=local`), so YARN is optional.
7. `make smoke PROFILE=<p>` is the acceptance gate: HDFS put, Pig group-by, Spark read and aggregate, diff against a golden file, all in under 10 minutes. A profile is demo-ready only if it passes.
8. Rollback drill: `make rollback-drill` switches to `lab`, runs the smoke test and the full pipeline on the frozen sample dataset, and compares outputs to the `modern` run. Differences must be zero after rounding to 4 decimals.

## 5. Dependencies (latest as of 22 Nov 2026, checked against PyPI and project sites)

**Infrastructure**

| Dependency | `modern` version | Notes |
| --- | --- | --- |
| Hadoop | 3.4.3 (3.5.0 released Apr 2026 requires Java 17 server-side) | `lab`: 3.2.2 |
| Pig | 0.18.0 (released 15 Sep 2025; still the latest) | supports Hadoop 3, Spark 3, Python 3 streaming |
| Spark | 4.3.0 (released 30 Oct 2026) | needs Java 17+ and Python 3.11+; `lab`: 3.2.2 |
| JDK | 11 and 17 (sdkman) | `lab`: 8 |
| Git, GNU Make, Linux | current | Ubuntu 22.04 or 24.04 recommended |

**Python, pipeline venv** (`requirements-pipeline.in`; pip-compile to `requirements-modern.lock` and `requirements-lab.lock`)

| Package | Latest | Purpose |
| --- | --- | --- |
| requests | 2.34.2 | API calls (lab lock resolves an older compatible release) |
| python-dotenv | 1.2.4 | load `.env` |
| pyyaml | 6.0.4 | config and schemas |
| pandas | below 3.0 | local inspection (latest 3.1.1 is dashboard-only) |
| pyspark | 4.3.0 | unit tests and local dev only; runtime uses `$SPARK_HOME` |

**Python, dashboard venv** (`requirements-dashboard.txt`): streamlit 1.67.0, plotly 7.1.0, pandas 3.1.1, pyarrow 26.0.1.

**Python, dev tooling** (`requirements-dev.txt`): pytest 9.2.0, pytest-cov 7.1.0, ruff 0.17.6, mypy 2.4.1, pre-commit 4.7.0, responses 0.26.3.

Pin policy: commit lock files. Versions above are the latest releases found; the lock files are the tested truth. Do not upgrade PySpark independently of `$SPARK_HOME`.

## 6. Functional requirements

Priority: M = must, S = should, C = could.

| ID | Requirement | Pri |
| --- | --- | --- |
| FR-01 | Collect PM2.5 and PM10 (plus NO2, O3 where present) from OpenAQ v3 for configured Bengaluru locations | M |
| FR-02 | Collect hourly temperature, relative humidity, wind speed and precipitation from Open-Meteo at each station's coordinates, `timezone=UTC` | M |
| FR-03 | `discover_stations`: query `/v3/locations` for Bengaluru, keep stations with a recent reading, and write `config/stations.json` | M |
| FR-04 | Save every raw API response untouched, with request metadata, before any processing | M |
| FR-05 | Normalise to the schemas in §8 | M |
| FR-06 | Keep UTC measurement timestamp and UTC ingestion timestamp on every record | M |
| FR-07 | Deduplicate on `(sensor_id, parameter, measurement_time_utc)` | M |
| FR-08 | Validate: required fields, numeric, non-negative, unit µg/m³, implausible-value flag above a configured ceiling, valid timestamp. Rejects keep a `reject_reason` | M |
| FR-09 | Retry with exponential backoff, configurable timeout, honour HTTP 429 and `Retry-After` | M |
| FR-10 | A failed or partial collection never destroys prior data (atomic writes: temp file, then rename) | M |
| FR-11 | Incremental collection using a per-sensor watermark | M |
| FR-12 | Historical backfill from OpenAQ (hours endpoint or the public OpenAQ archive) and Open-Meteo archive | M |
| FR-13 | Scheduled collection: local cron and a GitHub Actions cron workflow | S |
| FR-14 | HDFS layout `/skywatch/{raw,normalized,clean,analytics}/dt=YYYY-MM-DD/` | M |
| FR-15 | Idempotent upload to HDFS (re-running never duplicates partitions) | M |
| FR-16 | Offline mode: the pipeline runs entirely from cached local or HDFS data when APIs fail | M |
| FR-17 | Pig ETL: load, filter invalid rows, `DISTINCT`, cast types, write `clean/` | M |
| FR-18 | Pig aggregation: per station, parameter and date: count, average, min, max | M |
| FR-19 | Pig hourly rollup feeding Spark trend jobs | S |
| FR-20 | Spark station statistics and pollution ranking | M |
| FR-21 | Spark hourly and daily trends and peak-hour identification per station | M |
| FR-22 | Spark weather join at hourly granularity and Pearson correlation per station and variable, with the sample size `n` reported | M |
| FR-23 | Spark CPCB PM-based index: 24 h rolling mean per station and pollutant, valid only with at least 16 valid hourly values, sub-index by linear interpolation over the breakpoints in `config/breakpoints.yaml`, category assigned; overall = max of PM2.5 and PM10 sub-indices | M |
| FR-24 | Alerts for Poor or worse (threshold configurable), plus stale and offline station alerts | M |
| FR-25 | Freshness status per station: Fresh up to 3 h, Stale 3 to 24 h, Offline over 24 h (configurable) | M |
| FR-26 | Data-quality report: counts of accepted, rejected and duplicate records by reason | M |
| FR-27 | Rule-based spike flag: value above N× the station's rolling median (no ML) | C |
| FR-28 | Export dashboard-ready CSV and Parquet to `output/published/` | M |
| FR-29 | Dashboard pages: Overview, Stations and Map, Analytics, Alerts and Reports, Methodology | M |
| FR-30 | Map of stations coloured by CPCB category, with freshness badge | S |
| FR-31 | Downloadable CSV reports | S |
| FR-32 | Source attribution (OpenAQ, Open-Meteo, CPCB method) on every page footer | M |
| FR-33 | Dashboard empty-state and stale-data banners | M |
| FR-34 | One CLI entry point with subcommands `discover`, `collect`, `backfill`, `pipeline`, `export`, `smoke` | M |
| FR-35 | Profile switching and smoke test (§4) | M |
| FR-36 | Structured logs for every stage with run id | M |

**CPCB breakpoints (24 h average, µg/m³; stored in `config/breakpoints.yaml`, verify against the CPCB technical note before freezing).** PM2.5: 0–30 Good, 31–60 Satisfactory, 61–90 Moderate, 91–120 Poor, 121–250 Very Poor, above 250 Severe. PM10: 0–50, 51–100, 101–250, 251–350, 351–430, above 430. Sub-index bands: 0–50, 51–100, 101–200, 201–300, 301–400, 401–500. OpenAQ only distributes raw concentrations, so SkyWatch must compute the index itself and must label it "PM-based CPCB index".

## 7. Non-functional requirements

| ID | Area | Requirement and target |
| --- | --- | --- |
| NFR-01 | Reliability | Failed API calls leave existing data byte-identical (tested) |
| NFR-02 | Data integrity | Rejected records never reach analytics; every reject is counted |
| NFR-03 | Reproducibility | Same input gives identical output: fixed sort orders, no randomness, `spark.sql.shuffle.partitions` fixed |
| NFR-04 | Performance | On the frozen sample, Pig stage under 90 s and each Spark job under 60 s; end-to-end under 5 min; results reported, not guaranteed |
| NFR-05 | Availability | Dashboard and pipeline work from cache when APIs are down |
| NFR-06 | Security | No secrets in code or Git history (gitleaks in CI and pre-commit); HDFS and YARN ports bound to localhost; no public NameNode |
| NFR-07 | Maintainability | Modules separated by layer; type hints; Ruff clean; no function mixes I/O, cleaning and rendering |
| NFR-08 | Scalability | Benchmark at 1×, 10×, 100× data; no redesign needed (partitioned HDFS layout) |
| NFR-09 | Portability | Documented setup for Ubuntu; both profiles pass `make smoke` |
| NFR-10 | Rollback | Switch between profiles in under 5 minutes; outputs match after rounding |
| NFR-11 | Usability | Every chart shows station, pollutant, unit, observation time and freshness |
| NFR-12 | Observability | Run id, stage durations, row counts and rejects logged |
| NFR-13 | Freshness honesty | Old readings are never shown as live |
| NFR-14 | Privacy | No personal data collected |
| NFR-15 | Resource use | Runs on one machine: Spark driver 2 GB, Hadoop heap 1 GB |
| NFR-16 | Licence compliance | Attribution for OpenAQ and Open-Meteo displayed; dataset licences listed in README |
| NFR-17 | Test quality | At least 80% coverage on core logic modules (validation, normalisation, dedup, index, freshness) |
| NFR-18 | Documentation | README, architecture, data dictionary, API doc, test plan, setup, methodology, AI usage |
| NFR-19 | Python floor | Source parses under Python 3.8 syntax rules (checked by Ruff target) |

## 8. Data contracts

**Single source of truth:** `config/schemas.yaml`. Contract tests generate or verify the Pig `AS (...)` clause and the Spark `StructType` from it.

**Air quality (normalised)**: `record_key` (string, hash of sensor\_id|parameter|measurement\_time\_utc), `station_id`, `station_name`, `latitude` (double), `longitude` (double), `sensor_id`, `parameter`, `value` (double, null allowed before validation), `unit`, `measurement_time_utc` (ISO 8601 UTC), `ingestion_time_utc`, `source`, `is_valid` (boolean), `reject_reason` (string, null if valid).

**Weather (normalised)**: `location_id`, `latitude`, `longitude`, `temperature_c`, `humidity_pct`, `wind_speed`, `wind_unit`, `precipitation_mm`, `measurement_time_utc` (hour), `ingestion_time_utc`, `source`, `data_kind` (`forecast_model` or `archive_model`).

**Analytics outputs**: `station_statistics` (station\_id, station\_name, parameter, average\_value, minimum\_value, maximum\_value, reading\_count, last\_measurement\_time, freshness\_status); `trends_hourly` and `trends_daily` (station\_id, parameter, bucket\_start\_utc, average\_value, reading\_count); `weather_correlation` (station\_id, parameter, weather\_variable, pearson\_r, n); `cpcb_index` (station\_id, computed\_at\_utc, pm25\_24h\_avg, pm10\_24h\_avg, pm25\_subindex, pm10\_subindex, index\_value, category, valid\_hours); `alerts` (station\_id, station\_name, alert\_type, parameter, category, measurement\_time\_utc, threshold\_rule, generated\_at\_utc); `quality_report` (run\_id, stage, accepted, rejected, duplicates, reason\_counts).

**HDFS layout and file formats**: `normalized/` is CSV with header, partitioned by `dt`. `clean/` is Pig `PigStorage(',')` output, headerless. `analytics/` is Parquet, plus CSV copies exported to `output/published/`. The format rule is: whoever reads headerless data supplies an explicit schema.

## 9. API documentation and key requirements

### 9.1 OpenAQ API v3 (primary air-quality source)

| Item | Detail |
| --- | --- |
| Key | **Required, free.** Register at the OpenAQ Explorer (explore.openaq.org), then generate a key in account settings |
| Auth | HTTP header `X-API-Key: <key>` on every request |
| Base URL | `https://api.openaq.org/v3` |
| Documented limits | 60 requests per minute and 2,000 per hour on the free tier (confirm on your account; read the `x-ratelimit-*` response headers at runtime) |
| Data licence | Per-source; OpenAQ aggregates open data, so display attribution to OpenAQ and the originating provider |
| Coverage rule | OpenAQ only publishes raw concentrations from official outdoor sources, not AQI values |

**Endpoints used**

| Endpoint | Used for |
| --- | --- |
| `GET /locations` (bbox or coordinates + radius) | station discovery (FR-03) |
| `GET /locations/{id}` and `/locations/{id}/sensors` | metadata, sensor ids, parameters |
| `GET /locations/{id}/latest` | polling (FR-01); inspect the timestamp, "latest" does not mean recent |
| `GET /sensors/{id}/measurements` | raw history in a time window |
| `GET /sensors/{id}/hours` | hourly aggregates for backfill (FR-12) |

**Error handling contract**

| Status | Behaviour |
| --- | --- |
| 401 / 403 | Stop; log "authentication failed"; never retry; never print the key |
| 404 / empty | Log and skip the station; mark it for the staleness report |
| 408 / timeout / 5xx | Retry with exponential backoff and jitter, up to `max_retries` |
| 429 | Sleep for `Retry-After` (or the rate-limit reset), then retry |

**Key handling**

1. Local: put `OPENAQ_API_KEY=...` in `.env` (git-ignored). Commit only `.env.example` with a placeholder.
2. GitHub Actions: store as repository secret `OPENAQ_API_KEY`; only the scheduled-collect and manual live-contract workflows may read it.
3. Logs redact anything matching the key; gitleaks runs in pre-commit and CI.
4. If a key leaks, revoke it in OpenAQ, create a new one, and rewrite history only if the commit was pushed.

```
curl -H "X-API-Key: $OPENAQ_API_KEY" "https://api.openaq.org/v3/locations?bbox=77.45,12.83,77.75,13.12&limit=100"
```

### 9.2 Open-Meteo (weather context)

| Item | Detail |
| --- | --- |
| Key | **None** for free non-commercial use; fair-use rate limits apply (check the current terms page) |
| Live and recent | `https://api.open-meteo.com/v1/forecast?latitude=..&longitude=..&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation&past_days=2&timezone=UTC` |
| Historical backfill | `https://archive-api.open-meteo.com/v1/archive?...&start_date=YYYY-MM-DD&end_date=YYYY-MM-DD&hourly=...&timezone=UTC` |
| Nature of data | Gridded model output, not a physical Bengaluru weather station; stored with `data_kind` and labelled so on the dashboard |
| Attribution | Display "Weather data by Open-Meteo.com" (CC BY 4.0) |

### 9.3 Environment variables (`.env.example`)

```
OPENAQ_API_KEY=your_key_here
SKYWATCH_PROFILE=modern        # modern or lab
SKYWATCH_HTTP_TIMEOUT=20
SKYWATCH_MAX_RETRIES=5
SKYWATCH_FRESH_HOURS=3
SKYWATCH_STALE_HOURS=24
HDFS_ROOT=/skywatch
```

## 10. Existing GitHub projects: what we reuse

Ideas and patterns are reused, not whole architectures. Status shows what I confirmed in this session.

| Project | What it is | Reuse | Do not reuse |
| --- | --- | --- | --- |
| **openaq/openaq-python** (official SDK, v1.0.x on PyPI) | Low-level wrapper for the OpenAQ v3 API | Read its pagination, error classes and rate-limit handling when writing our client; optional use in the `modern` profile | As a hard dependency: it would break the Python 3.8 lab profile |
| **openaq/openaq-fetch** (OpenAQ's Node.js ingest) | How OpenAQ itself ingests and de-duplicates measurements | Concept: unique-measurement semantics, raw-concentration rule (justifies our own index calculation) | Code (Node.js, PostgreSQL) |
| **AliMarzouk/Paris-AQ** (MIT) | OpenAQ to Spark to GCS and BigQuery, Airflow, Flask dashboard | Separation of an API-to-lake job from a lake-to-warehouse job; Spark job layout; daily-extract idea | Airflow, GCP, BigQuery |
| **Winkielek/big-data-weather-analysis** | Weather plus air-pollution from two APIs into Hadoop, Hive, HBase, Spark via NiFi | Dual-API architecture and HDFS-centred layout as a viva reference | NiFi, Hive, HBase |
| **beltran-oscar/ETL-pipeline-ML** | OpenAQ ETL run hourly by GitHub Actions, DuckDB storage, ARIMA forecast | The scheduled GitHub Actions collection pattern (our FR-13) | DuckDB, forecasting |
| **AirQo pipeline** (open source, arXiv 2508.14451) | Production air-quality ETL with CI/CD and monitoring | Data-quality and observability design, as an architecture reference for the docs | Kubernetes, Kafka, Airflow |
| **Peerdeman MSc thesis** (University of Amsterdam, not a repo) | Air-quality workload implemented on MapReduce, Spark and Flink | Justification of Spark versus MapReduce in the viva | n/a |

**Unverified:** v1 listed five further repositories (Uditkumar-raj87/openaq-air-quality-monitoring-pipeline, nuthansai/openaq-data-engineering-pipeline, ajtr10/air-quality-dashboard, KAmrutha/air\_quality\_analysis\_spark, KavindaRajapaksha/AirQualityAnalysis-Big-Data-Project). I could not open them in this session. Inspect them yourself before citing or borrowing from them, and do not claim features you have not seen.

## 11. Function definitions

Style: Python 3.8-compatible type hints (`typing.List`, `Optional`), `from __future__ import annotations`, one responsibility per function, no function mixes network, cleaning, storage and rendering.

**`collector/openaq_client.py`**: `OpenAQClient(api_key, base_url, timeout, max_retries)`; `list_locations(bbox, limit) -> List[dict]`; `get_location(location_id) -> dict`; `get_location_sensors(location_id) -> List[dict]`; `get_latest(location_id) -> List[dict]`; `get_sensor_measurements(sensor_id, start, end) -> List[dict]`; `get_sensor_hours(sensor_id, start, end) -> List[dict]`; private `_request(path, params) -> dict` (retry, backoff, 429, redaction) and `_paginate(path, params) -> Iterator[dict]`.

**`collector/weather_client.py`**: `fetch_recent(lat, lon, past_days) -> dict`; `fetch_archive(lat, lon, start, end) -> dict`.

**`collector/discovery.py`**: `discover_stations(client, bbox, max_age_hours) -> List[Station]`; `write_station_config(stations, path) -> None`.

**`collector/collector.py`**: `collect_air_quality(client, stations, since) -> CollectionResult`; `collect_weather(stations) -> CollectionResult`; `backfill(stations, start, end) -> CollectionResult`; `run_collection(mode) -> RunSummary`.

**`data/validation.py`**: `validate_record(rec, rules) -> ValidationResult`; `validate_batch(records, rules) -> Tuple[List[dict], List[dict], QualityReport]`; `check_required_fields`, `check_numeric_range`, `check_unit`, `check_timestamp` (each returns a reject reason or None).

**`data/normalization.py`**: `normalize_openaq_latest(resp, station) -> List[dict]`; `normalize_openaq_hours(resp, station) -> List[dict]`; `normalize_weather(resp, location) -> List[dict]`; `to_utc_iso(ts) -> str`; `truncate_to_hour(ts) -> str`; `convert_unit(value, src, dst) -> float`.

**`data/deduplication.py`**: `make_record_key(rec) -> str`; `remove_duplicates(records) -> List[dict]`; `merge_new_records(existing_keys, incoming) -> List[dict]`.

**`analytics_core/cpcb.py`** (pure Python reference, mirrored by the Spark job and used to test it): `load_breakpoints(path) -> Breakpoints`; `subindex(parameter, concentration, breakpoints) -> Optional[float]`; `classify(index_value) -> str`; `overall_index(pm25_sub, pm10_sub) -> Optional[float]`; `is_valid_24h(valid_hours, minimum=16) -> bool`. **`analytics_core/freshness.py`**: `freshness_status(last_time, now, fresh_hours, stale_hours) -> str`.

**`storage/local_storage.py`**: `atomic_write_json(data, path)`; `atomic_write_csv(rows, path, fields)`; `read_csv(path)`; `load_watermarks(path) -> dict`; `save_watermarks(marks, path)`. **`storage/hdfs_storage.py`** (wraps the `hdfs dfs` CLI via `subprocess`): `check_connection() -> bool`; `ensure_dir(path)`; `put(local, remote, overwrite)`; `ls(path) -> List[str]`; `cat(path) -> str`; `exists(path) -> bool`; `upload_partition(local_dir, run_date)`.

**`pipeline/`**: `profile.py: load_profile() -> Profile`; `pig_runner.py: run_pig_script(script, params) -> RunResult`; `spark_runner.py: submit(job, args) -> RunResult`; `pipeline_runner.py: run_ingestion()`, `run_hdfs_upload()`, `run_pig()`, `run_spark()`, `run_export()`, `run_pipeline(offline=False) -> RunSummary`. The runner orchestrates only; it contains no analytics.

**`spark/`**: `common.py: get_spark(app_name)` (sets UTC, `ansi.enabled=false`, fixed shuffle partitions) and `read_clean(spark, path)` (explicit schema); `station_statistics.py: calculate_station_statistics(df)`, `rank_stations(stats_df)`; `trend_analysis.py: hourly_trends(df)`, `daily_trends(df)`, `peak_periods(hourly_df)`; `weather_analysis.py: join_hourly(air_df, weather_df)`, `correlations(joined_df)`; `index_job.py: rolling_24h(df)`, `compute_index(rolling_df, breakpoints)`; `alert_engine.py: generate_alerts(index_df, rules)`, `stale_alerts(stats_df)`; `quality_report.py: build_quality_report(...)`; `export.py: export_published(dfs, out_dir)`.

**`dashboard/`**: `data_access.py: load_published(dir) -> PublishedData`; `components.py: freshness_badge(status)`, `category_color(category)`, `csv_download_button(df, name)`; `app.py` plus `pages/` for the five pages. **`cli.py`**: `main(argv) -> int` routing `discover`, `collect`, `backfill`, `pipeline`, `export`, `smoke`.

**Pig scripts** (`pig/`): `clean_air_quality.pig` (load, filter, `DISTINCT`, cast, store); `station_stats.pig` (group by station, parameter and date; count, avg, min, max); `hourly_rollup.pig` (hour buckets). All take `$INPUT`, `$OUTPUT`, `$RUN_DATE`.

## 12. Project tree

```
SkyWatch/
├── README.md  LICENSE  CITATION.cff  .gitignore  .env.example  .gitleaks.toml
├── Makefile  pyproject.toml  .pre-commit-config.yaml  .editorconfig
├── requirements-pipeline.in  requirements-modern.lock  requirements-lab.lock
├── requirements-dashboard.txt  requirements-dev.txt
├── profiles/        lab.env  modern.env
├── config/          stations.json  settings.yaml  schemas.yaml  breakpoints.yaml
├── src/skywatch/
│   ├── cli.py
│   ├── collector/       openaq_client.py  weather_client.py  discovery.py  collector.py
│   ├── data/            validation.py  normalization.py  deduplication.py  schemas.py
│   ├── analytics_core/  cpcb.py  freshness.py
│   ├── storage/         local_storage.py  hdfs_storage.py
│   ├── pipeline/        profile.py  pig_runner.py  spark_runner.py  pipeline_runner.py
│   ├── spark/           common.py  station_statistics.py  trend_analysis.py
│   │                    weather_analysis.py  index_job.py  alert_engine.py
│   │                    quality_report.py  export.py
│   └── dashboard/       app.py  data_access.py  components.py  pages/
├── pig/             clean_air_quality.pig  station_stats.pig  hourly_rollup.pig
├── scripts/         use_profile.sh  install_stack.sh  collect_data.py  upload_to_hdfs.sh
│                    run_pig.sh  run_spark.sh  smoke.sh  rollback_drill.sh  make_synthetic.py
├── benchmarks/      run_scale_benchmark.py  results.md
├── tests/
│   ├── unit/  integration/  contract/  profile/  dashboard/
│   └── fixtures/        sample_openaq.json  sample_weather.json  golden/
├── data/            raw/  normalized/  sample/ (frozen demo dataset, committed)
├── output/          pig/  spark/  published/        (git-ignored except sample)
├── deploy/          streamlit_app.py  README.md
├── docs/            architecture.md  data_dictionary.md  api_documentation.md
│                    test_plan.md  setup.md  methodology.md  profiles.md
│                    AI_USAGE.md  viva_notes.md  screenshots/
└── .github/
    ├── workflows/       ci.yml  scheduled-collect.yml  publish-data.yml  release.yml  live-contract.yml
    ├── PULL_REQUEST_TEMPLATE.md  ISSUE_TEMPLATE/  CODEOWNERS  dependabot.yml
```

Every file must hold a real responsibility; do not create empty modules to match the tree.

## 13. Test cases

Markers: `unit` (no Spark, no HDFS, runs in CI), `spark_local` (CI with local Spark), `integration` (local lab only), `profile` (per stack), `live` (manual, uses the real key).

**13.1 Unit tests**

| ID | Test | Expected |
| --- | --- | --- |
| UT-01 | Valid PM2.5 record | Accepted |
| UT-02 | Missing station id / sensor id | Rejected, reason `missing_field` |
| UT-03 | Missing parameter | Rejected |
| UT-04 | Negative value | Rejected, `negative_value` |
| UT-05 | Null value | Flagged, excluded from analytics |
| UT-06 | Value above plausibility ceiling | Flagged `implausible` |
| UT-07 | Unit other than µg/m³ | Converted if known, else rejected |
| UT-08 | Duplicate record key | One kept |
| UT-09 | Same key re-ingested in a later run | Not appended twice |
| UT-10 | Invalid timestamp | Rejected |
| UT-11 | Local-time input | Correct UTC output |
| UT-12 | `truncate_to_hour` on 10:59:59 | 10:00:00 |
| UT-13 | CPCB sub-index, PM2.5 = 30 | 50 |
| UT-14 | CPCB sub-index, PM2.5 = 45.5 | 75.5 (linear interpolation, band 51 to 100) |
| UT-15 | CPCB sub-index, PM2.5 = 60 / 61 | 100 / 101 |
| UT-16 | CPCB sub-index, PM10 = 100 / 251 | 100 / 201 |
| UT-17 | Category boundaries (50, 51, 100, 101, 200, 201, 300, 301, 400, 401) | Correct category each |
| UT-18 | 15 versus 16 valid hours in a 24 h window | Invalid versus valid |
| UT-19 | Overall index with only one PM sub-index | Uses the available one, flagged `partial` |
| UT-20 | Freshness at exactly 3 h, 3 h 1 s, 24 h, 24 h 1 s | Fresh, Stale, Stale, Offline |
| UT-21 | Atomic write interrupted | Original file intact |
| UT-22 | Watermark load, save, missing file | Correct values, graceful default |
| UT-23 | Backoff schedule | Exponential with jitter, capped |
| UT-24 | Schema YAML versus Python schema | Identical field names and types |

**13.2 API tests (mocked in CI; live ones separate)**

| ID | Test | Expected |
| --- | --- | --- |
| API-01 | Valid response | Parsed and normalised |
| API-02 | Missing or wrong key (401/403) | Clear error, no retry, key not logged |
| API-03 | Unknown location (404) | Skipped and logged |
| API-04 | Timeout | Retries then logs failure; data untouched |
| API-05 | 429 with Retry-After | Waits, then succeeds |
| API-06 | Malformed JSON or unexpected shape | Graceful error |
| API-07 | Pagination over several pages | All records collected once |
| API-08 | Open-Meteo normal and archive responses | Normalised to the weather schema |
| API-LIVE-01 | Real call with a real key (manual workflow) | HTTP 200 and schema matches |

**13.3 HDFS, Pig and Spark integration (local, per profile)**

| ID | Test | Expected |
| --- | --- | --- |
| HDFS-01 to 03 | Connection; create directory; upload file | Succeeds |
| HDFS-04 | Re-upload same partition | No duplicates (idempotent) |
| HDFS-05 | List and read back | Matches local file |
| HDFS-06 | HDFS down | Logged; pipeline falls back to offline mode |
| PIG-01 | Load valid CSV | Rows loaded |
| PIG-02 | Invalid and duplicate rows | Filtered and de-duplicated |
| PIG-03 | Group, average, max, min, count | Equal to golden file |
| PIG-04 | Output readable by Spark with explicit schema | Row counts match |
| SP-01 | Load clean data | DataFrame with expected schema |
| SP-02 | Station statistics | Equal to golden |
| SP-03 | Ranking | Correct order, ties broken deterministically |
| SP-04 | Hourly and daily trends | Correct buckets |
| SP-05 | Weather join | Correct hourly matches; unmatched rows counted |
| SP-06 | Correlation | Matches a reference computed in plain Python; `n` reported |
| SP-07 | CPCB index in Spark versus `analytics_core.cpcb` | Identical to 4 decimals |
| SP-08 | Alert generation | Correct alerts and categories |
| SP-09 | ANSI off: malformed numeric string | Null, no exception, on both profiles |
| SP-10 | Output stored | Files present in HDFS and exported |

**13.4 Contract, profile, pipeline, dashboard, security, scale**

| ID | Test | Expected |
| --- | --- | --- |
| CT-01 | Pig `AS` clause matches `schemas.yaml` | Pass |
| CT-02 | Spark `StructType` matches `schemas.yaml` | Pass |
| CT-03 | Published CSV columns match dashboard expectations | Pass |
| PF-01 | `make smoke PROFILE=modern` | Pass |
| PF-02 | `make smoke PROFILE=lab` | Pass |
| PF-03 | `use_profile.sh` sets distinct `JAVA_HOME` for HDFS/Pig and Spark | Correct per tool |
| PF-04 | Rollback drill: lab output versus modern output on sample data | Equal after rounding to 4 decimals |
| PF-05 | Pipeline venv imports on Python 3.8/3.9 (lab) | No syntax errors |
| PIPE-01 | Full pipeline on sample | All stages complete, outputs present |
| PIPE-02 | API unavailable | Offline mode succeeds |
| PIPE-03 | Pig failure | Logged; later stages skipped; prior outputs untouched |
| PIPE-04 | Spark failure | Logged; prior outputs untouched |
| PIPE-05 | Partial collection (some stations fail) | Available data processed; failures reported |
| PIPE-06 | Run twice | No duplicate records |
| PIPE-07 | Same input twice | Byte-identical outputs |
| UI-01 | Dashboard loads with sample data | Pass (Streamlit `AppTest`) |
| UI-02 | No data | Empty-state message |
| UI-03 | Station and pollutant selection | Correct data |
| UI-04 | Stale and offline station | Badge and banner shown |
| UI-05 | Alert panel | Matches `alerts` output |
| UI-06 | CSV download | Content equals exported file |
| UI-07 | Attribution and methodology present on all pages | Pass |
| SEC-01 | Secret scan on repo and history | No findings |
| SEC-02 | Logs after a 401 | Key absent |
| SEC-03 | HDFS/YARN ports | Bound to localhost |
| SC-01 to 03 | Pipeline on 1×, 10×, 100× data | Runtimes recorded in `benchmarks/results.md`; outputs consistent |

## 14. Expected output

**14.1 Raw collection**: `data/raw/openaq/YYYY-MM-DD_HH-MM.json`, `data/raw/weather/YYYY-MM-DD_HH-MM.json`, plus `data/raw/_manifest.csv` (file, source, request params, HTTP status, sha256, collected\_at\_utc).

**14.2 Pig clean and statistics** (illustrative values only):

```
station_id,parameter,date,reading_count,average_value,minimum_value,maximum_value
1001,pm25,2026-11-21,24,45.7,12.4,89.2
1002,pm10,2026-11-21,23,68.4,20.1,120.5
```

**14.3 Spark station ranking**: rank, station name, parameter, 24 h average, maximum, readings, last measurement (UTC), freshness.

**14.4 Trend output**: station, parameter, hour (UTC), average, count (hourly) and the same by day; peak hour per station.

**14.5 CPCB index output**: station, computed at (UTC), PM2.5 24 h average, PM10 24 h average, sub-indices, overall PM-based index, category, valid hours, flag `partial` when only one pollutant is available.

**14.6 Alerts**: station, alert type (`category_poor`, `category_very_poor`, `category_severe`, `stale`, `offline`), parameter, category, measurement time (UTC), rule applied, generated at (UTC). Alert thresholds come from `config/settings.yaml` and the CPCB categories.

**14.7 Scale benchmark** (`benchmarks/results.md`, synthetic data clearly labelled):

| Scale | Rows | Pig clean (s) | Spark stats (s) | Spark index (s) | End-to-end (s) |
| --- | --- | --- | --- | --- | --- |
| 1× (real sample) | measured | measured | measured | measured | measured |
| 10× (synthetic replica with jittered timestamps) | measured | measured | measured | measured | measured |
| 100× (synthetic) | measured | measured | measured | measured | measured |

Report the hardware and profile used. Synthetic data is for runtime scaling only and must never appear in analytical results.

**14.8 Dashboard pages**: Overview (stations count, average PM2.5 and PM10, stale count, latest collection and observation times); Stations and Map; Analytics (trends, rankings, weather correlation with `n`); Alerts and Reports (alerts, CSV export); Methodology (CPCB method, data sources, limitations, profile and AI-usage note).

## 15. Git commit and pull request standards

**Branches**: `main` (stable, demo-ready, protected), `develop` (integration), `feature/*`, `fix/*`, `docs/*`, `test/*`, `chore/*`. Short-lived branches; merge into `develop` by squash; release by merging `develop` into `main` with a tag.

**Branch protection on `main` and `develop`**: pull request required; status checks `lint`, `unit`, `spark-local`, `secrets-scan` must pass; no force-push; linear history; CODEOWNERS review when working in a team.

**Conventional Commits**: `type(scope): description`, imperative, under 72 characters.

- Types: `feat`, `fix`, `docs`, `test`, `refactor`, `perf`, `build`, `ci`, `chore`, `revert`.
- Scopes: `collector`, `data`, `storage`, `pipeline`, `pig`, `spark`, `index`, `dashboard`, `profile`, `config`, `ci`, `docs`, `bench`.
- Examples: `feat(collector): add station discovery by bounding box`; `feat(index): implement CPCB PM sub-index`; `fix(data): reject negative pollutant values`; `feat(profile): add lab and modern stack switcher`; `test(spark): compare Spark index with reference implementation`; `ci: add local Spark test job`.
- One logical change per commit. No `update`, `fix`, `final`. No generated output, secrets, virtualenvs, large raw datasets, or screenshots of every step. The frozen sample under `data/sample/` is the only committed dataset.

**Pull request template** (`.github/PULL_REQUEST_TEMPLATE.md`): Title in Conventional Commit form; Description (what and why); Implementation (files, functions, dependencies); Testing (commands and results; profiles tested); Data impact (schema, outputs, API); Rollback impact (does it work on `lab`? evidence); Documentation (README, data dictionary, architecture, test plan); Checklist: reviewed, tests pass, Ruff clean, no secrets, docs updated, lab-compatible syntax.

**PR rules**: small and single-purpose; link an issue; CI green before merge; no unrelated changes; new dependencies justified in the description; `requirements-*.lock` regenerated by a PR of its own.

**Versioning**: semantic versions with tags `v0.1.0` (collector and storage), `v0.2.0` (Pig and Spark), `v0.3.0` (dashboard), `v1.0.0` (submission). Each tag has release notes and the sample outputs attached.

## 16. CI/CD pipeline standards

**Principles.** CI must be fast, deterministic and independent of live APIs and credentials. Hadoop is not started in CI; Spark and Pig run in local mode on the frozen sample.

**`ci.yml`** (on push and pull request):

| Job | Steps |
| --- | --- |
| `lint` | Ruff check and format check (target `py38`); mypy on core modules |
| `unit` | Python matrix 3.9, 3.11, 3.13; install locked deps; pytest `unit` and `contract`; coverage with gate at 80% on core modules; upload coverage report |
| `spark-local` | JDK 17; install PySpark 4.3.0; run Spark tests on sample (local master); compare with golden files and `analytics_core` |
| `pig-local` | JDK 11; download and cache Pig 0.18.0; run Pig scripts in `-x local` on the sample; compare with golden |
| `dashboard` | Streamlit `AppTest` against sample published data |
| `secrets-scan` | gitleaks on the diff and full history |
| `deps-check` | `pip check`; lock files consistent with `.in` files |
| `docs-check` | Markdown link check; data dictionary generated from `schemas.yaml` is up to date |

**`scheduled-collect.yml`**: cron every 30 minutes (GitHub scheduled runs can be delayed or skipped, so treat it as best-effort), reads `OPENAQ_API_KEY` from secrets, runs `skywatch collect`, commits new raw JSON and manifest to the `data` branch. The local pipeline later pulls from that branch into HDFS (watermarks prevent duplicates).

**`publish-data.yml`**: on push to `published-data` (from `make publish`), validates schema with the contract tests and triggers the dashboard redeploy.

**`live-contract.yml`**: manual dispatch only; uses the real key to confirm API shapes still match the mocks.

**`release.yml`**: on tag `v*`; builds the release notes, attaches sample outputs, and publishes a source archive.

**Also**: Dependabot for GitHub Actions and Python (grouped, weekly); pre-commit hooks mirror `lint` and `secrets-scan`; every action pinned by commit SHA; least-privilege `permissions:` in each workflow; cache keyed on lock files.

**CD scope**: continuous delivery of the dashboard and published data. The Hadoop, Pig and Spark pipeline is deployed by documented scripts (`make install-stack`, `make smoke`), not by CI, because a pseudo-distributed cluster on a student laptop is not a CI target.

## 17. Deployment platform

| Component | Where it runs | How |
| --- | --- | --- |
| Collector (continuous) | GitHub Actions scheduled workflow, plus local cron | Writes raw JSON to the `data` branch |
| Hadoop, Pig, Spark pipeline | Your Linux machine (pseudo-distributed HDFS) | `make pipeline`, profile chosen with `use_profile.sh` |
| Dashboard | **Streamlit Community Cloud** (default) | Deploys `deploy/streamlit_app.py` from the repo; reads dashboard-ready files from the `published-data` branch; no secrets required |
| Fallback dashboard host | Hugging Face Spaces (Streamlit) or Render | Same repo and data files |
| Live demo | Your Linux machine | Run the complete pipeline on the frozen sample, then show the deployed dashboard |

Rules: never expose NameNode, DataNode, YARN or Spark ports publicly; the cloud dashboard consumes exported files only; the dashboard shows the publication timestamp and the data's latest observation time; a `DEMO_MODE` banner appears when the sample dataset is being served.

## 18. Authorship, honesty and viva readiness

- `docs/AI_USAGE.md` states which parts were written with AI assistance, what was verified manually, and what each team member can explain. Present the work as it is: AI-assisted engineering that you understand and can defend.
- `docs/viva_notes.md`: for every module, a half-page explanation of what it does, why it exists and how to demonstrate it. Must be able to answer: why Pig for ETL and Spark for analytics; what Pig compiles to; why HDFS partitions by date; how the CPCB index is computed; why 24 h averaging; what the profile system does and how rollback is proven; what the scale benchmark does and does not show.
- Be able to run `make smoke` and the rollback drill live, and to change one threshold in `settings.yaml` and show the alert change.

## 19. Pre-freeze verification checklist (do these before coding)

1. Confirm with the lecturer which stack is required for evaluation (`lab` or `modern`), and what the lab machine has installed.
2. Run `discover_stations` and confirm 5 or more Bengaluru stations with readings in the last 24 hours.
3. Confirm the CPCB breakpoint table (PM2.5, PM10, sub-index bands, 16-hour validity rule) against the official CPCB technical note and record the source in `docs/methodology.md`.
4. Confirm your OpenAQ account's actual rate limits and the exact path of the OpenAQ historical archive before writing the backfill.
5. Run `make smoke` on `modern` (Hadoop 3.4.3, Pig 0.18.0, Spark 4.3.0). If Pig fails on JDK 11 with Hadoop 3.4.3, fall back to Hadoop 3.3.6 for HDFS and Pig. If everything passes on Hadoop 3.5.0 with JDK 17, you may promote `modern-max`.
6. Run `make smoke` on `lab` (Hadoop 3.2.2, JDK 8, Spark 3.2.2, Python 3.8 or 3.9) and complete one rollback drill.
7. Freeze `data/sample/` only after at least several days of real collection and backfill.
