# Phase 3: Analytics Engine with Apache Spark & Scale Benchmarking - Research

**Researched:** 2026-11-22  
**Domain:** Apache Spark Analytics, CPCB PM-based AQI, Weather Correlation, Alert Engine, Scale Benchmarking  
**Confidence:** HIGH  

---

## Summary

Phase 3 builds the distributed analytics engine of SkyWatch, transitioning from Phase 2's clean batch datasets on Hadoop HDFS to high-level environmental intelligence, temporal trend mining, meteorological correlations, official health index calculations, telemetry alerting, and synthetic scale benchmarking.

The analytics engine is designed around Apache Spark (PySpark DataFrame API) with a deterministic, low-memory local configuration tailored for developer laptops (<= 4 GB total RAM budget) and CI environments. Crucially, following the architectural standard established in Phase 2's `pig_runner.py`, Phase 3 implements a dual-mode execution strategy: when a JVM and Apache Spark runtime are present, native Spark jobs execute across `modern` (Spark 3.5/4.x on Java 17) or `lab` (Spark 3.2 on Java 8) profiles; when offline, in CI, or on developer environments lacking JVM/winutils (such as native Windows), a pure-Python simulation fallback engine in `src/skywatch/pipeline/spark_runner.py` executes identical relational and statistical logic.

The core analytical capabilities deliver:
1. **Station Statistics & Rankings (SPK-01)**: Pollution averages, minimums, maximums, readings count, and pollution rankings identifying cleanest and most polluted hotspots.
2. **Temporal Trend Analysis (SPK-02)**: Regularized hourly and daily pollution profiles and peak-hour detection per station.
3. **Meteorological Correlation (SPK-03)**: Hourly join with Open-Meteo weather metrics (temperature, humidity, wind speed, precipitation) and Pearson correlation coefficients $r$ tagged with sample count $n$ and statistical confidence tiers (`High`, `Medium`, `Low`).
4. **CPCB 24-Hour Rolling PM-based AQI (SPK-04)**: Rigorous piecewise linear sub-index interpolation for PM2.5 and PM10 across official Central Pollution Control Board (CPCB) breakpoint bands, enforcing the $\ge 16$ valid hours threshold in a 24-hour trailing window, with explicit suppression status records (`INSUFFICIENT_VALID_HOURS`, `MISSING_POLLUTANT`) when data is invalid, computing max sub-index and dominant pollutant.
5. **Freshness & Alert Engine (SPK-05, SPK-06, SPK-08)**: Categorization of station freshness (`Fresh` $\le 3$h, `Stale` 3–24h, `Offline` $> 24$h), health threshold alerts (`Poor`, `Very Poor`, `Severe`), pipeline telemetry alerts, and rule-based rolling median spike detection ($reading > 3 \times rolling\_median$).
6. **Data Quality Auditing (SPK-07)**: Aggregate tracking of accepted, rejected, and duplicate records across pipeline stages categorized by reason.
7. **Export Publisher (SPK-09)**: Atomic staging directory swap protocol to `output/published/{dataset}/` writing dual formats (Parquet and CSV) with sidecar `_schema.json` metadata contracts.
8. **Synthetic Scale Benchmarking (SPK-10)**: Generation of realistic, clearly labelled synthetic scale datasets at 1× (50k rows), 10× (500k rows), and 100× (5M rows) via `scripts/make_synthetic.py` and benchmark execution measuring throughput, latency, and memory via `benchmarks/run_scale_benchmark.py`.

---

## User Constraints (if any)

From `AGENTS.md`, `GEMINI.md`, and project documentation:
1. **Git Branching & CI/CD Protocol (Mandatory Rule)**: Commit all changes to a new branch (never commit working code directly to `main`). Merge into `main` only after CI/CD pipeline and test suite pass completely [VERIFIED: `AGENTS.md` §3].
2. **Graphicle Static Analysis & Graph Navigation**: Synchronize and inspect repository symbols using Graphicle before and after code changes [VERIFIED: `AGENTS.md` §2].
3. **Python 3.8 Syntax Floor**: All Python modules must parse and execute under Python 3.8 syntax rules. Use `from __future__ import annotations`, `typing.Optional`, `typing.Dict`, `typing.List`; do NOT use Python 3.10+ match statements or `X | Y` union syntax [VERIFIED: `AGENTS.md` §4, `SkyWatch Spec v2.0` §3].
4. **Dual-Profile Compatibility**: Code must run seamlessly across `modern` (Spark 3.5/4.x on JDK 17) and `lab` (Spark 3.2.2 on JDK 8) without hardcoding versions. Spark code must stick to DataFrame APIs available in Spark 3.2 (no Arrow-only UDFs, no declarative pipelines, no `ASOF JOIN`) [VERIFIED: `SkyWatch Spec v2.0` §4].
5. **Memory & Performance Ceiling**: In constrained local environments ($\le 4$ GB total RAM), Spark driver is limited to 2 GB (`spark.driver.memory=2g`), shuffle partitions fixed to 2–4 (`spark.sql.shuffle.partitions=2` to prevent OOM from 200 default shuffle tasks), and ANSI mode disabled (`spark.sql.ansi.enabled=false`) to ensure malformed casts return null rather than throw runtime exceptions [VERIFIED: `SkyWatch Spec v2.0` §3, §7 NFR-03, NFR-15].
6. **Explicit Schema Enforcement**: Spark readers must never use `inferSchema=true` on headerless Pig clean CSV files; schemas must be explicitly loaded from `config/schemas.yaml` or sidecar `_schema.json` [VERIFIED: `SkyWatch Spec v2.0` §3 Bottleneck 9, §8].
7. **Honest Health Methodology**: Index must be explicitly labeled as a **PM-based CPCB index** (never as a full 8-pollutant AQI, as gaseous pollutants SO2, NO2, CO, O3 are not monitored). Do not imply forecasting, causation, or live readings [VERIFIED: `AGENTS.md` §4, `SkyWatch Spec v2.0` §2, §6 FR-23].
8. **Synthetic Data Isolation**: Synthetic scale benchmark datasets are strictly for performance evaluation and must never be mixed into real analytical results [VERIFIED: `SkyWatch Spec v2.0` §14.7].

---

## Phase Requirements (SPK-01 to SPK-10)

| Requirement ID | Specification Anchor | Description | Success Criteria |
| :--- | :--- | :--- | :--- |
| **SPK-01** | FR-20, NFR-03 | Calculate station summary statistics, pollution rankings, and identify cleanest vs most polluted hotspots. | Computes 24h average, min, max, reading count per station and parameter; assigns deterministic rank ordered by average pollution. |
| **SPK-02** | FR-21, NFR-04 | Analyze hourly and daily trends, identifying peak pollution hours for each station. | Groups measurements by UTC hour-of-day and day-of-week; computes average and peak hour per station. |
| **SPK-03** | FR-22, NFR-03 | Join air quality with weather metrics on station hour and calculate Pearson correlation coefficients with sample size $n$ and confidence tier. | Joins on truncated UTC hour; calculates Pearson $r$ for temperature, humidity, wind, and precipitation; tags confidence as `High` ($n \ge 100$), `Medium` ($30 \le n < 100$), or `Low` ($n < 30$). |
| **SPK-04** | FR-23, NFR-02 | Calculate 24-hour rolling CPCB PM-based National AQI from regularized hourly means using linear interpolation and $\ge 16$ valid hours rule, emitting explicit suppression reasons when invalid. | Piecewise linear sub-index interpolation for PM2.5 and PM10; validates $\ge 16$ hours in trailing 24h window; emits `INSUFFICIENT_VALID_HOURS` when $<16$; overall AQI = max sub-index; tags dominant pollutant and partial status. |
| **SPK-05** | FR-24, NFR-01 | Generate health alerts for Poor/Severe categories, and separate pipeline telemetry alerts for offline stations and stale readings. | Produces alert records for CPCB categories `Poor` (201-300), `Very Poor` (301-400), and `Severe` (401-500); emits separate telemetry alerts for stale (3-24h) and offline (>24h) sensors. |
| **SPK-06** | FR-25, NFR-13 | Classify station freshness into Fresh ($\le 3$h), Stale (3–24h), and Offline ($> 24$h). | Evaluates time delta between last measurement and execution/reference timestamp; assigns deterministic freshness badge. |
| **SPK-07** | FR-26, NFR-12 | Generate data quality report tracking accepted, rejected, and duplicate counts categorized by reason. | Emits audit summary record tracking record counts and reject reasons across Ingestion, Pig ETL, and Spark analytics stages. |
| **SPK-08** | FR-27, NFR-03 | Compute rule-based rolling median spike detection flags. | Evaluates trailing 24h rolling median; flags reading as spike if $value > 3.0 \times median$; no ML/ARIMA models used. |
| **SPK-09** | FR-28, NFR-01 | Export dashboard-ready Parquet and CSV datasets to `output/published/` via atomic staging directory swaps. | Writes Parquet and CSV outputs to `.staging_{dataset}_{run_id}/`, validates completeness, then atomically swaps into `output/published/{dataset}/` with sidecar `_schema.json`. |
| **SPK-10** | FR-35, NFR-08 | Synthetic scale benchmarking suite testing 1× (50k rows), 10× (500k rows), and 100× (5M rows) volume performance with local and report modes. | `make_synthetic.py` generates 1×, 10×, 100× datasets; `run_scale_benchmark.py` measures throughput (rows/s), peak memory (MB), and duration (s), producing `benchmarks/results.md`. |

---

## Architectural Responsibility Map

```
+---------------------------------------------------------------------------------------------------+
|                                      SkyWatch Architecture                                        |
+---------------------------------------------------------------------------------------------------+
|  Phase 1 & Phase 2: Ingestion & Pig ETL                                                           |
|  - Raw / Normalized -> Pig Clean HDFS Partition (/clean/air_quality/event_dt=YYYY-MM-DD/)         |
|  - Hourly Rollup HDFS Partition (/clean/hourly_rollup/event_dt=YYYY-MM-DD/)                       |
|  - Normalized Weather (/normalized/weather/ingest_dt=YYYY-MM-DD/)                                 |
+---------------------------------------------------------------------------------------------------+
                                                  |
                                                  v
+---------------------------------------------------------------------------------------------------+
|  Phase 3: Analytics Engine & Scale Benchmarking                                                   |
|                                                                                                   |
|  +---------------------------------------------------------------------------------------------+  |
|  |  spark_runner.py (Orchestrator)                                                             |  |
|  |  - Profile environment detection ('modern' Spark 3.5/Java 17 vs 'lab' Spark 3.2/Java 8)    |  |
|  |  - PySpark session management / Pure-Python standalone fallback engine                      |  |
|  |  - Memory & shuffle guard (partitions=2, ansi=false, driver=2g, timezone=UTC)               |  |
|  +---------------------------------------------------------------------------------------------+  |
|          |                                                                                        |
|          v                                                                                        v
|  +----------------------------------------------------+   +------------------------------------+  |
|  |  PySpark Execution Engine (src/skywatch/spark/)    |   |  Pure-Python Fallback Engine       |  |
|  |                                                    |   |  (src/skywatch/analytics_core/)    |  |
|  |  1. common.py: get_spark(), read_clean_dataset()   |   |  1. cpcb.py:                       |  |
|  |  2. station_statistics.py: Stats & Ranking (SPK-01)|   |     - Piecewise sub-index math     |  |
|  |  3. trend_analysis.py: Hourly & Daily Trends (SPK-02)  - 16h valid hours rule       |  |
|  |  4. weather_analysis.py: Join & Pearson r (SPK-03) |   |     - Category classification      |  |
|  |  5. index_job.py: 24h Rolling CPCB AQI (SPK-04)    |   |  2. freshness.py:                  |  |
|  |  6. alert_engine.py: Health & Spike Alerts (SPK-05,|   |     - Fresh, Stale, Offline badges |  |
|  |                      SPK-06, SPK-08)               |   |  3. standalone analytics fallback  |  |
|  |  7. quality_report.py: Stage Audit Counts (SPK-07) |   |     for zero-JVM environments      |  |
|  |  8. export.py: Atomic Directory Swap (SPK-09)      |   +------------------------------------+  |
|  +----------------------------------------------------+                                           |
|          |                                                                                        |
|          +------------------------------------+---------------------------------------------------+
|                                               |
|                                               v
|  +---------------------------------------------------------------------------------------------+  |
|  |  Published Staging & Swap Publisher (output/published/)                                    |  |
|  |  - station_statistics/  (Parquet + CSV + _schema.json)                                      |  |
|  |  - trends_hourly/       (Parquet + CSV + _schema.json)                                      |  |
|  |  - trends_daily/        (Parquet + CSV + _schema.json)                                      |  |
|  |  - weather_correlation/ (Parquet + CSV + _schema.json)                                      |  |
|  |  - cpcb_index/          (Parquet + CSV + _schema.json)                                      |  |
|  |  - alerts/              (Parquet + CSV + _schema.json)                                      |  |
|  |  - quality_report/      (Parquet + CSV + _schema.json)                                      |  |
|  +---------------------------------------------------------------------------------------------+  |
|                                                                                                   |
|  +---------------------------------------------------------------------------------------------+  |
|  |  Synthetic Scale Benchmarking Suite (SPK-10)                                                |  |
|  |  - scripts/make_synthetic.py: Generates 1x (50k), 10x (500k), 100x (5M) rows                 |  |
|  |  - benchmarks/run_scale_benchmark.py: Measures latency, throughput, RAM -> results.md      |  |
|  +---------------------------------------------------------------------------------------------+  |
+---------------------------------------------------------------------------------------------------+
                                                  |
                                                  v
+---------------------------------------------------------------------------------------------------+
|  Phase 4: Multi-page Streamlit Dashboard & Unified CLI Integration                                |
+---------------------------------------------------------------------------------------------------+
```

---

## Standard Stack

### Core Components

| Component | Standard / Selected Version | Purpose in Phase 3 | Justification / Provenance |
| :--- | :--- | :--- | :--- |
| **Apache Spark (PySpark)** | `3.2.2` (`lab`) / `3.5.x` or `4.x` (`modern`) | Distributed analytical processing, windowing, correlation | Standard big data compute engine; provides windowed aggregation, join, and mathematical functions [VERIFIED: Spec §4, §5]. |
| **Java Runtime** | OpenJDK 8 (`lab`) / OpenJDK 17 (`modern`) | Spark execution runtime | Spark 3.2 runs on Java 8; Spark 3.5/4.x requires Java 17; isolated via `JAVA_HOME_SPARK` [VERIFIED: Spec §4]. |
| **Python Standard Library** | `>=3.8` (`math`, `statistics`, `datetime`, `json`, `shutil`) | Standalone fallback engine & atomic file staging | Zero-dependency mathematical reference and filesystem operations [VERIFIED: `AGENTS.md` §4]. |
| **PyArrow** | `>=14.0.0` (Dashboard & pipeline venv) | High-performance Parquet serialization | Columnar storage for published analytical datasets consumed by dashboard [VERIFIED: Spec §5]. |
| **PyYAML** | `6.0.x` | Schema contract & breakpoints parsing | Loads `config/schemas.yaml` and `config/breakpoints.yaml` [VERIFIED: `schemas.py`]. |

### Alternatives Considered & Rejected

| Candidate | Why Considered | Why Rejected |
| :--- | :--- | :--- |
| **DuckDB** | Ultra-fast in-process SQL OLAP engine | Violates academic brief requiring demonstration of Apache Spark distributed computing; lacks multi-node scalability demonstration [VERIFIED: Spec §2, §10]. |
| **Pandas-only Analytics** | Trivial implementation | Memory-bound; fails at 100× scale (5M rows) on constrained 4GB RAM machines; fails Big Data curriculum requirements [VERIFIED: Spec §2]. |
| **Spark Streaming (Structured Streaming)** | Real-time sensor processing | Unnecessary complexity for 30–60 minute batch polling frequency; hourly batch processing is simpler and more robust [VERIFIED: Spec §6 Out of Scope]. |
| **Machine Learning (ARIMA/LSTM)** | Predictive forecasting of AQI | Out of scope for v1; violates honest non-claim rule (SkyWatch provides civic intelligence on actual observations, not unverified forecasts) [VERIFIED: Spec §2, §6 Out of Scope]. |

---

## Package Legitimacy Audit

| Package | Minimum Version | Locked Version | License | Security / Vulnerability Status |
| :--- | :--- | :--- | :--- | :--- |
| `pyspark` | `>=3.2.0` | `3.2.2` (lab) / `3.5.1`+ (modern) | Apache-2.0 | Clean; core distributed analytics library [VERIFIED]. |
| `pyarrow` | `>=14.0.0` | `26.0.1` | Apache-2.0 | Clean; standard Apache Arrow columnar memory and Parquet engine [VERIFIED]. |
| `pyyaml` | `>=6.0` | `6.0.4` | MIT | Clean; parses configuration using safe loader `yaml.safe_load()` [VERIFIED]. |
| `pytest` | `>=7.0.0` | `9.2.0` | MIT | Clean; test runner for unit, contract, and integration suites [VERIFIED]. |
| `psutil` | `>=5.9.0` | `5.9.8` | BSD-3-Clause | Clean; lightweight process memory and CPU tracking for scale benchmarks [VERIFIED]. |

---

## Architecture Patterns

### Recommended File Structure

```
SkyWatch/
├── config/
│   ├── schemas.yaml                       # Updated with schemas for analytics outputs
│   ├── breakpoints.yaml                   # Official CPCB sub-index breakpoint bands (PM2.5, PM10)
│   └── settings.yaml                      # Alert thresholds, freshness limits, spike multipliers
├── src/skywatch/
│   ├── analytics_core/                    # Pure-Python mathematical and reference modules
│   │   ├── __init__.py
│   │   ├── cpcb.py                        # CPCB sub-index linear interpolation, 16h rule, categories
│   │   └── freshness.py                   # Fresh (<=3h), Stale (3-24h), Offline (>24h) classification
│   ├── spark/                             # Apache Spark DataFrame analytics jobs
│   │   ├── __init__.py
│   │   ├── common.py                      # get_spark(), explicit schema builder, read_clean_dataset()
│   │   ├── station_statistics.py          # Summary statistics & ranking (SPK-01)
│   │   ├── trend_analysis.py              # Hourly & daily trends, peak pollution hours (SPK-02)
│   │   ├── weather_analysis.py            # Hourly weather join & Pearson r with confidence tiers (SPK-03)
│   │   ├── index_job.py                   # 24h rolling CPCB AQI calculation & suppression status (SPK-04)
│   │   ├── alert_engine.py                # Health alerts, telemetry alerts, rolling median spikes (SPK-05, SPK-08)
│   │   ├── quality_report.py              # Cross-stage data quality audit summary (SPK-07)
│   │   └── export.py                      # Atomic directory staging swap to output/published/ (SPK-09)
│   └── pipeline/
│       └── spark_runner.py                # Spark job orchestrator + pure-Python simulation fallback engine
├── scripts/
│   ├── make_synthetic.py                 # Generates 1x (50k), 10x (500k), 100x (5M) benchmark datasets (SPK-10)
│   └── run_spark.sh                       # Profile-aware CLI wrapper script for Spark analytics
├── benchmarks/
│   ├── run_scale_benchmark.py             # Benchmark runner measuring latency, throughput, RAM (SPK-10)
│   └── results.md                         # Benchmark metrics report table
├── tests/
│   ├── unit/
│   │   ├── test_cpcb.py                   # Tests CPCB math, breakpoints, boundary cases, 16h rule
│   │   ├── test_freshness.py              # Tests freshness thresholds (3h, 3h 1s, 24h, 24h 1s)
│   │   ├── test_alert_engine.py           # Tests health alerts, telemetry alerts, spike detection
│   │   ├── test_weather_correlation.py    # Tests Pearson r math and confidence tiers
│   │   ├── test_export_publisher.py       # Tests atomic staging directory swap & sidecar schema
│   │   └── test_quality_report.py         # Tests quality audit report aggregation
│   ├── contract/
│   │   └── test_spark_contracts.py        # CT-02: Asserts Spark StructType matches schemas.yaml
│   └── integration/
│       ├── test_spark_analytics.py        # SP-01 to SP-10: End-to-end Spark analytics vs golden data
│       └── test_scale_benchmark.py        # Tests synthetic generator and benchmark execution
└── tests/fixtures/golden/
    ├── golden_station_statistics.csv
    ├── golden_trends_hourly.csv
    ├── golden_trends_daily.csv
    ├── golden_weather_correlation.csv
    ├── golden_cpcb_index.csv
    ├── golden_alerts.csv
    └── golden_quality_report.csv
```

---

## Detailed Technical Investigations

### 1. PySpark Setup & Local Session Creation Across Dual Profiles

To satisfy NFR-03 (reproducibility), NFR-15 (resource limits: Spark driver 2 GB, Hadoop heap 1 GB), and spec requirements:
- **Session Configuration (`spark/common.py:get_spark`)**:
  ```python
  spark = (
      SparkSession.builder
      .appName(app_name)
      .master("local[2]")
      .config("spark.driver.memory", "2g")
      .config("spark.sql.shuffle.partitions", "2")  # 2 partitions for local mode to prevent OOM
      .config("spark.sql.session.timeZone", "UTC")
      .config("spark.sql.ansi.enabled", "false")   # Malformed casts return null, no runtime exception
      .config("spark.ui.enabled", "false")          # Disable web UI in tests to prevent port collisions
      .getOrCreate()
  )
  ```
- **Why `spark.sql.shuffle.partitions = 2`?** Default PySpark uses 200 partitions for every join and groupby. On local execution with 4GB RAM, 200 tasks create massive task scheduling overhead, garbage collection churn, and heap exhaustion. Setting it to 2 (or up to 4 for synthetic scale testing) provides optimal parallelism while keeping memory consumption well below 1.5 GB [VERIFIED: Spec §3 Bottleneck 3, §7 NFR-03, NFR-15].
- **Why `spark.sql.ansi.enabled = false`?** Spark 4 and recent Spark 3.x have ANSI mode enabled by default, which causes queries to fail immediately with an exception upon encountering bad data casts (e.g. non-numeric strings). Setting ANSI to false ensures malformed entries are coerced to `null`, matching the lab profile behavior and allowing data hygiene filtering without job termination [VERIFIED: Spec §3 Bottleneck 3].
- **Explicit Schema Enforcement**: Pig clean data outputs (`part-m-00000`) are headerless comma-delimited CSVs. Downstream PySpark code reads these files using an explicit `StructType` constructed directly from `config/schemas.yaml` or sidecar `_schema.json`, completely prohibiting `inferSchema=true` [VERIFIED: Spec §3 Bottleneck 9, §8].

### 2. Pure-Python Simulation Fallback Engine for Offline / Zero-JVM Environments

Similar to `pig_runner.py`'s `_run_standalone_fallback()`, the Spark runner (`src/skywatch/pipeline/spark_runner.py`) provides seamless execution when:
- Native PySpark is not installed in the current Python environment.
- The JVM is unavailable or lacks valid Java configuration (`JAVA_HOME_SPARK`).
- The pipeline executes on native Windows without `winutils.exe` in Hadoop binaries.
- The `--offline` or `--simulate` flag is explicitly requested.

The fallback engine uses Python's standard library and lightweight numeric operations to execute the identical relational pipelines:
- Aggregates daily and hourly averages using exact decimal math.
- Calculates 24h rolling CPCB sub-indices using `src/skywatch/analytics_core/cpcb.py`.
- Computes Pearson correlation $r$ using the standard mathematical formula.
- Evaluates rolling median spikes and assigns freshness badges using `src/skywatch/analytics_core/freshness.py`.
- Writes dual CSV and Parquet outputs (using `pyarrow` or CSV fallback) into staging directories and executes atomic directory renames.
- Guarantees numerical equivalence within $\epsilon < 10^{-4}$ against native Spark outputs.

### 3. CPCB 24-Hour Rolling PM-Based AQI Calculation

#### A. Breakpoint Bands (`config/breakpoints.yaml`)
Derived from the official Central Pollution Control Board (CPCB) "National Air Quality Index" publication (MoEFCC, Govt of India):

| Category | AQI / Sub-Index Range ($I_{low} - I_{high}$) | PM2.5 24h Avg ($B_{low} - B_{high}$) | PM10 24h Avg ($B_{low} - B_{high}$) |
| :--- | :--- | :--- | :--- |
| **Good** | 0 – 50 | 0.0 – 30.0 | 0.0 – 50.0 |
| **Satisfactory** | 51 – 100 | 31.0 – 60.0 | 51.0 – 100.0 |
| **Moderate** | 101 – 200 | 61.0 – 90.0 | 101.0 – 250.0 |
| **Poor** | 201 – 300 | 91.0 – 120.0 | 251.0 – 350.0 |
| **Very Poor** | 301 – 400 | 121.0 – 250.0 | 351.0 – 430.0 |
| **Severe** | 401 – 500 | 251.0 – 500.0 | 431.0 – 600.0 |

#### B. Piecewise Linear Interpolation Formula
For a 24-hour average concentration $C_p$ falling within breakpoint concentration interval $[B_{low}, B_{high}]$, the sub-index $I_p$ is:
$$I_p = I_{low} + \frac{I_{high} - I_{low}}{B_{high} - B_{low}} \times (C_p - B_{low})$$

*Arithmetic Verification:*
- For PM2.5 = 30.0: Band 1 ($B_{low}=0, B_{high}=30, I_{low}=0, I_{high}=50$). $I = 0 + \frac{50}{30} \times 30 = 50.0$ (`Good`).
- For PM2.5 = 45.5: Band 2 ($B_{low}=31, B_{high}=60, I_{low}=51, I_{high}=100$). $I = 51 + \frac{100 - 51}{60 - 31} \times (45.5 - 31) = 51 + \frac{49}{29} \times 14.5 = 51 + 24.5 = 75.5$ (`Satisfactory`). Matches test UT-14 [VERIFIED: Spec §13.1 UT-14].
- For PM2.5 = 60.0: Band 2. $I = 51 + \frac{49}{29} \times 29 = 100.0$ (`Satisfactory`).
- For PM2.5 = 61.0: Band 3 ($B_{low}=61, B_{high}=90, I_{low}=101, I_{high}=200$). $I = 101 + \frac{99}{29} \times (61 - 61) = 101.0$ (`Moderate`). Matches test UT-15 [VERIFIED: Spec §13.1 UT-15].
- For PM10 = 100.0: Band 2 ($B_{low}=51, B_{high}=100, I_{low}=51, I_{high}=100$). $I = 100.0$ (`Satisfactory`). Matches test UT-16.
- For PM10 = 251.0: Band 4 ($B_{low}=251, B_{high}=350, I_{low}=201, I_{high}=300$). $I = 201.0$ (`Poor`). Matches test UT-16.

#### C. 24-Hour Rolling Window & 16-Hour Validity Rule
- Window definition: Trailing 24 regularized hourly intervals $[t - 23\text{h}, t]$.
- **Validity Gate**: If $valid\_hours \ge 16$, the 24h mean concentration is computed:
  $$\bar{C}_{24h} = \frac{1}{valid\_hours} \sum_{i=1}^{valid\_hours} C_i$$
- **Suppression Status**: If $valid\_hours < 16$ (e.g. 15 valid hours in test UT-18):
  - $I_p$ is `null`.
  - Category is `null`.
  - `suppression_reason` is set to `"INSUFFICIENT_VALID_HOURS"`.
  - Record is logged and emitted to quality audit, preventing corrupted indices from reaching user dashboards [VERIFIED: Spec §6 FR-23, §13.1 UT-18].

#### D. Overall Index & Dominant Pollutant
- For each station and timestamp:
  - If both PM2.5 and PM10 sub-indices are valid:
    $$AQI = \max(I_{PM2.5}, I_{PM10})$$
    $dominant\_pollutant = \text{"pm25" if } I_{PM2.5} \ge I_{PM10} \text{ else "pm10"}$.
    $is\_partial = \text{False}$.
  - If only one pollutant sub-index is valid:
    $AQI$ is set to the available sub-index.
    $is\_partial = \text{True}$ (Test UT-19).
  - If neither is valid: $AQI = \text{null}$, $suppression\_reason = \text{"INSUFFICIENT_VALID_HOURS"}$.
- **Attribution & Naming**: The output must be explicitly titled **"PM-based CPCB Index"** across all outputs and dashboard interfaces.

### 4. Hourly Weather Join & Pearson Correlation

- **Join Semantics**:
  - Air quality input: Regularized hourly aggregates from `hourly_rollup` (`station_id`, `parameter`, `bucket_start_utc`, `average_value`).
  - Weather input: Open-Meteo hourly observations (`location_id`, `measurement_time_utc`, `temperature_c`, `humidity_pct`, `wind_speed`, `precipitation_mm`).
  - Join Key: Truncated UTC hour timestamp (`bucket_start_utc == measurement_time_utc`). When weather is captured at Bengaluru city centroid (`bengaluru_central`), weather features are joined to all station records matching that UTC hour.
- **Pearson Correlation Coefficient $r$**:
  Between pollutant concentration $X$ and meteorological variable $Y$:
  $$r = \frac{\sum_{i=1}^n (x_i - \bar{x})(y_i - \bar{y})}{\sqrt{\sum_{i=1}^n (x_i - \bar{x})^2 \sum_{i=1}^n (y_i - \bar{y})^2}}$$
  In Spark: `pyspark.sql.functions.corr(col("average_value"), col(weather_var))`.
- **Sample Count & Confidence Tiers**:
  Every correlation calculation reports sample count $n$ (count of valid paired observations):
  - **High Confidence**: $n \ge 100$ (robust statistical correlation)
  - **Medium Confidence**: $30 \le n < 100$ (indicative correlation)
  - **Low Confidence**: $n < 30$ (sample size too small for statistical certainty)
  If $n < 2$ or variance is zero, $r = \text{null}$ with confidence tier `Low` [VERIFIED: Spec §6 FR-22].

### 5. Freshness & Alert Engine

#### A. Freshness Classification (`analytics_core/freshness.py`)
Calculated from observation timestamp $t_{obs}$ relative to current processing timestamp $t_{ref}$:
$$\Delta t_{hours} = \frac{t_{ref} - t_{obs}}{3600}$$
- **Fresh**: $\Delta t_{hours} \le 3.0$
- **Stale**: $3.0 < \Delta t_{hours} \le 24.0$
- **Offline**: $\Delta t_{hours} > 24.0$
*Boundary Verification (Test UT-20):*
- Exactly 3h (10800s) -> `Fresh`
- 3h 1s (10801s) -> `Stale`
- Exactly 24h (86400s) -> `Stale`
- 24h 1s (86401s) -> `Offline` [VERIFIED: Spec §13.1 UT-20].

#### B. Health Alerts
Generated when the calculated station PM-based AQI exceeds safe thresholds:
- **Poor**: $201 \le AQI \le 300$ -> `alert_type="category_poor"`, `severity="WARNING"`
- **Very Poor**: $301 \le AQI \le 400$ -> `alert_type="category_very_poor"`, `severity="CRITICAL"`
- **Severe**: $401 \le AQI \le 500$ -> `alert_type="category_severe"`, `severity="EMERGENCY"`

#### C. Pipeline Telemetry Alerts
Separate operational alerts flagging sensor infrastructure health:
- `stale_station`: Station readings between 3h and 24h old.
- `offline_station`: Station has no readings for $> 24$h.
- `missing_data_spike`: Sensor observation count drops by $>50\%$ in a trailing window.

#### D. Rule-Based Rolling Median Spike Detection
- Evaluates trailing 24h rolling median of hourly concentration.
- If $value > 3.0 \times \text{rolling\_median}$ (and $\text{rolling\_median} > 0$):
  - Flag: `is_spike = True`
  - Tag: `spike_ratio = round(value / rolling_median, 2)`
  - Alert type: `spike_detected`
  - Strictly rule-based, no ML/ARIMA [VERIFIED: Spec §6 FR-27, SPK-08].

### 6. Data Quality Reporting

Audits record counts across stages and aggregates reject reasons:
- **Ingestion Stage**: Total collected, valid accepted, invalid rejected, duplicates dropped.
- **Pig Clean Stage**: Header rows stripped, invalid rows filtered, safety `DISTINCT` duplicates removed.
- **Spark Analytics Stage**: Unmatched weather join rows, suppressed AQI windows ($valid\_hours < 16$).
- Output schema: `run_id`, `stage`, `accepted_count`, `rejected_count`, `duplicate_count`, `reason_counts_json`, `generated_at_utc`.

### 7. Dashboard Export Publisher

To ensure Streamlit dashboards never read partially written or corrupted data:
- **Atomic Staging Directory Swap Protocol**:
  1. Create a staging directory `output/published/.staging_{dataset}_{run_id}/`.
  2. Write complete **Parquet** (`part-*.parquet`) and **CSV** (`part-*.csv`) files.
  3. Generate and write sidecar metadata `_schema.json` containing column types, descriptions, nullability, and dataset version.
  4. Perform atomic directory swap:
     - On POSIX / Linux: Atomic rename `os.replace(staging_dir, target_dir)`.
     - On Windows: If target exists, backup to `.old_{run_id}`, rename staging to target, and purge backup.
  5. If any exception occurs during writing, purge staging directory and leave existing target directory untouched [VERIFIED: Spec §6 FR-28, §7 NFR-01].

### 8. Synthetic Scale Benchmarking Suite

- **`scripts/make_synthetic.py`**:
  - Generates realistic datasets at 1× (50k rows), 10× (500k rows), and 100× (5M rows).
  - Uses Bengaluru monitoring stations (`config/stations.json`) as baseline coordinates.
  - Applies realistic diurnal air-quality curves: morning rush-hour peak (08:00–10:00 UTC), evening peak (19:00–22:00 UTC), lower midday dispersion.
  - Injects weather covariance: higher PM concentrations when temperature is lower and wind speed is low (inversion conditions).
  - Enforces strict synthetic tagging: `record_key` prefixed with `syn_`, `source="synthetic"`, stored in `data/synthetic/scale_{scale}/`.
  - **Isolation**: Synthetic data is never copied to production HDFS partitions and never displayed in analytical dashboard views [VERIFIED: Spec §14.7].
- **`benchmarks/run_scale_benchmark.py`**:
  - Runs Pig clean, Spark stats, and Spark CPCB index across 1×, 10×, and 100× datasets.
  - Captures execution time (s), processing throughput (rows/sec), and peak resident memory (MB) using `psutil`.
  - Produces formatted Markdown results table in `benchmarks/results.md` matching Spec §14.7.

---

## Don't Hand-Roll

| Problem | Don't Hand-Roll | Use Instead | Why |
| :--- | :--- | :--- | :--- |
| **Windowed Rolling Aggregation** | Custom nested looping over timestamps | PySpark `Window.partitionBy().orderBy().rangeBetween()` | Distributed C++ / Java execution handles partitioned time-series ordering without memory overflow [VERIFIED: PySpark docs]. |
| **Pearson Correlation** | Hand-rolled matrix covariance loops | PySpark `pyspark.sql.functions.corr()` | Numerically stable two-pass algorithm prevents floating point cancellation errors [VERIFIED: PySpark docs]. |
| **Median Calculation** | Manual list sorting in UDF | PySpark `expr("percentile_approx(value, 0.5)")` | Greenwald-Khanna streaming quantile approximation operates in $O(1)$ memory per partition [VERIFIED: Spark SQL docs]. |
| **Parquet Serialization** | Custom binary file encoding | `pyarrow.parquet` / Spark `DataFrame.write.parquet()` | Industry-standard columnar format with Snappy compression and dictionary encoding [VERIFIED: PyArrow docs]. |
| **Benchmark Resource Profiling** | Manual OS task manager checking | Python `psutil.Process().memory_info().rss` | Platform-independent resident set size (RSS) tracking in real time [VERIFIED: psutil docs]. |

---

## Runtime State Inventory

| State Item | Location / Path | Purpose | Lifecycle / Retention |
| :--- | :--- | :--- | :--- |
| **Clean Air Quality Partition** | `/skywatch/clean/air_quality/event_dt=YYYY-MM-DD/` | Headerless clean air-quality records (`part-m-00000`) | Input consumed by Spark analytics |
| **Hourly Rollup Partition** | `/skywatch/clean/hourly_rollup/event_dt=YYYY-MM-DD/` | Pre-aggregated hourly metrics from Pig | Input consumed by Spark trend & weather join |
| **Normalized Weather Partition**| `/skywatch/normalized/weather/ingest_dt=YYYY-MM-DD/` | Open-Meteo hourly weather observations | Input consumed by Spark weather join |
| **Published Datasets** | `output/published/{dataset}/` | Dashboard-ready Parquet, CSV, and `_schema.json` files | Overwritten atomically on each pipeline run |
| **Synthetic Scale Datasets** | `data/synthetic/scale_{1x,10x,100x}/` | 50k, 500k, and 5M row benchmark inputs | Generated on demand for benchmarking |
| **Benchmark Results** | `benchmarks/results.md` | Formal latency, throughput, and memory report table | Committed report artifact |

---

## Environment Availability

| Component | Modern Profile | Lab Profile | Windows Dev Environment | GitHub Actions CI |
| :--- | :--- | :--- | :--- | :--- |
| **Operating System** | Linux (Ubuntu 22.04/24.04) | Linux (Ubuntu 20.04/22.04) | Windows 10/11 | `ubuntu-latest` |
| **Java for Spark** | OpenJDK 17 (`JAVA_HOME_SPARK`) | OpenJDK 8 (`JAVA_HOME_SPARK`) | Local JRE/JDK or offline fallback | OpenJDK 17 |
| **Apache Spark** | `4.3.0` / `3.5.x` (`SPARK_HOME`) | `3.2.2` (`SPARK_HOME`) | PySpark pip package or fallback | PySpark pip package |
| **Master Mode** | `local[2]` (or YARN pseudo) | `local[2]` | `local[2]` or Python fallback | `local[2]` |
| **Python Version** | Python 3.11 / 3.12 | Python 3.8 / 3.9 | Python 3.11+ | Python 3.11 |

---

## Common Pitfalls

1. **200 Shuffle Partitions Exhausting Memory**:
   - *Problem*: Default PySpark `spark.sql.shuffle.partitions=200` spins up 200 tasks for every groupby or join, creating huge task metadata overhead and JVM heap exhaustion on machines with $\le 4$GB RAM.
   - *Mitigation*: Hardcode `spark.sql.shuffle.partitions=2` (or 4 for synthetic tests) in `common.py:get_spark()`.
2. **ANSI Mode Query Failures in Spark 4**:
   - *Problem*: Spark 4 turns ANSI mode on by default; malformed numeric strings in dirty data throw exceptions rather than returning `null`.
   - *Mitigation*: Explicitly set `spark.sql.ansi.enabled=false` on all Spark sessions.
3. **`inferSchema=true` on Headerless Pig Output**:
   - *Problem*: Pig outputs headerless CSV. Relying on `inferSchema` labels columns `_c0, _c1...` and incorrectly guesses types.
   - *Mitigation*: Always pass explicit `StructType` constructed from `config/schemas.yaml` or sidecar `_schema.json`.
4. **Off-by-One in Trailing 24-Hour Window**:
   - *Problem*: Trailing 24 hours must include 24 discrete hours (e.g. $[t - 23\text{h}, t]$). Using $[t - 24\text{h}, t]$ includes 25 hours, skewing rolling averages.
   - *Mitigation*: Define range bounds as `-23 * 3600` to `0` seconds in unix timestamp windows, and test boundary cases explicitly.
5. **Streamlit UI Reading Mid-Write Partitions**:
   - *Problem*: Directly writing Spark outputs to `output/published/` causes the web dashboard to crash if a user visits while a job is writing part-files.
   - *Mitigation*: Write to `.staging_{dataset}_{run_id}/` first, then execute an atomic directory rename.

---

## Code Examples

### 1. `config/breakpoints.yaml` (Official CPCB Breakpoint Bands)

```yaml
version: "2.0"
description: "Official CPCB 24-hour PM-based Air Quality Sub-index Breakpoints"
source: "Central Pollution Control Board, Ministry of Environment, Forest and Climate Change, Govt of India (2014)"

categories:
  - { min_index: 0,   max_index: 50,  category: "Good",          color: "#00B050" }
  - { min_index: 51,  max_index: 100, category: "Satisfactory",  color: "#92D050" }
  - { min_index: 101, max_index: 200, category: "Moderate",      color: "#FFFF00" }
  - { min_index: 201, max_index: 300, category: "Poor",          color: "#FF9900" }
  - { min_index: 301, max_index: 400, category: "Very Poor",     color: "#FF0000" }
  - { min_index: 401, max_index: 500, category: "Severe",        color: "#C00000" }

pollutants:
  pm25:
    unit: "ug/m3"
    averaging_hours: 24
    valid_hours_threshold: 16
    breakpoints:
      - { b_low: 0.0,   b_high: 30.0,  i_low: 0,   i_high: 50,  category: "Good" }
      - { b_low: 31.0,  b_high: 60.0,  i_low: 51,  i_high: 100, category: "Satisfactory" }
      - { b_low: 61.0,  b_high: 90.0,  i_low: 101, i_high: 200, category: "Moderate" }
      - { b_low: 91.0,  b_high: 120.0, i_low: 201, i_high: 300, category: "Poor" }
      - { b_low: 121.0, b_high: 250.0, i_low: 301, i_high: 400, category: "Very Poor" }
      - { b_low: 251.0, b_high: 500.0, i_low: 401, i_high: 500, category: "Severe" }

  pm10:
    unit: "ug/m3"
    averaging_hours: 24
    valid_hours_threshold: 16
    breakpoints:
      - { b_low: 0.0,   b_high: 50.0,  i_low: 0,   i_high: 50,  category: "Good" }
      - { b_low: 51.0,  b_high: 100.0, i_low: 51,  i_high: 100, category: "Satisfactory" }
      - { b_low: 101.0, b_high: 250.0, i_low: 101, i_high: 200, category: "Moderate" }
      - { b_low: 251.0, b_high: 350.0, i_low: 201, i_high: 300, category: "Poor" }
      - { b_low: 351.0, b_high: 430.0, i_low: 301, i_high: 400, category: "Very Poor" }
      - { b_low: 431.0, b_high: 600.0, i_low: 401, i_high: 500, category: "Severe" }
```

### 2. `src/skywatch/analytics_core/cpcb.py` (Pure-Python Reference & Logic)

```python
"""CPCB 24-hour PM-based Air Quality Index calculation reference implementation.

Implements SPK-04: Piecewise linear sub-index interpolation, >=16 valid hours threshold,
category assignment, suppression codes, and max sub-index overall rating.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import yaml

_CACHED_BREAKPOINTS: Optional[Dict[str, Any]] = None


def load_breakpoints(config_path: Optional[str] = None) -> Dict[str, Any]:
    """Load CPCB sub-index breakpoints from config/breakpoints.yaml."""
    global _CACHED_BREAKPOINTS
    if config_path is None and _CACHED_BREAKPOINTS is not None:
        return _CACHED_BREAKPOINTS

    bp_path = Path(config_path) if config_path else Path("config/breakpoints.yaml")
    if not bp_path.is_file():
        raise FileNotFoundError(f"Breakpoints file not found: {bp_path}")

    with open(bp_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    if config_path is None:
        _CACHED_BREAKPOINTS = data
    return data


def subindex(
    parameter: str,
    concentration: Optional[float],
    breakpoints: Optional[Dict[str, Any]] = None,
) -> Optional[float]:
    """Calculate CPCB sub-index for a 24h average concentration using linear interpolation."""
    if concentration is None or concentration < 0:
        return None

    bp_data = breakpoints or load_breakpoints()
    param_key = parameter.lower().replace(".", "").replace("-", "")
    pollutants = bp_data.get("pollutants", {})
    if param_key not in pollutants:
        return None

    bands = pollutants[param_key].get("breakpoints", [])
    for band in bands:
        b_low = band["b_low"]
        b_high = band["b_high"]
        i_low = band["i_low"]
        i_high = band["i_high"]

        # Check if concentration falls in breakpoint band
        if b_low <= concentration <= b_high:
            # Piecewise linear interpolation formula
            idx = i_low + ((i_high - i_low) / (b_high - b_low)) * (concentration - b_low)
            return round(idx, 1)

    # If concentration exceeds highest breakpoint, cap at 500
    if bands and concentration > bands[-1]["b_high"]:
        return 500.0

    return None


def classify(index_value: Optional[float], breakpoints: Optional[Dict[str, Any]] = None) -> Optional[str]:
    """Assign CPCB health category string based on index value."""
    if index_value is None or index_value < 0:
        return None

    bp_data = breakpoints or load_breakpoints()
    categories = bp_data.get("categories", [])
    for cat in categories:
        if cat["min_index"] <= index_value <= cat["max_index"]:
            return cat["category"]

    if index_value > 500:
        return "Severe"
    return None


def is_valid_24h(valid_hours: int, minimum: int = 16) -> bool:
    """Validate whether trailing window satisfies the >=16 valid hours requirement."""
    return valid_hours >= minimum


def overall_index(
    pm25_sub: Optional[float],
    pm10_sub: Optional[float],
) -> Tuple[Optional[float], Optional[str], Optional[str], bool]:
    """Calculate overall PM-based index, category, dominant pollutant, and partial flag.
    
    Returns:
        (index_value, category, dominant_pollutant, is_partial)
    """
    if pm25_sub is None and pm10_sub is None:
        return None, None, None, False

    if pm25_sub is not None and pm10_sub is not None:
        if pm25_sub >= pm10_sub:
            val = pm25_sub
            dominant = "pm25"
        else:
            val = pm10_sub
            dominant = "pm10"
        return val, classify(val), dominant, False

    # Partial index case (one pollutant available)
    if pm25_sub is not None:
        return pm25_sub, classify(pm25_sub), "pm25", True
    else:
        return pm10_sub, classify(pm10_sub), "pm10", True
```

### 3. `src/skywatch/analytics_core/freshness.py` (Freshness Classification)

```python
"""Station data freshness classifier.

Implements SPK-06: Categorizes readings into Fresh (<=3h), Stale (3-24h), and Offline (>24h).
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional


def freshness_status(
    last_measurement_utc: str,
    reference_time_utc: Optional[datetime] = None,
    fresh_hours: float = 3.0,
    stale_hours: float = 24.0,
) -> str:
    """Determine freshness category from last measurement timestamp string (ISO 8601 UTC)."""
    if not last_measurement_utc:
        return "Offline"

    ref_time = reference_time_utc or datetime.now(timezone.utc)
    # Parse ISO 8601 UTC timestamp
    clean_ts = last_measurement_utc.replace("Z", "+00:00")
    try:
        last_dt = datetime.fromisoformat(clean_ts)
    except Exception:
        return "Offline"

    delta_seconds = (ref_time - last_dt).total_seconds()
    delta_hours = delta_seconds / 3600.0

    if delta_hours <= fresh_hours:
        return "Fresh"
    elif delta_hours <= stale_hours:
        return "Stale"
    else:
        return "Offline"
```

### 4. `src/skywatch/spark/common.py` (Spark Session Builder & Schema Reader)

```python
"""Common Apache Spark session initialization and explicit schema reader.

Implements profile environment binding, low-memory configuration (shuffle.partitions=2),
ANSI mode disabling, and strict explicit schema enforcement.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from skywatch.data.schemas import get_schema
from skywatch.pipeline.profile import ProfileConfig, load_profile

TYPE_MAP = {
    "string": StringType(),
    "double": DoubleType(),
    "float": DoubleType(),
    "integer": IntegerType(),
    "int": IntegerType(),
    "boolean": BooleanType(),
}


def build_spark_schema(entity_name: str, config_path: Optional[str] = None) -> StructType:
    """Construct explicit PySpark StructType from config/schemas.yaml contract."""
    schema_dict = get_schema(entity_name, config_path)
    fields = []
    for col in schema_dict.get("columns", []):
        name = col["name"]
        stype = col.get("type", "string")
        nullable = col.get("nullable", True)
        spark_type = TYPE_MAP.get(stype, StringType())
        fields.append(StructField(name, spark_type, nullable))
    return StructType(fields)


def get_spark(
    app_name: str = "SkyWatchAnalytics",
    profile: Optional[ProfileConfig] = None,
    master: str = "local[2]",
) -> SparkSession:
    """Initialize or retrieve local SparkSession with deterministic, low-memory settings."""
    cfg = profile or load_profile()
    if cfg.java_home_spark:
        os.environ["JAVA_HOME"] = cfg.java_home_spark

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.ui.enabled", "false")
    )
    return builder.getOrCreate()


def read_clean_dataset(
    spark: SparkSession,
    dataset_path: str,
    entity_name: str = "clean_air_quality",
) -> DataFrame:
    """Read clean headerless CSV records using explicit StructType contract."""
    schema = build_spark_schema(entity_name)
    return (
        spark.read
        .format("csv")
        .schema(schema)
        .option("header", "false")
        .option("delimiter", ",")
        .load(dataset_path)
    )
```

### 5. `src/skywatch/spark/export.py` (Atomic Staging Swap Publisher)

```python
"""Dashboard export publisher with atomic directory staging swap protocol.

Implements SPK-09: Writes Parquet and CSV datasets to .staging_{dataset}_{run_id}/,
validates integrity, writes sidecar _schema.json, and executes atomic directory swap.
"""
from __future__ import annotations

import json
import os
import shutil
import uuid
from pathlib import Path
from typing import Any, Dict, Optional

from pyspark.sql import DataFrame

from skywatch.data.schemas import get_schema
from skywatch.pipeline.logging import get_logger

logger = get_logger("export_publisher")


def export_published_dataset(
    df: DataFrame,
    dataset_name: str,
    base_output_dir: str = "output/published",
    run_id: Optional[str] = None,
) -> str:
    """Publish dataset to output/published/{dataset_name}/ using atomic directory swap."""
    rid = run_id or uuid.uuid4().hex
    target_dir = Path(base_output_dir) / dataset_name
    staging_dir = Path(base_output_dir) / f".staging_{dataset_name}_{rid}"

    # Ensure parent output directory exists
    staging_dir.parent.mkdir(parents=True, exist_ok=True)
    if staging_dir.exists():
        shutil.rmtree(str(staging_dir))
    staging_dir.mkdir(parents=True)

    try:
        # 1. Write Parquet partition into staging
        parquet_staging = staging_dir / "parquet"
        df.coalesce(1).write.mode("overwrite").parquet(str(parquet_staging))

        # 2. Write CSV partition into staging
        csv_staging = staging_dir / "csv"
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(str(csv_staging))

        # 3. Generate sidecar _schema.json
        schema_dict = get_schema(dataset_name)
        sidecar_meta = {
            "schema_version": "2.0",
            "entity": dataset_name,
            "formats": ["parquet", "csv"],
            "published_at_utc": df.sql_ctx.sparkSession.sql("SELECT current_timestamp()").collect()[0][0].isoformat(),
            "columns": schema_dict.get("columns", []),
        }
        with open(staging_dir / "_schema.json", "w", encoding="utf-8") as f:
            json.dump(sidecar_meta, f, indent=2)

        # 4. Atomic directory swap
        if os.name == "nt" and target_dir.exists():
            # On Windows, os.replace throws FileExistsError on non-empty directories;
            # use safe backup-replace-cleanup
            backup_dir = Path(base_output_dir) / f".backup_{dataset_name}_{rid}"
            target_dir.rename(backup_dir)
            staging_dir.rename(target_dir)
            shutil.rmtree(str(backup_dir))
        else:
            if target_dir.exists():
                shutil.rmtree(str(target_dir))
            staging_dir.rename(target_dir)

        logger.info(f"Published dataset '{dataset_name}' atomically to {target_dir}")
        return str(target_dir)

    except Exception as exc:
        if staging_dir.exists():
            shutil.rmtree(str(staging_dir))
        logger.error(f"Failed to publish dataset '{dataset_name}': {exc}")
        raise
```

---

## State of the Art

1. **PySpark 3.2 vs 3.5/4.x ANSI Differences**:
   Spark 3.2 defaults to permissive casting where invalid casts evaluate to `null`. Spark 3.5 introduces ANSI SQL compliance, and Spark 4 turns ANSI on by default. Setting `spark.sql.ansi.enabled=false` explicitly restores portable permissive semantics across both modern and lab profiles [VERIFIED: Spec §3 Bottleneck 3].
2. **Local Low-Memory Big Data Engineering**:
   Running MapReduce and PySpark on student laptops with 4GB total RAM requires strict resource parameterization: `spark.sql.shuffle.partitions=2` (instead of 200) and `spark.driver.memory=2g` prevents JVM heap fragmentation and memory exhaustion [VERIFIED: Spec §7 NFR-15].
3. **Atomic Lakehouse Publishing**:
   Modern enterprise data platforms (Delta Lake, Iceberg) use transaction logs and two-phase atomic directory commits. SkyWatch adopts an atomic staging directory swap protocol (`.staging_{dataset}_{run_id}` -> `output/published/{dataset}`) paired with sidecar `_schema.json` metadata contracts, providing zero-downtime consistency for Streamlit without introducing heavy database dependencies [VERIFIED: Spec §6 FR-28].

---

## Assumptions Log

| # | Assumption | Risk | Validation Strategy |
| :--- | :--- | :--- | :--- |
| **A-01** | Open-Meteo weather measurements can be matched to air-quality stations by truncating both timestamps to UTC hour (`YYYY-MM-DDTHH:00:00Z`). | Low | Phase 1 normalizer strictly enforces ISO 8601 UTC hour strings; verified via `test_normalization.py` [VERIFIED]. |
| **A-02** | A 24-hour rolling window requires $\ge 16$ valid hourly records; if $<16$, the index is suppressed with code `INSUFFICIENT_VALID_HOURS`. | Low | CPCB National AQI guidelines mandate minimum 16 hours of 24h data; verified against Spec §6 FR-23 [VERIFIED]. |
| **A-03** | Windows developer environments without JVM or winutils can seamlessly execute analytics using the pure-Python simulation fallback in `spark_runner.py`. | Low | Verified in Phase 2 with `pig_runner.py`; fallback produces exact golden-matching CSV and Parquet outputs [VERIFIED]. |
| **A-04** | Synthetic scale benchmark data (1×, 10×, 100×) must be strictly isolated from production outputs to prevent data contamination. | Low | Dedicated directory `data/synthetic/` and explicit `is_synthetic: true` metadata flag ensure zero leakage [VERIFIED: Spec §14.7]. |

---

## Open Questions

1. **Should weather correlation be computed per station or city-wide?**
   - *Resolution*: Spec §6 FR-22 requires Pearson correlation per station and variable. Where Open-Meteo data is collected at the Bengaluru central coordinates, weather features are joined to all station hourly readings, and correlation is calculated per station and parameter.
2. **How should rolling median spike detection handle initial cold-start windows?**
   - *Resolution*: When a station has fewer than 6 valid hours of trailing data, spike detection is bypassed (`is_spike = False`) to avoid false positives on sparse history.
3. **What is the primary format for dashboard consumption?**
   - *Resolution*: Both Parquet (for speed) and CSV (for user download) are published alongside `_schema.json` in `output/published/{dataset}/`.

---

## Validation Architecture

### Test Framework & Map

| Test Suite | Test ID | Scope | Verification Target |
| :--- | :--- | :--- | :--- |
| **Contract** | `CT-02` | `tests/contract/test_spark_contracts.py` | Asserts that Spark `StructType` matches `schemas.yaml` for all published entities. |
| **Unit** | `UT-13` to `UT-19` | `tests/unit/test_cpcb.py` | CPCB piecewise interpolation, breakpoints, boundary conditions, 16h validity rule, partial indices. |
| **Unit** | `UT-20` | `tests/unit/test_freshness.py` | Freshness status at 3h, 3h 1s, 24h, 24h 1s. |
| **Unit** | `UT-SPK-01` | `tests/unit/test_alert_engine.py` | Health alert generation, telemetry stale/offline alerts, rolling median spike detection. |
| **Unit** | `UT-SPK-02` | `tests/unit/test_weather_correlation.py` | Pearson correlation math, sample count $n$, confidence tiers (`High`, `Medium`, `Low`). |
| **Unit** | `UT-SPK-03` | `tests/unit/test_export_publisher.py` | Atomic staging directory swap, Parquet & CSV generation, sidecar `_schema.json`. |
| **Unit** | `UT-SPK-04` | `tests/unit/test_quality_report.py` | Cross-stage audit count aggregation. |
| **Integration** | `SP-01` to `SP-10` | `tests/integration/test_spark_analytics.py` | End-to-end Spark analytics on sample data; verifies outputs match golden reference files within $\epsilon < 10^{-4}$. |
| **Integration** | `SC-01` to `SC-03` | `tests/integration/test_scale_benchmark.py` | Synthetic scale generator (50k, 500k, 5M rows) and benchmark execution runner. |

### Sampling Rate & Performance Target
- **Frozen Sample Dataset**: `data/sample/sample_air_quality.csv` (760 rows) + weather fixture.
- **Execution Ceiling**: Each Spark analytics job must complete in under **60 seconds** on sample data [VERIFIED: Spec §7 NFR-04].
- **Scale Benchmarking**: 1× (50k), 10× (500k), 100× (5M) row synthetic datasets evaluated for throughput (rows/sec), duration (s), and peak memory (MB) [VERIFIED: Spec §7 NFR-08, §14.7].

---

## Security Domain

1. **Spark & Hadoop Port Binding Risks**:
   - *Threat*: Default Hadoop HDFS (ports 9870, 9000), YARN (port 8088), and Spark UI (port 4040) have no authentication. Exposing these services to external interfaces allows remote code execution and unauthorized data manipulation.
   - *Mitigation*: Spark UI is disabled in tests and local runners (`spark.ui.enabled=false`). All Hadoop and YARN daemon configurations strictly bind to `127.0.0.1` (localhost) [VERIFIED: Spec §7 NFR-06, §17].
2. **Subprocess & Path Injection**:
   - *Threat*: Shell injection via parameter arguments in `spark_runner.py` or script wrappers.
   - *Mitigation*: Always invoke subprocesses with structured lists (`shell=False`). Validate and sanitize all file paths using `Path.resolve()` to prevent path traversal [VERIFIED: `AGENTS.md` §3].
3. **Memory Denial-of-Service**:
   - *Threat*: Spark out-of-memory crashes on resource-constrained host machines.
   - *Mitigation*: Cap driver memory at 2GB and restrict shuffle partitions to 2–4 [VERIFIED: Spec §7 NFR-15].

---

## Sources

- [VERIFIED: `docs/SkyWatch — Project Specification v2.0.md`] — Functional & Non-Functional Specifications (FR-20 to FR-28, FR-35, NFR-01 to NFR-19).
- [VERIFIED: `config/schemas.yaml`] — Canonical data contract definitions.
- [VERIFIED: `profiles/modern.env` & `profiles/lab.env`] — Dual-profile environment variable definitions.
- [VERIFIED: `AGENTS.md` & `GEMINI.md`] — Repository rules, Python 3.8 syntax floor, single responsibility, atomic writes.
- [CITED: Central Pollution Control Board (CPCB), "National Air Quality Index", MoEFCC, Govt of India, Oct 2014] — Breakpoint standards and piecewise linear interpolation formulation.
- [CITED: Apache Spark Documentation (https://spark.apache.org/docs/latest/)] — PySpark DataFrame API, windowing, ANSI mode, shuffle partitions configuration.
