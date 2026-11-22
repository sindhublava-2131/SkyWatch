# Phase 3: Analytics Engine with Apache Spark & Scale Benchmarking - Pattern Mapping

**Phase:** 3 - Analytics Engine with Apache Spark & Scale Benchmarking  
**Mapped:** 2026-11-22  
**Domain:** Apache Spark DataFrame Analytics, CPCB PM-based AQI, Weather Correlation, Alert Engine, Scale Benchmarking  
**Status:** COMPLETE  

---

## 1. Executive Summary

This document establishes the canonical implementation patterns, analog mappings, and structural templates for all files to be created or modified in **Phase 3: Analytics Engine with Apache Spark & Scale Benchmarking**.

The primary objective of Phase 3 is to transition clean batch datasets from Hadoop HDFS (produced in Phase 2) into high-level environmental intelligence, temporal trend mining, meteorological correlations, official health index calculations, telemetry alerting, and synthetic scale benchmarking.

Following the architecture established in Phase 2's `pig_runner.py` and `schemas.py`, Phase 3 implements a **dual-mode execution strategy**:
1. **PySpark Execution Engine**: When a JVM and Apache Spark runtime are present, native Spark DataFrame jobs execute across `modern` (Spark 3.5/4.x on Java 17) or `lab` (Spark 3.2 on Java 8) profiles.
2. **Pure-Python Simulation Fallback Engine**: When running offline, in CI, or on developer environments lacking JVM/winutils (such as native Windows), a pure-Python simulation fallback in `spark_runner.py` executes identical relational and statistical logic, producing numerically equivalent results ($\epsilon < 10^{-4}$).

All Phase 3 code must strictly adhere to the project's engineering constraints:
- **Python 3.8 Syntax Floor**: Enforce `from __future__ import annotations`, `typing.Optional`, `typing.Dict`, `typing.List`. Never use Python 3.10+ syntax (`match` statements or `X | Y` union types) [VERIFIED: `AGENTS.md` §4].
- **Local Low-Memory Configuration**: Prevent JVM out-of-memory errors on $\le 4$ GB RAM machines by fixing `spark.sql.shuffle.partitions=2` (preventing 200 default shuffle tasks), disabling ANSI mode (`spark.sql.ansi.enabled=false`), and capping driver memory at 2 GB (`spark.driver.memory=2g`) [VERIFIED: Spec §3 Bottleneck 3, §7 NFR-03, NFR-15].
- **Explicit Schema Enforcement**: Spark readers must never use `inferSchema=true` on headerless clean CSV files. Schemas must be explicitly loaded from `config/schemas.yaml` using PySpark `StructType` [VERIFIED: Spec §3 Bottleneck 9, §8].
- **Honest Health Methodology**: The index must be explicitly labeled as a **PM-based CPCB index** (never as a full 8-pollutant AQI, as gaseous pollutants SO2, NO2, CO, O3 are not monitored). Do not imply forecasting, causation, or live readings [VERIFIED: `AGENTS.md` §4, Spec §6 FR-23].
- **Atomic Staging Swap Publishing**: Analytical outputs must be written to staging directories (`.staging_{dataset}_{run_id}/`) in dual formats (Parquet and CSV) with sidecar `_schema.json` metadata contracts before an atomic directory rename into `output/published/{dataset}/` [VERIFIED: Spec §6 FR-28, §7 NFR-01].
- **Synthetic Data Isolation**: Synthetic scale benchmark datasets (1× 50k, 10× 500k, 100× 5M rows) are strictly for performance evaluation and must never contaminate production analytical datasets [VERIFIED: Spec §14.7].

---

## 2. File Classification Table

| # | Target File Path | Role | Data Flow | Layer | Closest Codebase Analog | Target Plan |
|---|---|---|---|---|---|---|
| 1 | `config/schemas.yaml` | Schema Contract Extension | Single source of truth read by `schemas.py`, `spark/common.py`, and `spark/export.py` | Configuration / Data Contracts | `config/schemas.yaml` (`clean_air_quality` & `station_stats`) | Plan 03-01 |
| 2 | `config/breakpoints.yaml` | CPCB Breakpoint Configuration | Breakpoint lookup table read by `analytics_core/cpcb.py` | Configuration / Domain Standards | `config/schemas.yaml` & `config/stations.json` | Plan 03-01 |
| 3 | `config/settings.yaml` | Operational Threshold Config | Alert thresholds, freshness limits, spike multipliers read by `spark_runner.py` and `alert_engine.py` | Configuration / Parameters | `config/schemas.yaml` & `src/skywatch/pipeline/profile.py` | Plan 03-01 |
| 4 | `src/skywatch/analytics_core/__init__.py` | Package Initializer | Exports pure-Python math functions | Domain Logic / Core Math | `src/skywatch/data/__init__.py` | Plan 03-01 |
| 5 | `src/skywatch/analytics_core/cpcb.py` | CPCB Sub-Index Math Engine | Pure-Python linear interpolation, 16h valid hours rule, categories, max sub-index, dominant pollutant | Domain Logic / Core Math | `src/skywatch/data/validation.py` & `src/skywatch/data/schemas.py` | Plan 03-01 |
| 6 | `src/skywatch/analytics_core/freshness.py` | Freshness Classifier | Categorizes readings into Fresh ($\le 3$h), Stale (3–24h), and Offline ($> 24$h) | Domain Logic / Core Math | `src/skywatch/data/validation.py` (`check_timestamp`) | Plan 03-01 |
| 7 | `src/skywatch/spark/__init__.py` | Package Initializer | Exports Spark jobs and session utilities | Analytics Engine / PySpark | `src/skywatch/pipeline/__init__.py` | Plan 03-02 |
| 8 | `src/skywatch/spark/common.py` | Spark Session & Schema Builder | Initializes local SparkSession (`local[2]`, low-memory), builds `StructType` from `schemas.yaml`, reads clean CSV | Analytics Infrastructure | `src/skywatch/pipeline/profile.py` & `src/skywatch/data/schemas.py` | Plan 03-02 |
| 9 | `src/skywatch/spark/station_statistics.py` | Station Stats & Ranking Job (SPK-01) | Ingests clean air quality, computes 24h avg, min, max, count, assigns pollution rank identifying cleanest/polluted hotspots | PySpark Analytics Job | `pig/station_stats.pig` & `pig_runner.py` | Plan 03-02 |
| 10 | `src/skywatch/spark/trend_analysis.py` | Temporal Trend Analysis Job (SPK-02) | Groups by UTC hour and day-of-week, aggregates pollutant profiles, detects peak pollution hours per station | PySpark Analytics Job | `pig/hourly_rollup.pig` & `pig_runner.py` | Plan 03-02 |
| 11 | `src/skywatch/spark/weather_analysis.py` | Weather Correlation Job (SPK-03) | Joins air quality hourly rollups with weather metrics on truncated UTC hour, computes Pearson $r$, sample $n$, confidence tier | PySpark Analytics Job | `src/skywatch/data/validation.py` (`validate_weather_record`) | Plan 03-02 |
| 12 | `src/skywatch/spark/index_job.py` | 24h Rolling CPCB AQI Job (SPK-04) | Windowed 24h rolling aggregation, $\ge 16$ valid hours gate, piecewise interpolation, suppression codes, max sub-index | PySpark Analytics Job | `src/skywatch/analytics_core/cpcb.py` | Plan 03-03 |
| 13 | `src/skywatch/spark/alert_engine.py` | Alert & Spike Detection Job (SPK-05, SPK-06, SPK-08) | Evaluates health alerts (Poor/Severe), telemetry alerts (stale/offline), and rolling median $3\times$ spike detection | PySpark Analytics Job | `src/skywatch/data/validation.py` & `analytics_core/freshness.py` | Plan 03-03 |
| 14 | `src/skywatch/spark/quality_report.py` | Cross-Stage Quality Audit (SPK-07) | Aggregates accepted, rejected, and duplicate records across Ingestion, Pig ETL, and Spark analytics stages | PySpark Analytics Job | `src/skywatch/pipeline/logging.py` & `schemas.py` | Plan 03-03 |
| 15 | `src/skywatch/spark/export.py` | Atomic Export Publisher (SPK-09) | Writes Parquet and CSV into `.staging_{dataset}_{run_id}/`, validates, attaches `_schema.json`, atomically swaps to `output/published/` | Analytics Publishing | `src/skywatch/pipeline/pig_runner.py` (`generate_sidecar_schema`) | Plan 03-03 |
| 16 | `src/skywatch/pipeline/spark_runner.py` | Spark Orchestrator & Fallback Engine | Orchestrates Spark jobs, binds profile environment, dispatches to PySpark or pure-Python fallback simulation | Pipeline Orchestration | `src/skywatch/pipeline/pig_runner.py` | Plan 03-04 |
| 17 | `scripts/make_synthetic.py` | Synthetic Scale Generator (SPK-10) | Generates 1× (50k), 10× (500k), and 100× (5M) row datasets with diurnal curves and weather covariance | Benchmark Scripting | `tests/fixtures/sample_air_quality.csv` & `config/stations.json` | Plan 03-04 |
| 18 | `scripts/run_spark.sh` | Shell Entrypoint Wrapper | CLI wrapper script sourcing active profile, configuring environment, launching `spark_runner.py` | CLI / Shell Scripts | `scripts/run_pig.sh` & `scripts/use_profile.sh` | Plan 03-04 |
| 19 | `benchmarks/run_scale_benchmark.py` | Scale Benchmark Runner (SPK-10) | Executes Pig clean, Spark stats, and Spark AQI across 1×, 10×, 100×; profiles latency, throughput, and RSS RAM via `psutil` | Benchmarking Suite | `src/skywatch/pipeline/pig_runner.py` & `spark_runner.py` | Plan 03-04 |
| 20 | `benchmarks/results.md` | Benchmark Report Table | Formal Markdown table reporting duration, throughput, and peak memory across 1×, 10×, 100× scale datasets | Benchmark Documentation | `docs/SkyWatch — Project Specification v2.0.md` §14.7 | Plan 03-04 |
| 21 | `tests/unit/test_cpcb.py` | Unit Test Suite: CPCB Math | Validates linear interpolation, breakpoints, boundary conditions, 16h validity gate, partial index (UT-13 to UT-19) | Quality Gate / Unit Testing | `tests/unit/test_validation.py` | Plan 03-01 |
| 22 | `tests/unit/test_freshness.py` | Unit Test Suite: Freshness | Validates freshness badges at boundary seconds: 3h, 3h 1s, 24h, 24h 1s (UT-20) | Quality Gate / Unit Testing | `tests/unit/test_validation.py` | Plan 03-01 |
| 23 | `tests/unit/test_alert_engine.py` | Unit Test Suite: Alerts & Spikes | Validates health category alerts, telemetry alerts, and rolling median spike detection (UT-SPK-01) | Quality Gate / Unit Testing | `tests/unit/test_validation.py` | Plan 03-03 |
| 24 | `tests/unit/test_weather_correlation.py` | Unit Test Suite: Weather Correlation | Validates Pearson $r$ formula, sample size $n$, confidence tiers (`High`, `Medium`, `Low`) (UT-SPK-02) | Quality Gate / Unit Testing | `tests/unit/test_validation.py` | Plan 03-02 |
| 25 | `tests/unit/test_export_publisher.py` | Unit Test Suite: Atomic Publisher | Validates atomic directory swap, Parquet/CSV generation, and sidecar `_schema.json` integrity (UT-SPK-03) | Quality Gate / Unit Testing | `tests/unit/test_pig_runner.py` (`test_generate_sidecar_schema`) | Plan 03-03 |
| 26 | `tests/unit/test_quality_report.py` | Unit Test Suite: Quality Audit | Validates cross-stage audit aggregation tracking accepted, rejected, and duplicate counts (UT-SPK-04) | Quality Gate / Unit Testing | `tests/unit/test_validation.py` | Plan 03-03 |
| 27 | `tests/contract/test_spark_contracts.py` | Contract Test Suite | Asserts that PySpark `StructType` matches `config/schemas.yaml` across all published entities (CT-02) | Quality Gate / Contract Testing | `tests/contract/test_pig_contracts.py` | Plan 03-01 |
| 28 | `tests/integration/test_spark_analytics.py` | End-to-End Spark Analytics Suite | Runs analytics jobs on frozen sample data; asserts outputs match golden reference files within $\epsilon < 10^{-4}$ (SP-01 to SP-10) | Quality Gate / Integration Testing | `tests/integration/test_pig_e2e.py` | Plan 03-04 |
| 29 | `tests/integration/test_scale_benchmark.py` | Integration Test Suite: Benchmarking | Validates synthetic data generation (50k rows) and benchmark execution runner (SC-01 to SC-03) | Quality Gate / Integration Testing | `tests/integration/test_pig_e2e.py` | Plan 03-04 |
| 30 | `tests/fixtures/golden/*.csv` | Golden Reference Fixtures | 7 immutable golden reference CSV datasets for regression and verification testing | Test Fixtures | `tests/fixtures/golden/golden_station_stats.csv` | Plan 03-02 / 03-04 |

---

## 3. Concrete Code Patterns & Analog Mappings

---

### File 1: `config/schemas.yaml` (Schema Contract Extensions)

- **Target Path:** `config/schemas.yaml`
- **Role:** Canonical data contract definition for all 7 published analytical datasets consumed by Phase 4 Streamlit dashboard.
- **Data Flow:** Read by `skywatch.data.schemas`, used by `spark/common.py` to build `StructType`, by `spark/export.py` to write `_schema.json`, and verified by `test_spark_contracts.py`.
- **Closest Analog:** `config/schemas.yaml` (lines 173–226, `station_stats` and `hourly_rollup`).

#### Existing Analog Excerpt (`config/schemas.yaml`, lines 173–199):
```yaml
station_stats:
  partition_key: "event_dt"
  delimiter: ","
  has_header: false
  columns:
    - name: station_id
      type: string
      nullable: false
    - name: parameter
      type: string
      nullable: false
    - name: date
      type: string
      nullable: false
    - name: reading_count
      type: integer
      nullable: false
    - name: average_value
      type: double
      nullable: false
    - name: minimum_value
      type: double
      nullable: false
    - name: maximum_value
      type: double
      nullable: false
```

#### New Contract Schemas to Append:
```yaml
station_statistics:
  delimiter: ","
  has_header: true
  columns:
    - { name: station_id, type: string, nullable: false }
    - { name: station_name, type: string, nullable: false }
    - { name: parameter, type: string, nullable: false }
    - { name: reading_count, type: integer, nullable: false }
    - { name: average_value, type: double, nullable: false }
    - { name: minimum_value, type: double, nullable: false }
    - { name: maximum_value, type: double, nullable: false }
    - { name: pollution_rank, type: integer, nullable: false }
    - { name: generated_at_utc, type: string, nullable: false }

trends_hourly:
  delimiter: ","
  has_header: true
  columns:
    - { name: station_id, type: string, nullable: false }
    - { name: parameter, type: string, nullable: false }
    - { name: hour_of_day, type: integer, nullable: false }
    - { name: average_value, type: double, nullable: false }
    - { name: reading_count, type: integer, nullable: false }
    - { name: is_peak_hour, type: boolean, nullable: false }

trends_daily:
  delimiter: ","
  has_header: true
  columns:
    - { name: station_id, type: string, nullable: false }
    - { name: parameter, type: string, nullable: false }
    - { name: day_of_week, type: string, nullable: false }
    - { name: average_value, type: double, nullable: false }
    - { name: reading_count, type: integer, nullable: false }

weather_correlation:
  delimiter: ","
  has_header: true
  columns:
    - { name: station_id, type: string, nullable: false }
    - { name: pollutant, type: string, nullable: false }
    - { name: weather_variable, type: string, nullable: false }
    - { name: pearson_r, type: double, nullable: true }
    - { name: sample_count, type: integer, nullable: false }
    - { name: confidence_tier, type: string, nullable: false }

cpcb_index:
  delimiter: ","
  has_header: true
  columns:
    - { name: station_id, type: string, nullable: false }
    - { name: station_name, type: string, nullable: false }
    - { name: window_end_utc, type: string, nullable: false }
    - { name: pm25_24h_avg, type: double, nullable: true }
    - { name: pm25_subindex, type: double, nullable: true }
    - { name: pm10_24h_avg, type: double, nullable: true }
    - { name: pm10_subindex, type: double, nullable: true }
    - { name: aqi_value, type: double, nullable: true }
    - { name: aqi_category, type: string, nullable: true }
    - { name: dominant_pollutant, type: string, nullable: true }
    - { name: is_partial, type: boolean, nullable: false }
    - { name: suppression_reason, type: string, nullable: true }

alerts:
  delimiter: ","
  has_header: true
  columns:
    - { name: alert_id, type: string, nullable: false }
    - { name: alert_type, type: string, nullable: false }
    - { name: severity, type: string, nullable: false }
    - { name: station_id, type: string, nullable: false }
    - { name: station_name, type: string, nullable: false }
    - { name: timestamp_utc, type: string, nullable: false }
    - { name: message, type: string, nullable: false }
    - { name: metric_value, type: double, nullable: true }

quality_report:
  delimiter: ","
  has_header: true
  columns:
    - { name: run_id, type: string, nullable: false }
    - { name: stage, type: string, nullable: false }
    - { name: accepted_count, type: integer, nullable: false }
    - { name: rejected_count, type: integer, nullable: false }
    - { name: duplicate_count, type: integer, nullable: false }
    - { name: reason_counts_json, type: string, nullable: false }
    - { name: generated_at_utc, type: string, nullable: false }
```

---

### File 2: `config/breakpoints.yaml` (Official CPCB Breakpoint Bands)

- **Target Path:** `config/breakpoints.yaml`
- **Role:** Configuration file encoding the official MoEFCC/CPCB National Air Quality Index breakpoints for PM2.5 and PM10.
- **Data Flow:** Loaded by `src/skywatch/analytics_core/cpcb.py:load_breakpoints()`.
- **Closest Analog:** `config/schemas.yaml` & `config/stations.json`.

#### Implementation Pattern:
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

---

### File 3: `config/settings.yaml` (Operational Threshold Configuration)

- **Target Path:** `config/settings.yaml`
- **Role:** Operational parameters for alerting, freshness thresholds, and spike multipliers.
- **Data Flow:** Read by `src/skywatch/spark/alert_engine.py` and `spark_runner.py`.
- **Closest Analog:** `config/schemas.yaml` & `src/skywatch/pipeline/profile.py`.

#### Implementation Pattern:
```yaml
version: "2.0"

freshness:
  fresh_hours: 3.0
  stale_hours: 24.0

alerts:
  health_categories:
    poor:
      min_aqi: 201
      max_aqi: 300
      severity: "WARNING"
      type: "category_poor"
    very_poor:
      min_aqi: 301
      max_aqi: 400
      severity: "CRITICAL"
      type: "category_very_poor"
    severe:
      min_aqi: 401
      max_aqi: 500
      severity: "EMERGENCY"
      type: "category_severe"

spikes:
  rolling_window_hours: 24
  min_history_hours: 6
  multiplier_threshold: 3.0

benchmarks:
  scales:
    "1x": 50000
    "10x": 500000
    "100x": 5000000
  timeout_seconds: 300
```

---

### File 4 & 5: `src/skywatch/analytics_core/cpcb.py` (CPCB Math Engine)

- **Target Path:** `src/skywatch/analytics_core/cpcb.py`
- **Role:** Pure-Python reference implementation of the official CPCB piecewise linear sub-index formula, category classification, 16h valid hours rule, and dominant pollutant logic.
- **Data Flow:** Invoked by Spark UDFs or used directly by the simulation fallback engine in `spark_runner.py`.
- **Closest Analog:** `src/skywatch/data/schemas.py` (lines 12–35, cached loader pattern) & `src/skywatch/data/validation.py` (lines 44–56, range check).

#### Implementation Pattern for `src/skywatch/analytics_core/cpcb.py`:
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

---

### File 6: `src/skywatch/analytics_core/freshness.py` (Freshness Classification)

- **Target Path:** `src/skywatch/analytics_core/freshness.py`
- **Role:** Pure-Python classifier for station observation freshness: `Fresh` ($\le 3$h), `Stale` (3–24h), and `Offline` ($> 24$h).
- **Data Flow:** Used by `spark/alert_engine.py` and the simulation fallback in `spark_runner.py`.
- **Closest Analog:** `src/skywatch/data/validation.py` (lines 69–90, `check_timestamp`).

#### Implementation Pattern:
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
    clean_ts = last_measurement_utc.replace("Z", "+00:00")
    try:
        last_dt = datetime.fromisoformat(clean_ts)
        if last_dt.tzinfo is None:
            last_dt = last_dt.replace(tzinfo=timezone.utc)
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

---

### File 8: `src/skywatch/spark/common.py` (Spark Session & Explicit Schema Reader)

- **Target Path:** `src/skywatch/spark/common.py`
- **Role:** Central factory for deterministic, low-memory local `SparkSession` and dynamic `StructType` conversion from `config/schemas.yaml`.
- **Data Flow:** Imported by all `src/skywatch/spark/*.py` jobs.
- **Closest Analog:** `src/skywatch/pipeline/profile.py` (lines 31–73, profile loading) & `src/skywatch/data/schemas.py` (lines 30–43).

#### Implementation Pattern:
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

---

### File 9: `src/skywatch/spark/station_statistics.py` (Station Summary Stats & Ranking)

- **Target Path:** `src/skywatch/spark/station_statistics.py`
- **Role:** Calculates station summary statistics (24h average, minimum, maximum, count) and assigns pollution rankings identifying cleanest and most polluted hotspots (SPK-01).
- **Data Flow:** Ingests clean air quality records (`clean_air_quality`), outputs `station_statistics` dataset.
- **Closest Analog:** `src/skywatch/pipeline/pig_runner.py` (lines 237–270, `station_stats` aggregation logic).

#### Implementation Pattern:
```python
"""Station statistics and pollution ranking job.

Implements SPK-01: Summary statistics (count, avg, min, max) and deterministic
pollution rank identifying cleanest vs most polluted hotspots.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from skywatch.spark.common import get_spark, read_clean_dataset


def compute_station_statistics(df_clean: DataFrame) -> DataFrame:
    """Compute summary statistics and rank stations by average pollutant concentration."""
    stats_df = (
        df_clean
        .groupBy("station_id", "station_name", "parameter")
        .agg(
            F.count("value").alias("reading_count"),
            F.round(F.avg("value"), 2).alias("average_value"),
            F.round(F.min("value"), 2).alias("minimum_value"),
            F.round(F.max("value"), 2).alias("maximum_value"),
        )
    )

    window_spec = Window.partitionBy("parameter").orderBy(F.desc("average_value"), "station_id")
    ranked_df = stats_df.withColumn("pollution_rank", F.dense_rank().over(window_spec))

    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return ranked_df.withColumn("generated_at_utc", F.lit(now_utc))
```

---

### File 10: `src/skywatch/spark/trend_analysis.py` (Temporal Trend Analysis)

- **Target Path:** `src/skywatch/spark/trend_analysis.py`
- **Role:** Aggregates hourly and daily pollution profiles and flags station peak-pollution hours (SPK-02).
- **Data Flow:** Ingests `clean_air_quality` or `hourly_rollup`, outputs `trends_hourly` and `trends_daily`.
- **Closest Analog:** `src/skywatch/pipeline/pig_runner.py` (lines 271–304, `hourly_rollup` logic).

#### Implementation Pattern:
```python
"""Temporal trend analysis and peak-hour detection.

Implements SPK-02: Hourly and daily trend aggregation with peak-hour detection.
"""
from __future__ import annotations

from typing import Tuple
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def compute_temporal_trends(df_clean: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """Compute hourly and daily trends, tagging peak pollution hour per station."""
    df_with_time = (
        df_clean
        .withColumn("timestamp", F.to_timestamp("measurement_time_utc"))
        .withColumn("hour_of_day", F.hour("timestamp"))
        .withColumn("day_of_week", F.date_format("timestamp", "EEEE"))
    )

    hourly_df = (
        df_with_time
        .groupBy("station_id", "parameter", "hour_of_day")
        .agg(
            F.round(F.avg("value"), 2).alias("average_value"),
            F.count("value").alias("reading_count"),
        )
    )

    window_station = Window.partitionBy("station_id", "parameter")
    max_avg_col = F.max("average_value").over(window_station)
    hourly_trends = hourly_df.withColumn("is_peak_hour", F.col("average_value") == max_avg_col)

    daily_trends = (
        df_with_time
        .groupBy("station_id", "parameter", "day_of_week")
        .agg(
            F.round(F.avg("value"), 2).alias("average_value"),
            F.count("value").alias("reading_count"),
        )
    )

    return hourly_trends, daily_trends
```

---

### File 11: `src/skywatch/spark/weather_analysis.py` (Hourly Weather Join & Pearson Correlation)

- **Target Path:** `src/skywatch/spark/weather_analysis.py`
- **Role:** Joins regularized hourly air quality with Open-Meteo weather metrics on truncated UTC hour, calculating Pearson correlation $r$, sample size $n$, and confidence tier (`High`, `Medium`, `Low`) (SPK-03).
- **Data Flow:** Ingests `hourly_rollup` and normalized `weather`, outputs `weather_correlation`.
- **Closest Analog:** `src/skywatch/data/validation.py` (`validate_weather_record`).

#### Implementation Pattern:
```python
"""Hourly air quality and weather join with Pearson correlation analysis.

Implements SPK-03: Truncated UTC hour join and Pearson correlation r with confidence tiers.
"""
from __future__ import annotations

from typing import List
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def compute_weather_correlation(
    df_air_hourly: DataFrame,
    df_weather: DataFrame,
) -> DataFrame:
    """Join hourly rollups with weather metrics and compute Pearson correlation coefficients."""
    air_joined = df_air_hourly.withColumn(
        "join_hour",
        F.date_trunc("hour", F.to_timestamp("bucket_start_utc")),
    )
    weather_joined = df_weather.withColumn(
        "join_hour",
        F.date_trunc("hour", F.to_timestamp("measurement_time_utc")),
    )

    joined_df = air_joined.join(weather_joined, on="join_hour", how="inner")

    weather_vars = ["temperature_c", "humidity_pct", "wind_speed", "precipitation_mm"]
    correlation_rows = []

    for w_var in weather_vars:
        corr_df = (
            joined_df
            .groupBy("station_id", "parameter")
            .agg(
                F.round(F.corr("average_value", w_var), 4).alias("pearson_r"),
                F.count(F.when(F.col("average_value").isNotNull() & F.col(w_var).isNotNull(), 1)).alias("sample_count"),
            )
            .withColumn("weather_variable", F.lit(w_var))
            .withColumnRenamed("parameter", "pollutant")
        )
        correlation_rows.append(corr_df)

    all_corr_df = correlation_rows[0]
    for cdf in correlation_rows[1:]:
        all_corr_df = all_corr_df.unionByName(cdf)

    return all_corr_df.withColumn(
        "confidence_tier",
        F.when(F.col("sample_count") >= 100, F.lit("High"))
        .when(F.col("sample_count") >= 30, F.lit("Medium"))
        .otherwise(F.lit("Low")),
    )
```

---

### File 12: `src/skywatch/spark/index_job.py` (24-Hour Rolling CPCB AQI Calculation)

- **Target Path:** `src/skywatch/spark/index_job.py`
- **Role:** Calculates 24h rolling PM-based AQI enforcing the $\ge 16$ valid hours threshold, computing PM2.5 and PM10 sub-indices, max sub-index, dominant pollutant, and suppression status (SPK-04).
- **Data Flow:** Ingests regularized `hourly_rollup`, outputs `cpcb_index`.
- **Closest Analog:** `src/skywatch/analytics_core/cpcb.py`.

#### Implementation Pattern:
```python
"""24-Hour Rolling CPCB PM-based Air Quality Index Spark job.

Implements SPK-04: Trailing 24h rolling window, >=16 valid hours threshold,
piecewise linear sub-index interpolation, category assignment, and suppression status.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DoubleType, StringType

from skywatch.analytics_core.cpcb import classify, overall_index, subindex


def compute_cpcb_rolling_index(df_hourly: DataFrame) -> DataFrame:
    """Calculate 24-hour rolling CPCB index with 16h validity gate."""
    df_timed = df_hourly.withColumn("epoch_sec", F.unix_timestamp("bucket_start_utc"))

    window_24h = (
        Window.partitionBy("station_id", "parameter")
        .orderBy("epoch_sec")
        .rangeBetween(-23 * 3600, 0)
    )

    rolling_df = df_timed.withColumn(
        "valid_hours", F.count("average_value").over(window_24h)
    ).withColumn(
        "rolling_24h_avg",
        F.when(F.col("valid_hours") >= 16, F.round(F.avg("average_value").over(window_24h), 1))
        .otherwise(F.lit(None).cast(DoubleType())),
    ).withColumn(
        "suppression_reason",
        F.when(F.col("valid_hours") < 16, F.lit("INSUFFICIENT_VALID_HOURS"))
        .otherwise(F.lit(None).cast(StringType())),
    )

    return rolling_df
```

---

### File 13: `src/skywatch/spark/alert_engine.py` (Alerts & Rolling Median Spike Engine)

- **Target Path:** `src/skywatch/spark/alert_engine.py`
- **Role:** Generates health category alerts (Poor, Very Poor, Severe), pipeline telemetry alerts (stale, offline), and rule-based rolling median spike flags ($reading > 3 \times median$) (SPK-05, SPK-06, SPK-08).
- **Data Flow:** Ingests `cpcb_index`, `hourly_rollup`, and station metadata, outputs `alerts`.
- **Closest Analog:** `src/skywatch/data/validation.py` & `src/skywatch/analytics_core/freshness.py`.

#### Implementation Pattern:
```python
"""Alert generation and rule-based rolling median spike detection engine.

Implements SPK-05, SPK-06, and SPK-08: Health alerts, telemetry alerts, and spike detection.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import List, Optional

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def generate_health_alerts(df_cpcb: DataFrame) -> DataFrame:
    """Produce health category alerts for Poor (201-300), Very Poor (301-400), Severe (401-500)."""
    return (
        df_cpcb
        .filter(F.col("aqi_category").isin(["Poor", "Very Poor", "Severe"]))
        .withColumn("alert_id", F.expr("uuid()"))
        .withColumn(
            "alert_type",
            F.when(F.col("aqi_category") == "Poor", F.lit("category_poor"))
            .when(F.col("aqi_category") == "Very Poor", F.lit("category_very_poor"))
            .otherwise(F.lit("category_severe")),
        )
        .withColumn(
            "severity",
            F.when(F.col("aqi_category") == "Poor", F.lit("WARNING"))
            .when(F.col("aqi_category") == "Very Poor", F.lit("CRITICAL"))
            .otherwise(F.lit("EMERGENCY")),
        )
        .withColumn("timestamp_utc", F.col("window_end_utc"))
        .withColumn("message", F.concat(F.lit("Air Quality Index is "), F.col("aqi_category"), F.lit(" (AQI: "), F.col("aqi_value"), F.lit(")")))
        .withColumn("metric_value", F.col("aqi_value"))
        .select("alert_id", "alert_type", "severity", "station_id", "station_name", "timestamp_utc", "message", "metric_value")
    )


def detect_rolling_median_spikes(df_hourly: DataFrame, multiplier: float = 3.0) -> DataFrame:
    """Detect rule-based spikes where hourly reading > 3.0 * trailing 24h rolling median."""
    df_timed = df_hourly.withColumn("epoch_sec", F.unix_timestamp("bucket_start_utc"))
    window_24h = (
        Window.partitionBy("station_id", "parameter")
        .orderBy("epoch_sec")
        .rangeBetween(-23 * 3600, 0)
    )

    df_spikes = (
        df_timed
        .withColumn("rolling_median", F.expr("percentile_approx(average_value, 0.5)").over(window_24h))
        .withColumn("valid_history", F.count("average_value").over(window_24h))
        .withColumn(
            "is_spike",
            (F.col("valid_history") >= 6) & (F.col("rolling_median") > 0) & (F.col("average_value") > multiplier * F.col("rolling_median")),
        )
    )
    return df_spikes
```

---

### File 14: `src/skywatch/spark/quality_report.py` (Cross-Stage Data Quality Audit)

- **Target Path:** `src/skywatch/spark/quality_report.py`
- **Role:** Aggregates accepted, rejected, and duplicate counts categorized by reason across Ingestion, Pig ETL, and Spark analytics stages (SPK-07).
- **Data Flow:** Ingests manifest, Pig logs, and Spark stage metrics, outputs `quality_report`.
- **Closest Analog:** `src/skywatch/pipeline/logging.py` & `src/skywatch/data/schemas.py`.

#### Implementation Pattern:
```python
"""Data quality audit reporting across pipeline stages.

Implements SPK-07: Summarizes accepted, rejected, and duplicate records by reason.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def build_quality_report(
    spark: SparkSession,
    run_id: str,
    stage_summaries: List[Dict[str, Any]],
) -> DataFrame:
    """Build quality audit DataFrame from stage summaries."""
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    rows = []
    for s in stage_summaries:
        rows.append((
            run_id,
            s["stage"],
            int(s.get("accepted_count", 0)),
            int(s.get("rejected_count", 0)),
            int(s.get("duplicate_count", 0)),
            json.dumps(s.get("reason_counts", {})),
            now_utc,
        ))

    columns = [
        "run_id", "stage", "accepted_count", "rejected_count",
        "duplicate_count", "reason_counts_json", "generated_at_utc"
    ]
    return spark.createDataFrame(rows, columns)
```

---

### File 15: `src/skywatch/spark/export.py` (Dashboard Export Publisher)

- **Target Path:** `src/skywatch/spark/export.py`
- **Role:** Atomic staging directory swap protocol publishing Parquet and CSV alongside sidecar `_schema.json` metadata contracts to `output/published/{dataset}/` (SPK-09).
- **Data Flow:** Consumes analytics DataFrames, atomically writes to disk.
- **Closest Analog:** `src/skywatch/pipeline/pig_runner.py` (lines 64–94, `generate_sidecar_schema`, and lines 96–114, pre-clearing).

#### Implementation Pattern:
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

    staging_dir.parent.mkdir(parents=True, exist_ok=True)
    if staging_dir.exists():
        shutil.rmtree(str(staging_dir))
    staging_dir.mkdir(parents=True)

    try:
        parquet_staging = staging_dir / "parquet"
        df.coalesce(1).write.mode("overwrite").parquet(str(parquet_staging))

        csv_staging = staging_dir / "csv"
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(str(csv_staging))

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

        if os.name == "nt" and target_dir.exists():
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

### File 16: `src/skywatch/pipeline/spark_runner.py` (Spark Orchestrator & Fallback Engine)

- **Target Path:** `src/skywatch/pipeline/spark_runner.py`
- **Role:** Main entrypoint orchestrating all Spark analytics jobs with profile environment detection (`modern` vs `lab`) and seamless pure-Python simulation fallback for zero-JVM/Windows/CI environments.
- **Data Flow:** Ingests clean HDFS / local datasets, runs analytics, coordinates export.
- **Closest Analog:** `src/skywatch/pipeline/pig_runner.py` (lines 43–62 binary check, lines 173–332 fallback simulation, lines 334–476 run orchestrator).

#### Implementation Pattern:
```python
"""Apache Spark runner orchestrator with standalone Python simulation fallback.

Implements SPK-01 through SPK-09 orchestration, profile environment binding,
and transparent offline simulation fallback when JVM/Spark runtime is unavailable.
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import os
import shutil
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from skywatch.analytics_core.cpcb import classify, overall_index, subindex
from skywatch.analytics_core.freshness import freshness_status
from skywatch.pipeline.logging import get_logger, run_context
from skywatch.pipeline.profile import ProfileConfig, load_profile

logger = get_logger("spark_runner")


@dataclass
class SparkRunResult:
    """Execution result container for Spark analytics runs."""
    job_name: str
    returncode: int
    duration_seconds: float
    output_dir: str
    records_processed: int = 0
    success: bool = True
    error_message: Optional[str] = None


def is_spark_available(spark_home: Optional[str] = None) -> bool:
    """Inspect environment to determine if PySpark and Java runtime are executable."""
    try:
        import pyspark  # noqa: F401
    except ImportError:
        return False

    home = spark_home or os.environ.get("SPARK_HOME")
    java_home = os.environ.get("JAVA_HOME")
    if not java_home or not Path(java_home).is_dir():
        return False

    return True


def _run_standalone_spark_fallback(
    input_clean_dir: str,
    input_weather_dir: str,
    output_published_dir: str,
    run_id: str,
) -> List[SparkRunResult]:
    """Execute analytical calculations in pure Python for zero-JVM environments."""
    # 1. Read clean air quality CSV
    # 2. Compute station statistics & rankings
    # 3. Compute temporal hourly and daily trends
    # 4. Join weather and compute Pearson r
    # 5. Compute rolling 24h CPCB AQI and suppression status
    # 6. Generate alerts and rolling median spikes
    # 7. Aggregate data quality audit
    # 8. Export each dataset to output/published/{dataset}/ with _schema.json
    ...
```

---

### File 17: `scripts/make_synthetic.py` (Synthetic Scale Generator)

- **Target Path:** `scripts/make_synthetic.py`
- **Role:** Generates realistic, strictly isolated synthetic datasets at 1× (50k rows), 10× (500k rows), and 100× (5M rows) with diurnal patterns and weather covariance (SPK-10).
- **Data Flow:** Outputs to `data/synthetic/scale_{1x,10x,100x}/`.
- **Closest Analog:** `tests/fixtures/sample_air_quality.csv` & `config/stations.json`.

#### Implementation Pattern:
```python
"""Synthetic scale dataset generator for SkyWatch benchmarking.

Implements SPK-10: Generates 1x (50k), 10x (500k), and 100x (5M) row synthetic datasets
with diurnal curve modeling and meteorological covariance.
Strictly isolated in data/synthetic/ to prevent test contamination.
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path


def generate_synthetic_dataset(
    scale: str,
    target_row_count: int,
    output_dir: Path,
    stations_file: Path = Path("config/stations.json"),
) -> Path:
    """Generate deterministic synthetic air quality records."""
    output_dir.mkdir(parents=True, exist_ok=True)
    out_file = output_dir / f"synthetic_air_quality_{scale}.csv"

    random.seed(42)

    with open(stations_file, "r", encoding="utf-8") as f:
        stations = json.load(f)

    start_time = datetime(2026, 11, 1, 0, 0, 0, tzinfo=timezone.utc)
    rows_written = 0

    with open(out_file, "w", encoding="utf-8", newline="") as fp:
        writer = csv.writer(fp)
        writer.writerow([
            "record_key", "station_id", "station_name", "latitude", "longitude",
            "sensor_id", "parameter", "value", "unit", "measurement_time_utc",
            "ingestion_time_utc", "source", "is_valid", "reject_reason"
        ])

        curr_time = start_time
        while rows_written < target_row_count:
            hour = curr_time.hour
            diurnal_factor = 1.0 + 0.4 * math.sin(2 * math.pi * (hour - 3) / 24)

            for station in stations:
                for param, sinfo in station["sensors"].items():
                    base_val = 35.0 if param == "pm25" else 75.0
                    val = round(base_val * diurnal_factor * random.uniform(0.8, 1.2), 1)
                    rec_key = f"syn_{scale}_{rows_written:08d}"

                    writer.writerow([
                        rec_key,
                        station["station_id"],
                        station["name"],
                        station["latitude"],
                        station["longitude"],
                        sinfo["sensor_id"],
                        param,
                        val,
                        sinfo["units"],
                        curr_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                        (curr_time + timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "synthetic",
                        "true",
                        ""
                    ])
                    rows_written += 1
                    if rows_written >= target_row_count:
                        break
                if rows_written >= target_row_count:
                    break
            curr_time += timedelta(hours=1)

    return out_file
```

---

### File 18: `scripts/run_spark.sh` (Profile-Aware Shell Wrapper)

- **Target Path:** `scripts/run_spark.sh`
- **Role:** CLI execution wrapper script sourcing active profile and triggering `spark_runner.py`.
- **Closest Analog:** `scripts/run_pig.sh` (lines 1–74).

#### Implementation Pattern:
```bash
#!/usr/bin/env bash
# ==============================================================================
# SkyWatch Analytics: Profile-Aware Spark Execution Wrapper
# Sources profile environment variables and launches spark_runner.py.
# Usage:
#   ./scripts/run_spark.sh [PROFILE] [INPUT_CLEAN] [INPUT_WEATHER] [OUTPUT_DIR]
# ==============================================================================

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

PROFILE="${1:-${SKYWATCH_PROFILE:-modern}}"
INPUT_CLEAN="${2:-data/clean/air_quality}"
INPUT_WEATHER="${3:-data/normalized/weather}"
OUTPUT_DIR="${4:-output/published}"

LOG_DIR="$PROJECT_ROOT/data/logs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/spark_runner.log"

trap 'echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] ERROR: Spark execution failed on line $LINENO" >> "$LOG_FILE"' ERR

echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] Starting Spark execution: profile=$PROFILE clean=$INPUT_CLEAN" | tee -a "$LOG_FILE"

PROFILE_FILE="$PROJECT_ROOT/profiles/${PROFILE}.env"
if [ -f "$PROFILE_FILE" ]; then
    echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] Sourcing profile: $PROFILE_FILE" | tee -a "$LOG_FILE"
    set -a
    source "$PROFILE_FILE"
    set +a
fi

python3 -m skywatch.pipeline.spark_runner \
    --input-clean "$INPUT_CLEAN" \
    --input-weather "$INPUT_WEATHER" \
    --output "$OUTPUT_DIR" \
    --profile "$PROFILE" 2>&1 | tee -a "$LOG_FILE"
```

---

### File 19 & 20: `benchmarks/run_scale_benchmark.py` & `benchmarks/results.md` (Scale Benchmarking Suite)

- **Target Path:** `benchmarks/run_scale_benchmark.py` and `benchmarks/results.md`
- **Role:** Measures throughput (rows/sec), execution latency (s), and peak memory (MB) across 1× (50k), 10× (500k), and 100× (5M) row synthetic datasets (SPK-10).
- **Closest Analog:** `src/skywatch/pipeline/pig_runner.py` & `spark_runner.py`.

#### Implementation Pattern:
```python
"""Scale benchmarking suite for SkyWatch ETL and Analytics engines.

Implements SPK-10: Benchmarks Pig clean, Spark stats, and Spark AQI across
1x (50k), 10x (500k), and 100x (5M) row volumes.
Measures latency, throughput, and peak memory via psutil.
"""
from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Dict, List
import psutil

from scripts.make_synthetic import generate_synthetic_dataset
from skywatch.pipeline.pig_runner import run_pig_script
from skywatch.pipeline.spark_runner import SparkRunResult


def benchmark_scale(scale: str, row_count: int, out_dir: Path) -> Dict[str, float]:
    """Execute benchmark run and track execution metrics."""
    process = psutil.Process(os.getpid())
    start_rss = process.memory_info().rss / (1024 * 1024)

    start_time = time.monotonic()

    dataset_path = generate_synthetic_dataset(scale, row_count, out_dir / "input")

    peak_rss = process.memory_info().rss / (1024 * 1024)
    duration = time.monotonic() - start_time
    throughput = row_count / duration if duration > 0 else 0.0

    return {
        "scale": scale,
        "rows": row_count,
        "duration_sec": round(duration, 2),
        "throughput_rows_sec": round(throughput, 1),
        "peak_rss_mb": round(peak_rss, 1),
    }
```

---

### File 21: `tests/unit/test_cpcb.py` (CPCB Math Unit Tests)

- **Target Path:** `tests/unit/test_cpcb.py`
- **Role:** Comprehensive verification of CPCB linear interpolation arithmetic, breakpoint bands, 16h validity gate, suppression codes, and partial status (UT-13 to UT-19).
- **Closest Analog:** `tests/unit/test_validation.py` (lines 30–67, numeric range & ceiling tests).

#### Implementation Pattern:
```python
"""Unit tests for CPCB PM-based Air Quality Index calculation.

Implements UT-13 through UT-19: Breakpoints, interpolation arithmetic,
16h valid hours threshold, category assignment, and partial index handling.
"""
from __future__ import annotations

import pytest
from skywatch.analytics_core.cpcb import classify, is_valid_24h, overall_index, subindex


def test_ut14_pm25_piecewise_linear_interpolation():
    """UT-14: Test PM2.5 = 45.5 ug/m3 produces index 75.5 (Satisfactory)."""
    val = subindex("pm25", 45.5)
    assert val == 75.5
    assert classify(val) == "Satisfactory"


def test_ut15_pm25_boundary_value():
    """UT-15: Test PM2.5 = 61.0 ug/m3 produces index 101.0 (Moderate)."""
    val = subindex("pm25", 61.0)
    assert val == 101.0
    assert classify(val) == "Moderate"


def test_ut16_pm10_interpolation():
    """UT-16: Test PM10 breakpoint calculations."""
    val_100 = subindex("pm10", 100.0)
    assert val_100 == 100.0
    assert classify(val_100) == "Satisfactory"

    val_251 = subindex("pm10", 251.0)
    assert val_251 == 201.0
    assert classify(val_251) == "Poor"


def test_ut17_invalid_or_negative_concentration():
    """UT-17: Test negative or null concentrations return None."""
    assert subindex("pm25", -5.0) is None
    assert subindex("pm25", None) is None
    assert classify(None) is None


def test_ut18_valid_hours_threshold():
    """UT-18: Test >=16 valid hours requirement."""
    assert is_valid_24h(16) is True
    assert is_valid_24h(24) is True
    assert is_valid_24h(15) is False
    assert is_valid_24h(0) is False


def test_ut19_partial_index():
    """UT-19: Test partial index when only one pollutant is available."""
    idx, cat, dominant, is_partial = overall_index(pm25_sub=75.5, pm10_sub=None)
    assert idx == 75.5
    assert cat == "Satisfactory"
    assert dominant == "pm25"
    assert is_partial is True
```

---

### File 22: `tests/unit/test_freshness.py` (Freshness Classifier Unit Tests)

- **Target Path:** `tests/unit/test_freshness.py`
- **Role:** Tests exact boundary seconds for freshness categorization (UT-20).
- **Closest Analog:** `tests/unit/test_validation.py` (lines 69–90).

#### Implementation Pattern:
```python
"""Unit tests for station freshness classification.

Implements UT-20: Boundary condition tests at 3h, 3h 1s, 24h, and 24h 1s.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from skywatch.analytics_core.freshness import freshness_status


def test_ut20_freshness_boundaries():
    """UT-20: Test freshness transitions across exact boundary seconds."""
    ref_time = datetime(2026, 11, 22, 12, 0, 0, tzinfo=timezone.utc)

    ts_3h = (ref_time - timedelta(hours=3)).strftime("%Y-%m-%dT%H:%M:%SZ")
    assert freshness_status(ts_3h, reference_time_utc=ref_time) == "Fresh"

    ts_3h_1s = (ref_time - timedelta(hours=3, seconds=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    assert freshness_status(ts_3h_1s, reference_time_utc=ref_time) == "Stale"

    ts_24h = (ref_time - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ")
    assert freshness_status(ts_24h, reference_time_utc=ref_time) == "Stale"

    ts_24h_1s = (ref_time - timedelta(hours=24, seconds=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    assert freshness_status(ts_24h_1s, reference_time_utc=ref_time) == "Offline"

    assert freshness_status("", reference_time_utc=ref_time) == "Offline"
```

---

### File 27: `tests/contract/test_spark_contracts.py` (Contract Tests)

- **Target Path:** `tests/contract/test_spark_contracts.py`
- **Role:** Verifies that PySpark `StructType` schemas match `config/schemas.yaml` contracts exactly (CT-02).
- **Closest Analog:** `tests/contract/test_pig_contracts.py` (lines 44–85).

#### Implementation Pattern:
```python
"""Contract tests for PySpark StructType schemas and canonical schemas.yaml alignment.

Implements CT-02: Asserts that build_spark_schema() constructs field names, order,
nullability, and mapped types that exactly match config/schemas.yaml.
"""
from __future__ import annotations

from skywatch.data.schemas import get_schema
from skywatch.spark.common import TYPE_MAP, build_spark_schema


def test_ct02_clean_air_quality_spark_contract():
    """Assert clean_air_quality Spark schema matches canonical contract."""
    spark_schema = build_spark_schema("clean_air_quality")
    canonical = get_schema("clean_air_quality")

    assert len(spark_schema.fields) == len(canonical["columns"])
    for f, c in zip(spark_schema.fields, canonical["columns"]):
        assert f.name == c["name"]
        expected_type = TYPE_MAP[c["type"]]
        assert f.dataType == expected_type


def test_ct02_published_datasets_spark_contracts():
    """Assert all 7 Phase 3 analytical published entities match schemas.yaml."""
    entities = [
        "station_statistics",
        "trends_hourly",
        "trends_daily",
        "weather_correlation",
        "cpcb_index",
        "alerts",
        "quality_report",
    ]
    for ent in entities:
        spark_schema = build_spark_schema(ent)
        canonical = get_schema(ent)
        assert len(spark_schema.fields) == len(canonical["columns"]), f"Column mismatch in {ent}"
        for f, c in zip(spark_schema.fields, canonical["columns"]):
            assert f.name == c["name"]
```

---

### File 28: `tests/integration/test_spark_analytics.py` (End-to-End Spark Integration Tests)

- **Target Path:** `tests/integration/test_spark_analytics.py`
- **Role:** Runs full analytics pipeline on frozen sample data; asserts published outputs match golden reference files within $\epsilon < 10^{-4}$ (SP-01 to SP-10).
- **Closest Analog:** `tests/integration/test_pig_e2e.py` (lines 17–100).

#### Implementation Pattern:
```python
"""End-to-end integration tests for Apache Spark analytics engine and golden alignment.

Implements SP-01 through SP-10: Verifies station statistics, temporal trends,
weather correlation, CPCB AQI, and alerts match golden reference datasets within epsilon < 1e-4.
"""
from __future__ import annotations

import csv
import json
from pathlib import Path

from skywatch.pipeline.spark_runner import _run_standalone_spark_fallback


def _read_csv_dict(path: Path):
    with open(path, "r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def test_spark_analytics_end_to_end_matches_golden(tmp_path):
    """Run full analytics pipeline and assert published outputs match golden datasets."""
    clean_input = "tests/fixtures/sample_air_quality.csv"
    weather_input = "tests/fixtures/sample_weather_archive.json"
    published_dir = tmp_path / "published"

    results = _run_standalone_spark_fallback(
        input_clean_dir=clean_input,
        input_weather_dir=weather_input,
        output_published_dir=str(published_dir),
        run_id="test_run_01",
    )

    for res in results:
        assert res.success is True

    # 1. Verify Station Statistics matches golden
    actual_stats = _read_csv_dict(published_dir / "station_statistics" / "csv" / "part-00000.csv")
    golden_stats = _read_csv_dict(Path("tests/fixtures/golden/golden_station_statistics.csv"))
    assert len(actual_stats) == len(golden_stats)
    for act, gld in zip(actual_stats, golden_stats):
        assert act["station_id"] == gld["station_id"]
        assert act["parameter"] == gld["parameter"]
        assert abs(float(act["average_value"]) - float(gld["average_value"])) < 1e-4
        assert int(act["pollution_rank"]) == int(gld["pollution_rank"])

    # 2. Verify CPCB AQI matches golden
    actual_aqi = _read_csv_dict(published_dir / "cpcb_index" / "csv" / "part-00000.csv")
    golden_aqi = _read_csv_dict(Path("tests/fixtures/golden/golden_cpcb_index.csv"))
    assert len(actual_aqi) == len(golden_aqi)
    for act, gld in zip(actual_aqi, golden_aqi):
        assert act["station_id"] == gld["station_id"]
        if act["aqi_value"]:
            assert abs(float(act["aqi_value"]) - float(gld["aqi_value"])) < 1e-4
```

---

## 4. Golden Reference Fixture Patterns

To ensure deterministic verification in both CI and local developer setups, 7 golden reference files must be established in `tests/fixtures/golden/`:

| Fixture File | Target Schema | Key Assertions / Columns | Reference Source |
| :--- | :--- | :--- | :--- |
| `golden_station_statistics.csv` | `station_statistics` | `station_id,station_name,parameter,reading_count,average_value,minimum_value,maximum_value,pollution_rank,generated_at_utc` | Generated from `sample_air_quality.csv` |
| `golden_trends_hourly.csv` | `trends_hourly` | `station_id,parameter,hour_of_day,average_value,reading_count,is_peak_hour` | Hourly diurnal distribution |
| `golden_trends_daily.csv` | `trends_daily` | `station_id,parameter,day_of_week,average_value,reading_count` | Day of week aggregation |
| `golden_weather_correlation.csv`| `weather_correlation`| `station_id,pollutant,weather_variable,pearson_r,sample_count,confidence_tier` | Weather join with Pearson $r$ |
| `golden_cpcb_index.csv` | `cpcb_index` | `station_id,station_name,window_end_utc,pm25_24h_avg,pm25_subindex,pm10_24h_avg,pm10_subindex,aqi_value,aqi_category,dominant_pollutant,is_partial,suppression_reason` | 24h rolling CPCB sub-index math |
| `golden_alerts.csv` | `alerts` | `alert_id,alert_type,severity,station_id,station_name,timestamp_utc,message,metric_value` | Poor/Severe & spike alerts |
| `golden_quality_report.csv` | `quality_report` | `run_id,stage,accepted_count,rejected_count,duplicate_count,reason_counts_json,generated_at_utc` | Stage audit summary counts |

---

## 5. Anti-Patterns & Common Traps

| # | Anti-Pattern | Why It Breaks | How to Avoid |
|---|---|---|---|
| **AP-01** | Using `inferSchema=true` on CSV readers | Headerless clean CSV has no headers; Spark guesses column types incorrectly and assigns generic `_c0, _c1` names. | Always pass explicit `StructType` constructed by `build_spark_schema(entity_name)`. |
| **AP-02** | Default 200 shuffle partitions | Spins up 200 tasks for every groupby and join, exhausting JVM heap on 4 GB RAM laptops and causing CI timeouts. | Set `spark.sql.shuffle.partitions=2` explicitly in `common.py:get_spark()`. |
| **AP-03** | Relying on ANSI mode default | Spark 4 turns ANSI on by default; invalid casts throw fatal exceptions rather than returning `null`. | Set `spark.sql.ansi.enabled=false` on every SparkSession builder. |
| **AP-04** | Off-by-one in 24h trailing window | Using range bound `-24 * 3600` to `0` includes 25 discrete hourly buckets, distorting rolling averages. | Use exact `-23 * 3600` to `0` seconds range bounds. |
| **AP-05** | Direct writing to `output/published/` | Web dashboard reads incomplete part-files while Spark job is in-flight, causing crashes. | Write to `.staging_{dataset}_{run_id}/`, validate integrity, and perform atomic directory rename. |
| **AP-06** | Contaminating results with synthetic data | Mixing benchmark data into production analytical tables violates honest civic reporting. | Isolate synthetic data strictly to `data/synthetic/` and tag records with `source="synthetic"`. |
| **AP-07** | Using Python 3.10+ syntax | `match/case` or `int | str` causes syntax errors on Python 3.8/3.9 lab profile runtimes. | Stick strictly to `from __future__ import annotations`, `typing.Optional`, and `typing.Union`. |

---

## PATTERN MAPPING COMPLETE
**Phase:** 3 - Analytics Engine with Apache Spark & Scale Benchmarking  
**Files analyzed:** 30  
**File Created:** `.planning/phases/03-analytics-engine-with-apache-spark-scale-benchmarking/03-PATTERNS.md`
