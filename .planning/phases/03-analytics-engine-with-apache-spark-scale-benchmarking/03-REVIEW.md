---
phase: 03-analytics-engine-with-apache-spark-scale-benchmarking
reviewed: 2026-11-23T01:10:00Z
depth: standard
files_reviewed: 20
files_reviewed_list:
  - config/breakpoints.yaml
  - config/schemas.yaml
  - config/settings.yaml
  - src/skywatch/analytics_core/__init__.py
  - src/skywatch/analytics_core/cpcb.py
  - src/skywatch/analytics_core/freshness.py
  - src/skywatch/pipeline/spark_runner.py
  - src/skywatch/spark/__init__.py
  - src/skywatch/spark/alert_engine.py
  - src/skywatch/spark/common.py
  - src/skywatch/spark/export.py
  - src/skywatch/spark/index_job.py
  - src/skywatch/spark/quality_report.py
  - src/skywatch/spark/station_statistics.py
  - src/skywatch/spark/trend_analysis.py
  - src/skywatch/spark/weather_analysis.py
  - scripts/run_spark.sh
  - scripts/make_synthetic.py
  - scripts/make_sample_weather.py
  - benchmarks/run_scale_benchmark.py
findings:
  critical: 0
  warning: 5
  info: 2
  total: 7
status: issues_found
---

# Phase 3: Code Review Report

**Reviewed:** 2026-11-23T01:10:00Z  
**Depth:** standard  
**Files Reviewed:** 20  
**Status:** issues_found  

## Summary

Phase 3 delivers an end-to-end analytical processing engine across PySpark and pure-Python standalone fallbacks. The implementation satisfies core algorithmic requirements including CPCB 24h rolling AQI with suppression reasoning, Pearson weather correlation, multi-tier alerting, atomic directory staging swaps, and synthetic scale benchmarking.

An adversarial review of all 20 changed source, script, and configuration files identified **5 Warnings** and **2 Info** findings. Key concerns include a bash argument shift defect in `scripts/run_spark.sh` that breaks default execution, non-deterministic `inferSchema=true` usage in `spark_runner.py`, ranking tie-breaking divergence between PySpark and Python fallbacks in `station_statistics.py`, join multiplication risks in `weather_analysis.py`, and breakpoint category bypassing in `cpcb.py`. Zero Critical/Blocker security or crash defects were detected.

---

## Warnings

### WR-01: Positional Argument Shift Defect in `scripts/run_spark.sh`

**File:** `scripts/run_spark.sh:20-22`  
**Issue:** When `run_spark.sh` is invoked with fewer than 4 arguments (e.g. `./scripts/run_spark.sh modern`), `shift 4 2>/dev/null || true` fails and does not shift any arguments. Consequently, `EXTRA_FLAGS="$*"` preserves the initial arguments (`modern`), which are appended as trailing positional arguments to `python -m skywatch.pipeline.spark_runner`. Because `spark_runner.py`'s `argparse` does not define positional arguments, execution fails immediately with `argparse: error: unrecognized arguments: modern`.  
**Fix:**
```bash
if [ "$#" -ge 4 ]; then
    shift 4
    EXTRA_FLAGS="$*"
else
    EXTRA_FLAGS=""
fi
```

### WR-02: Non-Deterministic `inferSchema=true` in `spark_runner.py`

**File:** `src/skywatch/pipeline/spark_runner.py:526`  
**Issue:** Native PySpark execution loads normalized weather data using `.option("inferSchema", "true")`:
```python
df_weather = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(str(weather_path))
```
This violates repository contract-first rules (`config/schemas.yaml` is the single source of truth) and introduces an unnecessary distributed scan of the weather dataset before execution, increasing I/O latency and risking column type mismatches.  
**Fix:**
```python
from skywatch.spark.common import read_clean_dataset
df_weather = read_clean_dataset(spark, str(weather_path), "weather")
```

### WR-03: Ranking Window Tie-Breaker Discrepancy in `station_statistics.py`

**File:** `src/skywatch/spark/station_statistics.py:54-61`  
**Issue:** In `station_statistics.py`, the ranking window is defined as:
```python
ranking_window = Window.partitionBy("parameter").orderBy(
    col("average_value").desc(),
    col("station_id").asc(),
)
```
Because `station_id.asc()` is included in the window's `orderBy` clause, every station receives a unique ordering key, preventing `dense_rank()` from ever assigning equal ranks to stations with identical pollution averages. In contrast, the standalone simulation fallback (`spark_runner.py:206-212`) awards identical ranks to tied averages.  
**Fix:**
Define the ranking window on `average_value` only, deferring `station_id` tie-breaking to the final DataFrame `.orderBy()`:
```python
ranking_window = Window.partitionBy("parameter").orderBy(col("average_value").desc())
df_ranked = df_stats.withColumn(
    "pollution_rank",
    dense_rank().over(ranking_window).cast("integer"),
)
return df_ranked.select(...).orderBy("parameter", "pollution_rank", "station_id")
```

### WR-04: Cartesian Explosion Risk on Weather Join in `weather_analysis.py`

**File:** `src/skywatch/spark/weather_analysis.py:91-100`  
**Issue:** `compute_weather_correlation()` truncates timestamps to hour and inner joins `df_air` with `df_weather`. If `df_weather` contains multiple observations for the same hour (e.g. from overlapping incremental collections or historical backfills), the join duplicates the air quality records for that hour. This artificially inflates `sample_count` and distorts the Pearson correlation coefficient. In contrast, `spark_runner.py:338` keys weather by timestamp in a dictionary, automatically deduplicating.  
**Fix:**
Deduplicate `df_w` by `join_hour` prior to the join:
```python
df_w_dedup = df_w.dropDuplicates(["join_hour"])
df_joined = df_air.join(df_w_dedup, on="join_hour", how="inner")
```

### WR-05: Parameter Bypass and Integer Gap Handling in `cpcb.py:classify()`

**File:** `src/skywatch/analytics_core/cpcb.py:132-142`  
**Issue:** In `classify(index_value, breakpoints=None)`, the dynamic category loop is gated behind `if breakpoints is None:`. When a caller explicitly passes a `breakpoints` dictionary, this block is skipped entirely, causing `classify()` to ignore the provided dictionary and fall through to hardcoded default thresholds. Furthermore, `config/breakpoints.yaml` specifies integer bounds (`[0, 50]`, `[51, 100]`), causing fractional values (e.g. `50.5`) to fail `i_min <= val <= i_max` in the loop.  
**Fix:**
```python
bp = breakpoints if breakpoints is not None else load_breakpoints()
categories = bp.get("categories", [])
for cat in categories:
    i_min = float(cat["index_min"])
    i_max = float(cat["index_max"])
    if i_min <= val <= i_max or (val < i_min and val > i_min - 1.0):
        return str(cat["name"])
```

---

## Info

### IN-01: Driver-Side `collect()` in `compute_cpcb_rolling_index()`

**File:** `src/skywatch/spark/index_job.py:196-197`  
**Issue:** `compute_cpcb_rolling_index()` collects all hourly rollup records to the Spark driver (`df_hourly.collect()`) to run the pure-Python `calculate_cpcb_rolling_index_records()` function before converting back to a DataFrame. While this guarantees exact numerical parity with the simulation fallback on small datasets, it bypasses Spark's distributed execution and introduces memory pressure on large multi-station datasets.  
**Fix:** Re-implement the 24-hour rolling window calculation using native PySpark DataFrame window functions (`Window.partitionBy("station_id", "parameter").orderBy("ts").rangeBetween(-23 * 3600, 0)`) for large production clusters.

### IN-02: CSV Fallback for Parquet in `export_published_dataset()`

**File:** `src/skywatch/spark/export.py:112`  
**Issue:** When `pyarrow` is not installed, `export_published_dataset()` copies the CSV output into `part-00000.parquet`. Downstream tools expecting binary Parquet magic bytes (`PAR1`) will encounter parse errors if attempting to read this fallback file with Parquet readers.  
**Fix:** Log a visible warning when PyArrow is absent, or omit creating the `parquet/` subdirectory when binary Parquet cannot be generated.

---

_Reviewed: 2026-11-23T01:10:00Z_  
_Reviewer: Antigravity (gsd-code-reviewer)_  
_Depth: standard_
