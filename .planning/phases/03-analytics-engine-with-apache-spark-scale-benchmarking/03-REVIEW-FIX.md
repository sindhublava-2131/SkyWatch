---
phase: 03-analytics-engine-with-apache-spark-scale-benchmarking
iteration: 1
findings_in_scope: 5
fixed: 5
skipped: 0
status: all_fixed
---

# Phase 3: Code Review Fix Report

**Phase:** `03-analytics-engine-with-apache-spark-scale-benchmarking`  
**Iteration:** 1  
**Scope:** `critical_warning`  
**Findings in Scope:** 5  
**Fixed:** 5  
**Skipped:** 0  
**Status:** `all_fixed`  

## Summary

Applied intelligent atomic fixes to all 5 Warning findings identified during the Phase 3 code review. No Critical/Blocker findings were present, and all targeted fixes were verified with syntax checks, unit tests, and minimal git diffs.

---

## Applied Fixes

### WR-01: Positional Argument Shift Defect in `scripts/run_spark.sh`
- **File:** `scripts/run_spark.sh:17-25`
- **Commit:** `7bd01de` (`fix(03-spark): resolve positional argument shift defect in run_spark.sh`)
- **Fix:** Guarded `shift 4` behind `if [ "$#" -ge 4 ]; then shift 4; EXTRA_FLAGS="$*"; else EXTRA_FLAGS=""; fi` to prevent argument pollution when invoking the script with default positional parameters.
- **Verification:** Verified with `bash -n scripts/run_spark.sh` and argument parsing assertions.

### WR-02: Non-Deterministic `inferSchema=true` in `spark_runner.py`
- **File:** `src/skywatch/pipeline/spark_runner.py:526`
- **Commit:** `b227610` (`fix(03-spark): enforce canonical schema reader for weather dataset without inferSchema`)
- **Fix:** Replaced `.option("inferSchema", "true")` with `read_clean_dataset(spark, str(weather_path), "weather")` to enforce the canonical contract from `config/schemas.yaml` without full dataset pre-scanning.
- **Verification:** Python compilation and schema contract validation.

### WR-03: Ranking Window Tie-Breaker Discrepancy in `station_statistics.py`
- **File:** `src/skywatch/spark/station_statistics.py:50-78`
- **Commit:** `4d4e122` (`fix(03-spark): align dense ranking window tie-breaking in station_statistics`)
- **Fix:** Removed `station_id.asc()` from the `dense_rank()` window partition, ordering only by `average_value.desc()` so tied averages receive identical ranks, and applying deterministic tie-breaking in the outer DataFrame `.orderBy()`.
- **Verification:** Python syntax check and alignment with `spark_runner.py` ranking logic.

### WR-04: Cartesian Explosion Risk on Weather Join in `weather_analysis.py`
- **File:** `src/skywatch/spark/weather_analysis.py:90-103`
- **Commit:** `a6902f7` (`fix(03-spark): deduplicate weather observations on join hour to avoid cartesian join expansion`)
- **Fix:** Added `.dropDuplicates(["join_hour"])` to `df_w` before performing the inner join with `df_air` to prevent row duplication on multiple weather observations for the same hour.
- **Verification:** AST parsing and DataFrame join verification.

### WR-05: Parameter Bypass and Integer Gap Handling in `cpcb.py:classify()`
- **File:** `src/skywatch/analytics_core/cpcb.py:129-148`
- **Commit:** `eb96eed` (`fix(03-cpcb): support explicit breakpoints dictionary and continuous interval matching in classify`)
- **Fix:** Checked `bp = breakpoints if breakpoints is not None else load_breakpoints()` so caller-provided configurations are respected, and expanded boundary evaluation to cover continuous fractional gaps (e.g. `50.5`).
- **Verification:** Unit tests with and without explicit breakpoint dictionaries.
