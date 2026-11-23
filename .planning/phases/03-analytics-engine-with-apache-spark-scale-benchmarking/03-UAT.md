---
status: testing
phase: 03-analytics-engine-with-apache-spark-scale-benchmarking
source:
  - .planning/phases/03-analytics-engine-with-apache-spark-scale-benchmarking/03-01-SUMMARY.md
  - .planning/phases/03-analytics-engine-with-apache-spark-scale-benchmarking/03-02-SUMMARY.md
  - .planning/phases/03-analytics-engine-with-apache-spark-scale-benchmarking/03-03-SUMMARY.md
started: 2026-11-23T23:35:00+05:30
updated: 2026-11-23T23:35:00+05:30
---

## Current Test

number: 1
name: Station Summary Statistics & Pollution Rankings
expected: |
  Executing station statistics calculation (via PySpark or simulation runner) aggregates reading counts, averages, minimums, and maximums per parameter across monitoring stations, and assigns dense pollution rank integers starting from 1 for the most polluted station.
awaiting: user response

## Tests

### 1. Station Summary Statistics & Pollution Rankings
expected: Executing station statistics calculation (via PySpark or simulation runner) aggregates reading counts, averages, minimums, and maximums per parameter across monitoring stations, and assigns dense pollution rank integers starting from 1 for the most polluted station.
result: [pending]

### 2. Hourly & Daily Temporal Trends with Peak Hour Detection
expected: Temporal trend analysis groups observations by hour of day and date, calculating mean pollution profiles and accurately flagging the diurnal peak pollution hour with is_peak_hour=True.
result: [pending]

### 3. Weather Correlation Join & Confidence Tiers
expected: Hourly air quality observations join with weather metrics on truncated UTC hour to compute Pearson correlation coefficient r and sample size n, categorizing confidence into High (n >= 100), Medium (30 <= n < 100), or Low (n < 30).
result: [pending]

### 4. 24-Hour Rolling CPCB AQI & Valid Hours Rule Suppression
expected: Trailing 24-hour CPCB AQI calculation evaluates whether at least 16 valid hourly readings exist; when >= 16h valid, it computes piecewise linear sub-indices and overall AQI category, and when < 16h, it suppresses output with explicit code INSUFFICIENT_VALID_HOURS.
result: [pending]

### 5. Alert Engine for Health Categories & Rolling Median Spikes
expected: Alert generation triggers health alerts for Poor, Very Poor, and Severe categories, pipeline telemetry alerts for Stale (>3h) and Offline (>24h) stations, and flags anomalous 3x rolling median spikes over 24-hour trailing windows.
result: [pending]

### 6. Atomic Dataset Publishing with Staging Directory Swaps & Schema Sidecars
expected: The export publisher writes output datasets into a temporary staging folder, atomically renames to output/published/{dataset}/ containing dual Parquet and CSV formats, and generates an authoritative _schema.json metadata sidecar.
result: [pending]

### 7. Synthetic Scale Benchmarking & Civic Isolation
expected: Running scale benchmarks generates isolated synthetic data tagged with source="synthetic" under data/synthetic/, measures row throughput and peak memory usage, and records execution metrics in benchmarks/results.md without contaminating production datasets.
result: [pending]

## Summary

total: 7
passed: 0
issues: 0
pending: 7
skipped: 0
blocked: 0

## Gaps

[none yet]
