---
last_mapped_commit: 831a73a6e8025d4970ff7b506be5de6d304b4c61
last_mapped_at: 2026-11-23
---
# Testing Patterns

**Analysis Date:** 2026-11-23

## Test Framework

**Runner:**
- `pytest >= 7.0.0` configured in `pyproject.toml`:
  ```toml
  [tool.pytest.ini_options]
  testpaths = ["tests"]
  python_files = ["test_*.py"]
  pythonpath = ["src"]
  ```
- `pythonpath = ["src"]` enables running tests directly against the local tree without package installation.

**Mocking & Interception:**
- `responses >= 0.23.0` for intercepting and asserting HTTP requests to OpenAQ and Open-Meteo APIs.
- `unittest.mock` for mocking subprocess calls, filesystem boundaries, and external commands.

## Test File Organization

```text
tests/
├── contract/                    # Contract and schema compliance tests
│   ├── test_pig_contracts.py    # Validates Pig headerless output & sidecar schemas
│   ├── test_schemas.py          # Validates config/schemas.yaml completeness & integrity
│   └── test_spark_contracts.py  # Validates all 7 published Spark dataset schemas
├── fixtures/                    # Mock responses and frozen datasets
│   ├── golden/                  # Golden truth reference CSVs for regression testing
│   │   ├── golden_clean_air_quality.csv
│   │   ├── golden_cpcb_index.csv
│   │   ├── golden_station_statistics.csv
│   │   ├── golden_trends_daily.csv
│   │   ├── golden_trends_hourly.csv
│   │   └── golden_weather_correlation.csv
│   ├── sample_air_quality.csv
│   ├── sample_openaq_latest.json
│   ├── sample_openaq_locations.json
│   ├── sample_openaq_measurements.json
│   ├── sample_weather_archive.json
│   └── sample_weather_forecast.json
├── integration/                 # End-to-end and performance test suites
│   ├── test_collector_e2e.py    # Full ingestion cycle with mock APIs
│   ├── test_pig_e2e.py          # End-to-end Pig ETL pipeline
│   ├── test_scale_benchmark.py  # 1x, 10x, 100x scale benchmark performance tests
│   ├── test_spark_analytics.py  # End-to-end Spark analytics pipeline
│   ├── test_storage.py          # LocalStorage and HDFSStorageAdapter operations
│   └── test_watermark.py        # Watermark persistence and concurrency tests
└── unit/                        # Granular unit test suites
    ├── test_alert_engine.py     # CPCB threshold & median spike alerts
    ├── test_clients.py          # OpenAQ & Weather HTTP client retries/errors
    ├── test_cpcb.py             # CPCB sub-index math & suppression codes
    ├── test_deduplication.py    # Primary key deduplication
    ├── test_discovery.py        # Station discovery filtering & persistence
    ├── test_export_publisher.py # Atomic staging swap export mechanics
    ├── test_freshness.py        # Fresh/stale/offline classification
    ├── test_logging.py          # JSONFormatter and run_id propagation
    ├── test_normalization.py    # Field normalization and type casting
    ├── test_pig_runner.py       # Pig Latin runner orchestration & fallback
    ├── test_profile.py          # Modern/lab profile loading & validation
    ├── test_quality_report.py   # Multi-stage data quality auditing
    ├── test_raw_manifest.py     # Audit manifest append operations
    ├── test_validation.py       # Numeric bounds & invalid reason flagging
    └── test_weather_correlation.py # Pearson r correlation calculation
```

## Test Structure

Standard Arrange-Act-Assert pattern:

```python
def test_cpcb_subindex_calculation():
    # Arrange
    val = 45.2
    pollutant = "pm25"

    # Act
    sub = subindex(pollutant, val)

    # Assert
    assert sub is not None
    assert 50 <= sub <= 100
```

## Mocking

- **HTTP Requests:** Intercepted with `@responses.activate`. HTTP endpoints simulate successful payloads, error status codes (401, 429, 500), and rate-limit headers.
- **Hadoop & Spark:** Subprocesses (`subprocess.run`) are mocked when testing cluster execution paths, while standalone pure-Python fallbacks are exercised to verify algorithmic logic without JVM overhead.

## Fixtures and Factories

- **Sample Fixtures:** Located in `tests/fixtures/sample_*.json` representing realistic OpenAQ and Open-Meteo responses.
- **Golden Datasets:** Located in `tests/fixtures/golden/` providing baseline verified outputs for Pig and Spark transformations. Tests compare calculated results against golden CSVs using tight numerical tolerances (tolerance `0.15` to account for single-decimal rounding variances).
- **Synthetic Scale Generator:** `scripts/make_synthetic.py` deterministically generates synthetic air quality records with diurnal curves for benchmark testing.

## Coverage

- All public functions and classes across `src/skywatch/` are covered by targeted unit and contract tests.
- High branch coverage across fallback execution paths (`pig_runner.py`, `spark_runner.py`).

## Test Types

1. **Unit Tests (`tests/unit/`):** Fast, isolated tests verifying single functions or classes with zero network or filesystem dependencies.
2. **Contract Tests (`tests/contract/`):** Strict verification that all emitted datasets match column names, types, and constraints defined in `config/schemas.yaml`.
3. **Integration Tests (`tests/integration/`):** Multi-step pipeline execution verifying end-to-end data flow from raw ingestion to Pig ETL, Spark analytics, and atomic publishing.
4. **Scale Benchmark Tests (`tests/integration/test_scale_benchmark.py`):** Performance stress testing on synthetic datasets (1x: 50k rows, 10x: 500k rows, 100x: 5M rows) measuring execution duration, memory consumption (`psutil`), and throughput.

## CI Pipeline

- Configured in `.github/workflows/ci.yml`.
- Runs on Ubuntu latest with Python 3.11.
- Steps:
  1. Ruff linting check (`ruff check .`)
  2. Dependency installation (`pip install -e .` with optional dev dependencies, pyarrow, pyspark)
  3. Pytest suite execution (`pytest -v`) with failure annotations.
- Scheduled collection is verified via `.github/workflows/scheduled-collect.yml`.

## Common Patterns

- Use `tmp_path` fixture for filesystem tests to avoid polluting the repository tree.
- Validate dataset rows against `config/schemas.yaml` using `skywatch.data.schemas.validate_record()`.
- Use deterministic seeds when generating synthetic data for repeatable benchmark comparisons.
