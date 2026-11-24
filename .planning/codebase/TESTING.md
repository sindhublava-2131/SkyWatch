---
last_mapped_commit: 6b4cc6dd01b87e70230978c94258f457e4b5fcb4
last_mapped_at: 2026-11-24
---
# Testing Patterns

**Analysis Date:** 2026-11-24

## Test Frameworks

**Backend Test Runner:**
- `pytest >= 7.0.0` configured in `pyproject.toml`:
  ```toml
  [tool.pytest.ini_options]
  testpaths = ["tests"]
  python_files = ["test_*.py"]
  pythonpath = ["src"]
  ```
- `pythonpath = ["src"]` enables running tests directly against the local source tree without packaging or installation.

**Mocking & Interception:**
- `responses >= 0.23.0` for intercepting and asserting HTTP requests to OpenAQ and Open-Meteo APIs.
- `unittest.mock` for mocking subprocess calls, filesystem boundaries, and external commands.

**Coverage Gate:**
- `pytest-cov >= 4.0.0` enforcing an 80% coverage floor on core business logic modules:
  - `skywatch.data.validation` (88%)
  - `skywatch.data.normalization` (85%)
  - `skywatch.data.deduplication` (91%)
  - `skywatch.analytics_core.cpcb` (81%)
  - `skywatch.analytics_core.freshness` (84%)
  - Total core logic coverage: 84.79% (exceeds 80% gate).
  - Executed via: `make coverage PYTHON=.venv/bin/python`

**Frontend Validation & Mocks:**
- TypeScript strict compiler verification (`pnpm --dir frontend build` / `tsc --noEmit`).
- Static offline fixtures in `frontend/lib/mocks/index.ts` enable deterministic frontend previewing with `NEXT_PUBLIC_USE_MOCKS=true`.
- Runtime contract validation via Zod schemas in `frontend/lib/schemas.ts` asserts that incoming JSON payloads conform to expected data types.

## Backend Test Suite Structure

```text
tests/
├── contract/                    # Contract and schema compliance tests
│   ├── test_pig_contracts.py    # Validates Pig headerless output & sidecar schemas
│   ├── test_schemas.py          # Validates config/schemas.yaml completeness & integrity
│   └── test_spark_contracts.py  # Validates all published Spark dataset schemas
├── fixtures/                    # Mock responses and frozen datasets
│   ├── golden/                  # Golden truth reference CSVs for regression testing
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
│   ├── test_spark_analytics.py  # PySpark analytics, engine tags, and require-real-engine
│   ├── test_storage.py          # Atomic file writing and CSV partition management
│   └── test_watermark.py        # High-water mark lifecycle and recovery
└── unit/                        # Isolated unit tests across all components
    ├── test_alert_engine.py     # Threshold exceedance, dropout, and spike alerts
    ├── test_api.py              # Station freshness, static JSON export, and WSGI router
    ├── test_clients.py          # OpenAQ client retry logic and offline fallback
    ├── test_cpcb.py             # CPCB breakpoint interpolation and partial index rules
    ├── test_deduplication.py    # Key computation and batch deduplication
    ├── test_discovery.py        # Station bounding box filtering
    ├── test_export_publisher.py # Atomic staging directory swap
    ├── test_freshness.py        # Freshness boundary checks (Fresh, Stale, Offline)
    ├── test_logging.py          # JSON formatter and credential redaction
    ├── test_normalization.py    # Field mapping and timestamp normalization
    ├── test_pig_runner.py       # Pig script execution, sidecars, and simulation fallback
    ├── test_profile.py          # Profile loading, validation, and Java version checks
    ├── test_quality_report.py   # Multi-stage quality accounting
    ├── test_raw_manifest.py     # Raw manifest audit writing
    ├── test_validation.py       # Record range, ceiling, and unit validation
    └── test_weather_correlation.py # Pearson r calculations and confidence tiers
```

## Running Tests

- Run all backend tests: `.venv/bin/pytest -v` (111 tests passing)
- Run unit tests: `.venv/bin/pytest tests/unit/`
- Run integration tests: `.venv/bin/pytest tests/integration/`
- Run contract tests: `.venv/bin/pytest tests/contract/`
- Run coverage check: `make coverage PYTHON=.venv/bin/python`
- Run smoke tests: `make smoke` (real engines) or `make smoke-simulation` (standalone fallback)
