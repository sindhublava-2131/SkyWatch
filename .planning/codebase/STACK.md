# Technology Stack

**Analysis Date:** 2026-11-22

## Languages

**Primary:**
- Python 3.8+ — declared floor in `pyproject.toml` (`requires-python = ">=3.8"`); every module under `src/skywatch/` uses `from __future__ import annotations` and `typing.Optional`/`Dict`/`List` style annotations. Ruff enforces `target-version = "py38"` in `pyproject.toml`.
- Observed local interpreter: Python 3.12.6. CI pins Python 3.11 in `.github/workflows/ci.yml` and `.github/workflows/scheduled-collect.yml`.

**Secondary:**
- Bash — `scripts/use_profile.sh` (profile activation) and `scripts/cron_collect.sh` (scheduled collection with `flock` and log file).
- YAML — `config/schemas.yaml` is the canonical data contract; `profiles/modern.env` and `profiles/lab.env` use dotenv `KEY="value"` syntax.
- JSON — `config/stations.json`, `data/.watermarks.json`, `data/.backfill_checkpoint.json`, and raw API payloads.
- CSV — normalized partitions, `data/raw/_manifest.csv`, and schema-driven serialization.

**Planned, not implemented in source:**
- Pig Latin — Phase 2 (`clean_air_quality.pig`, `station_stats.pig`, `hourly_rollup.pig`) per `.planning/ROADMAP.md`. No `.pig` files exist.
- PySpark — Phase 3 analytics, CPCB index, correlations, alerts, exports per `.planning/ROADMAP.md`. No `pyspark` import or Spark job exists.
- Streamlit — Phase 4 dashboard per `.planning/ROADMAP.md`. No `streamlit` import or `deploy/` directory exists.

## Runtime

**Environment:**
- CPython 3.8+; local verification used Python 3.12.6, CI uses 3.11.
- Operating system: Linux is the target for Hadoop/Pig/Spark. `profiles/*.env` reference `/opt/bigdata/...` and `/usr/lib/jvm/...`; `scripts/*.sh` are bash. The current development workspace is Windows (`win32`), where `HDFSStorageAdapter` falls back to `data/local_hdfs/`.
- Java: JDK 8 for Pig and JDK 17 for Spark in the `modern` profile; JDK 8 for both in the `lab` profile. Java is only needed for the planned Hadoop/Pig/Spark stack, not for the implemented ingestion path.
- No container runtime, Dockerfile, or orchestration manifest exists.

**Package Manager:**
- pip with setuptools/PEP 621 (`[build-system]` in `pyproject.toml`, backend `setuptools.build_meta`).
- Editable install used by CI: `pip install -e .`.
- Console script: `skywatch = "skywatch.cli:main"` (`pyproject.toml`, `src/skywatch.egg-info/entry_points.txt`).
- Lockfile: missing. No `requirements*.txt`, `*.lock`, `Pipfile`, or `poetry.lock` exists; dependencies are version ranges only.

## Frameworks

**Core:**
- `argparse` (standard library) — CLI parser and subcommands in `src/skywatch/cli.py`.
- `requests>=2.28.0` — synchronous HTTP client for OpenAQ and Open-Meteo in `src/skywatch/collector/openaq_client.py` and `src/skywatch/collector/weather_client.py`.
- `PyYAML>=6.0` — loads `config/schemas.yaml` in `src/skywatch/data/schemas.py`.
- `python-dotenv>=1.0.0` — parses profile `.env` files in `src/skywatch/pipeline/profile.py`.
- Standard-library pipeline primitives: `csv`, `hashlib` (SHA-256 dedupe/raw digests), `json`, `logging` + `contextvars` (structured run logs), `subprocess` (HDFS CLI), `tempfile` + `os.replace` (atomic writes).

**Testing:**
- `pytest>=7.0.0` — test runner; observed 9.0.2 locally. Config in `pyproject.toml`: `testpaths = ["tests"]`, `python_files = ["test_*.py"]`, `pythonpath = ["src"]`.
- `responses>=0.23.0` — HTTP mocking for client tests (`tests/unit/test_clients.py`); observed 0.26.3.
- `pytest-cov>=4.0.0` — declared in the `dev` extra; not observed installed locally.
- `unittest.mock` — used for subprocess/Java mocking (`tests/unit/test_profile.py`, `tests/integration/test_storage.py`).

**Build/Dev:**
- Ruff `>=0.1.0` — linting; observed 0.17.5. Configuration: `line-length = 100`, `select = ["E", "F", "I", "W"]`, `ignore = ["E501"]`, `target-version = "py38"`.
- setuptools `>=61.0` — build backend.
- GitHub Actions — `.github/workflows/ci.yml` runs `ruff check .` and `pytest` on `ubuntu-latest` with Python 3.11.
- No Makefile, pre-commit config, mypy config, or formatter config exists.

## Key Dependencies

**Critical:**
- `requests` — all external API access; retry/backoff, 429 `Retry-After`, 401/403 `AuthenticationError`, JSON decoding.
- `PyYAML` — schema loading; `config/schemas.yaml` drives CSV column order and validation.
- `python-dotenv` — profile value loading.
- Python standard library `hashlib` — SHA-256 record keys and raw-payload digests.

**Infrastructure:**
- `hdfs` CLI binary — `src/skywatch/storage/hdfs_storage.py` shells out to `hdfs version`, `hdfs dfsadmin -safemode get`, `hdfs dfs -mkdir/-put/-rm/-mv`. There is no WebHDFS, `pyarrow`, or `hdfs` Python client.
- Local filesystem — default storage when HDFS is unavailable or in safemode (`data/local_hdfs/`).

**Planned dependencies (documented, not present in the repository):**
- Hadoop 3.4.3 (`modern`) / 3.2.2 (`lab`); Apache Pig 0.18.0; Spark 4.3.0 (`modern`) / 3.2.2 (`lab`) — declared in `profiles/*.env` and `docs/SkyWatch - Project Specification v2.0.md` §4–5.
- `pyspark` for unit tests/local dev only; runtime intended to use `$SPARK_HOME/spark-submit` per the spec.
- Dashboard stack: Streamlit 1.67.0, Plotly 7.1.0, pandas 3.1.1, PyArrow 26.0.1 per spec §5; no such dependencies in `pyproject.toml`.
- `mypy`, `pre-commit`, lock files (`requirements-modern.lock`, `requirements-lab.lock`) per spec §5; none exist.

## Configuration

**Environment:**
- Profiles: `profiles/modern.env` and `profiles/lab.env`, loaded by `src/skywatch/pipeline/profile.py` or exported by `scripts/use_profile.sh`.
- `.env` is not present in the working tree and is git-ignored (`.gitignore`). No `.env.example` exists, although the spec plans one.
- `OPENAQ_API_KEY` is read from the process environment by `OpenAQClient` (`src/skywatch/collector/openaq_client.py`); GitHub Actions supplies it as a repository secret.
- Offline switches: `OPENAQ_OFFLINE`, `WEATHER_OFFLINE`, `SKYWATCH_OFFLINE` cause fixture reads instead of HTTP.

| Variable | Source | Purpose |
|----------|--------|---------|
| `SKYWATCH_PROFILE` | `profiles/*.env`, shell | Selects `modern` or `lab`; default `modern` |
| `HADOOP_HOME`, `PIG_HOME`, `SPARK_HOME` | `profiles/*.env` | Planned stack install locations |
| `JAVA_HOME_PIG`, `JAVA_HOME_SPARK`, `JAVA_HOME` | `profiles/*.env` | Per-tool JDK selection for Pig/Spark |
| `HDFS_ROOT` | `profiles/*.env` | HDFS namespace root (default `/skywatch`) |
| `SKYWATCH_HTTP_TIMEOUT` | `profiles/*.env` | Declared HTTP timeout (modern 20, lab 30) |
| `SKYWATCH_MAX_RETRIES` | `profiles/*.env` | Declared retry count (5) |
| `SKYWATCH_FRESH_HOURS`, `SKYWATCH_STALE_HOURS` | `profiles/*.env` | Freshness thresholds (3 h / 24 h) |
| `OPENAQ_API_KEY` | environment / GitHub secret | OpenAQ v3 authentication |
| `OPENAQ_OFFLINE`, `WEATHER_OFFLINE`, `SKYWATCH_OFFLINE` | environment | Fixture-only execution |

- Wiring note: `load_profile()` and `validate_environment()` are exercised by `tests/unit/test_profile.py` but are not called from `src/skywatch/cli.py` or `IngestionOrchestrator`. `OpenAQClient`/`WeatherClient` read environment variables directly and default to `timeout=20`, `max_retries=5`; profile values for `SKYWATCH_HTTP_TIMEOUT`/`SKYWATCH_MAX_RETRIES` do not currently reach the clients. `HDFSStorageAdapter` defaults `hdfs_root="/skywatch"` independently of `HDFS_ROOT`.
- Profile drift: the spec describes JDK 11 for Pig under `modern`, while `profiles/modern.env` pins `JAVA_HOME_PIG` to Java 8. Treat the profile file as the executable configuration.

**Build:**
- `pyproject.toml` — packaging, dependencies, console script, Ruff, and pytest configuration.
- `config/schemas.yaml` — schema contract (`version: "2.0"`) for `air_quality`, `weather`, and `manifest`.
- `config/stations.json` — eight discovered Bengaluru stations with `pm25`/`pm10` sensor IDs.
- `.github/workflows/ci.yml` — lint and test pipeline.
- `.github/workflows/scheduled-collect.yml` — cron ingestion pipeline.

## Platform Requirements

**Development:**
- Python 3.8+; 3.11 in CI; 3.12.6 observed locally.
- Bash for `scripts/use_profile.sh` and `scripts/cron_collect.sh` (Linux/macOS/Git Bash).
- Optional `hdfs` CLI and a reachable NameNode for HDFS uploads; otherwise local fallback is automatic.
- Network access to OpenAQ/Open-Meteo for live runs, or fixtures in `tests/fixtures/` for offline runs.
- No Java, Hadoop, Pig, or Spark installation is required to run the implemented Phase 1 ingestion and tests.

**Production:**
- No production deployment configuration exists in the repository.
- Target architecture per `.planning/PROJECT.md` and `docs/SkyWatch - Project Specification v2.0.md`: a single Linux machine with pseudo-distributed HDFS, Apache Pig, and Apache Spark, plus a Streamlit dashboard (planned Phase 4). The spec also plans GitHub Actions for scheduled collection and Streamlit Community Cloud for the dashboard; neither deployment is implemented.

---

*Stack analysis: 2026-11-22*
