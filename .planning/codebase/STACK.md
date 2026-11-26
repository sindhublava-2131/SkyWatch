---
last_mapped_commit: 6b4cc6dd01b87e70230978c94258f457e4b5fcb4
last_mapped_at: 2026-11-26
---
# Technology Stack

**Analysis Date:** 2026-11-26

## Languages

**Backend & Data Engineering:**
- Python 3.8+ — declared syntax floor in `pyproject.toml` (`requires-python = ">=3.8"`); all modules across `src/skywatch/`, `api/`, and `tests/` use `from __future__ import annotations` and standard `typing` annotations (`Optional`, `Dict`, `List`, `Tuple`, `Union`, `Any`). Ruff enforces `target-version = "py38"`.
- Apache Pig Latin (`.pig`) — Phase 2 ETL pipeline located in `pig/` (`clean_air_quality.pig`, `station_stats.pig`, `hourly_rollup.pig`).
- Structured Query / PySpark DataFrame API — Phase 3 analytics engine in `src/skywatch/spark/`.
- Bash (`.sh`) — execution wrappers and profile switches in `scripts/`.

**Frontend Application:**
- TypeScript 5.7+ — strict static typing throughout `frontend/` (`tsconfig.json`).
- JavaScript / JSX / TSX — Next.js React component tree (`frontend/app/`, `frontend/components/`).
- CSS — Tailwind CSS v4 (`@tailwindcss/postcss`, `frontend/app/globals.css`).

**Data & Configuration Serialization:**
- YAML (`.yaml`) — canonical contracts (`config/schemas.yaml`), operational settings (`config/settings.yaml`), and breakpoints (`config/breakpoints.yaml`).
- JSON (`.json`) — station registries (`config/stations.json`), ingestion state (`data/.watermarks.json`), sidecar schemas (`_schema.json`), and static lake exports (`output/published/api/v1/*.json`).
- Parquet & CSV — snappy-compressed Parquet and partitioned CSV datasets.

## Frameworks & Runtimes

**Frontend Tier (`frontend/`):**
- Runtime: Node.js with `pnpm@12.3.4` package manager (`frontend/package.json`).
- Application Framework: Next.js 15.5.29 (App Router: `/`, `/stations`, `/analytics`, `/alerts`, `/methodology`).
- UI & Component Library: React 19, `@base-ui/react`, Radix UI / Shadcn UI (`frontend/components/ui/`), Lucide React icons.
- Data Visualization & Maps:
  - Recharts 3.11.0 — diurnal hourly trends, daily trends, and correlation charts.
  - MapLibre GL 6.19.0 & React Map GL 8.1.3 — interactive geospatial CAAQMS station visualization.
- State & Schema Validation:
  - TanStack React Query 5.108.3 — asynchronous state and API data fetching.
  - Zod 4.6.7 — runtime schema validation for API responses (`frontend/lib/schemas.ts`).
  - date-fns 4.4.0 & date-fns-tz 3.2.0 — timezone-aware temporal formatting.

**Backend & API Tier:**
- Runtime: CPython 3.8+ (tested against Python 3.11 in CI and 3.12 local).
- REST API: FastAPI (`fastapi>=0.100.0`, optional) with standard-library WSGI serverless fallback in `api/index.py`.
- Big Data Distributed Engines:
  - Apache Hadoop HDFS (CLI adapter or local fallback in `data/local_hdfs/`).
  - Apache Pig (local mode `pig -x local` or MapReduce on YARN; pure-Python standalone fallback in `src/skywatch/pipeline/pig_runner.py`).
  - Apache Spark (PySpark DataFrame API; pure-Python standalone fallback in `src/skywatch/pipeline/spark_runner.py`).

**Runtime Profiles (`profiles/`):**
- `modern.env`: Apache Hadoop 3.3.6, Apache Pig 0.18.0, Apache Spark 3.5.0, Java 17/8.
- `lab.env`: Rollback profile targeting Apache Hadoop 3.2.2, Apache Pig 0.17.0, Apache Spark 3.2.2, Java 8 (OpenJDK 1.8.0).

## Core Dependencies

**Backend Production (`pyproject.toml`):**
- `requests>=2.28.0` — HTTP client for OpenAQ v3 API and Open-Meteo.
- `pyyaml>=6.0` — YAML configuration and schema parsing.
- `pyarrow>=12.0.0` — Parquet columnar format processing.
- `pyspark>=3.2.0` — PySpark analytical aggregations and window functions.
- `python-dotenv>=1.0.0` — environment profile loader.
- `psutil>=5.9.0` — host system metrics and memory profiling.

**Development & Quality Assurance:**
- `pytest>=7.0.0`, `pytest-cov>=4.0.0` — test runner with 80% coverage gate.
- `ruff>=0.1.0` — linter and code formatter targeting Python 3.8.
- `responses>=0.23.0` — HTTP request mocking.
