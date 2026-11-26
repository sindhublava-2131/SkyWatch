---
last_mapped_commit: 6b4cc6dd01b87e70230978c94258f457e4b5fcb4
last_mapped_at: 2026-11-26
---
<!-- refreshed: 2026-11-26 -->

# Codebase Concerns

**Analysis Date:** 2026-11-26

## Tech Debt

**Synchronous Ingestion Engine:**
- **Issue:** The ingestion orchestrator (`src/skywatch/collector/collector.py`) iterates sequentially through stations and sensors with synchronous HTTP roundtrips.
- **Impact:** Backfilling large date ranges across multiple monitoring stations takes considerable time due to sequential network latency.
- **Remediation:** Introduce asynchronous polling using `asyncio`/`aiohttp` or a worker pool with bounded concurrency and rate-limit backpressure.

**Subprocess-Driven HDFS Operations:**
- **Issue:** `HDFSStorageAdapter` (`src/skywatch/storage/hdfs_storage.py`) invokes external `hdfs dfs` CLI commands via `subprocess.run` rather than utilizing native Python HDFS client bindings (e.g. `pyarrow.fs.HadoopFileSystem`).
- **Impact:** Subprocess spawning incurs process fork overhead and depends on shell path resolution and environment variables.
- **Remediation:** Wire `pyarrow.fs.HadoopFileSystem` for direct RPC interactions when Hadoop is configured, retaining local directory fallback.

**Dual Profile Environment Sourcing:**
- **Issue:** Profiles are declared as bash dotenv files (`profiles/modern.env`, `profiles/lab.env`) and parsed in Python via `python-dotenv`.
- **Impact:** Shell environments and Python processes can drift if wrapper scripts (`scripts/use_profile.sh`) are not sourced before manual CLI invocation.
- **Remediation:** Centralize profile declarations in a validated YAML configuration (`config/profiles.yaml`) parsed and enforced directly by `src/skywatch/pipeline/profile.py`.

## Schema & Contract Alignment Items (Resolved)

**Frontend Schema Normalization and Placeholder Elimination:**
- **Status:** Fully Resolved.
- **Resolution:** `frontend/lib/schemas.ts` and `frontend/lib/api.ts` have been reconciled to handle numeric/string station ID unions (`z.union([z.string(), z.number()]).transform(v => String(v))`) and optional metadata fields. Furthermore, `frontend/lib/mocks/index.ts` was populated with genuine published datasets (`stats`, `daily`, `correlations`, `quality`) and `frontend/components/skywatch.tsx` has replaced all hardcoded figures with dynamic computations from real analytical feeds.

## Known Bugs

- None currently identified in backend or pipeline jobs. All 119 unit, integration, and contract tests pass cleanly with verified golden outputs.

## Security Considerations

- **API Credentials:** OpenAQ API keys are passed via the `OPENAQ_API_KEY` environment variable and injected into request headers. No credentials or secrets are stored in repository files or configuration.
- **Browser Security Boundary:** Direct browser calls to external telemetry APIs (OpenAQ v3) are explicitly removed from frontend contracts to eliminate any risk of leaking client API keys.
- **Logging Sanitization:** `src/skywatch/pipeline/logging.py` includes `redact_sensitive()` to sanitize dictionaries and strings before emitting JSON log lines.
- **Big Data Services Exposure:** Hadoop NameNode, DataNode, YARN ResourceManager, and Spark Master services are intended for local or private cluster networks and must never be exposed publicly.

## Performance Bottlenecks

- **Large-Scale Window Operations:** Rolling 24-hour window aggregations in PySpark across millions of records require substantial shuffle partitions. Local execution memory must be tuned accordingly via `spark.driver.memory` and `spark.sql.shuffle.partitions`.
- **Pure-Python Fallback Memory Usage:** The standalone pure-Python fallbacks in `pig_runner.py` and `spark_runner.py` load records into in-memory dictionaries and lists. While ideal for unit/integration testing on small to medium datasets, executing 100x scale benchmarks (5M rows) requires actual PySpark cluster execution.

## Fragile Areas

- **Golden Fixture Numerical Tolerances:** Differences in floating-point aggregation between PySpark SQL (IEEE 754 double) and pure-Python floats can lead to minor fractional variances in standard deviation or Pearson $r$. Test comparisons maintain a `0.15` tolerance to avoid brittle assertions.
- **File Overwrites Across Operating Systems:** Cross-platform atomic swaps require directory removal prior to renaming on Windows systems (`Errno 39` Directory not empty on POSIX vs Windows file locks), handled via `_safe_remove()` in `src/skywatch/spark/export.py`.

## Scaling Limits & Lab Profile Compatibility

- **Scale Benchmark Status:** Evaluated up to 100x (5,000,000 synthetic records) in PySpark local mode and synthetic generator simulation. The 100x run in a single-node local environment takes ~25s in simulation; a distributed cluster is required for true production cluster stress measurements.
- **Lab Profile Compatibility Floor:** The rollback profile (`profiles/lab.env`) requires Hadoop 3.2.2, JDK 8, and Spark 3.2.2. Legacy Spark 2.4.8 cannot be used because its PySpark engine fails on Python >=3.8 syntax and typing imports.
