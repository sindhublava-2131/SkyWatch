---
last_mapped_commit: 831a73a6e8025d4970ff7b506be5de6d304b4c61
last_mapped_at: 2026-11-23
---
<!-- refreshed: 2026-11-23 -->

# Codebase Concerns

**Analysis Date:** 2026-11-23

## Tech Debt

**Synchronous Ingestion Engine:**
- **Issue:** Ingestion orchestrator (`src/skywatch/collector/collector.py`) iterates sequentially through stations and sensors with synchronous HTTP roundtrips.
- **Impact:** Backfilling large date ranges (e.g. 90+ days) across multiple monitoring stations takes considerable time due to sequential network latency.
- **Remediation:** Introduce asynchronous polling using `asyncio`/`aiohttp` or a worker pool with bounded concurrency and rate-limit backpressure.

**Subprocess-Driven HDFS Operations:**
- **Issue:** `HDFSStorageAdapter` (`src/skywatch/storage/hdfs_storage.py`) invokes external `hdfs dfs` CLI commands via `subprocess.run` rather than utilizing native Python HDFS client bindings (e.g. `pyarrow.fs.HadoopFileSystem`).
- **Impact:** Subprocess spawning incurs process fork overhead and depends on shell path resolution and environment variables.
- **Remediation:** Wire `pyarrow.fs.HadoopFileSystem` for direct RPC interactions when Hadoop is configured, retaining local directory fallback.

**Dual Profile Environment Sourcing:**
- **Issue:** Profiles are declared as bash dotenv files (`profiles/modern.env`, `profiles/lab.env`) and parsed in Python via `python-dotenv`.
- **Impact:** Shell environments and Python processes can drift if the wrapper scripts (`scripts/use_profile.sh`) are not sourced before manual Python CLI execution.
- **Remediation:** Centralize profile declarations in a validated YAML configuration (`config/profiles.yaml`) that is directly parsed and enforced by `src/skywatch/pipeline/profile.py`.

## Known Bugs

- None currently identified in Phase 1, Phase 2, or Phase 3. All contract, unit, and integration tests pass cleanly with verified golden outputs.

## Security Considerations

- **API Credentials:** OpenAQ API keys are passed via the `OPENAQ_API_KEY` environment variable and injected into request headers. No credentials or secrets are stored in repository files or configuration.
- **Logging Sanitization:** `src/skywatch/pipeline/logging.py` includes `redact_sensitive()` to sanitize dictionaries and strings before emitting JSON log lines.
- **Big Data Services Exposure:** Hadoop NameNode, DataNode, YARN ResourceManager, and Spark Master services are intended for local or private cluster networks and must never be exposed publicly.

## Performance Bottlenecks

- **Large-Scale Window Operations:** Rolling 24-hour window aggregations in PySpark across millions of records require substantial shuffle partitions. Local execution memory must be tuned accordingly via `spark.driver.memory` and `spark.sql.shuffle.partitions`.
- **Pure-Python Fallback Memory Usage:** The standalone pure-Python fallbacks in `pig_runner.py` and `spark_runner.py` load records into in-memory dictionaries and lists. While ideal for unit/integration testing on small to medium datasets, executing 100x scale benchmarks (5M rows) requires actual PySpark cluster execution.

## Fragile Areas

- **Golden Fixture Numerical Tolerances:** Differences in floating-point aggregation between PySpark SQL (IEEE 754 double) and pure-Python floats can lead to minor fractional variances in standard deviation or Pearson $r$. Test comparisons maintain a `0.15` tolerance to avoid brittle assertions.
- **File Overwrites Across Operating Systems:** Cross-platform atomic swaps require directory removal prior to renaming on Windows systems (`Errno 39` Directory not empty on POSIX vs Windows file locks), handled via `_safe_remove()` in `src/skywatch/spark/export.py`.

## Scaling Limits

- Tested and benchmarked up to:
  - 1x Scale (50,000 records): Execution < 10 seconds locally.
  - 10x Scale (500,000 records): Execution ~ 45 seconds locally.
  - 100x Scale (5,000,000 records): Requires distributed cluster deployment with dedicated Spark executors.

## Dependencies at Risk

- **Python 3.8 Lifecycle:** Python 3.8 reached end-of-life upstream in October 2024. While SkyWatch maintains Python 3.8 syntax floor compatibility for constrained Big Data lab clusters, CI and modern execution leverage Python 3.11.

## Missing Critical Features

- **Phase 4 Interactive Dashboard:** Interactive Streamlit web interface for spatial air quality maps, temporal trend charts, correlation matrices, and health alert visualizations remains to be implemented in Phase 4.
- **Phase 4 Unified CLI:** CLI currently supports ingestion subcommands (`discover`, `collect`, `backfill`). Subcommands for running Pig ETL (`skywatch pig`), Spark analytics (`skywatch spark`), dashboard launch (`skywatch dashboard`), and profile validation (`skywatch validate`) will be unified in Phase 4.

## Test Coverage Gaps

- Integration testing of distributed YARN execution requires physical multi-node Hadoop clusters; verified locally via `pig -x local` and PySpark local mode.
