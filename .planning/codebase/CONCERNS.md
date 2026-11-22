<!-- refreshed: 2026-11-22 -->
# Codebase Concerns

**Analysis Date:** 2026-11-22

## Tech Debt

**Single-Threaded Synchronous Ingestion:**
- Issue: Ingestion orchestrator runs sequentially across stations, streams, and sensors with blocking HTTP requests.
- Files: `src/skywatch/collector/collector.py:95-180`
- Impact: Ingestion duration scales linearly with the number of monitored stations and historical backfill date ranges. A backfill of 30 days for 10 stations requires hundreds of serial HTTP roundtrips.
- Fix approach: Implement an asynchronous HTTP client session (e.g. `aiohttp` or `httpx` in async mode) or use a thread pool executor for concurrent station polling with rate-limit concurrency gating.

**Subprocess-Driven HDFS Operations:**
- Issue: `HDFSStorageAdapter` forks external `hdfs` CLI binaries via `subprocess.run` for directory checking, safemode queries, and file transfers instead of using an in-process native Python client library.
- Files: `src/skywatch/storage/hdfs_storage.py:31-60,78-120`
- Impact: High process fork overhead, fragility on non-standard Hadoop distributions, dependency on shell environment variables (`PATH`, `HADOOP_HOME`), and lack of granular streaming upload capabilities.
- Fix approach: Integrate `pyarrow.fs.HadoopFileSystem` or `snakebite-py3` for native HDFS RPC integration while preserving the local filesystem fallback for offline development.

**Dual Profile and Environment Synchronization:**
- Issue: Environment configuration is maintained across shell scripts (`scripts/use_profile.sh`), dotenv files (`profiles/modern.env`, `profiles/lab.env`), and Python helper functions (`src/skywatch/pipeline/profile.py`).
- Files: `profiles/modern.env`, `profiles/lab.env`, `scripts/use_profile.sh`, `src/skywatch/pipeline/profile.py`
- Impact: Risk of profile drift between shell invocations and Python runtime inspection; profile switching requires sourcing shell scripts prior to invoking Python processes.
- Fix approach: Centralize profile definitions in a structured configuration schema (`config/profiles.yaml`) that is directly parsed and applied by `src/skywatch/pipeline/profile.py` with automatic environment variable injection.

**Direct YAML Parsing Across Multiple Modules:**
- Issue: `config/schemas.yaml` is read and parsed via `yaml.safe_load` directly within `src/skywatch/data/schemas.py`, while other components rely on helper functions without cached pydantic/dataclass schema representations.
- Files: `src/skywatch/data/schemas.py:20-55`, `config/schemas.yaml`
- Impact: Repeated disk I/O and dictionary lookups without static type-checked accessors for column definitions, data types, and validation boundaries.
- Fix approach: Define Pydantic models or typed dataclasses for schema definitions, loading and validating `schemas.yaml` once into a frozen schema registry singleton.

## Known Bugs

**Watermark State Desynchronization on Non-ISO Timestamp Strings:**
- Issue: Watermark comparison in `src/skywatch/collector/collector.py:122` and `src/skywatch/data/normalization.py:15` uses lexicographical string comparison (`meas_time > current_wm`).
- Files: `src/skywatch/collector/collector.py:122`, `src/skywatch/storage/watermark.py:42-58`
- Impact: If an external upstream API returns timestamps with variable precision (e.g., millisecond `.000Z` vs second `Z` precision) or alternate timezone offsets, lexicographical string comparison produces incorrect temporal ordering, potentially skipping newer records or causing duplicate re-fetches.
- Fix approach: Parse timestamp strings into timezone-aware `datetime` objects in UTC before comparison, or strictly enforce ISO 8601 UTC canonicalization (`%Y-%m-%dT%H:%M:%SZ`) during normalization before evaluating watermarks.

**Subprocess `FileNotFoundError` on Non-Hadoop Environments:**
- Issue: On development machines (e.g. Windows without Hadoop binaries), calling `subprocess.run(["hdfs", ...])` raises `FileNotFoundError` before entering error status checks.
- Files: `src/skywatch/storage/hdfs_storage.py:31-39`
- Impact: While caught by `except Exception: return False`, unhandled `subprocess` failures emit warning logs and consume unnecessary subprocess lookup attempts on every sync operation.
- Fix approach: Cache the availability of the `hdfs` executable using `shutil.which("hdfs")` during adapter initialization to bypass subprocess execution entirely when Hadoop is absent.

## Security Considerations

**API Key Exposure in Raw Request Logging and Cache Dumps:**
- Issue: OpenAQ API token is supplied via environment variable (`OPENAQ_API_KEY`) and passed in headers.
- Files: `src/skywatch/collector/openaq_client.py:40-48`, `src/skywatch/storage/raw_manifest.py:38-75`, `src/skywatch/pipeline/logging.py:64-75`
- Risk: While `JsonPipelineFormatter` redacts known token keys in logs, if an HTTP library error or unhandled exception serializes the raw request headers or query strings into crash logs or standard error, tokens may be leaked into unredacted output.
- Current mitigation: Regex and dictionary sanitization in `src/skywatch/pipeline/logging.py` redacting `api_key`, `token`, `secret`, `password`, and `authorization`.
- Recommendations: Add header sanitization directly in `save_raw_response` and ensure request headers are scrubbed before writing raw manifest audit records.

**Unauthenticated Hadoop/HDFS Lab Endpoints:**
- Issue: Traditional Hadoop/HDFS configurations in the `lab` profile operate with simple pseudo-distributed authentication without Kerberos or SSL encryption.
- Files: `profiles/lab.env:1-35`
- Risk: Inadvertent exposure of NameNode (port 9870/50070) or DataNode (port 9864/50075) ports on public networks allows arbitrary unauthenticated file modification or exfiltration.
- Current mitigation: Explicit architectural guideline forbidding public binding of Hadoop, HDFS, YARN, or Spark services.
- Recommendations: Ensure Hadoop configuration binds exclusively to loopback interfaces (`127.0.0.1`) and enforce firewall rules restricting HDFS ports to local processes.

## Performance Bottlenecks

**Sequential Multi-Station Weather Polling:**
- Issue: Weather metrics are retrieved per-station with individual Open-Meteo API requests.
- Files: `src/skywatch/collector/collector.py:150-175`, `src/skywatch/collector/weather_client.py:38-75`
- Cause: Open-Meteo allows multi-coordinate batch queries in a single HTTP request, but the current implementation iterates over `stations` one by one.
- Improvement path: Leverage Open-Meteo's comma-separated multi-location endpoint (`latitude=12.91,12.93&longitude=77.61,77.63`) to fetch all monitored stations in a single network roundtrip.

**Small File Problem in Partitioned CSV Generation:**
- Issue: Every collection cycle writes separate CSV partition files per stream and ingest date (`data/normalized/air_quality/ingest_dt=YYYY-MM-DD/`).
- Files: `src/skywatch/storage/local_storage.py:45-85`, `src/skywatch/collector/collector.py:180-220`
- Cause: Hourly or sub-hourly cron execution appends to or creates small daily partition slices, accumulating hundreds of tiny files.
- Improvement path: Introduce a compaction job or consolidate partitions during Phase 2 Pig ETL and Phase 3 Spark batching to coalesce micro-files into optimal HDFS block sizes (128 MB).

## Fragile Areas

**OpenAQ v3 API Schema Variations:**
- Files: `src/skywatch/data/normalization.py:65-130`, `src/skywatch/collector/openaq_client.py:60-110`
- Why fragile: OpenAQ v3 API response structures occasionally omit sensor fields, shift datetime nesting between `datetime.utc` and `period.datetimeTo.utc`, or return null coordinates for temporary mobile monitors.
- Safe modification: Normalization logic currently uses defensive chained `.get()` lookups with default fallbacks; any updates to field parsing must be backed by contract fixture tests in `tests/contract/` and unit tests in `tests/unit/test_normalization.py`.
- Test coverage: Covered by `test_normalization.py` for standard and malformed payloads, but lacks coverage for newly introduced v3 sensors.

**Watermark State File Concurrency:**
- Files: `src/skywatch/storage/watermark.py:20-80`
- Why fragile: Watermark state is stored in a single JSON file (`data/.watermarks.json`). While writes use temporary files and `os.replace`, concurrent executions of `skywatch collect` outside `cron_collect.sh` (which provides `flock`) could race, overwriting watermark updates from competing runs.
- Safe modification: Always execute automated collections via `scripts/cron_collect.sh` or acquire a file lock (`fcntl.flock` or cross-platform equivalent) inside `WatermarkManager.commit()`.
- Test coverage: Verified for atomic rename and single-process rollback; concurrent race conditions are not currently tested.

## Scaling Limits

**OpenAQ Free-Tier API Rate Limits:**
- Current capacity: OpenAQ v3 free-tier API keys provide 60 requests per minute.
- Limit: With 10 stations and 6 parameters per station, an incremental cycle makes up to 60 HTTP calls per run, hitting rate limits if run more frequently than once every few minutes.
- Scaling path: Transition from per-sensor latest queries to location-level multi-sensor queries or upgrade to higher-tier OpenAQ API access; implement client-side exponential backoff with jitter.

**Single-Node Memory Footprint During Massive Backfills:**
- Current capacity: In-memory record lists in `IngestionOrchestrator.run_chunked_backfill` hold normalized records for an entire chunk.
- Limit: A multi-year backfill across all Bangalore stations can exceed hundreds of thousands of records, elevating Python heap consumption.
- Scaling path: Stream records directly to partitioned temporary files in bounded batches rather than accumulating full chunks in memory before flushing.

## Dependencies at Risk

**Python 3.8 End-of-Life:**
- Package: Python 3.8
- Risk: Reached official end-of-life in October 2024. Security vulnerabilities in CPython 3.8 will not receive upstream security patches.
- Impact: Modern dependencies (e.g., `pytest`, `ruff`, future `pyspark` and `pandas` releases) are progressively deprecating Python 3.8 support.
- Migration plan: Maintain 3.8 compatibility for Phase 1/Phase 2 big data environments (Hadoop/Pig lab profiles), but plan migration to Python 3.10/3.11 for Phase 3 and Phase 4.

**Apache Pig Project Dormancy:**
- Package: Apache Pig (0.17.0)
- Risk: Apache Pig has had no active releases since 2017 and is in Apache Attic / retirement status.
- Impact: Pig scripts depend on legacy Java 8 runtimes and legacy Hadoop versions, creating friction with modern Spark runtimes requiring Java 17+.
- Migration plan: Isolate Pig ETL scripts to Phase 2 processing using the dedicated `lab.env` profile; ensure all Pig outputs conform to clean partitioned CSV/Parquet formats easily consumed by Spark in Phase 3.

## Missing Critical Features

**Phase 2 Pig Latin Batch Processing:**
- Problem: Big data ETL scripts (`clean_air_quality.pig`, `station_stats.pig`, `hourly_rollup.pig`) described in `.planning/ROADMAP.md` are not yet created.
- Blocks: Batch cleaning, deduplication on HDFS, and hourly aggregation rollups.

**Phase 3 Spark Analytics & CPCB AQI Calculation Engine:**
- Problem: Real-time/batch calculation of India National Air Quality Index (CPCB methodology), weather-pollutant cross-correlation, and alert triggers are not yet implemented.
- Blocks: Downstream analytics, trend analysis, and health advisory generation.

**Phase 4 Interactive Dashboard:**
- Problem: Streamlit web dashboard and automated reporting components have not yet been built.
- Blocks: End-user visualization and operational observability.

**Prometheus / Health Telemetry Metrics Exporter:**
- Problem: Pipeline health, ingestion latency, and record volume metrics are emitted solely as JSON logs, lacking a scrapeable Prometheus metrics endpoint.
- Blocks: Automated alerting and dashboard monitoring via Grafana or Prometheus.

## Test Coverage Gaps

**Distributed HDFS Cluster Live Verification:**
- What's not tested: Real network interaction with a distributed Apache Hadoop NameNode and DataNode cluster under network partitions or DataNode saturation.
- Files: `src/skywatch/storage/hdfs_storage.py`
- Risk: HDFS operations pass in CI and local testing via `local_hdfs` offline fallback, but may encounter permission or Kerberos issues on bare-metal Hadoop clusters.
- Priority: Medium

**Network Fault Injection and Connection Dropouts:**
- What's not tested: Real socket timeouts, SSL handshake failures, and truncated chunked transfers during active HTTP streaming.
- Files: `src/skywatch/collector/openaq_client.py`, `src/skywatch/collector/weather_client.py`
- Risk: Unhandled transient connection resets during long-running backfills could cause premature process termination.
- Priority: Medium

**Concurrent CLI Process Watermark Contention:**
- What's not tested: Simultaneous invocation of `skywatch collect` from separate terminal sessions without external lock orchestration.
- Files: `src/skywatch/storage/watermark.py`
- Risk: Silent overwrite of watermark milestones causing missing or re-processed records.
- Priority: Low

---

*Concerns audit: 2026-11-22*
