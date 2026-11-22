# Goldfish Review Report — Stage 5: Initial Zoom Out Review

**Targets:** `.planning/ROADMAP.md`, `.planning/REQUIREMENTS.md`
**Stage:** 5 — Initial Zoom Out Review
**Reviewers:** `egm-zoom-out`, `egm-inventive-catalyst`
**GoldfishContext:** `.planning/PROJECT.md`
**Date:** 2026-11-22

---

## Reviewer Status

| Reviewer | Status |
| --- | --- |
| `egm-zoom-out` | completed |
| `egm-inventive-catalyst` | completed |

---

## Prioritized Findings

### [F1] No Machine-Enforced Data Contract Between Pig and Spark — Severity: Blocker

- **Reviewer Lens:** `egm-zoom-out`
- **Affected Scope:** Phase 2→3 boundary / PIG-01..03 / SPK-01..09
- **Discrepancy Description:** Spark (Phase 3) is declared dependent on Pig (Phase 2), but nothing in Phase 3's success criteria or requirements names the HDFS path, schema, or partition format that Pig must produce before Spark can proceed. PIG-03's hourly_rollup is described as "feeding downstream Spark trend analysis jobs," but there is no binding data contract. If Spark reads directly from Phase 1's normalized CSV (which is the more natural path given that Pig's output is also just CSV), the entire Phase 2→Phase 3 dependency is cosmetic and unverifiable. Phase 3 can be — and probably will be — built to bypass Pig entirely, making Phase 2 academically vestigial.
- **Recommendation:** Add a data contract requirement (DC-01) specifying the exact HDFS path, file format, and column schema that `clean_air_quality.pig` and `hourly_rollup.pig` must produce. Phase 3's success criteria 1 and 3 must reference those paths explicitly as their inputs. Add a Phase 3 pre-condition step confirming Pig output exists and passes schema validation before Spark jobs run.

---

### [F2] HDFS Cluster Bring-Up Is Not a Requirement or Success Criterion Anywhere — Severity: Blocker

- **Reviewer Lens:** `egm-zoom-out`
- **Affected Scope:** Phase 1 / HYG-06 / HDFS on single machine
- **Discrepancy Description:** Every phase after Phase 1 requires a healthy local HDFS instance (NameNode/DataNode running, out of safe mode, `hdfs dfs` commands working). No requirement, success criterion, roadmap section, or plan addresses Hadoop cluster bring-up or liveness verification. The smoke test (OPS-06) tests profile switching, not Hadoop reachability. In an academic lab environment where Hadoop may not be pre-configured, every downstream phase silently fails at the HDFS layer with no diagnostic gate.
- **Recommendation:** Add a "Prerequisites / Environment Readiness" section to ROADMAP.md (or a Phase 0 checklist) requiring: HDFS out of safe mode, NameNode reachable at localhost, Pig on PATH, Spark master accessible. Phase 4's smoke suite (OPS-06) must include a `hdfs dfs -ls /skywatch` liveness gate that fails fast with a clear diagnostic before any pipeline logic runs.

---

### [F3] Discovery/Collection Success Criteria Are Externally Dependent With No Fixture/Mock Path — Severity: Blocker

- **Reviewer Lens:** `egm-zoom-out`
- **Affected Scope:** Phase 1 / ING-01 / ING-03 / OpenAQ v3 rate limits
- **Discrepancy Description:** Phase 1 success criterion 1 requires the discovery script to "find Bengaluru monitoring stations via OpenAQ v3 and output valid config/stations.json." This is a live API call that cannot pass in an air-gapped environment, with a revoked API key, or with an exhausted rate limit. No requirement or plan specifies how the API key is provisioned, stored, or validated before tests run. HYG-05 addresses HTTP 429 handling but there is no requirement for a fixture/mock layer. In an academic lab setting with inconsistent network access, Phase 1 integration tests become non-repeatable.
- **Recommendation:** Extend HYG-07 (offline mode) or add a new requirement explicitly requiring a local fixture/mock layer for OpenAQ and Open-Meteo responses. The Phase 1 discovery criterion must be verifiable against that fixture if live API is unavailable. API key provisioning (env var name, `.env` file) must be a concrete deliverable in plan 01-01, not left implicit.

---

### [F4] CPCB AQI Silent Non-Emission Conflates Operational Failure With Scientific Exclusion — Severity: Major

- **Reviewer Lens:** `egm-zoom-out` + `egm-inventive-catalyst`
- **Affected Scope:** Phase 3 / SPK-04 / CPCB AQI success criterion 3
- **Discrepancy Description:** The >=16 valid hours rule is treated as a binary gate: either an AQI is emitted or the row is silently absent. No distinction is drawn between "not enough data was collected" (operational failure — collection gap) and "not enough valid readings existed" (legitimate scientific exclusion). Both collapse to the same silent non-emission. Additionally, SPK-04's success criterion does not specify what the AQI column contains when the rule is not met — NaN, null, "Insufficient Data" string, or a placeholder row with an explicit reason. If implementations diverge, the alert engine (SPK-05) and dashboard produce inconsistent behavior across runs and profiles.
- **Recommendation:** Extend SPK-04 to explicitly specify the output value for sub-threshold windows (e.g., null AQI with separate `aqi_suppression_reason` ∈ {INSUFFICIENT_VALID_HOURS | COLLECTION_GAP | BELOW_THRESHOLD} and `collection_completeness_pct` metric). Emit a placeholder record rather than omitting the row. This must appear in schemas.yaml as a Phase 1 data contract so both Pig and Spark layers handle it uniformly.

---

### [F5] 90-Second Pig SLA Is Unverifiable — "Sample Data" Is Undefined — Severity: Major

- **Reviewer Lens:** `egm-zoom-out`
- **Affected Scope:** Phase 2 / PIG-01..03 / success criterion 4
- **Discrepancy Description:** The 90-second wall-clock SLA for the Pig runner is measured against "sample data," but sample data size (row count, station count, date range) is never specified. Cold JVM startup on a 1 GB Hadoop heap alone consumes 20–40 seconds. A 10-row sample passes trivially; a 10,000-row sample may not. The criterion will be gamed by whoever sizes the sample, making it unverifiable as a meaningful performance assertion.
- **Recommendation:** Define "sample data" precisely — minimum row count, date range, station count — as a committed artifact alongside Pig scripts. Measure the SLA after JVM warmup (second sequential run), or explicitly include JVM startup in the time budget and size the sample accordingly.

---

### [F6] "Matching Outputs" for Dual-Profile Verification Is Undefined — Severity: Major

- **Reviewer Lens:** `egm-zoom-out`
- **Affected Scope:** Phase 4 / OPS-06 / dual-profile success criterion 4
- **Discrepancy Description:** Phase 4 success criterion 4 requires that dual-profile switching "passes smoke tests and matches outputs." JDK 8 (Spark 3.2.2) versus JDK 17 (Spark 3.5+) can produce numerically different floating-point results for Pearson correlation coefficients, AQI piecewise interpolation, and sort orders due to changes in shuffle behavior and adaptive query execution. If "matches outputs" means byte-for-byte identical, this criterion is likely to fail by design. If it means "within epsilon," the epsilon and comparison method are unspecified.
- **Recommendation:** Define "matching outputs" explicitly: either exact row count + schema match only (ignoring floating-point values), or a specified numeric tolerance (e.g., abs diff < 0.01 for all float columns). The smoke test script must implement this comparison programmatically.

---

### [F7] Scale Benchmark Will OOM at 100× on 2 GB Spark Heap With No Failure-Mode Requirement — Severity: Major

- **Reviewer Lens:** `egm-zoom-out` + `egm-inventive-catalyst`
- **Affected Scope:** Phase 3 / OPS-08 / scale benchmark
- **Discrepancy Description:** The synthetic 100× benchmark runs inside the same 2 GB Spark driver heap and 1 GB Hadoop heap that constrains production. The benchmark design conflates "demonstrating scalability theory" with "measuring scalability" — these are different claims. A 100× job on this hardware will almost certainly OOM. No failure-mode requirement is specified: does the benchmark report the OOM as a result, or must the job complete? Phase 3 success criterion 5 says only "measures runtime performance," not "completes." Additionally, OPS-08 is assigned to Phase 3 in REQUIREMENTS.md but listed inconsistently across ROADMAP.md.
- **Recommendation:** Structurally separate the benchmark into two modes: `--mode=local` (runs within the 2 GB constraint, measures algorithmic scaling) and `--mode=report` (generates projected extrapolation from local measurements). Report mode makes the academic claim honest. Clarify whether 100× must complete or whether "partial results with recorded failure" is acceptable. Fix OPS-08 phase assignment to be consistent (choose Phase 3 in both documents).

---

### [F8] Logging Schema Is Not Defined — OPS-07 Cannot Be Mechanically Verified — Severity: Major

- **Reviewer Lens:** `egm-zoom-out`
- **Affected Scope:** Phase 1 / Phase 4 / OPS-07
- **Discrepancy Description:** OPS-07 requires "structured run logging with unique run identifiers across all pipeline stages." No requirement or plan specifies the log format (plain text, JSON, structured fields), the run identifier schema (UUID? timestamp? pipeline-name+date?), or how GitHub Actions logs relate to local logs. Plan 04-02 addresses the pipeline runner but makes no mention of a logging contract. Without a logging spec, Phase 4's smoke suite cannot verify OPS-07 mechanically — it can only assert that some output exists.
- **Recommendation:** Define the logging schema (minimum: run_id, stage, timestamp, level, message) in Phase 1 plan 01-01 alongside schemas.yaml. The Phase 4 smoke suite must assert that a completed pipeline run produces a parseable log file containing a `run_id` field.

---

### [F9] Watermark and Atomic Write Are Not Specified as a Single Commit — Severity: Major

- **Reviewer Lens:** `egm-inventive-catalyst`
- **Affected Scope:** Phase 1 / ING-05 / HYG-05
- **Discrepancy Description:** Exponential backoff and atomic writes are specified as independent behaviors. Incremental per-sensor watermarks are separately specified. If a backoff retry cycle is interrupted mid-collection (process killed, CI timeout), the watermark for a sensor may advance past the last successfully written record — because the watermark update and the atomic rename are not specified to be a single commit. Under failure, watermarks can drift forward past actual data, creating permanent silent gaps that are never backfilled because the system believes the window was already ingested.
- **Recommendation:** Specify explicitly that the watermark advance must occur only *after* the atomic rename of the data file succeeds, as a single logged commit. The watermark file itself must be written atomically (temp→rename) using the same pattern prescribed for data files. Document this explicitly in Plan 01-03 so it cannot be implemented as two separate operations.

---

### [F10] Phase 1 Headerless Pig Output Schema Is Only Validated at Write Time — Severity: Major

- **Reviewer Lens:** `egm-inventive-catalyst`
- **Affected Scope:** Phase 1→2 boundary / HYG-06 / PIG-01
- **Discrepancy Description:** Pig reads normalized CSV from HDFS; Spark reads Pig's headerless clean output. The schema contract is encoded implicitly in column-position ordering. Schema correctness is validated at Phase 1 write time (HYG-01, schemas.yaml) but must be re-assumed at Phase 2 and Phase 3 read time — with no machine-enforced contract at hand-off boundaries. Any column reorder, partial-write recovery, or Pig script modification silently corrupts downstream Spark jobs with no detection until analytics produce wrong numbers.
- **Recommendation:** Insert a sidecar `_schema.json` written atomically alongside each Pig output partition (the Pig runner wrapper in plan 02-02 is the natural place for this). The Spark reader validates column count and inferred types against the sidecar before processing. This requires zero change to Pig scripts and zero change to Pig output format.

---

### [F11] Two-Layer Deduplication (Python + Pig DISTINCT) Is Unexplained — Severity: Nit

- **Reviewer Lens:** `egm-zoom-out`
- **Affected Scope:** Phase 1 / HYG-03 / Phase 2 / PIG-01
- **Discrepancy Description:** HYG-03 specifies deduplication on `(sensor_id, parameter, measurement_time_utc)` at ingestion time. PIG-01 also applies `DISTINCT` after loading from HDFS normalized CSV. Two-layer dedup is not explained or justified. If the ingestion layer deduplicates correctly, Pig's DISTINCT is redundant. If Pig DISTINCT is load-bearing, HYG-03 is misleadingly specified. Implementers may skip ingestion-layer dedup assuming Pig handles it, breaking normalized CSV integrity before HDFS upload.
- **Recommendation:** Document explicitly which dedup layer is authoritative. If both are intentional (defense-in-depth), say so.

---

### [F12] Pearson Correlation Has No Minimum n Threshold for Display — Severity: Nit

- **Reviewer Lens:** `egm-inventive-catalyst`
- **Affected Scope:** Phase 3 / SPK-03
- **Discrepancy Description:** SPK-03 requires reporting sample size `n` alongside Pearson correlation coefficients, but specifies no minimum `n` threshold. A station with 3 paired hourly readings produces a Pearson coefficient of ±1.0 that is statistically meaningless but would be emitted and potentially displayed on the dashboard as authoritative.
- **Recommendation:** Add a `correlation_confidence` field: HIGH (n≥30), MODERATE (n≥10), LOW (n<10), SUPPRESSED (n<3). The dashboard must render the confidence tier alongside the coefficient.

---

### [F13] ">=80% Test Coverage" Is Not Measurably Defined — Severity: Nit

- **Reviewer Lens:** `egm-zoom-out`
- **Affected Scope:** Phase 4 / success criterion 5
- **Discrepancy Description:** ">=80% test coverage on core logic" does not specify the coverage tool, the included paths, line vs. branch coverage, or how the gate is enforced (CI step failure). Plan 04-03 provides none of these specifics.
- **Recommendation:** Specify: tool (pytest-cov), scope (e.g., `src/` excluding `dashboard/pages/`, `scripts/`, generated files), metric (line coverage), and enforcement (CI step fails if below threshold). Must be in 04-03's plan criteria.

---

## Subsystems With No Issues Found (Inventive Catalyst)

- ING-04 (raw response preservation before parsing): correctly separates audit layer from processing layer.
- HYG-03 composite key `(sensor_id, parameter, measurement_time_utc)`: correctly scoped, no tension.
- ING-06 (cron + GitHub Actions): appropriately additive dual-mode scheduling.
- SPK-06 (Fresh/Stale/Offline freshness thresholds): explicit and labeled.
- OPS-04 (empty-state banners and attribution): correctly specified as explicit UI states.

---

## Summary

| Severity | Count |
|---|---|
| 🔴 Blocker | 3 (F1, F2, F3) |
| 🟡 Major | 7 (F4, F5, F6, F7, F8, F9, F10) |
| 🔵 Nit | 3 (F11, F12, F13) |
| **Total** | **13** |

**Key structural risk:** F1, F2, and F3 are Blockers that must be resolved before execution begins. F1 means Phase 2→3 integration is not verifiable as specified. F2 means every HDFS-dependent phase can fail silently. F3 means Phase 1 success criteria cannot be evaluated in a closed lab environment.
