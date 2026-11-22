# Critic Review Report — Stage 7: Antagonistic Critic Review

**Targets:** `.planning/ROADMAP.md`, `.planning/REQUIREMENTS.md`  
**Stage:** 7 — Critic Review  
**Reviewers:** `egm-design`, `egm-structural`, `egm-style`  
**Context:** `.planning/PROJECT.md`  
**Date:** 2026-11-22  

---

## Reviewer Status

| Reviewer | Lens | Status |
|---|---|---|
| `egm-design` | Architecture, Reliability, Economics | completed |
| `egm-structural` | Layout, Hierarchy, Traceability | completed |
| `egm-style` | Tone, Objectivity, Buzzwords | completed |

---

## Prioritized Findings

### 🔴 Blockers

#### [CR-01] Java 17+ Incompatibility with Apache Pig 0.17 in Modern Profile
- **Lens:** `egm-design`
- **Scope:** `.planning/PROJECT.md` (Runtime Profiles), `.planning/ROADMAP.md` (Phase 1/4), `OPS-06`
- **Description:** The `modern` profile specifies OpenJDK 17/21 alongside Apache Pig 0.17. Apache Pig development ceased prior to Java 17; it relies on Hadoop/Pig internal reflection that triggers fatal `java.lang.reflect.InaccessibleObjectException` crashes under modern JDK modularity restrictions.
- **Recommendation:** Standardize Pig execution on Java 8/11 (or run Pig under Java 8 across both profiles while allowing Spark to run on Java 17 with explicit `--add-opens` flags).

#### [CR-02] YARN Container Memory Exhaustion on 1GB-2GB Host Constraints
- **Lens:** `egm-design`
- **Scope:** `.planning/PROJECT.md` (Constraints), `.planning/ROADMAP.md` (Phases 2 & 3), `PIG-01`, `SPK-01`
- **Description:** Attempting to run Hadoop NameNode/DataNode, YARN NodeManager/ResourceManager, MapReduce ApplicationMaster, and Spark on a single machine with a 1–2 GB heap constraint will cause immediate YARN container allocation failures or host OOM kills.
- **Recommendation:** Mandate Pig local execution mode (`pig -x local`) or specify standalone Hadoop local runner mode for single-machine environments. Disable YARN virtual memory checks (`yarn.nodemanager.vmem-check-enabled=false`) and set minimal container allocations (256MB–512MB) in profile configurations.

#### [CR-03] Unresolved External Requirement Identifiers (`FR-01`–`FR-36`, `NFR-08`)
- **Lens:** `egm-structural`
- **Scope:** `.planning/REQUIREMENTS.md` (Lines 10–54)
- **Description:** Every v1 requirement cites phantom external identifiers (e.g., `(FR-01)`, `(NFR-08)`), but neither document defines or links these to the master specification (`docs/SkyWatch — Project Specification v2.0.md`).
- **Recommendation:** Add an explicit traceability header mapping `FR-xx` and `NFR-xx` to `docs/SkyWatch — Project Specification v2.0.md`.

#### [CR-04] Stacked Promotional Hyperbole & Uncalibrated Buzzwords
- **Lens:** `egm-style`
- **Scope:** `.planning/ROADMAP.md` (Lines 5, 41, 73); `.planning/REQUIREMENTS.md` (Line 50)
- **Description:** Document text repeatedly uses ungrounded promotional adjectives and subjective claims: *"production-grade"*, *"robust"*, *"rigorous"*, *"seamlessly toggles"*, *"honest empty/stale state banners"*, and *"executes scripts cleanly"*.
- **Recommendation:** Strip promotional qualifiers. Replace with objective, verifiable engineering criteria (e.g., zero exit codes, explicit HTTP status handling, condition-triggered visual warnings).

---

### 🟡 Major Findings

#### [CR-05] Data Integrity: Hourly Regularization Missing Before 24h Rolling Window
- **Lens:** `egm-design`
- **Scope:** `.planning/REQUIREMENTS.md` (`SPK-04`), `.planning/ROADMAP.md` (Phase 3)
- **Description:** CPCB National AQI requires a 24-hour rolling average over distinct *hourly means*. If a sensor reports sub-hourly (e.g. 15-minute bursts), a simple rolling window of 16 raw readings could be satisfied in 4 hours, producing an invalid AQI.
- **Recommendation:** Formally mandate a two-stage aggregation: Stage 1 groups into regularized clock-hour means `(station_id, parameter, hour_timestamp)`; Stage 2 computes rolling 24h window requiring $\ge 16$ non-null hourly records.

#### [CR-06] Ambiguous Partition Semantics in HDFS Layout (`dt=YYYY-MM-DD`)
- **Lens:** `egm-design`
- **Scope:** `.planning/REQUIREMENTS.md` (`HYG-06`), `.planning/ROADMAP.md` (Phase 1)
- **Description:** The layout `/skywatch/.../dt=YYYY-MM-DD/` does not distinguish `ingestion_date` from `measurement_date`. Using measurement date for raw feeds causes non-atomic updates on backfill; using ingestion date prevents partition pruning on time-series queries.
- **Recommendation:** Explicitly separate partition keys: `/raw/` and `/normalized/` use `ingest_dt=YYYY-MM-DD`; `/clean/` and `/analytics/` partition by `event_dt=YYYY-MM-DD`.

#### [CR-07] Redundant Compute Passes Between Apache Pig & Spark
- **Lens:** `egm-design`
- **Scope:** `.planning/REQUIREMENTS.md` (`PIG-02`, `PIG-03`, `SPK-01`, `SPK-02`)
- **Description:** Both Pig and Spark calculate station averages and daily rollups, creating duplicated business logic and metric divergence risk.
- **Recommendation:** Clarify engine boundaries: restrict Pig to row-level hygiene, schema validation, and distinct deduplication (`PIG-01`). Designate Pig hourly rollups (`PIG-03`) as the direct input to Spark trend analysis, or eliminate redundant summaries from Pig.

#### [CR-08] Weather Join Spatial Mismatch & Open-Meteo Rate Limits
- **Lens:** `egm-design`
- **Scope:** `.planning/REQUIREMENTS.md` (`ING-02`, `SPK-03`)
- **Description:** Open-Meteo is coordinate-based. Fetching weather per station during multi-month backfills risks hitting rate limits. Fetching for a single city centroid requires an explicit broadcast join.
- **Recommendation:** Formally specify weather coordinates: fetch for a single canonical Bengaluru station coordinate (with documented spatial limitation in methodology), and define the join key as `date_trunc('hour', aq.measurement_time_utc) == weather.time_utc`.

#### [CR-09] Missing Page-Level Resumability in Backfill API Ingestion
- **Lens:** `egm-design`
- **Scope:** `.planning/REQUIREMENTS.md` (`ING-05`, `HYG-05`), `.planning/ROADMAP.md` (Phase 1)
- **Description:** Long-running paginated backfills from OpenAQ v3 have no intermediate page checkpoints; an HTTP 429 or network disconnect forces a full restart.
- **Recommendation:** Implement page-level commit checkpoints or chunk backfills into isolated 7-day partition blocks committed atomically.

#### [CR-10] Taxonomy & Phase Allocation Mismatch for `OPS-08`
- **Lens:** `egm-structural`
- **Scope:** `.planning/REQUIREMENTS.md` (`OPS-08`), `.planning/ROADMAP.md` (Phase 3 vs 4)
- **Description:** `OPS-08` (*Synthetic scale benchmarking suite*) has an `OPS-` prefix but is mapped to Phase 3 (Spark analytics) in the roadmap and traceability matrix, while all other `OPS-xx` items reside in Phase 4.
- **Recommendation:** Re-tag `OPS-08` as `SPK-10` or `BENCH-01` to align with Phase 3 Spark analytics.

#### [CR-11] Traceability Matrix Completely Omits v2 Requirements
- **Lens:** `egm-structural`
- **Scope:** `.planning/REQUIREMENTS.md` (Lines 72–114)
- **Description:** The traceability matrix claims 100% coverage (`Unmapped: 0 ✅`) but omits all `v2` requirements (`EXT-01`–`EXT-04`).
- **Recommendation:** Include `EXT-01`–`EXT-04` mapped to `v2 / Unscheduled`, or explicitly label the matrix `v1 Traceability Matrix`.

#### [CR-12] Lack of Plan-Level Requirements Traceability in Roadmap
- **Lens:** `egm-structural`
- **Scope:** `.planning/ROADMAP.md` (All phases)
- **Description:** Requirements are listed only at the phase level; individual work plans (`01-01`, `01-02`, etc.) do not declare which specific requirements they satisfy.
- **Recommendation:** Annotate each plan with its target requirement IDs (e.g., `01-01: Project scaffolding [HYG-01, OPS-06]`).

#### [CR-13] Duplicate Pseudo-Headings (`Plans:`) Violating Hierarchy
- **Lens:** `egm-structural`
- **Scope:** `.planning/ROADMAP.md` (Phase definitions)
- **Description:** Metadata keys `**Plans**: N plans` are followed immediately by an unstyled, bare label `Plans:`.
- **Recommendation:** Replace bare labels with standard sub-headings (`#### Plans`).

---

### 🔵 Nits

#### [CR-14] Uncalibrated Benchmark Sizing & Driver Spill Risk
- **Lens:** `egm-design`
- **Scope:** `.planning/REQUIREMENTS.md` (`OPS-08`)
- **Description:** 1× baseline volume is undefined, creating risk that 100× causes JVM thrashing or disk spilling.
- **Recommendation:** Define 1× volume explicitly (e.g., 50,000 rows / 5MB) and fix shuffle partitions to 4–8 for local execution.

#### [CR-15] File Contention Between Spark Exporter and Streamlit Reader
- **Lens:** `egm-design`
- **Scope:** `.planning/REQUIREMENTS.md` (`SPK-09`, `OPS-01`)
- **Description:** Direct overwrites in `output/published/` can cause read locks or partial reads in Streamlit.
- **Recommendation:** Use atomic directory staging (`output/published/.staging_<id>` -> rename to `current`).

#### [CR-16] Markdownlint Spacing and Checkbox Style Inconsistencies (MD032)
- **Lens:** `egm-structural`
- **Scope:** `.planning/ROADMAP.md`, `.planning/REQUIREMENTS.md`
- **Description:** Missing blank lines before lists; indentation inconsistencies; task syntax (`- [ ]`) vs plain bullets (`- **ID**`).
- **Recommendation:** Reformat to conform with standard markdownlint rules.

---

## Summary

| Severity | Count |
|---|---|
| 🔴 Blocker | 4 (CR-01, CR-02, CR-03, CR-04) |
| 🟡 Major | 9 (CR-05 to CR-13) |
| 🔵 Nit | 3 (CR-14 to CR-16) |
| **Total** | **16** |
