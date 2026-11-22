# Comprehension Review Report — Stage 6: Comprehension Test

**Targets:** `.planning/ROADMAP.md`, `.planning/REQUIREMENTS.md`  
**Stage:** 6 — Comprehension Test  
**Reviewer:** `egm-comprehension`  
**Date:** 2026-11-22  

---

## Paraphrase

### **What this proposes**
This document set proposes building "SkyWatch", an automated big data environmental analytics platform for Bengaluru, India. It ingests PM2.5 and PM10 particulate readings from OpenAQ v3 alongside meteorological data from Open-Meteo, storing both raw and normalized datasets locally and on Hadoop HDFS. Ingestion is followed by Apache Pig ETL cleaning and aggregation, Apache Spark analytical processing (weather correlation, trend analysis, and a 24-hour rolling CPCB AQI index), and publishing to a multi-page Streamlit dashboard and a unified CLI. Additionally, the proposal mandates a dual-profile environment (`modern` vs `lab`) to guarantee backwards compatibility and rollback, verified through synthetic scale benchmarks (1×, 10×, 100×) and automated smoke tests.

### **How the existing system works, per this document**
Per these documents, no functional software system currently exists. All 33 specified requirements are marked "Pending" with 0 out of 11 phase plans executed, and no pre-existing codebase, baseline data pipeline, or legacy architecture is described. The operational state is greenfield, relying entirely on planned implementation of schemas (`config/schemas.yaml`), station discovery (`config/stations.json`), and target directory structures that have not yet been created.

---

## Prioritized Findings

### [C1] Dangling & Unlinked External Requirement Traceability Keys (`FR-01` to `FR-36`, `NFR-08`) — Severity: Major
- **Reviewer Lens:** `egm-comprehension`
- **Affected Scope:** `REQUIREMENTS.md` (Lines 10–54, Section: "v1 Requirements")
- **Discrepancy Description & Rationale:** Every requirement item lists parenthetical citations (e.g., `(FR-01)`, `(FR-11, FR-12)`, `(NFR-08)`), but neither `REQUIREMENTS.md` nor `ROADMAP.md` contains an index, glossary, or mapping table defining what "FR" or "NFR" stands for, what external document these codes point to (such as `docs/SkyWatch — Project Specification v2.0.md`), or the specific text of the referenced specifications. A reader without external context cannot verify whether the requirements fully satisfy the referenced source specs.

### [C2] Undefined Domain Acronyms, Standards, & Fallback Behavior for CPCB AQI — Severity: Major
- **Reviewer Lens:** `egm-comprehension`
- **Affected Scope:** `ROADMAP.md` (Lines 5, 11, 49, 55, 71); `REQUIREMENTS.md` (Requirement SPK-04, Line 38; OPS-04, Line 50)
- **Discrepancy Description & Rationale:** The acronyms "CPCB" (Central Pollution Control Board) and "AQI" (Air Quality Index) are never expanded or defined. While the document mentions "piecewise linear sub-index interpolation", the ">=16 valid hours rule", and "max-subindex overall rating", it provides no breakpoint table, formulas, or standard references. Crucially, the logic fails to specify system behavior when a station has fewer than 16 valid hours in a 24-hour rolling window (e.g., whether the AQI evaluates to NULL, displays a specific warning status, or is omitted from alert evaluation).

### [C3] Unstated Profile Switching Mechanics & Environment Profile Specs (`modern` vs `lab`) — Severity: Major
- **Reviewer Lens:** `egm-comprehension`
- **Affected Scope:** `ROADMAP.md` (Lines 9, 25, 73, 79, 80); `REQUIREMENTS.md` (Requirements OPS-06, Line 52; Core Value, Line 4)
- **Discrepancy Description & Rationale:** The roadmap requires dual execution profiles (`modern` and `lab`), a switching script (`use_profile.sh`), and a "profile rollback drill (`rollback_drill.sh`)". However, neither document specifies what constitutes `modern` versus `lab` (such as OS, Java/OpenJDK versions, Python versions, Hadoop/Spark distribution versions, or configuration differences). Furthermore, the mechanism for executing identical code across disparate versions without hardcoding is left completely unexplained.

### [C4] Missing Spatial Join Logic Between Weather Coordinates & Air-Quality Stations — Severity: Major
- **Reviewer Lens:** `egm-comprehension`
- **Affected Scope:** `ROADMAP.md` (Phase 3 Success Criteria #2, Line 54); `REQUIREMENTS.md` (Requirement SPK-03, Line 37; ING-02, Line 11)
- **Discrepancy Description & Rationale:** ING-02 specifies collecting weather metrics from Open-Meteo, and SPK-03 specifies joining air quality with weather metrics on "station hour". However, Open-Meteo is a coordinate-based weather model, not a physical station network like OpenAQ. The document does not describe how weather coordinates are mapped to air-quality stations (e.g., nearest-neighbor coordinates per station, a single city-center centroid for Bengaluru, or station metadata coordinate queries), leaving the exact join key and data alignment unresolved.

### [C5] Undefined Watermark State Persistence & Cold-Start Lifecycle — Severity: Major
- **Reviewer Lens:** `egm-comprehension`
- **Affected Scope:** `ROADMAP.md` (Phase 1 Success Criteria #4, Line 24; Plan 01-03, Line 31); `REQUIREMENTS.md` (Requirement ING-05, Line 14)
- **Discrepancy Description & Rationale:** Requirement ING-05 specifies incremental ingestion using "per-sensor watermarks". The document does not clarify what constitutes a watermark (high watermark timestamp vs ingestion epoch), where or how this state is persisted (local disk, HDFS, JSON, SQLite), how race conditions or pipeline failures update this state, or how initial collection (cold start/bootstrap) handles sensors with no prior watermark.

### [C6] Undefined Baseline Volume for Synthetic Scale Benchmarking — Severity: Major
- **Reviewer Lens:** `egm-comprehension`
- **Affected Scope:** `ROADMAP.md` (Phase 3 Success Criteria #5, Line 57; Plan 03-03, Line 63); `REQUIREMENTS.md` (Requirement OPS-08, Line 54)
- **Discrepancy Description & Rationale:** Requirement OPS-08 mandates testing runtime performance across "1×, 10×, and 100× volume" using `make_synthetic.py`. The documents never define what base volume "1×" represents (e.g., number of rows, station-years, file size in megabytes/gigabytes, or temporal duration). Without a defined 1× baseline, the benchmark targets are ambiguous and unverifiable.

### [C7] Unclear Data Handoff Contract & Schema Propagation Between Apache Pig & Spark — Severity: Major
- **Reviewer Lens:** `egm-comprehension`
- **Affected Scope:** `ROADMAP.md` (Phase 2 Success Criteria #1 & #3, Lines 38–40; Phase 3 Goal, Line 49); `REQUIREMENTS.md` (Requirements PIG-01, PIG-03, SPK-04, Lines 29, 31, 38)
- **Discrepancy Description & Rationale:** PIG-01 specifies outputting "headerless clean records" to HDFS, and PIG-03 specifies producing "hourly rollups feeding downstream Spark trend analysis". However, SPK-04 requires computing a 24-hour rolling CPCB index. It is unclear whether Spark consumes the headerless clean records, the hourly rollups, or normalized raw data. Because Pig outputs headerless CSV records, the document does not specify how Spark dynamically or explicitly discovers and binds field names and data types without risking schema drift.

### [C8] Ambiguous Acceptance Criteria Regarding "Plausible Ceilings" & Validation Rules — Severity: Nit
- **Reviewer Lens:** `egm-comprehension`
- **Affected Scope:** `REQUIREMENTS.md` (Requirement HYG-04, Line 22)
- **Discrepancy Description & Rationale:** HYG-04 mandates validating records for "numeric range, unit sanity, and plausible ceilings", but neither `REQUIREMENTS.md` nor `ROADMAP.md` lists the acceptable range bounds or numerical threshold limits (e.g., maximum μg/m³ for PM2.5/PM10, temperature limits) or where those constraints are codified.

### [C9] Requirement Prefix & Lifecycle Phase Mismatch for Scale Benchmarking — Severity: Nit
- **Reviewer Lens:** `egm-comprehension`
- **Affected Scope:** `REQUIREMENTS.md` (Requirement OPS-08, Line 54 and Traceability Line 108); `ROADMAP.md` (Line 51)
- **Discrepancy Description & Rationale:** OPS-08 ("Synthetic scale benchmarking suite") is categorized under "User Interface & Operations" in `REQUIREMENTS.md` alongside UI and CLI tasks, but is assigned to Phase 3 (Analytics Engine with Apache Spark) rather than Phase 4 (UI & Operations) in both the roadmap and the traceability matrix. The `OPS-` prefix obscures the analytical/Spark nature of the requirement.

### [C10] Conflation of Pipeline Telemetry Alerts and Environmental Public Health Alerts — Severity: Nit
- **Reviewer Lens:** `egm-comprehension`
- **Affected Scope:** `REQUIREMENTS.md` (Requirements SPK-05, SPK-06, Lines 39–40); `ROADMAP.md` (Line 56)
- **Discrepancy Description & Rationale:** Requirement SPK-05 specifies generating alerts for "Poor/Severe categories, offline stations, and stale readings". Combining public health advisories (Poor/Severe ambient air pollution) and infrastructure telemetry (offline sensor, stale pipeline data) into a single alert mechanism creates confusion regarding recipient audience and notification semantics.

---

## Summary

| Severity | Count |
|---|---|
| 🔴 Blocker | 0 |
| 🟡 Major | 7 (C1, C2, C3, C4, C5, C6, C7) |
| 🔵 Nit | 3 (C8, C9, C10) |
| **Total** | **10** |
