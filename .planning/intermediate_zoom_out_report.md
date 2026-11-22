# Intermediate Zoom Out Review Report — Stage 8

**Targets:** `.planning/ROADMAP.md`, `.planning/REQUIREMENTS.md`  
**Stage:** 8 — Intermediate Zoom Out Review  
**Reviewer:** `egm-zoom-out`  
**Date:** 2026-11-22  

---

## 1. Overall Evaluation

The planning documents (`.planning/ROADMAP.md`, `.planning/REQUIREMENTS.md`, `.planning/PROJECT.md`) have been thoroughly remediated following Stage 5 (Initial Zoom Out) and Stage 7 (Antagonistic Critic) reviews. Strategic risks, architectural blockers, and operational ambiguities have been cleanly resolved.

Key improvements verified:
- **Runtime & Resource Realism**: Pig 0.17 is pinned to Java 8 across all profiles, eliminating JDK 17 reflection crash risks (`CR-01`). Local mode execution (`pig -x local`) is formalized to prevent YARN memory exhaustion on 1–2 GB heap envelopes (`CR-02`).
- **Data & Boundary Contracts**: HDFS directory contracts distinguish ingest time (`ingest_dt`) from event time (`event_dt`) (`CR-06`). Pig-to-Spark handoffs are bound by sidecar `_schema.json` verification (`F1`, `F10`).
- **Scientific & Calculation Rigor**: CPCB 24-hour PM-based AQI mandates two-stage aggregation (clock-hour means first, then $\ge 16$ hour rolling window) with explicit suppression reason codes for sub-threshold records (`F4`, `CR-05`).
- **Traceability & Verifiability**: Specification anchors link requirements to master functional specs (`CR-03`). Plan-level requirement tags (`CR-12`), v1 and v2 traceability matrices (`CR-11`), quantitative dual-profile tolerances ($\epsilon < 0.01$, matching row counts) (`F6`), and scale benchmark volume definitions (`F7`, `CR-14`) make the plan mechanically auditable.

The roadmap and requirement set are structurally sound, strategically coherent, and free of blocking architectural contradictions.

---

## 2. Review Status of Prior Blockers and Major Issues

### Stage 5 Findings (Initial Zoom Out)

| Finding ID | Title | Prior Severity | Status | Verification Detail |
|---|---|---|---|---|
| **F1** | No Machine-Enforced Data Contract Between Pig & Spark | Blocker | **Resolved** | PIG-01, Plan 02-02, and Plan 03-01 explicitly mandate sidecar `_schema.json` emission and Spark pre-read validation. |
| **F2** | HDFS Cluster Bring-Up Missing from Requirements | Blocker | **Resolved** | Added *Prerequisites & Environment Readiness* section to `ROADMAP.md` requiring NameNode/DataNode liveness and safe mode verification before Phase 1. |
| **F3** | Live API External Dependency Without Fixtures | Blocker | **Resolved** | HYG-07 and Plan 01-02 explicitly require mock fixture layers; Phase 1 discovery success criterion accepts verified fixtures. |
| **F4** | CPCB Silent Non-Emission Conflates Operational Failure | Major | **Resolved** | SPK-04 and Phase 3 success criterion 3 specify structured suppression records when valid hours < 16. |
| **F5** | Pig SLA Measured Against Undefined Sample Data | Major | **Resolved** | Bound to committed frozen sample dataset in Plan 02-02 and `PROJECT.md`. |
| **F6** | Undefined "Matching Outputs" for Dual-Profile Verification | Major | **Resolved** | Phase 4 success criterion 4 specifies programmatic verification: matching row counts and floating-point difference $\epsilon < 0.01$. |
| **F7** | Scale Benchmark 100× OOM Failure Mode | Major | **Resolved** | Retagged as SPK-10; benchmark formalizes `--mode=local` and `--mode=report` to prevent unhandled driver crashes. |
| **F8** | Undefined Logging Schema (`OPS-07`) | Major | **Resolved** | Plan 01-01 introduces structured logging contract with unique `run_id` field; verified mechanically in smoke suite. |
| **F9** | Watermark Drift Risk via Separate Atomic Operations | Major | **Resolved** | ING-05 and Plan 01-03 mandate atomic watermark commits coupled with file renames. |
| **F10** | Unenforced Pig Schema Handoff | Major | **Resolved** | Sidecar `_schema.json` integrated into Plan 02-02 and Plan 03-01. |

---

### Stage 7 Findings (Antagonistic Critic)

| Finding ID | Title | Prior Severity | Status | Verification Detail |
|---|---|---|---|---|
| **CR-01** | Java 17+ Incompatibility with Apache Pig 0.17 | Blocker | **Resolved** | `PROJECT.md` and `ROADMAP.md` mandate Java 8 pinned for Apache Pig across both `modern` and `lab` profiles. |
| **CR-02** | YARN Container Memory Exhaustion on 1GB–2GB Constraints | Blocker | **Resolved** | Prerequisites section specifies Pig local execution mode (`pig -x local`) / standalone local runner. |
| **CR-03** | Unresolved External Requirement Identifiers (`FR-01`–`FR-36`) | Blocker | **Resolved** | `REQUIREMENTS.md` adds *Specification Anchor* linking directly to `docs/SkyWatch — Project Specification v2.0.md`. |
| **CR-04** | Promotional Hyperbole and Subjective Buzzwords | Blocker | **Resolved** | All subjective adjectives stripped; replaced with objective, testable conditions across both files. |
| **CR-05** | Missing Hourly Regularization in CPCB 24h Rolling Window | Major | **Resolved** | Mandated two-stage aggregation in `PROJECT.md` Key Decisions, `ROADMAP.md` Phase 3, and `SPK-04`. |
| **CR-06** | Ambiguous HDFS Partition Semantics | Major | **Resolved** | Partition keys separated: `/raw/` and `/normalized/` use `ingest_dt`; `/clean/` and `/analytics/` use `event_dt`. |
| **CR-07** | Redundant Compute Passes Between Pig & Spark | Major | **Resolved** | Boundaries clarified: Pig handles hygiene, distinct filtering, and hourly rollups (`PIG-01`–`03`); Spark consumes rollups for ranking, trend analytics, and AQI. |
| **CR-08** | Weather Join Spatial Mismatch & Rate Limits | Major | **Resolved** | Join defined on station-hour; spatial methodology documented in Phase 4 OPS-01/04. |
| **CR-09** | Missing Page-Level Resumability in Backfills | Major | **Resolved** | ING-05 and Plan 01-03 mandate chunked backfilling with checkpoint commits. |
| **CR-10** | Taxonomy Mismatch for `OPS-08` | Major | **Resolved** | Re-tagged as `SPK-10` in Phase 3 in both `REQUIREMENTS.md` and `ROADMAP.md`. |
| **CR-11** | Traceability Matrix Omitting v2 Requirements | Major | **Resolved** | Explicit v1 Traceability Matrix (33 items) and v2 Traceability Matrix (4 deferred items) documented with 0 unmapped items. |
| **CR-12** | Plan-Level Requirements Traceability Missing in Roadmap | Major | **Resolved** | Every plan (01-01 through 04-03) annotated with specific requirement tags. |
| **CR-13** | Duplicate Pseudo-Headings Violating Hierarchy | Major | **Resolved** | Converted to standard Markdown `#### Plans` sub-headings throughout `ROADMAP.md`. |

---

## 3. Residual and Newly Discovered Findings

### [RES-01] Weather Join Spatial Strategy Clarification During Implementation
- **Severity**: Nit / Informational
- **Scope**: Phase 1 (`ING-02`) / Phase 3 (`SPK-03`)
- **Issue**: `ING-02` specifies collecting hourly weather in UTC and `SPK-03` joins air quality with weather metrics on station hour. It does not hardcode whether Open-Meteo is polled per station coordinate (feasible within 10k daily API limit) or at a central Bengaluru centroid coordinate.
- **Recommendation**: During execution of Plan 01-02 and Plan 03-02, document the coordinate selection policy in code and surface the spatial aggregation assumption on the dashboard's Methodology page (`OPS-01`).

### [RES-02] Prerequisite Scaffolding Tagging in Plan 01-01
- **Severity**: Nit / Informational
- **Scope**: Plan 01-01 (`[HYG-01, HYG-02, OPS-06, OPS-07]`)
- **Issue**: Plan 01-01 tags `OPS-06` (profile switching) and `OPS-07` (logging), whereas `REQUIREMENTS.md` assigns completion of both requirements to Phase 4.
- **Recommendation**: No structural conflict exists because Plan 01-01 only creates the foundational environment profiles (`modern.env`, `lab.env`) and defines the logging schema (`run_id`), while Phase 4 validates switching scripts and end-to-end logging. Ensure Plan 01-01 test assertions only check env file parsing rather than full profile switching drills.

---

## 4. Final Recommendation

**Recommendation: GO**

The requirements and roadmap are structurally coherent, mathematically sound, and aligned with system resource constraints. There are zero blocking issues. The project is ready to advance immediately to implementation planning for Phase 1.
