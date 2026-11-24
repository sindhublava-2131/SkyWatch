# SkyWatch: Comprehensive System Setup and Operations Guide

This guide details the complete prerequisites, installation, execution, and troubleshooting workflows for the SkyWatch Environmental Intelligence platform.

---

## 1. System Requirements

### Hardware Prerequisites
- Operating System: Linux (Ubuntu 20.04 LTS / 22.04 LTS / Debian 12 recommended) or macOS
- Processor: x86_64 or ARM64 (Apple Silicon) with at least 4 physical cores
- Memory: Minimum 8 GB RAM (16 GB recommended for full local Big Data stack)
- Disk: Minimum 10 GB free space for HDFS staging, partitions, and dependencies

### Software Dependencies
- Python: Version 3.8, 3.9, 3.10, 3.11, or 3.12 (Python 3.8 syntax floor strictly preserved)
- Node.js & Package Manager: Node.js 18+ and pnpm 9+ (for Next.js 15 interactive frontend)
- Java Runtime:
  - OpenJDK 8 (`java-8-openjdk`): Required for Apache Pig 0.18.0 and Lab Profile Spark 3.2.2
  - OpenJDK 17 (`java-17-openjdk`): Recommended for Modern Profile Spark
- Distributed Big Data Frameworks (Optional for Simulation Mode):
  - Apache Hadoop (3.4.3 or 3.2.2)
  - Apache Pig 0.18.0
  - Apache Spark (4.3.0 or 3.2.2)

---

## 2. Installation and Virtual Environment Setup

### 2.1 Backend Virtual Environment
```bash
# Clone the SkyWatch repository
git clone https://github.com/google/SkyWatch.git
cd SkyWatch

# Create Python virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Upgrade packaging tooling
pip install --upgrade pip setuptools wheel

# Install package in editable mode with development dependencies
pip install -e ".[dev]"
```

### 2.2 Frontend Dependencies
```bash
cd frontend
pnpm install
cd ..
```

---

## 3. Dual-Profile Runtime Architecture

SkyWatch supports two operational profiles to ensure portability between modern cluster environments and legacy university laboratory stacks:

| Component | Modern Profile (2026 Production) | Lab Profile (Agreed Rollback Stack) |
| :--- | :--- | :--- |
| Hadoop | Apache Hadoop 3.4.3 | Apache Hadoop 3.2.2 |
| Pig | Apache Pig 0.18.0 | Apache Pig 0.18.0 |
| Spark | Apache Spark 4.3.0 | Apache Spark 3.2.2 (compatible with Python 3.8) |
| Java for Pig | OpenJDK 8 | OpenJDK 8 |
| Java for Spark | OpenJDK 17 | OpenJDK 8 |
| Default Timeout | 20 seconds | 30 seconds |

### Profile Switching
```bash
# Activate modern profile
source scripts/use_profile.sh modern

# Activate lab rollback profile
source scripts/use_profile.sh lab

# Inspect environment diagnostics
bash scripts/use_profile.sh modern
```

---

## 4. Unified CLI Operations (`skywatch`)

The `skywatch` command-line interface provides unified control over all pipeline stages:

### Station Discovery
Scans the Bengaluru metropolitan bounding box (12.70N to 13.20N, 77.40E to 77.85E) for active CAAQMS monitoring stations:
```bash
skywatch discover --max-age-hours 72 --output config/stations.json
```

### Data Collection
Performs scheduled incremental or latest telemetry polling:
```bash
# Incremental collection
skywatch collect --mode incremental

# Offline collection using frozen fixtures
skywatch collect --mode incremental --offline
```

### Historical Chunked Backfill
Executes date-partitioned historical ingestion without API rate limit exhaustion:
```bash
skywatch backfill --start-date 2026-01-01 --end-date 2026-03-31 --chunk-days 7 --offline
```

### Multi-Stage Pipeline Execution
Executes the four-stage batch pipeline:
```bash
# Full offline pipeline with simulation fallback
skywatch pipeline --profile modern --offline --simulate

# Full pipeline requiring real Apache Spark and Apache Pig binaries
skywatch pipeline --profile modern --require-real-engine
```

### Static API Lake Publishing
Exports published analytical tables to static JSON endpoints:
```bash
skywatch export --base-output-dir output/published --stations-file config/stations.json
```

### Acceptance Smoke Testing
Executes comprehensive end-to-end smoke verification:
```bash
skywatch smoke --simulate --offline
```

---

## 5. Next.js 15 Interactive Dashboard

The interactive frontend provides five unified analytical views:
1. **Overview**: Real-time city-wide average AQI card, dominant pollutant badge, active station counters, and simulation indicator.
2. **Stations Map**: MapLibre GL geospatial map rendered with CPCB category colors, observation timestamps, and freshness badges.
3. **Analytics**: Recharts diurnal curves bound to IST time ranges (08:30 - 09:30 IST) and Pearson correlation matrices.
4. **Alerts & Reports**: Filterable health and telemetry alerts table with direct CSV report downloads.
5. **Methodology**: MoEFCC/CPCB sub-index mathematical breakpoints reference, data pipeline architecture, and profile switcher guide.

### Running Frontend Locally
```bash
cd frontend

# Development server
pnpm dev

# Type checking
pnpm exec tsc --noEmit

# Optimized production build
pnpm build
```

---

## 6. Automation Makefile Reference

```bash
# Run Ruff linting
make lint PYTHON=.venv/bin/python

# Run complete pytest test suite (118+ tests)
make test PYTHON=.venv/bin/python

# Run test coverage gate (enforces >=80% on core logic)
make coverage PYTHON=.venv/bin/python

# Run smoke test gate requiring real engines
make smoke PROFILE=modern

# Run smoke test gate allowing simulation fallback
make smoke-simulation PROFILE=modern

# Run frontend typecheck and production build
make frontend-check
make frontend-build

# Run full-stack backend-to-frontend end-to-end test suite
make e2e PYTHON=.venv/bin/python

# Run dual-profile rollback verification drill
make drill

# Clean build and test artifacts
make clean
```
