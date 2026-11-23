---
last_mapped_commit: 831a73a6e8025d4970ff7b506be5de6d304b4c61
last_mapped_at: 2026-11-23
---
# Coding Conventions

**Analysis Date:** 2026-11-23

## Naming Patterns

- **Modules and Packages:** `snake_case` (e.g. `src/skywatch/spark/weather_analysis.py`, `src/skywatch/pipeline/spark_runner.py`).
- **Classes:** `PascalCase` (e.g. `IngestionOrchestrator`, `WatermarkManager`, `SparkRunResult`).
- **Functions and Methods:** `snake_case` (e.g. `calculate_cpcb_rolling_index_records`, `export_published_dataset`).
- **Constants:** `UPPER_SNAKE_CASE` (e.g. `BENGALURU_BBOX`, `CPCB_BREAKPOINTS`, `MANIFEST_HEADERS`).
- **Timestamps:** Variables storing UTC timestamps explicitly end in `_utc` or `_str` (e.g. `measurement_time_utc`, `window_end_utc`, `generated_at_utc`).
- **Directories and Files:** Shell scripts use `.sh`, Pig scripts use `.pig`, Python uses `.py`, configuration uses `.yaml` and `.json`.

## Code Style

- **Formatter & Linter:** Ruff (`ruff>=0.1.0`) configured in `pyproject.toml`:
  - `target-version = "py38"`
  - `line-length = 100`
  - Rule selection: `select = ["E", "F", "I", "W"]`, `ignore = ["E501"]` (long lines tolerated where readability of strings or URLs benefits).
- **Type Annotations:** Type annotations are required on all public function signatures, dataclasses, and module interfaces. Standard `typing` constructs (`Optional`, `Dict`, `List`, `Tuple`, `Any`) are used.
- **Docstrings:** Google/Sphinx style docstrings describing function purpose, parameters, return types, and potential exceptions.

## Python 3.8 Floor (Mandatory)

SkyWatch strictly enforces compatibility with Python 3.8:
- Every Python file must include `from __future__ import annotations` at the top.
- Do NOT use Python 3.10+ union types (e.g. `int | str`). Always use `Union[int, str]` or `Optional[T]`.
- Do NOT use structural pattern matching (`match` / `case` statements). Use `if/elif/else` ladders.
- Do NOT use built-in collection types as type hints (e.g. `list[str]`, `dict[str, Any]`). Use `typing.List` and `typing.Dict`.
- Ensure standard library imports do not rely on 3.9+ additions (e.g. `removeprefix`, `removesuffix`, or `zoneinfo`).

## Import Organization

Imports are ordered according to PEP 8 and sorted via Ruff (`I` rules):
1. Future imports (`from __future__ import annotations`)
2. Standard library modules (e.g. `os`, `sys`, `json`, `pathlib`, `typing`)
3. Third-party packages (e.g. `yaml`, `requests`, `pyarrow`, `pyspark`)
4. Internal project imports (`from skywatch...`)

```python
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

from skywatch.analytics_core.cpcb import subindex
from skywatch.pipeline.logging import get_logger
```

## Error Handling

- **Domain-Specific Exceptions:** Custom exception classes inherit from standard Python exceptions (e.g. `AuthenticationError` in `openaq_client.py`).
- **Non-Fatal vs Fatal Errors:**
  - Network timeouts and HTTP 5xx errors are caught and retried using exponential backoff with jitter.
  - Authentication errors (HTTP 401/403) fail immediately without retry.
  - Malformed records in ingestion are logged, marked `is_valid=False` with a `reject_reason`, and routed to audit logs rather than crashing the pipeline.
- **Fail-Safe Fallbacks:**
  - `pig_runner.py` and `spark_runner.py` check runtime availability (`is_pig_available()`, `is_spark_available()`). If absent, they log warnings and execute pure-Python standalone fallbacks to guarantee testability and zero-JVM execution.

## Logging

- Centralized logging utility: `src/skywatch/pipeline/logging.py`.
- Format: Structured JSON logging (`JsonPipelineFormatter`) including `timestamp`, `level`, `logger`, `message`, and dynamic `run_id`.
- Sensitive Data Redaction: `redact_sensitive()` masks credentials, tokens, and API keys before output.
- Log instances: Modules instantiate loggers using `logger = get_logger(__name__)`.

## Comments

- Focus on **why** rather than **what**.
- Cite standards and official specifications where applicable (e.g. citing CPCB 2014 guidelines in `src/skywatch/analytics_core/cpcb.py`).
- Document mathematical boundaries and interpolation formulae in docstrings.

## Function Design

- Single responsibility principle: Data extraction, validation, transformation, and storage are kept strictly in separate functions.
- Deterministic behavior: Given the same input records, transformation functions return identical, deterministic output tuples or dictionaries.
- Side-effect isolation: Computation functions do not perform network or disk I/O; I/O is isolated in dedicated storage adapters or client classes.

## Module Design

- Clean package boundaries with explicit `__all__` exports where helpful.
- Low coupling: Components communicate through well-defined data structures (dictionaries adhering to `config/schemas.yaml` or PySpark DataFrames).
- Profile-agnostic logic: Source code must not check `if profile == "modern"` for fundamental business logic; profile differences are confined to configuration, environment variables, and Spark tuning parameters.
