---
last_mapped_commit: 6b4cc6dd01b87e70230978c94258f457e4b5fcb4
last_mapped_at: 2026-11-26
---
# Coding Conventions

**Analysis Date:** 2026-11-26

## Naming Patterns

- **Backend Modules and Packages:** `snake_case` (e.g. `src/skywatch/spark/weather_analysis.py`, `src/skywatch/pipeline/spark_runner.py`, `api/index.py`).
- **Frontend Source Files:**
  - Pages & Layouts: `page.tsx`, `layout.tsx` following Next.js App Router conventions.
  - Components: `kebab-case` or `PascalCase` (e.g. `frontend/components/skywatch.tsx`, `frontend/components/ui/button.tsx`).
  - Utilities & Schemas: `camelCase` (e.g. `frontend/lib/api.ts`, `frontend/lib/schemas.ts`, `frontend/lib/utils.ts`).
- **Classes & Types:** `PascalCase` (e.g. `IngestionOrchestrator`, `WatermarkManager`, `Meta`, `Station`, `AirIndex`).
- **Functions and Methods:** `snake_case` for Python (`calculate_cpcb_rolling_index_records`), `camelCase` for TypeScript (`loadMeta`, `loadStations`, `getData`).
- **Constants:** `UPPER_SNAKE_CASE` (e.g. `BENGALURU_BBOX`, `CPCB_BREAKPOINTS`, `MANIFEST_HEADERS`).
- **Timestamps:** Variables storing UTC timestamps explicitly end in `_utc` or `_str` (e.g. `measurement_time_utc`, `window_end_utc`, `generated_at_utc`, `last_measurement_time_utc`).

## Code Style & Tooling

**Python (Backend):**
- **Formatter & Linter:** Ruff (`ruff>=0.1.0`) configured in `pyproject.toml`:
  - `target-version = "py38"`
  - `line-length = 100`
  - Rule selection: `select = ["E", "F", "I", "W"]`, `ignore = ["E501"]`.
- **Type Annotations:** Full type annotations required on all function signatures, dataclasses, and module interfaces using standard `typing` constructs (`Optional`, `Dict`, `List`, `Tuple`, `Union`, `Any`).

**TypeScript & React (Frontend):**
- Strict TypeScript configuration (`tsconfig.json`) with strict null checks enabled.
- Zod schemas in `frontend/lib/schemas.ts` serve as runtime type guards validating external API data against TypeScript interfaces (`z.infer<typeof schema>`).
- Tailwind CSS v4 utility classes organized using `clsx` and `tailwind-merge` via `cn()` helper (`frontend/lib/utils.ts`).

## Python 3.8 Floor (Mandatory)

SkyWatch strictly enforces compatibility with Python 3.8:
- Every Python file must include `from __future__ import annotations` at the top.
- Do NOT use Python 3.10+ union types (e.g. `int | str`). Always use `Union[int, str]` or `Optional[T]`.
- Do NOT use structural pattern matching (`match` / `case` statements). Use `if/elif/else` ladders.
- Do NOT use built-in collection types as type hints (e.g. `list[str]`, `dict[str, Any]`). Use `typing.List` and `typing.Dict`.
- Ensure standard library imports do not rely on Python 3.9+ additions (e.g. `removeprefix`, `removesuffix`, or `zoneinfo`).

## Git Branching, PR & Review Protocol (Mandatory Rule)

As specified in `AGENTS.md` and repository instructions:
- **Never commit directly to `main`:** All modifications must be developed and committed on a dedicated descriptive branch (e.g. `fix/...`, `feature/...`, `chore/...`).
- **Never merge without a PR:** Every branch must be pushed to GitHub and opened as a Pull Request with complete change summaries, verification evidence, and CI/CD results.
- **Passing CI Gate:** Automated checks (Ruff linting, Secrets scanner, 80% coverage gate, and local ETL jobs) must pass before merging.
- **Merge Method:** Pull requests are squashed or rebased into `main` after review.

## Logging & Engine Transparency

- **Structured JSON Logging:** All backend pipeline events use `skywatch.pipeline.logging.get_logger()`, outputting timestamp, `run_id`, log level, component name, and message as structured JSON lines.
- **Credential Redaction:** Sensitive variables (API keys, passwords) are automatically scrubbed via `redact_sensitive()`.
- **Engine Transparency:** Pig and Spark runners log their active execution engine tag (`[ENGINE: APACHE_SPARK]`, `[ENGINE: APACHE_PIG]`, or `[ENGINE: STANDALONE_PYTHON_SIMULATION]`) and record the engine in run result objects and metadata exports.
