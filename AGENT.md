# SkyWatch Agent Instructions

These are the canonical repository rules for Codex and any agent working in SkyWatch. Claude Code, Gemini CLI, and VS Code Copilot have small native entry points that refer here. These files guide agent behavior; they cannot guarantee that a host loaded or obeyed them.

## 1. Establish project and task context

- Before acting, read `.planning/STATE.md` and confirm the task belongs to the active phase. Read `.planning/PROJECT.md`, `.planning/ROADMAP.md`, `.planning/REQUIREMENTS.md`, and the applicable phase's spec, plan, research, context, and verification artifacts before implementation. Read the relevant project specification and docs under `docs/`; for architecture- or repository-wide work, review all applicable planning and project docs.
- Treat planned modules as plans, not proof that source code exists. Inspect the actual repository and ask only when a scope or contract is genuinely ambiguous.
- State a narrow goal and identify affected files, functions, callers/callees, data contracts, configuration, and side effects before changing code.

## 2. Graphicle and impact review before code changes

- Before a code change, run `graphicle sync` for this repository. Inspect the graph's relevant nodes/edges and its file/symbol coverage (use the Graphicle MCP tools when available; otherwise use `graphicle status` and `graphicle context "<question>"`).
- Treat Graphicle as static relationship evidence, not a runtime call graph or a complete inventory. Record gaps and stale/missing source coverage. Do not infer that an unlisted caller, side effect, or dependency does not exist.
- Read the complete touched functions and relevant source files. Cross-check likely callers/callees, configuration, schemas, tests, and side effects with source search and direct inspection. For a narrow change, inventory symbols in touched files and impacted callers/callees; build a full function inventory only for repository-wide or architectural work.
- If Graphicle is unavailable or sync/query fails, report that limitation and continue with direct source reading and search; never present a failed query as a successful review.

## 3. Make safe, scoped changes

- **Git Branching & CI/CD Merge Protocol (Mandatory Rule)**: Commit all changes to a new branch (never commit working code directly to `main`). For each phase completion, if the CI/CD pipeline and test suite are a success, merge the branch into `main`.
- Use an isolated sandbox/worktree when one is available. If not, record the starting worktree state, make the smallest reviewable change, and preserve a clear rollback path. Do not overwrite or revert pre-existing user work; revert only changes you made.
- Keep each change focused. Avoid unrelated files and unnecessary permanent catalogs or generated artifacts. Update relevant documentation and contracts when behavior changes.
- Never commit secrets, credentials, personal data, or sensitive API responses. Do not expose Hadoop, HDFS, YARN, or Spark services publicly. Avoid destructive Git actions (including `reset --hard`, `clean`, force-push, or broad checkout/revert commands); do not run them unless explicitly requested and the exact affected files are confirmed.

## 4. SkyWatch engineering constraints

- Preserve the Python 3.8 syntax floor: use compatible annotations and `from __future__ import annotations` where appropriate; do not use newer-only syntax such as `match` or `X | Y`.
- Use type hints and single-responsibility functions. Keep network I/O, normalization/validation, storage, analytics, orchestration, and rendering separate.
- Keep `modern` and `lab` profiles switchable without hardcoding runtime versions in application logic. Check rollback compatibility for affected code and dependencies.
- Treat `config/schemas.yaml` as the data-contract source of truth when present. Readers of headerless data must use explicit schemas; preserve UTC, deterministic outputs, idempotency, and atomic-write guarantees where relevant.
- Keep health outputs and methodology honest: label the index as PM-based CPCB where applicable; do not imply forecasting, causation, live readings, or unverified profile compatibility.

## 5. Verify and report honestly

- Add or update a targeted regression test when behavior changes. Run the relevant tests and applicable lint, type, build, smoke, and profile checks. Inspect CI workflow definitions and status when available; do not claim CI is green unless it was actually observed.
- Prefer reproducible local/frozen data and mocks; do not depend on live APIs or credentials for ordinary CI tests. Report exact commands and pass/fail results. If a check cannot run, say why and what remains unverified.
- Before finishing, review the scoped diff and worktree; confirm protected or unrelated files were not changed. Summarize changed files, graph coverage/limitations, verification evidence, and any remaining risk.

<!-- graphicle:start (added by graphicle setup; graphicle disconnect removes it) -->
## Graphicle
Use the `graphicle` MCP server for where or how something works, for questions across files or history, and before changing unfamiliar code: `recall_project` or `query_graph` with the question, then `get_node` and `get_neighbors` for one symbol; answers cite file and lines. For an exact identifier you already know, grep is enough.
Without the server, run `graphicle context "<question>"` in a terminal.
<!-- graphicle:end -->
