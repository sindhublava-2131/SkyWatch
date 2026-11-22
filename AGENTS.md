# SkyWatch Agent Instructions

These are the canonical repository rules for Codex and any agent working in SkyWatch. Claude Code, Gemini CLI, and VS Code Copilot have small native entry points that refer here. These files guide agent behavior; they cannot guarantee that a host loaded or obeyed them.

## 1. Establish project and task context

- Before acting, read `.planning/STATE.md` and confirm the task belongs to the active phase. Read `.planning/PROJECT.md`, `.planning/ROADMAP.md`, `.planning/REQUIREMENTS.md`, and the applicable phase's spec, plan, research, context, and verification artifacts before implementation. Read the relevant project specification and docs under `docs/`; for architecture- or repository-wide work, review all applicable planning and project docs.
- Treat planned modules as plans, not proof that source code exists. Inspect the actual repository and ask only when a scope or contract is genuinely ambiguous.
- State a narrow goal and identify affected files, functions, callers/callees, data contracts, configuration, and side effects before changing code.

## 2. Graphicle and impact review before code changes

- Before changing code, always synchronize and inspect the project code graph using Graphicle:
  1. **Sync repository state**: Run `graphicle sync` (or `graphicle sync --quiet` / `graphicle sync --background`) to ensure on-disk symbols, dependencies, and imports are fully indexed in the graph store.
  2. **Inspect project status**: Run `graphicle status` to verify graph generation, node/edge counts, symbol coverage, and agent connectivity.
  3. **Assess symbol provenance and caller/callee context**:
     - Run `graphicle graph explain "<symbol>"` (or with `--all`) to inspect node provenance, definitions, and connections.
     - Run `graphicle graph affected "<symbol>"` (`--depth N`, `--relation R`) to analyze downstream blast radius, callers, and reverse dependencies.
     - Run `graphicle graph path "<source>" "<target>"` (`--directed` / `--undirected`) to trace the shortest dependency path between components.
  4. **Extract targeted context**:
     - Run `graphicle context "<question>"` to extract cited code evidence without invoking an LLM.
     - Run `graphicle graph query "<question>"` (`--budget N`, `--depth N`, `--seed <node/path>`) to traverse scoped subgraphs.
     - Check architecture hubs with `graphicle graph god-nodes` (`--top N`) when altering central modules.
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

## 6. Graphicle Comprehensive Command & Feature Reference

Graphicle provides static code graph analysis, repository memory, change planning, and agent MCP integration. Below is the complete reference covering all features and commands exposed by `graphicle --help`, `graphicle sync --help`, and `graphicle graph --help`.

### 6.1 Top-Level CLI Commands & Global Options

**Global Options:**
- `--data-dir <DIR>`: Path to Graphicle data directory (defaults to `GRAPHICLE_DATA_DIR` or platform default).
- `--config <FILE>`: Model configuration file path (default: `models.json` in data directory).
- `--read-only`: Open store read-only (disallows mutations; also activated via `GRAPHICLE_READ_ONLY=1`).
- `-h, --help`: Display command assistance.
- `-V, --version`: Display Graphicle version.

**Start Commands:**
- `graphicle setup`: Complete initial repository setup (sync, connect coding agents, register git hooks).
- `graphicle ask "<question>"`: Answer codebase questions using synthesized code evidence and project documentation.
- `graphicle context "<question>"`: Extract cited code evidence for a question without running an LLM.
- `graphicle open`: Launch Graphicle interactive web interface in browser.
- `graphicle status`: Display repository sync status, code graph metrics, connected agents, and model configuration.
- `graphicle doctor`: Diagnose environment issues, verify dependencies, and repair common configuration faults.

**Agent & MCP Integration:**
- `graphicle connect`: Configure and connect an agent environment to Graphicle MCP tools.
- `graphicle disconnect`: Remove Graphicle MCP configuration and hooks from agent environments.
- `graphicle mcp`: Run the MCP server over standard I/O (stdio) for agent integration.

**Data Management:**
- `graphicle sessions`: Capture, inspect, browse, and prune Claude Code and Codex agent sessions.
- `graphicle backup`: Create an online hot backup of the Graphicle data store without stopping background processes.
- `graphicle restore <backup>`: Restore a previously created backup archive into a target data directory.
- `graphicle offboard`: Completely remove a project's data and backup archives, issuing a verification receipt.

**Server & Collaboration:**
- `graphicle serve`: Start the persistent application server and streamable HTTP MCP server.
- `graphicle init`: Initialize a shared workspace for team server installations.
- `graphicle login`: Authenticate and attach a local workspace to a remote server using an access key.

**Specialized Agent Utilities:**
- `graphicle pack "<question>"`: Assemble project code relevant to a question into a single token-budgeted prompt.
- `graphicle plan "<change>"`: Generate an implementation plan with ordered steps, affected files, and unified diff sketches.
- `graphicle review`: Analyze change diffs between the current `HEAD` and its merge base with the target branch.
- `graphicle fix "<issue>"`: Automated bug fixing workflow on a new branch with issue-focused token-budgeted context.
- `graphicle wiki`: Automatically generate and update markdown project documentation from code, docs, and memory.
- `graphicle usage`: Inspect token spend, API call counts, and cache performance logs (`ask --pack`, `plan`, `fix`).
- `graphicle pixel`: Visual-to-code mapping using SAM 3.5; points from UI screenshot coordinates to source code.
- `graphicle see`: Autonomous browser driving using model computer-use capabilities (Muse Wonder / Spark).
- `graphicle import-sqlite`: Migrate legacy SQLite stores (`reviews.sqlite3`, Graphicle <=0.1.12) to the current format.
- `graphicle config`: View or update configuration properties (e.g., answer models, token budgets).
- `graphicle update`: Check for and display instructions to upgrade Graphicle.
- `graphicle uninstall`: Completely remove Graphicle from agents and the local host.

---

### 6.2 Repository Synchronization (`graphicle sync`)

`graphicle sync` imports or refreshes project files, parses symbols, resolves imports, respects `.gitignore`, and generates or updates the code graph.

**Usage:** `graphicle sync [OPTIONS] [PATH]`

**Arguments & Flags:**
- `[PATH]`: Path to repository folder to synchronize (default: current project folder).
- `--project <PROJECT>`: Associate with an explicit project ID, key, or existing project folder.
- `--background`: Asynchronously build the code graph in a background worker and return immediately.
- `--no-graph`: Ingest files and update file metadata while deferring graph construction to next MCP call or sync.
- `--read-only`: Verify synchronization state without writing to disk.
- `--json`: Output synchronization progress and results as structured JSON (default in non-terminal environments).
- `--quiet`: Suppress output on success and exit immediately if no files changed since last sync (ideal for git hooks).
- `--data-dir <DIR>`: Explicit data directory location.
- `--config <FILE>`: Explicit model configuration file.

---

### 6.3 Code Graph Engine (`graphicle graph`)

The `graphicle graph` suite handles static analysis, semantic indexing, topological traversal, reverse impact assessment, and artifact generation.

#### 6.3.1 Graph Extraction & Maintenance
- `graphicle graph extract <path>`: Build code graph from source files and semantic material.
  - Extraction flags:
    - `--code-only`: Exclude semantic documentation/markdown; parse source code AST only.
    - `--force`: Force re-extraction of all files, bypassing cache.
    - `--no-cluster`: Skip graph clustering phase after AST extraction.
    - `--no-dedup`: Skip symbol and semantic deduplication.
    - `--allow-partial`: Retain graph even if individual source files fail to parse.
    - `--timing`: Emit fine-grained phase performance benchmarks.
    - `--allow-dedup-shrink`: Permit deduplication to decrease node count aggressively.
    - `--backend <NAME>` & `--model <NAME>`: Specify LLM backend and model name for semantic enrichment.
    - `--mode deep`: Perform deep multi-pass symbol resolution and relationship extraction.
    - `--out <DIR>` / `--output <DIR>`: Destination directory for generated graph artifacts (`graph.json`).
    - `--max-workers <N>` / `--max-concurrency <N>`: Thread and concurrency limits for extraction workers.
    - `--token-budget <N>`: Token budget ceiling for semantic extraction passes.
    - `--api-timeout <S>`: Timeout in seconds for backend LLM requests.
    - `--exclude <PATTERN>`: Glob pattern of files or directories to ignore.
    - `--no-gitignore`: Do not automatically exclude paths matched by `.gitignore`.
    - `--google-workspace`: Ingest Google Workspace documentation exports.
    - `--resolution <N>`: Louvain community clustering resolution parameter.
    - `--exclude-hubs <N>`: Omit high-degree hub nodes exceeding degree threshold N during clustering.
    - `--dedup-llm`: Use an LLM to disambiguate and merge fuzzy symbol duplicates.
    - `--postgres <DSN>`: Export or persist extracted graph directly into PostgreSQL / pgvector.
    - `--cargo`: Enable Rust Cargo dependency graph extraction.
    - `--global` / `--as <TAG>`: Register extracted graph into global multi-project index under tag.
- `graphicle graph update [path]`: Fast incremental code refresh without invoking an LLM (`--force`, `--no-cluster`).
- `graphicle graph watch [path]`: Continuous file-system watcher to rebuild graph upon code changes (`--debounce <S>`).
- `graphicle graph check-update <path>`: Check and report files with pending semantic updates without rebuilding.
- `graphicle graph cluster-only [path]`: Recompute hierarchical Louvain community clustering on an existing graph.
- `graphicle graph label [path]`: Assign descriptive domain names to graph communities (`--missing-only`, `--graph PATH`, `--no-viz`, `--no-label`, `--backend NAME`, `--model NAME`, `--max-concurrency N`, `--batch-size N`, `--resolution N`, `--exclude-hubs N`).

#### 6.3.2 Navigation, Impact & Topological Traversal
- `graphicle graph explain "<symbol>"`: Inspect node attributes, defining file/lines, docstrings, and relationships.
  - `--all`: Enumerate all incoming and outgoing connections (default: top 20 most connected).
  - `--graph <PATH>`: Target an explicit `graph.json` artifact.
- `graphicle graph affected "<symbol>"`: Perform reverse reachability analysis to discover blast radius and callers.
  - `--depth <N>`: Traversal depth limit (default: 2).
  - `--relation <R>`: Filter by edge type (e.g., `calls`, `imports`, `implements`, `references`).
- `graphicle graph path "<source>" "<target>"`: Compute shortest dependency path between two components.
  - `--directed`: Enforce directed edge traversal according to dependency arrows (default).
  - `--undirected`: Allow bidirectional traversal across edges.
- `graphicle graph query "<question>"`: Query scoped graph context around a question or topic.
  - `--budget <N>`: Cap output length to token budget (default: 2000; overrides `.graphiclerc`).
  - `--depth <N>`: Graph traversal depth between 1 and 6 (default: 2; overrides `.graphiclerc`).
  - `--dfs`: Use depth-first traversal instead of default breadth-first search.
  - `--seed <X>`: Explicit seed node (symbol ID, label, or file path; repeatable) instead of keyword matching.
  - `--context <C>`: Additional contextual constraint string.
- `graphicle graph god-nodes`: Identify architectural hub nodes with high connectivity degrees.
  - `--top <N>`: Display top N most connected hubs.
  - `--json`: Output results as structured JSON.

#### 6.3.3 Diagnostics, Visualization & Export
- `graphicle graph diagnose multigraph`: Detect and report multi-edge collapse opportunities (`--json`, `--max-examples <N>`).
- `graphicle graph benchmark [graph.json]`: Evaluate graph token savings ratio relative to the raw repository corpus.
- `graphicle graph tree`: Generate an interactive, collapsible HTML dependency tree (`--graph`, `--output`, `--root`).
- `graphicle graph export <format>`: Export graph to external visualization and graph database engines:
  - Formats: `html`, `callflow-html`, `obsidian`, `wiki`, `svg`, `graphml`, `neo4j`, `falkordb`.
  - Flags: `--graph <PATH>`, `--labels`, `--output <PATH>`.

#### 6.3.4 Memory, Results & Semantic Merging
- `graphicle graph save-result`: Persist Q&A pairs and execution outcomes into project work memory.
  - Flags: `--question`, `--answer` (or `--answer-file`), `--nodes`, `--type`, `--outcome`, `--correction`, `--memory-dir`.
- `graphicle graph reflect`: Consolidate and decay episodic memories into long-term architectural facts.
  - Flags: `--graph <PATH>`, `--if-stale`, `--half-life-days <N>`, `--min-corroboration <N>`.
- `graphicle graph cache-check <list>`: Validate entries against the semantic chunk cache (`--root`, `--deep`, `--prompt-file`).
- `graphicle graph merge-chunks <files>`: Combine validated parallel extraction chunks into one graph (`--out <PATH>`).
- `graphicle graph merge-semantic`: Reconcile cached fragments with newly extracted semantic nodes (`--cached`, `--new`, `--out`).
- `graphicle graph merge-graphs <files>`: Merge multiple subproject graphs into a unified repository graph (`--out PATH`, `--previous MERGED.json`).
- `graphicle graph merge-driver <base> <current> <other>`: Git-compatible 3-way union merge driver for `graph.json`.

#### 6.3.5 Remote Repositories, Providers & Global Subgraphs
- `graphicle graph add <url>`: Ingest remote documentation or web articles (`--dir DIR`, `--author`, `--contributor`).
- `graphicle graph clone <github-url>`: Clone and index a remote repository (`--branch NAME`, `--out DIR`).
- `graphicle graph global <subcommand>`: Manage cross-repository multi-project indices (`add`, `remove`, `list`, `path`, `--as TAG`).
- `graphicle graph provider <subcommand>`: Manage LLM provider credentials and endpoints (`list`, `show`, `add`, `remove`).

#### 6.3.6 Git Hooks, Guards & Platform Skills
- `graphicle graph hook <subcommand>`: Manage automated git integration hooks (`install`, `uninstall`, `status` for `post-commit`, `post-checkout`, `post-merge`, `post-rewrite`, and `graph.json` merge driver).
- `graphicle graph hook-guard <kind>`: Run assistant guard checks on tool calls (`assistant`, `read`, `search`, `gemini`, `--strict`).
- `graphicle graph hook-check`: Silent fast compatibility check executed by runtime hooks.
- `graphicle graph install [--platform <PLATFORM>]`: Copy the Graphicle skill definition into target agent environments.
  - Supported platforms: `claude`, `codex`, `opencode`, `kilo`, `aider`, `copilot`, `claw`, `droid`, `trae`, `trae-cn`, `hermes`, `kiro`, `pi`, `codebuddy`, `antigravity`, `antigravity-windows`, `windows`, `kimi`, `amp`, `agents`, `devin`, `gemini`, `cursor`, `vscode` (alias: `skills`, `--project`).
- `graphicle graph uninstall [platform]`: Remove configuration and skills from target platforms (`--project`, `--purge`).
- `graphicle graph pi guard <action>`: Manage Pi guard extension (`install`, `uninstall`, `path`).

#### 6.3.7 Headless & HTTP MCP Server
- `graphicle graph serve [graph.json]`: Serve code graph and memory tools via Model Context Protocol.
  - Flags: `--transport <stdio|streamable-http>`, `--host <HOST>`, `--port <N>`, `--api-key <KEY>`, `--path </mcp>`, `--json-response`, `--stateless`, `--session-timeout <S>`.

#### 6.3.8 Standard Navigation Exit Codes & Data Options
- `0`: Success / Answer found.
- `3`: Ambiguous symbol name (candidate disambiguation list returned).
- `4`: No matching node or symbol found in graph store.
- `--jsonl`: Stream output as JSON Lines (standard format for pipeline scripts and automated consumers).

---

<!-- graphicle:start (added by graphicle setup; graphicle disconnect removes it) -->
## Graphicle
Use the `graphicle` MCP server for where or how something works, for questions across files or history, and before changing unfamiliar code: `recall_project` or `query_graph` with the question, then `get_node` and `get_neighbors` for one symbol; answers cite file and lines. For an exact identifier you already know, grep is enough.
Without the server, run `graphicle context "<question>"` in a terminal.
<!-- graphicle:end -->
