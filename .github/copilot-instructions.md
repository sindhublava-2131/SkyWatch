# VS Code Copilot repository instructions

Before working, open and follow the canonical SkyWatch project rules in [`AGENTS.md`](../AGENTS.md). They define the required project context, Graphicle coverage and impact review, safe change process, engineering constraints, and verification/reporting expectations.

### Mandatory Git Branching & CI/CD Rule:
Commit all changes to a new branch (never commit working code directly to `main`). For each phase completion, if the CI/CD pipeline and test suite are a success, merge the branch into `main`.

### Graphicle Static Analysis & Graph Navigation:
Before modifying code, synchronize the repository and evaluate component impact using Graphicle:
1. Run `graphicle sync` (or `graphicle sync --quiet` / `graphicle sync --background`) to ensure on-disk AST and symbols are indexed.
2. Run `graphicle status` to check node/edge counts, symbol coverage, and agent connectivity.
3. Assess impact and blast radius:
   - `graphicle graph explain "<symbol>"` (`--all`): View symbol provenance, definitions, and relationships.
   - `graphicle graph affected "<symbol>"` (`--depth N`, `--relation R`): Discover callers, dependents, and blast radius.
   - `graphicle graph path "<source>" "<target>"` (`--directed` / `--undirected`): Discover dependency shortest path.
   - `graphicle context "<question>"`: Retrieve deterministic cited code evidence without model calls.
   - `graphicle graph query "<question>"` (`--budget N`, `--depth N`, `--seed X`): Perform scoped subgraph traversals.
4. Refer to Section 6 of `AGENTS.md` for the comprehensive command reference covering all extraction, traversal, diagnostic, export, memory, and hook flags.

Do not duplicate or silently override that policy here. These instructions guide Copilot behavior; they do not prove that a particular host or session loaded or followed them.
