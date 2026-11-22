---
status: resolved
trigger: "I'm not sure if Antigravity's gemini and claude follow AGENTS.md so i'd like you to make sure to create the right MD files for each coding agents like claude, gpt, gemini and also make sure that they use graphicle to go through the codebase first and make the changes and also follow codebase descipline mentioned in @attachment:SkyWatch — Project Specification v2.0.md and all the docs in .planning/"
created: 2026-11-22
updated: 2026-11-22T13:47:32+05:30
---

# Debug Session: Agent guidance is not followed

## Symptoms

- **Expected:** Agents should inspect all relevant project files and functions, run `graphicle sync`, review Graphicle's nodes and likely change impacts, test changes in a sandbox before applying them, or preserve a rollback path if no sandbox is available, and keep CI/CD green.
- **Actual:** The user reports that agents do not follow `AGENTS.md`; the current file contains almost no rules.
- **Errors:** None reported.
- **Timeline:** The repository is new.
- **Reproduction:** Test the instruction setup once.

## Current Focus

```yaml
reasoning_checkpoint:
  hypothesis: "Project guidance is not consistently available because root AGENTS.md only contains generic Graphicle connection text and Claude, Gemini, and VS Code Copilot have no native repository instruction entry points; both the policy gap and discovery gap prevent the requested cross-agent workflow."
  confirming_evidence:
    - "AGENTS.md originally contained only a Graphicle usage block, with none of the requested planning, impact, sandbox/rollback, testing, or project rules."
    - "No native CLAUDE.md, GEMINI.md, or .github/copilot-instructions.md existed; the current wrappers now point to the canonical AGENTS.md."
  falsification_test: "If complete policy and native entry points had existed before the change, the direct file inspection and pre-fix structural check would have passed; neither did."
  fix_rationale: "Expand the canonical AGENTS.md, retain its existing Graphicle setup block, and add thin native discovery files to avoid policy duplication or unsupported enforcement claims."
  blind_spots: "Claude Code and Gemini CLI are not installed; VS Code Copilot host loading cannot be observed here. Structural validation does not prove external runtime behavior."
  candidate_causes:
    - "config: canonical AGENTS.md lacks required SkyWatch workflow rules."
    - "environment: distinct agent-host discovery conventions had no corresponding native entry files."
  and_gate: "yes for the requested cross-agent outcome: both canonical rules and native host entry points are needed for consistent guidance. Actual adherence remains advisory."
bug_class: bohrbug
evidence_count: 12
eliminated_count: 0
current_result: "Local structural implementation passes after controlled revert/reapply; Graphicle sync passes. Native host loading remains unavailable/pending because the user cannot test and no host runtime is accessible here. No application source or CI suite exists yet. No commit was made."
next_action: "No remaining implementation action. Native host verification is unavailable/pending: test Claude Code, Gemini CLI, and VS Code Copilot loading only if those hosts become accessible."
awaiting: "External host verification unavailable/pending; user confirmed unable to test. Do not claim host-level loading or advisory behavior was verified."
phase_scope: "Phase 1 Foundation, Data Ingestion & Storage Architecture; in scope."
worktree_safety: "No isolated sandbox was available. Preserved pre-existing .mcp.json, .vscode/, .claude/settings.local.json, and all unrelated untracked content."
graphicle_coverage: "Final sync: 12 files, 11 graph-covered, 91 nodes, 81 edges, 80 symbols; one static reference edge from .github/copilot-instructions.md to AGENTS.md. Indexed project/doc/planning content, no implementation source. Static graph is not runtime call-flow evidence; standalone graph query was unavailable because .graphicle/graph.json is absent."
```

## Eliminated

## Evidence

- timestamp: 2026-11-22T13:34:30+05:30
  checked: `.planning/STATE.md`, `PROJECT.md`, `ROADMAP.md`, and `REQUIREMENTS.md`
  found: Current phase is Phase 1 Foundation, Data Ingestion & Storage Architecture; project architecture and pipeline setup are in-scope. Phase success criteria include robust storage, ingestion, dual profiles, reproducibility, security, and verified outcomes.
  implication: Repository guidance setup is aligned with active phase scope; it should favor Phase 1 artifacts and not assume later-phase implementation exists.
- timestamp: 2026-11-22T13:34:30+05:30
  checked: Root `AGENTS.md`
  found: Existing Graphicle setup instructions request MCP `recall_project` / `query_graph` and CLI `graphicle context` for cross-file questions, but do not require sync, coverage review, source inspection, or rollback discipline.
  implication: Preserve the existing Graphicle block and integrate practical constraints rather than replacing it with a generic blanket instruction.
- timestamp: 2026-11-22T13:35:12+05:30
  checked: `git status --short`, `.planning/config.json`, `.mcp.json`, `.claude/`, `.vscode/`, and project skill roots
  found: Worktree contains untracked `.mcp.json`, `.planning/debug/`, `.vscode/`, and root `AGENTS.md`; `.claude/` and `.vscode/` each contain existing config files; `.github/skills` and `.agents/skills` are absent; project config enables parallelization and documentation commits. `.mcp.json` configures Graphicle MCP.
  implication: No project-defined skills need loading. Must preserve `.mcp.json` and all existing `.vscode/` content. New VS Code guidance can be added under `.github/` without editing existing MCP/VS Code config.
- timestamp: 2026-11-22T13:36:03+05:30
  checked: Project specification section index and keyword matches
  found: Applicable requirements include Python 3.8-compatible syntax and typing, single-responsibility functions, dual-profile rollback, explicit schema contracts, security/no-secret rules, tests, Conventional Commits, CI gates, docs, and pre-freeze verification; the spec includes dedicated sections for these topics.
  implication: Root guidance should distill these project-specific engineering constraints, not replicate the full specification; wrappers should delegate consistently to one canonical source.
- timestamp: 2026-11-22T13:37:05+05:30
  checked: Project specification §§0–8, 11–13, and 15–19; `graphicle status` and CLI help
  found: Spec confirms Python 3.8 floor, typed single-responsibility functions, dual-stack and rollback checks, schemas, tests, security, CI, docs, and truthfully reported verification. Explicit `graphicle sync .` succeeded; it reports 8 repository files total and a static graph with 77 nodes, 70 edges, 70 symbols, and 7 graph-covered files. The project is currently docs/config only, with no implementation source. Graphicle's standalone `graph query` failed because `.graphicle/graph.json` does not exist, despite the graph being available through the Graphicle service/store.
  implication: Record this Graphicle interface/coverage limitation honestly. Root rules must require graph coverage review plus manual inspection/search, and must never treat graph absence as proof there are no callers or side effects.
- timestamp: 2026-11-22T13:39:30+05:30
  checked: Bounded `graphicle context` output, root file listing, and native agent files
  found: Graphicle cites the project specification, README, `.planning/PROJECT.md`, `ROADMAP.md`, `REQUIREMENTS.md`, and this debug session, while reporting no implementation-source coverage. No pre-existing native wrappers were found; `.github/` is absent, and no Claude/Gemini CLI is installed.
  implication: Root cause is confirmed at repository configuration/content level; structural validation is possible, but external runtime instruction loading is a user-verification boundary.
- timestamp: 2026-11-22T13:42:11+05:30
  checked: First structural validation run
  found: Canonical policy, wrappers, and Graphicle-block assertions reached the protected-path check; the test stopped because its fixture incorrectly expected `.vscode/settings.local.json`, which does not exist. Actual pre-existing VS Code file is `.vscode/mcp.json`; `.claude/settings.local.json` exists.
  implication: This is a validation-script path assumption, not a repository/fix failure. No repository files were changed by the test. Correct the fixture and rerun before concluding.
- timestamp: 2026-11-22T13:43:02+05:30
  checked: Corrected structural differential test against the observed pre-fix state and the current files
  found: Baseline fails the required-policy marker check; current AGENTS.md passes all required marker checks, the original Graphicle block is preserved verbatim, all three host-specific wrappers point to the canonical instructions, and `.mcp.json`, `.vscode/mcp.json`, `.claude/settings.local.json` remain present with no edits recorded by this session.
  implication: Instruction content and wrapper wiring pass the one requested structural regression test. External runtime auto-loading remains unverified because Claude/Gemini CLIs are absent and Copilot cannot be inspected from this session.
- timestamp: 2026-11-22T13:44:12+05:30
  checked: Final Graphicle sync and `git status --short`
  found: Graphicle sync succeeds with 12 files, 11 graph-covered files, 91 nodes, 81 edges, 80 symbols, and one extracted reference edge from `.github/copilot-instructions.md` to `AGENTS.md`; static-graph scope and absence of implementation source remain. Worktree shows only pre-existing `.mcp.json`, `.vscode/`, the active debug directory, and the intended guidance files as untracked.
  implication: New guidance is indexed and scoped. This index still provides no runtime call graph or application-code coverage. Next run the fix-attribution/revert check without touching unrelated pre-existing files.
- timestamp: 2026-11-22T13:45:25+05:30
  checked: Controlled revert-and-reconfirm on only the four guidance files
  found: In-memory byte snapshots were retained; AGENTS.md was temporarily restored to its observed Graphicle-only baseline and the three new wrappers were removed. The original missing-rules/missing-entrypoints condition returned. A `finally` block restored each file byte-for-byte and hash-equivalent content was confirmed.
  implication: The controlled revert reproduced the pre-fix condition, and the fix content was safely reapplied without touching `.mcp.json`, `.vscode/`, or other pre-existing work. Run the structural test once more on the restored state.
- timestamp: 2026-11-22T13:47:32+05:30
  checked: Final structural verification and worktree status after controlled reapply
  found: Canonical rule markers and all three native wrappers pass again; protected `.mcp.json`, `.vscode/mcp.json`, and `.claude/settings.local.json` are present. Git status contains only previously untracked protected/config content, the debug record, and the four requested guidance additions; no commits were made.
  implication: Local fix-acceptance signals pass or are explicitly skipped where no source/test suite exists. End-to-end runtime loading remains for human confirmation.

- timestamp: 2026-11-22T13:42:44+05:30
  checked: Human checkpoint disposition
  found: The user responded "unable to test." Claude Code and Gemini CLI are not installed in this environment, and the VS Code Copilot session/context could not be inspected.
  implication: Native host loading and actual advisory behavior remain unavailable/pending. This is an external-runtime limitation, not a local implementation failure. Do not claim Claude Code, Gemini CLI, or VS Code Copilot loading was verified.
  local_summary: "The canonical AGENTS.md is expanded, CLAUDE.md and GEMINI.md provide native entry points, .github/copilot-instructions.md references the canonical rules, structural differential verification passed (including controlled revert/reapply), final Graphicle sync indexed 12 files / 11 graph-covered / 91 nodes / 81 edges / 80 symbols, and unrelated .mcp.json, .vscode/, and .claude/settings.local.json work was preserved with no commits."

## Resolution

root_cause: "The repository has no usable canonical SkyWatch agent workflow: root AGENTS.md contains only generic Graphicle MCP guidance, and the requested Claude, Gemini, and VS Code Copilot native instruction entry points are absent. Cross-agent use requires both complete canonical rules and each host's discovery entry point."
fix: "Expanded root AGENTS.md into concise canonical project/workflow rules while preserving its Graphicle setup block; added CLAUDE.md and GEMINI.md imports and a VS Code .github/copilot-instructions.md reference to the canonical file."
files_changed:
  - AGENTS.md
  - CLAUDE.md
  - GEMINI.md
  - .github/copilot-instructions.md
verification:
  target_test:
    result: pass
    details: "Structural differential test failed against the observed pre-fix baseline and passed on the fixed setup, including after controlled revert/reapply."
  mutation_check:
    result: skipped
    reason_if_skipped: "Documentation/configuration-only fix; no source mutation framework or application test project exists."
    mutant_killed: not_applicable
  no_op_deletion:
    result: pass
    deletion_justified_by_rca: false
    details: "Only instruction content was added/expanded; the pre-existing Graphicle setup block was retained verbatim."
  adjacent_tests:
    result: skipped
    suites_run: []
    reason_if_skipped: "No application source or adjacent test suite exists in this repository yet."
  revert_and_reconfirm:
    result: pass
    bug_returned_on_revert: true
    fixed_on_reapply: true
    details: "Pre-fix Graphicle-only AGENTS.md and absent wrappers reproduced the structural failure; all four files were restored byte-for-byte, and the structural test passed again."
  guardrail_verdict: accepted
  human_runtime_verification:
    result: unavailable_pending
    details: "User unable to test. Claude Code and Gemini CLI are unavailable in this session; VS Code Copilot context could not be inspected. Native host instruction loading and obedience remain unverified."
