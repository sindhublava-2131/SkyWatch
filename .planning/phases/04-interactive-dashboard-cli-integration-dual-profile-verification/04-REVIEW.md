---
phase: 04-interactive-dashboard-cli-integration-dual-profile-verification
reviewed: 2026-11-24T18:30:00Z
depth: standard
files_reviewed: 12
files_reviewed_list:
  - src/skywatch/cli.py
  - src/skywatch/pipeline/pipeline_runner.py
  - scripts/rollback_drill.sh
  - scripts/use_profile.sh
  - api/index.py
  - frontend/lib/schemas.ts
  - frontend/lib/api.ts
  - frontend/components/skywatch.tsx
  - tests/integration/test_full_stack_e2e.py
  - tests/unit/test_cli.py
  - tests/unit/test_pipeline_runner.py
  - Makefile
findings:
  critical: 0
  warning: 0
  info: 0
  total: 0
status: resolved
---

# Phase 4: Code Review Report

**Reviewed:** 2026-11-24T18:30:00Z  
**Depth:** standard  
**Files Reviewed:** 12  
**Status:** issues_found  

## Summary

Phase 4 delivers the final operationalization layer for SkyWatch: a production Next.js 15 environmental dashboard wired to serverless REST endpoints and static JSON lakes, a unified command-line interface (`skywatch`) orchestrating all ingestion and analytical stages, an automated profile switcher and dual-profile rollback verification harness (`scripts/rollback_drill.sh`), and end-to-end full-stack integration tests.

The implementation satisfies core phase requirements: 100% numerical and schema parity ($\epsilon < 0.01$) across all 7 published analytical entities during rollback drills, clean TypeScript/React builds with zero type errors, strict contract validation using Zod, and full offline/online dual-mode support.

A thorough review of the 12 touched files identified **3 Warnings** and **2 Info** findings. Key issues include ambient environment variable propagation when passing `--profile` to the CLI runner, static import-time path bindings in the REST API handler during dynamic testing, and mock fallback type alignment for filtered station queries in the frontend client. Zero Critical/Blocker security or system crash defects were detected.

---

## Warnings

### WR-01: Static Import-Time Directory Bindings in `api/index.py`

**File:** `api/index.py:23-24`, `api/index.py:83-97`  
**Issue:** `PUBLISHED_DIR` and `STATIC_API_DIR` are instantiated at module import time:
```python
PUBLISHED_DIR = get_published_dir()
STATIC_API_DIR = get_static_api_dir()
```
While helper functions `get_published_dir()` and `get_static_api_dir()` dynamically inspect `os.environ.get("SKYWATCH_PUBLISHED_DIR")`, `get_stations()` uses `PROJECT_ROOT / "config" / "stations.json"` as its primary fallback when `_load_dataset("stations")` returns empty. If an execution environment dynamically alters `SKYWATCH_PUBLISHED_DIR` after initial module import without reloading `api.index`, legacy static references or config fallbacks may be consulted instead of freshly generated drill directories.  
**Fix:**
Remove top-level module constants or make them thin property accessors, and ensure `get_stations()` resolves `get_static_api_dir() / "stations.json"` dynamically before falling back to `PROJECT_ROOT / "config" / "stations.json"`:
```python
def get_stations(status: Optional[str] = None, freshness: Optional[str] = None) -> List[Dict[str, Any]]:
    stations = _load_dataset("stations")
    if not stations:
        static_stations = get_static_api_dir() / "stations.json"
        cfg_stations = static_stations if static_stations.is_file() else PROJECT_ROOT / "config" / "stations.json"
        if cfg_stations.is_file():
            ...
```

### WR-02: Type Mismatch and Unfiltered Fixture Fallback in `frontend/lib/api.ts`

**File:** `frontend/lib/api.ts:25-33`, `frontend/lib/api.ts:64-70`  
**Issue:** `getData()` immediately returns the bundled fallback `fixture` when `NEXT_PUBLIC_USE_MOCKS === 'true'` or when fetch fails:
```typescript
if (process.env.NEXT_PUBLIC_USE_MOCKS === 'true') {
  return fixture
}
```
In `loadStations(status?: string, freshness?: string)`, the caller can pass query parameters such as `?status=all` or `?freshness=Fresh`. When running against mock fixtures or when the network call fails, `getData()` returns the unfiltered static `stations` array, ignoring the requested query filters. Furthermore, if the raw fixture does not match the transformed shape expected by `stationSchema.array()`, downstream components encounter rendering discrepancies.  
**Fix:**
Apply client-side filtering on fallback fixtures when query parameters are supplied, and validate the fixture through `schema.safeParse(fixture)` in test environments:
```typescript
export const loadStations = async (status?: string, freshness?: string): Promise<Station[]> => {
  const params = new URLSearchParams()
  if (status) params.set('status', status)
  if (freshness) params.set('freshness', freshness)
  const qs = params.toString() ? `?${params.toString()}` : '?status=all'
  const data = await getData<Station[]>(`/api/v1/stations${qs}`, stationSchema.array(), stations)
  if (freshness) {
    return data.filter((s) => s.freshness_status?.toLowerCase() === freshness.toLowerCase())
  }
  return data
}
```

### WR-03: Ambient Environment Variable Propagation in CLI `pipeline` Subcommand

**File:** `src/skywatch/cli.py:74`, `src/skywatch/cli.py:147-157`, `src/skywatch/pipeline/pipeline_runner.py:92`  
**Issue:** When invoking `skywatch pipeline --profile lab`, `parsed.profile` is passed into `run_pipeline(profile_name="lab")`. Inside `run_pipeline()`, `load_profile("lab")` constructs the `ProfileConfig` object, but does not export `SKYWATCH_PROFILE=lab` into `os.environ`. Consequently, any underlying tools, child subshells, or downstream helper functions that read `os.environ.get("SKYWATCH_PROFILE", "modern")` continue to perceive the profile as `'modern'` unless `scripts/use_profile.sh lab` was previously sourced in the parent terminal.  
**Fix:**
Explicitly sync `os.environ["SKYWATCH_PROFILE"]` in `run_pipeline()` when an explicit profile is selected:
```python
active_profile = load_profile(profile_name)
os.environ["SKYWATCH_PROFILE"] = active_profile.name
```

---

## Info

### IN-01: Artifact Cleanup in `scripts/rollback_drill.sh`

**File:** `scripts/rollback_drill.sh:36-41`  
**Issue:** `rollback_drill.sh` creates temporary test directories `data/drill`, `output/drill_modern`, and `output/drill_lab`. Although `.gitignore` prevents them from being tracked in git, successive runs leave multi-megabyte directories on disk unless manually cleared.  
**Fix:** Add an optional `--keep-artifacts` flag, and default to removing `data/drill`, `output/drill_modern`, and `output/drill_lab` upon trap exit on successful completion:
```bash
cleanup() {
    if [ "${KEEP_ARTIFACTS:-0}" -ne 1 ]; then
        rm -rf data/drill output/drill_modern output/drill_lab
    fi
}
trap cleanup EXIT
```

### IN-02: Next.js Responsive Container Mounting Guard in `frontend/components/skywatch.tsx`

**File:** `frontend/components/skywatch.tsx:28-33`, `frontend/components/skywatch.tsx:113-125`  
**Issue:** Recharts `<ResponsiveContainer width="100%" height={...}>` relies on measuring DOM dimensions in the client browser. During server-side rendering or initial hydration in Next.js 15, responsive containers may emit harmless console warnings if rendered before DOM layout calculation completes.  
**Fix:** Add a client-mount check (`const [mounted, setMounted] = useState(false); useEffect(() => setMounted(true), []);`) and render chart placeholders or skeleton frames until mounted.

---

## Remediation Summary

All 5 review findings (3 Warnings, 2 Info) were addressed and resolved:
1. **WR-01 (api/index.py):** Implemented module `__getattr__` dynamic property accessors for `PUBLISHED_DIR` and `STATIC_API_DIR`, added `static_api_dir / "stations.json"` resolution prior to config fallback, and activated `status` query filtering.
2. **WR-02 (frontend/lib/api.ts):** Added schema validation for mock fixtures in `getData()`, and added client-side query parameter filtering for `status`, `freshness`, `station_id`, `category`, `severity`, and `parameter` across all endpoints when using mocks or on fallback.
3. **WR-03 (pipeline_runner.py & cli.py):** Added `os.environ["SKYWATCH_PROFILE"] = active_profile.profile_name` in `run_pipeline()`, and updated CLI entrypoint to export `SKYWATCH_PROFILE` immediately when `--profile` is parsed.
4. **IN-01 (scripts/rollback_drill.sh):** Added `--keep-artifacts` flag support and an EXIT cleanup trap that automatically removes `output/drill_modern`, `output/drill_lab`, and `data/drill` upon passing drill execution.
5. **IN-02 (frontend/components/skywatch.tsx):** Added client mounting state guards around Recharts `<ResponsiveContainer>` in `Overview` and `Analytics` views to prevent SSR dimension warnings during Next.js 15 hydration.

---

_Reviewed: 2026-11-24T18:30:00Z_  
_Reviewer: Antigravity (gsd-code-reviewer)_  
_Fixed: 2026-11-24T18:50:00Z (gsd-code-fixer)_  
_Depth: standard_
