#!/usr/bin/env bash
# ==============================================================================
# SkyWatch ETL: Profile-Aware Pig Execution Wrapper
# Sources profile environment variables, sets error traps, and launches pig_runner.
# Usage:
#   ./scripts/run_pig.sh [SCRIPT_NAME] [PROFILE] [INPUT_PATH] [OUTPUT_PATH] [RUN_DATE]
# ==============================================================================

set -euo pipefail

# 1. Resolve project root directory dynamically
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

# 2. Parse positional arguments with robust defaults
RUN_DATE="${5:-$(date -u +%Y-%m-%d)}"
SCRIPT_NAME="${1:-clean_air_quality.pig}"
PROFILE="${2:-${SKYWATCH_PROFILE:-modern}}"

case "$SCRIPT_NAME" in
    *station_stats*)
        INPUT_PATH="${3:-data/clean/air_quality/event_dt=${RUN_DATE}}"
        OUTPUT_PATH="${4:-data/analytics/station_stats/event_dt=${RUN_DATE}}"
        ;;
    *hourly_rollup*)
        INPUT_PATH="${3:-data/clean/air_quality/event_dt=${RUN_DATE}}"
        OUTPUT_PATH="${4:-data/analytics/hourly_rollup/event_dt=${RUN_DATE}}"
        ;;
    *)
        INPUT_PATH="${3:-data/normalized/air_quality/ingest_dt=${RUN_DATE}}"
        OUTPUT_PATH="${4:-data/clean/air_quality/event_dt=${RUN_DATE}}"
        ;;
esac

# 3. Configure logging directory and error trap
LOG_DIR="$PROJECT_ROOT/data/logs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/pig_runner.log"

trap 'echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] ERROR: Pig execution failed on line $LINENO with exit code $?" >> "$LOG_FILE"' ERR

echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] Starting Pig execution: script=$SCRIPT_NAME profile=$PROFILE input=$INPUT_PATH output=$OUTPUT_PATH run_date=$RUN_DATE" | tee -a "$LOG_FILE"

# 4. Source active execution profile if available
PROFILE_FILE="$PROJECT_ROOT/profiles/${PROFILE}.env"
if [ -f "$PROFILE_FILE" ]; then
    echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] Sourcing profile: $PROFILE_FILE" | tee -a "$LOG_FILE"
    # Sourcing profile while maintaining safe variable isolation
    set -a
    # shellcheck disable=SC1090
    source "$PROFILE_FILE"
    set +a
fi

# 5. Resolve script path (check pig/ directory if not absolute)
if [ -f "$SCRIPT_NAME" ]; then
    TARGET_SCRIPT="$SCRIPT_NAME"
elif [ -f "$PROJECT_ROOT/pig/$SCRIPT_NAME" ]; then
    TARGET_SCRIPT="$PROJECT_ROOT/pig/$SCRIPT_NAME"
else
    echo "ERROR: Pig script not found: $SCRIPT_NAME" | tee -a "$LOG_FILE" >&2
    exit 1
fi

# 6. Execute Python Pig runner orchestrator
EXTRA_ARGS=()
if [[ "${SKYWATCH_REQUIRE_REAL_ENGINE:-}" =~ ^(1|true|yes)$ ]]; then
    EXTRA_ARGS+=(--require-real-engine)
fi

python -m skywatch.pipeline.pig_runner \
    --script "$TARGET_SCRIPT" \
    --input "$INPUT_PATH" \
    --output "$OUTPUT_PATH" \
    --run-date "$RUN_DATE" \
    --profile "$PROFILE" \
    "${EXTRA_ARGS[@]}" \
    2>&1 | tee -a "$LOG_FILE"

echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] Pig execution completed successfully for $SCRIPT_NAME" | tee -a "$LOG_FILE"
