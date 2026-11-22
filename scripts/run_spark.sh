#!/usr/bin/env bash
# ==============================================================================
# SkyWatch Analytics: Profile-Aware Spark Analytics Execution Wrapper
# Sources profile environment variables, sets error traps, and launches spark_runner.
# Usage:
#   ./scripts/run_spark.sh [PROFILE] [INPUT_CLEAN] [INPUT_WEATHER] [OUTPUT_PUBLISHED] [EXTRA_FLAGS]
# ==============================================================================

set -euo pipefail

# 1. Resolve project root directory dynamically
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

# 2. Parse positional arguments with robust defaults
PROFILE="${1:-${SKYWATCH_PROFILE:-modern}}"
INPUT_CLEAN="${2:-data/clean/air_quality}"
INPUT_WEATHER="${3:-data/normalized/weather}"
OUTPUT_PUBLISHED="${4:-output/published}"
if [ "$#" -ge 4 ]; then
    shift 4
    EXTRA_FLAGS="$*"
else
    EXTRA_FLAGS=""
fi

# 3. Configure logging directory and error trap
LOG_DIR="$PROJECT_ROOT/data/logs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/spark_runner.log"

trap 'echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] ERROR: Spark execution failed on line $LINENO with exit code $?" >> "$LOG_FILE"' ERR

echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] Starting Spark Analytics execution: profile=$PROFILE clean=$INPUT_CLEAN weather=$INPUT_WEATHER output=$OUTPUT_PUBLISHED" | tee -a "$LOG_FILE"

# 4. Source active execution profile if available
PROFILE_FILE="$PROJECT_ROOT/profiles/${PROFILE}.env"
if [ -f "$PROFILE_FILE" ]; then
    echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] Sourcing profile: $PROFILE_FILE" | tee -a "$LOG_FILE"
    set -a
    # shellcheck disable=SC1090
    source "$PROFILE_FILE"
    set +a
fi

# 5. Execute Python Spark runner orchestrator
# shellcheck disable=SC2086
python -m skywatch.pipeline.spark_runner \
    --input-clean "$INPUT_CLEAN" \
    --input-weather "$INPUT_WEATHER" \
    --output "$OUTPUT_PUBLISHED" \
    --profile "$PROFILE" \
    $EXTRA_FLAGS \
    2>&1 | tee -a "$LOG_FILE"

echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] Spark Analytics execution completed successfully" | tee -a "$LOG_FILE"
