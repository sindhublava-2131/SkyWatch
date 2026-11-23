#!/usr/bin/env bash
# ==============================================================================
# SkyWatch Acceptance Gate: Smoke Pipeline Validation
# Executes end-to-end ingestion, Pig cleaning, and Spark analytics.
# Usage:
#   ./scripts/smoke.sh [PROFILE] [--allow-simulation]
# By default, fails if real Big Data engines (Apache Pig / Apache Spark) are unavailable.
# ==============================================================================

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

PROFILE="${1:-${SKYWATCH_PROFILE:-modern}}"
ALLOW_SIMULATION=0

for arg in "$@"; do
    if [[ "$arg" == "--allow-simulation" || "$arg" == "--simulate" ]]; then
        ALLOW_SIMULATION=1
    fi
done

if [[ "${ALLOW_SIMULATION}" == "1" || "${SKYWATCH_ALLOW_SIMULATION:-0}" == "1" ]]; then
    export SKYWATCH_REQUIRE_REAL_ENGINE=0
    echo "[SMOKE] Mode: Standalone Simulation Permitted (SKYWATCH_REQUIRE_REAL_ENGINE=0)"
else
    export SKYWATCH_REQUIRE_REAL_ENGINE=1
    echo "[SMOKE] Mode: Real Engine Required (SKYWATCH_REQUIRE_REAL_ENGINE=1)"
fi

echo "=============================================================================="
echo " SkyWatch Smoke Test Pipeline (Profile: ${PROFILE})"
echo "=============================================================================="

# 1. Validate environment configuration
echo "[1/5] Validating execution profile environment..."
python3 -c "
from skywatch.pipeline.profile import load_profile, validate_environment
p = load_profile('${PROFILE}')
v = validate_environment(p)
print(f'Profile: {p.profile_name} (HADOOP={p.hadoop_home}, PIG={p.pig_home}, SPARK={p.spark_home})')
print(f'Environment check results: {v}')
"

# 2. Check engine availability if real engine is required
echo "[2/5] Checking engine runtime availability..."
python3 -c "
import sys
from skywatch.pipeline.profile import load_profile
from skywatch.pipeline.pig_runner import is_pig_available
from skywatch.pipeline.spark_runner import is_spark_available

p = load_profile('${PROFILE}')
pig_ok = is_pig_available(p.pig_home)
spark_ok = is_spark_available(p.spark_home)

print(f'Pig runtime available: {pig_ok}')
print(f'Spark runtime available: {spark_ok}')

require_real = '${SKYWATCH_REQUIRE_REAL_ENGINE}' == '1'
if require_real:
    if not pig_ok:
        sys.stderr.write(f'ERROR: Apache Pig runtime not available (PIG_HOME={p.pig_home}) and real engine is required.\n')
        sys.exit(1)
    if not spark_ok:
        sys.stderr.write(f'ERROR: Apache Spark runtime not available (SPARK_HOME={p.spark_home}) and real engine is required.\n')
        sys.exit(1)
"

# 3. Stage smoke test datasets
echo "[3/5] Staging input datasets..."
SMOKE_DIR="$PROJECT_ROOT/data/smoke"
rm -rf "$SMOKE_DIR"
mkdir -p "$SMOKE_DIR/raw" "$SMOKE_DIR/clean" "$SMOKE_DIR/published"
cp "$PROJECT_ROOT/tests/fixtures/sample_air_quality.csv" "$SMOKE_DIR/raw/air_quality.csv"

# 4. Run Pig Cleaning Job
echo "[4/5] Executing Pig ETL Job (pig/clean_air_quality.pig)..."
PIG_OPTS=()
if [[ "$SKYWATCH_REQUIRE_REAL_ENGINE" == "1" ]]; then
    PIG_OPTS+=(--require-real-engine)
fi

python3 -m skywatch.pipeline.pig_runner \
    --script "$PROJECT_ROOT/pig/clean_air_quality.pig" \
    --input "$SMOKE_DIR/raw/air_quality.csv" \
    --output "$SMOKE_DIR/clean" \
    --profile "$PROFILE" \
    "${PIG_OPTS[@]}"

# 5. Run Spark Analytics Job
echo "[5/5] Executing Spark Analytics Job (7 published datasets)..."
SPARK_OPTS=()
if [[ "$SKYWATCH_REQUIRE_REAL_ENGINE" == "1" ]]; then
    SPARK_OPTS+=(--require-real-engine)
else
    SPARK_OPTS+=(--simulate)
fi

python3 -m skywatch.pipeline.spark_runner \
    --input-clean "$SMOKE_DIR/clean" \
    --input-weather "$PROJECT_ROOT/tests/fixtures/sample_weather_normalized.csv" \
    --output "$SMOKE_DIR/published" \
    --profile "$PROFILE" \
    "${SPARK_OPTS[@]}"

echo "=============================================================================="
echo " Smoke test completed successfully for profile: ${PROFILE}"
echo " Published datasets in: $SMOKE_DIR/published"
echo "=============================================================================="
