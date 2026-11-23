#!/usr/bin/env bash
# ==============================================================================
# SkyWatch Live Pipeline Demo Runner
# Runs the complete end-to-end pipeline on frozen sample datasets.
# Usage:
#   ./scripts/demo.sh [PROFILE] [--allow-simulation]
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
    echo "[DEMO] Mode: Standalone Simulation Permitted (SKYWATCH_REQUIRE_REAL_ENGINE=0)"
else
    export SKYWATCH_REQUIRE_REAL_ENGINE=1
    echo "[DEMO] Mode: Real Engine Required (SKYWATCH_REQUIRE_REAL_ENGINE=1)"
fi

echo "=============================================================================="
echo " Starting SkyWatch Live Pipeline Demo (Profile: ${PROFILE})"
echo "=============================================================================="

# Check engine availability if real engine is required
python3 -c "
import sys
from skywatch.pipeline.profile import load_profile
from skywatch.pipeline.pig_runner import is_pig_available
from skywatch.pipeline.spark_runner import is_spark_available

p = load_profile('${PROFILE}')
pig_ok = is_pig_available(p.pig_home)
spark_ok = is_spark_available(p.spark_home)

print(f'Pig engine available: {pig_ok} (PIG_HOME={p.pig_home})')
print(f'Spark engine available: {spark_ok} (SPARK_HOME={p.spark_home})')

require_real = '${SKYWATCH_REQUIRE_REAL_ENGINE}' == '1'
if require_real:
    if not pig_ok:
        sys.stderr.write(f'DEMO FAILURE: Apache Pig runtime not found at PIG_HOME={p.pig_home}. Demo cannot proceed without real engine.\n')
        sys.exit(1)
    if not spark_ok:
        sys.stderr.write(f'DEMO FAILURE: Apache Spark runtime not found at SPARK_HOME={p.spark_home}. Demo cannot proceed without real engine.\n')
        sys.exit(1)
"

DEMO_OUTPUT="$PROJECT_ROOT/output/demo"
rm -rf "$DEMO_OUTPUT"
mkdir -p "$DEMO_OUTPUT/clean" "$DEMO_OUTPUT/published"

echo "[DEMO] 1. Executing Pig ETL cleaning on frozen sample..."
PIG_OPTS=()
if [[ "$SKYWATCH_REQUIRE_REAL_ENGINE" == "1" ]]; then
    PIG_OPTS+=(--require-real-engine)
fi

python3 -m skywatch.pipeline.pig_runner \
    --script "$PROJECT_ROOT/pig/clean_air_quality.pig" \
    --input "$PROJECT_ROOT/tests/fixtures/sample_air_quality.csv" \
    --output "$DEMO_OUTPUT/clean" \
    --profile "$PROFILE" \
    "${PIG_OPTS[@]}"

echo "[DEMO] 2. Executing Spark Analytics on clean data..."
SPARK_OPTS=()
if [[ "$SKYWATCH_REQUIRE_REAL_ENGINE" == "1" ]]; then
    SPARK_OPTS+=(--require-real-engine)
else
    SPARK_OPTS+=(--simulate)
fi

python3 -m skywatch.pipeline.spark_runner \
    --input-clean "$DEMO_OUTPUT/clean" \
    --input-weather "$PROJECT_ROOT/tests/fixtures/sample_weather_normalized.csv" \
    --output "$DEMO_OUTPUT/published" \
    --profile "$PROFILE" \
    "${SPARK_OPTS[@]}"

echo "=============================================================================="
echo " SkyWatch Live Demo execution complete!"
echo " Outputs available under: $DEMO_OUTPUT/published"
echo "=============================================================================="
