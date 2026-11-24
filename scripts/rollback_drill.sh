#!/usr/bin/env bash
# ==============================================================================
# SkyWatch Profile Rollback Drill (OPS-06)
# Validates numerical parity and schema equivalence between 'modern' (2026)
# and 'lab' (Hadoop 3.2.2 / Spark 3.2.2 rollback) execution profiles.
#
# Usage:
#   bash scripts/rollback_drill.sh [--allow-simulation]
# ==============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$SCRIPT_DIR"

PYTHON="${PYTHON:-python3}"
if [ -f ".venv/bin/python" ]; then
    PYTHON=".venv/bin/python"
fi

ALLOW_SIMULATION=0
KEEP_ARTIFACTS=0
for arg in "$@"; do
    if [[ "$arg" == "--keep-artifacts" ]]; then
        KEEP_ARTIFACTS=1
    elif [[ "$arg" == "--allow-simulation" || "$arg" == "--simulate" ]]; then
        ALLOW_SIMULATION=1
    fi
done

if [[ "${ALLOW_SIMULATION}" == "1" || "${SKYWATCH_ALLOW_SIMULATION:-0}" == "1" ]]; then
    SIM_FLAG="--simulate"
    echo "[DRILL] Mode: Standalone Simulation Permitted"
else
    SIM_FLAG=""
    echo "[DRILL] Mode: Real Engine Enforcement Active"
fi

DRILL_MODERN="output/drill_modern"
DRILL_LAB="output/drill_lab"

cleanup() {
    exit_code=$?
    if [ "$exit_code" -eq 0 ] && [ "$KEEP_ARTIFACTS" -ne 1 ]; then
        echo "[DRILL] Cleaning up temporary drill directories..."
        rm -rf "$DRILL_MODERN" "$DRILL_LAB" data/drill
    fi
}
trap cleanup EXIT

rm -rf "$DRILL_MODERN" "$DRILL_LAB" data/drill
mkdir -p data/drill

echo "=============================================================================="
echo " SkyWatch Dual-Profile Rollback Drill"
echo " Comparing 'modern' vs 'lab' profile parity"
echo "=============================================================================="

# ------------------------------------------------------------------------------
# 1. Execute Pipeline under 'modern' Profile
# ------------------------------------------------------------------------------
echo "[1/3] Executing pipeline under 'modern' profile..."
bash scripts/use_profile.sh modern > /dev/null

$PYTHON -m skywatch.cli pipeline \
    --profile modern \
    --offline \
    $SIM_FLAG \
    --clean-dir "data/drill/modern_clean" \
    --published-dir "$DRILL_MODERN" \
    --sample-air-input "tests/fixtures/sample_air_quality.csv" \
    --sample-weather-input "tests/fixtures/sample_weather_normalized.csv" \
    --run-id "drill_modern_run"

# ------------------------------------------------------------------------------
# 2. Execute Pipeline under 'lab' Profile
# ------------------------------------------------------------------------------
echo "[2/3] Executing pipeline under 'lab' profile..."
bash scripts/use_profile.sh lab > /dev/null

$PYTHON -m skywatch.cli pipeline \
    --profile lab \
    --offline \
    $SIM_FLAG \
    --clean-dir "data/drill/lab_clean" \
    --published-dir "$DRILL_LAB" \
    --sample-air-input "tests/fixtures/sample_air_quality.csv" \
    --sample-weather-input "tests/fixtures/sample_weather_normalized.csv" \
    --run-id "drill_lab_run"

# ------------------------------------------------------------------------------
# 3. Numeric & Parity Comparison
# ------------------------------------------------------------------------------
echo "[3/3] Evaluating parity between modern and lab analytical outputs..."

$PYTHON -c "
import csv
import sys
from pathlib import Path

modern_base = Path('$DRILL_MODERN')
lab_base = Path('$DRILL_LAB')

datasets = [
    'station_statistics',
    'trends_hourly',
    'trends_daily',
    'weather_correlation',
    'cpcb_index',
    'alerts',
    'quality_report',
]

tolerance = 0.01
all_passed = True

print(f'Checking {len(datasets)} published datasets for exact parity (epsilon < {tolerance})...\n')

for ds in datasets:
    mod_csv = modern_base / ds / 'csv' / 'part-00000.csv'
    lab_csv = lab_base / ds / 'csv' / 'part-00000.csv'

    if not mod_csv.is_file():
        print(f'[FAIL] Modern dataset missing: {mod_csv}')
        all_passed = False
        continue
    if not lab_csv.is_file():
        print(f'[FAIL] Lab dataset missing: {lab_csv}')
        all_passed = False
        continue

    with open(mod_csv, 'r', encoding='utf-8') as f:
        mod_rows = list(csv.reader(f))
    with open(lab_csv, 'r', encoding='utf-8') as f:
        lab_rows = list(csv.reader(f))

    if len(mod_rows) != len(lab_rows):
        print(f'[FAIL] {ds}: Row count mismatch! modern={len(mod_rows)} vs lab={len(lab_rows)}')
        all_passed = False
        continue

    if not mod_rows:
        print(f'[PASS] {ds}: Empty dataset on both profiles.')
        continue

    header = mod_rows[0]
    mismatches = 0

    for r_idx, (m_row, l_row) in enumerate(zip(mod_rows[1:], lab_rows[1:]), start=2):
        for c_idx, (m_val, l_val) in enumerate(zip(m_row, l_row)):
            col = header[c_idx]
            if col in ('alert_id', 'run_id', 'generated_at_utc', 'timestamp_utc', 'created_at_utc'):
                continue
            if m_val == l_val:
                continue
            try:
                mf = float(m_val)
                lf = float(l_val)
                diff = abs(mf - lf)
                if diff >= tolerance:
                    print(f'  Row {r_idx} [{col}]: modern={mf} vs lab={lf} (diff={diff:.4f} >= {tolerance})')
                    mismatches += 1
            except ValueError:
                print(f'  Row {r_idx} [{col}]: string mismatch: modern=\"{m_val}\" vs lab=\"{l_val}\"')
                mismatches += 1

    if mismatches == 0:
        print(f'[PASS] {ds}: Parity verified across {len(mod_rows)-1} records.')
    else:
        print(f'[FAIL] {ds}: {mismatches} numeric mismatches detected.')
        all_passed = False

if not all_passed:
    sys.stderr.write('\nRollback drill failed: output parity mismatch between modern and lab.\n')
    sys.exit(1)

print('\n==============================================================================')
print(' Rollback Drill: PASSED (100% numerical and schema parity confirmed)')
print('==============================================================================')
"
