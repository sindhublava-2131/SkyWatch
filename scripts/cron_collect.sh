#!/usr/bin/env bash
# Automated incremental collection runner with file locking
# Usage: ./scripts/cron_collect.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$SCRIPT_DIR"

LOCKFILE="/tmp/skywatch_collect.lock"
LOGFILE="data/logs/cron_collect.log"

mkdir -p data/logs

# Error trap logging failure (IN-05)
trap 'echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] Error: Ingestion collection failed with exit code $?" >> "$LOGFILE"' ERR

# Cross-platform / POSIX flock guard
if command -v flock >/dev/null 2>&1; then
    exec 200>"$LOCKFILE"
    if ! flock -n 200; then
        echo "[$(date -u +'%Y-%m-%dT%H:%M:%SZ')] Another collection instance is running. Exiting." >> "$LOGFILE"
        exit 0
    fi
fi

# Source active profile if present (WR-23)
PROFILE="${SKYWATCH_PROFILE:-modern}"
if [ -f "profiles/${PROFILE}.env" ]; then
    set -a
    # shellcheck disable=SC1090
    source "profiles/${PROFILE}.env"
    set +a
fi

echo "[$(date -u +'%Y-%m-%dT%H:%M:%SZ')] Starting cron ingestion collection (profile: ${PROFILE})..." >> "$LOGFILE"

python -m skywatch.cli collect --mode incremental >> "$LOGFILE" 2>&1

echo "[$(date -u +'%Y-%m-%dT%H:%M:%SZ')] Ingestion collection completed successfully." >> "$LOGFILE"
