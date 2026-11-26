#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$SCRIPT_DIR"

mkdir -p docs/images

PORT=3456
echo "Starting Next.js production server on port ${PORT}..."
./frontend/node_modules/.bin/next start frontend -p "${PORT}" > /tmp/next_capture.log 2>&1 &
SERVER_PID=$!
TMP_PROF="$(mktemp -d)"

cleanup() {
    echo "Stopping Next.js server (PID ${SERVER_PID})..."
    kill "${SERVER_PID}" 2>/dev/null || true
    wait "${SERVER_PID}" 2>/dev/null || true
    rm -rf "${TMP_PROF}" 2>/dev/null || true
}
trap cleanup EXIT

echo "Waiting for Next.js server to be responsive on http://127.0.0.1:${PORT}..."
READY=0
for i in {1..30}; do
    if curl -s "http://127.0.0.1:${PORT}/" >/dev/null 2>&1; then
        READY=1
        break
    fi
    sleep 0.5
done

if [ "$READY" -ne 1 ]; then
    echo "Server failed to start. Logs:"
    cat /tmp/next_capture.log
    exit 1
fi

echo "Capturing screenshots via headless Firefox..."
firefox --headless --no-remote -profile "${TMP_PROF}" --window-size=1280,850 --screenshot="${SCRIPT_DIR}/docs/images/dashboard_overview.png" "http://127.0.0.1:${PORT}/"
firefox --headless --no-remote -profile "${TMP_PROF}" --window-size=1280,850 --screenshot="${SCRIPT_DIR}/docs/images/dashboard_stations.png" "http://127.0.0.1:${PORT}/stations"
firefox --headless --no-remote -profile "${TMP_PROF}" --window-size=1280,850 --screenshot="${SCRIPT_DIR}/docs/images/dashboard_analytics.png" "http://127.0.0.1:${PORT}/analytics"
firefox --headless --no-remote -profile "${TMP_PROF}" --window-size=1280,850 --screenshot="${SCRIPT_DIR}/docs/images/dashboard_alerts.png" "http://127.0.0.1:${PORT}/alerts"

echo "Screenshots captured successfully:"
ls -lh docs/images/
