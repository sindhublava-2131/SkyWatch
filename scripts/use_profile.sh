#!/usr/bin/env bash
# SkyWatch profile switching script
# Usage: source scripts/use_profile.sh [modern|lab]

PROFILE="${1:-modern}"
if [[ "$PROFILE" != "modern" && "$PROFILE" != "lab" ]]; then
    echo "Error: Invalid profile '$PROFILE'. Available profiles: modern, lab" >&2
    return 1 2>/dev/null || exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROFILE_FILE="${SCRIPT_DIR}/profiles/${PROFILE}.env"

if [ ! -f "$PROFILE_FILE" ]; then
    echo "Error: Profile file '$PROFILE_FILE' does not exist." >&2
    echo "Available profiles: modern, lab" >&2
    return 1 2>/dev/null || exit 1
fi

# Export all variables defined in profile file
set -a
# shellcheck disable=SC1090
source "$PROFILE_FILE"
set +a

echo "========================================="
echo " SkyWatch Profile Switched to: $SKYWATCH_PROFILE"
echo "========================================="
echo " HADOOP_HOME:      $HADOOP_HOME"
echo " PIG_HOME:         $PIG_HOME"
echo " SPARK_HOME:       $SPARK_HOME"
echo " JAVA_HOME_PIG:    $JAVA_HOME_PIG"
echo " JAVA_HOME_SPARK:  $JAVA_HOME_SPARK"
echo " JAVA_HOME:        $JAVA_HOME"
echo " HDFS_ROOT:        $HDFS_ROOT"
echo " HTTP_TIMEOUT:     $SKYWATCH_HTTP_TIMEOUT"
echo " MAX_RETRIES:      $SKYWATCH_MAX_RETRIES"
echo "========================================="
