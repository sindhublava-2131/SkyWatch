#!/usr/bin/env bash
# ==============================================================================
# SkyWatch Profile Switching and Validation Script
# Usage:
#   source scripts/use_profile.sh [modern|lab]
#   ./scripts/use_profile.sh [modern|lab] [--validate]
# ==============================================================================

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

# ------------------------------------------------------------------------------
# Runtime Prerequisite Validation
# ------------------------------------------------------------------------------
echo "--- Environment Validation for '${PROFILE}' ---"

# 1. Java for Pig check (JDK 8 required for Pig)
if [ -d "$JAVA_HOME_PIG" ] && [ -x "$JAVA_HOME_PIG/bin/java" ]; then
    JAVA_PIG_VER=$("$JAVA_HOME_PIG/bin/java" -version 2>&1 | head -n 1)
    echo "[OK] Pig Java: $JAVA_PIG_VER"
else
    echo "[WARN] Java for Pig not found at JAVA_HOME_PIG=$JAVA_HOME_PIG"
fi

# 2. Java for Spark check (JDK 8 or 17)
if [ -d "$JAVA_HOME_SPARK" ] && [ -x "$JAVA_HOME_SPARK/bin/java" ]; then
    JAVA_SPARK_VER=$("$JAVA_HOME_SPARK/bin/java" -version 2>&1 | head -n 1)
    echo "[OK] Spark Java: $JAVA_SPARK_VER"
else
    echo "[WARN] Java for Spark not found at JAVA_HOME_SPARK=$JAVA_HOME_SPARK"
fi

# 3. Hadoop / HDFS availability check
if [ -d "$HADOOP_HOME" ] && [ -x "$HADOOP_HOME/bin/hadoop" ]; then
    HADOOP_VER=$("$HADOOP_HOME/bin/hadoop" version 2>&1 | head -n 1)
    echo "[OK] Hadoop: $HADOOP_VER"
else
    echo "[INFO] Native Hadoop not installed at HADOOP_HOME=$HADOOP_HOME (Fallback: Local / Simulation mode)"
fi

# 4. Pig runtime check
if [ -d "$PIG_HOME" ] && [ -x "$PIG_HOME/bin/pig" ]; then
    echo "[OK] Apache Pig installed at $PIG_HOME"
else
    echo "[INFO] Apache Pig not installed at PIG_HOME=$PIG_HOME (Fallback: Standalone Python simulation)"
fi

# 5. Spark runtime and Python 3.8 compatibility check
if [ -d "$SPARK_HOME" ] && [ -x "$SPARK_HOME/bin/spark-submit" ]; then
    echo "[OK] Apache Spark installed at $SPARK_HOME"
else
    echo "[INFO] Apache Spark not installed at SPARK_HOME=$SPARK_HOME (Fallback: Standalone Python simulation)"
fi

echo "-----------------------------------------"
