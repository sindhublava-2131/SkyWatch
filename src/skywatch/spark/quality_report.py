"""Spark analytics job: Multi-stage data quality and pipeline audit reporting (SPK-07)."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

try:
    from pyspark.sql import DataFrame, SparkSession
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False
    DataFrame = Any  # type: ignore[misc,assignment]
    SparkSession = Any  # type: ignore[misc,assignment]


def build_quality_report_records(
    run_id: str,
    stage_summaries: List[Dict[str, Any]],
    generated_at_utc: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Build standardized multi-stage audit summary records in pure Python.

    Args:
        run_id: Unique pipeline execution identifier.
        stage_summaries: List of stage metric dicts (stage, accepted_count, rejected_count, duplicate_count, reason_counts).
        generated_at_utc: Optional ISO UTC timestamp string.

    Returns:
        List of dicts conforming to quality_report schema.
    """
    ts = generated_at_utc or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    records: List[Dict[str, Any]] = []

    for stage in stage_summaries:
        stage_name = str(stage.get("stage", "unknown"))
        accepted = int(stage.get("accepted_count", 0))
        rejected = int(stage.get("rejected_count", 0))
        duplicate = int(stage.get("duplicate_count", 0))

        reasons = stage.get("reason_counts") or stage.get("reason_counts_json") or {}
        if isinstance(reasons, str):
            reason_json = reasons
        else:
            reason_json = json.dumps(reasons, sort_keys=True)

        records.append({
            "run_id": run_id,
            "stage": stage_name,
            "accepted_count": accepted,
            "rejected_count": rejected,
            "duplicate_count": duplicate,
            "reason_counts_json": reason_json,
            "generated_at_utc": ts,
        })

    return records


def build_quality_report(
    spark: SparkSession,
    run_id: str,
    stage_summaries: List[Dict[str, Any]],
    generated_at_utc: Optional[str] = None,
) -> DataFrame:
    """Build PySpark DataFrame conforming to quality_report schema."""
    from skywatch.spark.common import build_spark_schema

    schema = build_spark_schema("quality_report")
    records = build_quality_report_records(run_id, stage_summaries, generated_at_utc)

    if not records:
        return spark.createDataFrame([], schema)

    return spark.createDataFrame(records, schema).orderBy("stage")
