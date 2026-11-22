"""Unit tests for quality report generation and stage summary aggregation."""
from __future__ import annotations

import json

from skywatch.spark.quality_report import build_quality_report_records


def test_build_quality_report_records():
    run_id = "test_run_123"
    stage_summaries = [
        {
            "stage": "ingestion",
            "accepted_count": 500,
            "rejected_count": 10,
            "duplicate_count": 5,
            "reason_counts": {"OUT_OF_RANGE": 7, "INVALID_TIMESTAMP": 3},
        },
        {
            "stage": "pig_etl",
            "accepted_count": 485,
            "rejected_count": 0,
            "duplicate_count": 0,
            "reason_counts": {},
        },
    ]

    records = build_quality_report_records(run_id, stage_summaries, "2026-11-23T12:00:00Z")
    assert len(records) == 2

    r0 = records[0]
    assert r0["run_id"] == run_id
    assert r0["stage"] == "ingestion"
    assert r0["accepted_count"] == 500
    assert r0["rejected_count"] == 10
    assert r0["duplicate_count"] == 5

    parsed_json = json.loads(r0["reason_counts_json"])
    assert parsed_json["OUT_OF_RANGE"] == 7
    assert parsed_json["INVALID_TIMESTAMP"] == 3

    r1 = records[1]
    assert r1["stage"] == "pig_etl"
    assert r1["accepted_count"] == 485
    assert json.loads(r1["reason_counts_json"]) == {}
