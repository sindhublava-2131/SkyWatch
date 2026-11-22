"""Contract tests for Apache Pig scripts and canonical schema alignment."""
from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, List, Tuple

from skywatch.data.schemas import get_column_names, get_schema

PIG_TO_CANONICAL_TYPE_MAP: Dict[str, str] = {
    "chararray": "string",
    "double": "double",
    "float": "double",
    "long": "integer",
    "int": "integer",
    "integer": "integer",
    "boolean": "boolean",
}


def parse_pig_load_schema(script_path: Path) -> List[Tuple[str, str]]:
    """Parse column names and mapped types from Pig LOAD ... AS (...) statement."""
    content = script_path.read_text(encoding="utf-8")
    # Match LOAD ... AS ( ... ) across multiple lines
    match = re.search(r"LOAD\s+['\"][^'\"]+['\"]\s+USING\s+PigStorage\([^)]*\)\s+AS\s*\((.*?)\);", content, re.DOTALL | re.IGNORECASE)
    assert match, f"Could not find LOAD ... AS (...) in {script_path}"

    schema_block = match.group(1).strip()
    columns: List[Tuple[str, str]] = []
    # Split by comma outside parentheses
    for raw_field in schema_block.split(","):
        field = raw_field.strip()
        if not field:
            continue
        parts = [p.strip() for p in field.split(":")]
        assert len(parts) == 2, f"Invalid field declaration '{field}' in {script_path}"
        name, pig_type = parts[0], parts[1].lower()
        canonical_type = PIG_TO_CANONICAL_TYPE_MAP.get(pig_type, pig_type)
        columns.append((name, canonical_type))

    return columns


def test_clean_air_quality_pig_matches_air_quality_contract():
    """Assert clean_air_quality.pig loads exact 14 columns matching air_quality schema contract."""
    script_path = Path("pig/clean_air_quality.pig")
    pig_cols = parse_pig_load_schema(script_path)

    schema = get_schema("air_quality")
    canonical_cols = schema["columns"]

    assert len(pig_cols) == len(canonical_cols), f"Column count mismatch: {len(pig_cols)} vs {len(canonical_cols)}"
    for (pig_name, pig_type), can_col in zip(pig_cols, canonical_cols):
        assert pig_name == can_col["name"], f"Column name mismatch: {pig_name} != {can_col['name']}"
        assert pig_type == can_col["type"], f"Column type mismatch for {pig_name}: {pig_type} != {can_col['type']}"


def test_station_stats_pig_matches_clean_air_quality_contract():
    """Assert station_stats.pig loads exact 14 columns matching clean_air_quality schema contract."""
    script_path = Path("pig/station_stats.pig")
    pig_cols = parse_pig_load_schema(script_path)

    schema = get_schema("clean_air_quality")
    canonical_cols = schema["columns"]

    assert len(pig_cols) == len(canonical_cols)
    for (pig_name, pig_type), can_col in zip(pig_cols, canonical_cols):
        assert pig_name == can_col["name"]
        assert pig_type == can_col["type"]


def test_hourly_rollup_pig_matches_clean_air_quality_contract():
    """Assert hourly_rollup.pig loads exact 14 columns matching clean_air_quality schema contract."""
    script_path = Path("pig/hourly_rollup.pig")
    pig_cols = parse_pig_load_schema(script_path)

    schema = get_schema("clean_air_quality")
    canonical_cols = schema["columns"]

    assert len(pig_cols) == len(canonical_cols)
    for (pig_name, pig_type), can_col in zip(pig_cols, canonical_cols):
        assert pig_name == can_col["name"]
        assert pig_type == can_col["type"]


def test_station_stats_output_matches_contract():
    """Assert station_stats schema defines has_header: false and the exact 7 columns."""
    schema = get_schema("station_stats")
    assert schema["has_header"] is False
    assert schema["partition_key"] == "event_dt"
    cols = get_column_names("station_stats")
    expected = [
        "station_id",
        "parameter",
        "date",
        "reading_count",
        "average_value",
        "minimum_value",
        "maximum_value",
    ]
    assert cols == expected


def test_hourly_rollup_output_matches_contract():
    """Assert hourly_rollup schema defines has_header: false and the exact 7 columns."""
    schema = get_schema("hourly_rollup")
    assert schema["has_header"] is False
    assert schema["partition_key"] == "event_dt"
    cols = get_column_names("hourly_rollup")
    expected = [
        "station_id",
        "parameter",
        "bucket_start_utc",
        "reading_count",
        "average_value",
        "minimum_value",
        "maximum_value",
    ]
    assert cols == expected
