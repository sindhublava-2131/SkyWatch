"""Contract tests for PySpark schema mapping (CT-02)."""
from __future__ import annotations

import pytest

from skywatch.data.schemas import get_schema
from skywatch.spark.common import build_spark_schema


def test_build_spark_schema_clean_air_quality():
    spark_schema = build_spark_schema("clean_air_quality")
    yaml_schema = get_schema("clean_air_quality")
    yaml_cols = yaml_schema["columns"]

    assert len(spark_schema.fields) == len(yaml_cols)
    for field, yaml_col in zip(spark_schema.fields, yaml_cols):
        assert field.name == yaml_col["name"]
        assert field.nullable == yaml_col.get("nullable", True)

        expected_type = yaml_col.get("type", "string")
        if expected_type == "string":
            assert field.dataType.__class__.__name__ == "StringType"
        elif expected_type in ("double", "float"):
            assert field.dataType.__class__.__name__ in ("DoubleType", "FloatType")
        elif expected_type in ("integer", "int"):
            assert field.dataType.__class__.__name__ == "IntegerType"
        elif expected_type == "boolean":
            assert field.dataType.__class__.__name__ == "BooleanType"


@pytest.mark.parametrize(
    "entity_name",
    [
        "station_statistics",
        "trends_hourly",
        "trends_daily",
        "weather_correlation",
        "cpcb_index",
        "alerts",
        "quality_report",
    ],
)
def test_build_spark_schema_phase_3_entities(entity_name: str):
    spark_schema = build_spark_schema(entity_name)
    yaml_schema = get_schema(entity_name)
    yaml_cols = yaml_schema["columns"]

    assert len(spark_schema.fields) == len(yaml_cols)
    for field, yaml_col in zip(spark_schema.fields, yaml_cols):
        assert field.name == yaml_col["name"]
        assert field.nullable == yaml_col.get("nullable", True)


def test_build_spark_schema_unknown_raises():
    with pytest.raises(KeyError):
        build_spark_schema("non_existent_stream")
