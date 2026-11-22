"""Contract tests for canonical data schemas."""
from __future__ import annotations

from pathlib import Path

from skywatch.data.schemas import (
    get_column_names,
    get_schema,
    load_schemas,
    validate_record_dict,
)


def test_schemas_yaml_exists_and_valid():
    schema_path = Path("config/schemas.yaml")
    assert schema_path.is_file(), "config/schemas.yaml must exist"
    schemas = load_schemas()
    assert schemas.get("version") == "2.0"
    assert "air_quality" in schemas
    assert "weather" in schemas
    assert "manifest" in schemas


def test_air_quality_schema_contract():
    aq = get_schema("air_quality")
    assert aq["partition_key"] == "ingest_dt"
    assert aq["has_header"] is True

    columns = get_column_names("air_quality")
    expected_cols = [
        "record_key",
        "station_id",
        "station_name",
        "latitude",
        "longitude",
        "sensor_id",
        "parameter",
        "value",
        "unit",
        "measurement_time_utc",
        "ingestion_time_utc",
        "source",
        "is_valid",
        "reject_reason",
    ]
    assert columns == expected_cols


def test_weather_schema_contract():
    w = get_schema("weather")
    assert w["partition_key"] == "ingest_dt"
    assert w["has_header"] is True

    columns = get_column_names("weather")
    expected_cols = [
        "record_key",
        "location_id",
        "latitude",
        "longitude",
        "temperature_c",
        "humidity_pct",
        "wind_speed",
        "wind_unit",
        "precipitation_mm",
        "measurement_time_utc",
        "ingestion_time_utc",
        "source",
        "data_kind",
    ]
    assert columns == expected_cols


def test_manifest_schema_contract():
    manifest_cols = get_column_names("manifest")
    expected_cols = [
        "run_id",
        "file_path",
        "source",
        "endpoint_url",
        "http_status",
        "sha256",
        "record_count",
        "collected_at_utc",
    ]
    assert manifest_cols == expected_cols


def test_validate_record_dict():
    valid_record = {
        "record_key": "abc123hash",
        "station_id": "2178",
        "station_name": "BTM Layout",
        "latitude": 12.91,
        "longitude": 77.61,
        "sensor_id": "3917",
        "parameter": "pm25",
        "value": 45.2,
        "unit": "ug/m3",
        "measurement_time_utc": "2026-11-22T08:00:00Z",
        "ingestion_time_utc": "2026-11-22T08:05:00Z",
        "source": "openaq",
        "is_valid": True,
        "reject_reason": None,
    }
    is_valid, reason = validate_record_dict(valid_record, "air_quality")
    assert is_valid is True
    assert reason is None

    # Missing non-nullable column
    invalid_record = dict(valid_record)
    del invalid_record["record_key"]
    is_valid, reason = validate_record_dict(invalid_record, "air_quality")
    assert is_valid is False
    assert "Missing non-nullable column 'record_key'" in reason

    # Null value on non-nullable column
    invalid_record2 = dict(valid_record)
    invalid_record2["measurement_time_utc"] = None
    is_valid, reason = validate_record_dict(invalid_record2, "air_quality")
    assert is_valid is False
    assert "cannot be null" in reason

    # Bad type on numeric column
    invalid_record3 = dict(valid_record)
    invalid_record3["value"] = "not-a-number"
    is_valid, reason = validate_record_dict(invalid_record3, "air_quality")
    assert is_valid is False
    assert "must be numeric" in reason
