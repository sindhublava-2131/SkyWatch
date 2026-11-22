"""Unit tests for feed normalization engine."""
from __future__ import annotations

import json
from pathlib import Path

from skywatch.data.normalization import (
    normalize_openaq_record,
    normalize_weather_response,
)
from skywatch.data.schemas import validate_record_dict


def test_normalize_openaq_record():
    fixtures_dir = Path("tests/fixtures")
    with open(fixtures_dir / "sample_openaq_latest.json", "r", encoding="utf-8") as f:
        latest = json.load(f)

    station_meta = {
        "station_id": "2178",
        "name": "BTM Layout, Bengaluru - CPCB",
        "latitude": 12.9135,
        "longitude": 77.6111,
    }
    raw_record = latest["results"][0]  # valid PM2.5 record
    norm = normalize_openaq_record(raw_record, station_meta, "2026-11-22T08:05:00Z")

    assert norm["station_id"] == "2178"
    assert norm["sensor_id"] == "3917"
    assert norm["parameter"] == "pm25"
    assert norm["value"] == 48.5
    assert norm["unit"] == "ug/m3"
    assert norm["measurement_time_utc"] == "2026-11-22T08:00:00Z"
    assert norm["ingestion_time_utc"] == "2026-11-22T08:05:00Z"
    assert norm["is_valid"] is True
    assert norm["reject_reason"] is None
    assert len(norm["record_key"]) == 64

    # Validate against canonical schema contract
    valid, err = validate_record_dict(norm, "air_quality")
    assert valid is True, f"Contract mismatch: {err}"


def test_normalize_openaq_invalid_record():
    station_meta = {
        "station_id": "2178",
        "name": "BTM Layout",
        "latitude": 12.91,
        "longitude": 77.61,
    }
    bad_record = {
        "datetime": {"utc": "2026-11-22T08:00:00Z"},
        "value": -10.0,
        "parameter": {"name": "pm25", "units": "ug/m3"},
        "sensorsId": 3917,
    }
    norm = normalize_openaq_record(bad_record, station_meta, "2026-11-22T08:05:00Z")
    assert norm["is_valid"] is False
    assert norm["reject_reason"] == "negative_value"


def test_normalize_weather_response():
    fixtures_dir = Path("tests/fixtures")
    with open(fixtures_dir / "sample_weather_forecast.json", "r", encoding="utf-8") as f:
        weather_data = json.load(f)

    rows = normalize_weather_response(weather_data, "bengaluru_central", "2026-11-22T08:05:00Z")
    assert len(rows) == 12

    first_row = rows[0]
    assert first_row["location_id"] == "bengaluru_central"
    assert first_row["temperature_c"] == 19.2
    assert first_row["measurement_time_utc"] == "2026-11-22T00:00:00Z"
    assert first_row["wind_unit"] == "km/h"

    # Validate against canonical weather schema contract
    valid, err = validate_record_dict(first_row, "weather")
    assert valid is True, f"Contract mismatch: {err}"
