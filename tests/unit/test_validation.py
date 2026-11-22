"""Unit tests for data hygiene validation engine."""
from __future__ import annotations

from datetime import datetime, timezone

from skywatch.data.validation import (
    check_numeric_range,
    check_plausible_ceiling,
    check_required_fields,
    check_timestamp,
    check_unit,
    validate_air_quality_record,
)


def test_check_required_fields():
    rec = {"station_id": "2178", "value": 45.0}
    assert check_required_fields(rec, ["station_id", "value"]).is_valid is True
    assert check_required_fields(rec, ["station_id", "missing"]).is_valid is False
    assert check_required_fields({"station_id": None}, ["station_id"]).is_valid is False
    assert check_required_fields({"station_id": "  "}, ["station_id"]).is_valid is False


def test_check_numeric_range():
    assert check_numeric_range(42.5).is_valid is True
    assert check_numeric_range(0.0).is_valid is True
    res_neg = check_numeric_range(-5.0)
    assert res_neg.is_valid is False
    assert res_neg.reject_reason == "negative_value"

    res_null = check_numeric_range(None)
    assert res_null.is_valid is False
    assert res_null.reject_reason == "null_value"


def test_check_plausible_ceiling():
    assert check_plausible_ceiling("pm25", 999.0).is_valid is True
    res_spike = check_plausible_ceiling("pm25", 1250.0)
    assert res_spike.is_valid is False
    assert res_spike.reject_reason == "implausible_spike"

    # Other parameters not subject to 1000 limit
    assert check_plausible_ceiling("co", 1500.0).is_valid is True


def test_check_unit():
    assert check_unit("ug/m3").is_valid is True
    assert check_unit("µg/m³").is_valid is True
    assert check_unit("ug/m^3").is_valid is True

    res_ppm = check_unit("ppm")
    assert res_ppm.is_valid is False
    assert "invalid_unit:ppm" in res_ppm.reject_reason


def test_check_timestamp():
    now = datetime(2026, 11, 22, 12, 0, 0, tzinfo=timezone.utc)
    # Exact / past time is valid
    assert check_timestamp("2026-11-22T10:00:00Z", reference_time=now).is_valid is True
    # Future within 1 hour is valid
    assert check_timestamp("2026-11-22T12:30:00Z", reference_time=now).is_valid is True

    # Far future (>1 hour) is rejected
    res_future = check_timestamp("2026-11-22T15:00:00Z", reference_time=now)
    assert res_future.is_valid is False
    assert res_future.reject_reason == "future_timestamp"

    # Malformed timestamp is rejected
    assert check_timestamp("invalid-date", reference_time=now).is_valid is False


def test_validate_air_quality_record_composite():
    now = datetime(2026, 11, 22, 12, 0, 0, tzinfo=timezone.utc)
    valid_rec = {
        "station_id": "2178",
        "sensor_id": "3917",
        "parameter": "pm25",
        "value": 45.0,
        "unit": "ug/m3",
        "measurement_time_utc": "2026-11-22T11:00:00Z",
    }
    assert validate_air_quality_record(valid_rec, reference_time=now).is_valid is True

    # Invalid unit test
    bad_unit_rec = dict(valid_rec, unit="ppm")
    assert validate_air_quality_record(bad_unit_rec, reference_time=now).is_valid is False

    # Negative value test
    neg_rec = dict(valid_rec, value=-10.0)
    assert validate_air_quality_record(neg_rec, reference_time=now).is_valid is False

    # Spike value test
    spike_rec = dict(valid_rec, value=1500.0)
    assert validate_air_quality_record(spike_rec, reference_time=now).is_valid is False


def test_check_timestamp_naive_iso_does_not_crash():
    # CR-04: naive ISO format without Z or offset should be safely handled as UTC
    now = datetime(2026, 11, 22, 12, 0, 0, tzinfo=timezone.utc)
    res = check_timestamp("2026-11-22T11:30:00", reference_time=now)
    assert res.is_valid is True


def test_validate_weather_record():
    # WR-15: weather validation
    now = datetime(2026, 11, 22, 12, 0, 0, tzinfo=timezone.utc)
    valid_w = {
        "location_id": "bengaluru_central",
        "observed_time_utc": "2026-11-22T11:00:00Z",
        "temperature_2m": 24.5,
        "relative_humidity_2m": 60.0,
        "surface_pressure": 915.2,
        "precipitation": 0.0,
        "wind_speed_10m": 3.5,
    }
    from skywatch.data.validation import validate_weather_record
    assert validate_weather_record(valid_w, reference_time=now).is_valid is True

    # Out of range humidity
    bad_humidity = dict(valid_w, relative_humidity_2m=150.0)
    assert validate_weather_record(bad_humidity, reference_time=now).is_valid is False

    # Negative precipitation
    bad_precip = dict(valid_w, precipitation=-2.0)
    assert validate_weather_record(bad_precip, reference_time=now).is_valid is False
