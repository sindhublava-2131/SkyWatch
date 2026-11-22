"""Data hygiene validation rules for environmental observation streams.

Implements HYG-04: Validate records for required fields, numeric range,
unit sanity (ug/m3), plausible ceilings (PM <= 1000 ug/m³), and timestamp validity.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


@dataclass
class ValidationResult:
    is_valid: bool
    reject_reason: Optional[str] = None


def check_required_fields(record: Dict[str, Any], required: List[str]) -> ValidationResult:
    """Verify all required fields exist and are not None or empty strings."""
    for field in required:
        if field not in record:
            return ValidationResult(False, f"missing_field:{field}")
        val = record[field]
        if val is None or (isinstance(val, str) and not val.strip()):
            return ValidationResult(False, f"missing_field:{field}")
    return ValidationResult(True, None)


def check_numeric_range(value: Any) -> ValidationResult:
    """Check that a measurement value is numeric and non-negative."""
    if value is None:
        return ValidationResult(False, "null_value")
    try:
        fval = float(value)
    except (ValueError, TypeError):
        return ValidationResult(False, "non_numeric_value")

    if fval < 0.0:
        return ValidationResult(False, "negative_value")
    return ValidationResult(True, None)


def check_plausible_ceiling(
    parameter: str,
    value: float,
    ceiling: float = 1000.0,
) -> ValidationResult:
    """Check that particulate concentrations do not exceed plausible physical limits (<= 1000 µg/m³)."""
    param_clean = (parameter or "").lower().replace(".", "")
    if param_clean in ("pm25", "pm10"):
        if value > ceiling:
            return ValidationResult(False, "implausible_spike")
    return ValidationResult(True, None)


def check_unit(unit: str) -> ValidationResult:
    """Validate concentration unit and check for incompatible gas units (ppm/ppb)."""
    if not unit or not isinstance(unit, str):
        return ValidationResult(False, "missing_unit")

    cleaned = unit.strip().lower()
    # Accepted concentration units for particulates
    if cleaned in ("ug/m3", "µg/m³", "ug/m^3", "mg/m3"):
        return ValidationResult(True, None)
    return ValidationResult(False, f"invalid_unit:{unit}")


def check_timestamp(
    ts_str: str,
    max_future_seconds: int = 3600,
    reference_time: Optional[datetime] = None,
) -> ValidationResult:
    """Validate ISO-8601 UTC timestamp format and reject timestamps > 1h in the future."""
    if not ts_str or not isinstance(ts_str, str):
        return ValidationResult(False, "missing_timestamp")

    try:
        dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return ValidationResult(False, "invalid_timestamp_format")

    now = reference_time or datetime.now(timezone.utc)
    if (dt - now).total_seconds() > max_future_seconds:
        return ValidationResult(False, "future_timestamp")

    return ValidationResult(True, None)


def validate_weather_record(
    record: Dict[str, Any],
    reference_time: Optional[datetime] = None,
) -> ValidationResult:
    """Validate meteorological hygiene rules on a weather record."""
    required = ["location_id", "observed_time_utc", "temperature_2m", "relative_humidity_2m"]
    res = check_required_fields(record, required)
    if not res.is_valid:
        return res

    res = check_timestamp(record["observed_time_utc"], reference_time=reference_time)
    if not res.is_valid:
        return res

    try:
        humidity = float(record["relative_humidity_2m"])
        if not (0.0 <= humidity <= 100.0):
            return ValidationResult(False, f"invalid_humidity_range:{humidity}")

        temp = float(record["temperature_2m"])
        if not (-50.0 <= temp <= 65.0):
            return ValidationResult(False, f"extreme_temperature_outlier:{temp}")

        if record.get("precipitation") is not None:
            precip = float(record["precipitation"])
            if precip < 0.0:
                return ValidationResult(False, f"negative_precipitation:{precip}")

        if record.get("wind_speed_10m") is not None:
            wind = float(record["wind_speed_10m"])
            if wind < 0.0:
                return ValidationResult(False, f"negative_wind_speed:{wind}")
    except (ValueError, TypeError):
        return ValidationResult(False, "weather_field_type_error")

    return ValidationResult(True, None)


def validate_air_quality_record(
    record: Dict[str, Any],
    reference_time: Optional[datetime] = None,
) -> ValidationResult:
    """Execute the full data hygiene validation pipeline on an air quality record."""
    # 1. Required fields
    required = ["station_id", "sensor_id", "parameter", "measurement_time_utc", "value", "unit"]
    res = check_required_fields(record, required)
    if not res.is_valid:
        return res

    # 2. Timestamp validity
    res = check_timestamp(record["measurement_time_utc"], reference_time=reference_time)
    if not res.is_valid:
        return res

    # 3. Numeric range
    res = check_numeric_range(record["value"])
    if not res.is_valid:
        return res

    val = float(record["value"])

    # 4. Plausible physical ceiling
    res = check_plausible_ceiling(record["parameter"], val)
    if not res.is_valid:
        return res

    # 5. Unit sanity
    res = check_unit(record["unit"])
    if not res.is_valid:
        return res

    return ValidationResult(True, None)
