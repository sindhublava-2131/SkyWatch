"""Feed normalization engine for OpenAQ and Open-Meteo observation streams.

Implements HYG-01 & HYG-02: Map heterogeneous payloads to canonical schemas
with explicit measurement_time_utc and ingestion_time_utc timestamps.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from skywatch.data.deduplication import compute_air_quality_key, compute_weather_key
from skywatch.data.validation import validate_air_quality_record


def _format_utc_timestamp(ts_str: str) -> str:
    """Ensure timestamp string is parsed and normalized to UTC ISO format with Z suffix (WR-16)."""
    if not ts_str:
        return ts_str
    try:
        # If in format YYYY-MM-DDTHH:MM, append :00
        if len(ts_str) == 16 and "T" in ts_str and ":" in ts_str:
            ts_str = f"{ts_str}:00"
        dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        if ts_str.endswith("+00:00"):
            return ts_str.replace("+00:00", "Z")
        if not ts_str.endswith("Z"):
            return f"{ts_str}Z"
        return ts_str


def normalize_openaq_record(
    raw_record: Dict[str, Any],
    station_meta: Dict[str, Any],
    ingestion_time_utc: str,
) -> Dict[str, Any]:
    """Normalize a raw OpenAQ measurement dictionary into canonical air_quality schema."""
    # Extract measurement timestamp
    dt_obj = raw_record.get("datetime") or {}
    period_obj = raw_record.get("period") or {}

    ts_utc = None
    if isinstance(dt_obj, dict) and "utc" in dt_obj:
        ts_utc = dt_obj["utc"]
    elif isinstance(period_obj, dict) and "datetimeTo" in period_obj:
        dt_to = period_obj["datetimeTo"]
        if isinstance(dt_to, dict):
            ts_utc = dt_to.get("utc")

    formatted_ts = _format_utc_timestamp(ts_utc or "")

    # Extract coordinates
    coords = raw_record.get("coordinates") or {}
    lat = coords.get("latitude") or station_meta.get("latitude", 0.0)
    lon = coords.get("longitude") or station_meta.get("longitude", 0.0)

    # Extract parameter and unit
    param_obj = raw_record.get("parameter")
    param_name = ""
    unit = "ug/m3"
    if isinstance(param_obj, dict):
        param_name = param_obj.get("name", "")
        unit = param_obj.get("units", "ug/m3")
    elif isinstance(param_obj, str):
        param_name = param_obj

    # Normalize unit spelling
    if unit in ("µg/m³", "ug/m^3"):
        unit = "ug/m3"

    sensor_id = str(raw_record.get("sensorsId") or raw_record.get("sensor_id") or "")
    # Fallback lookup from station metadata sensors dict (IN-04)
    if not sensor_id and param_name:
        sensor_id = str(station_meta.get("sensors", {}).get(param_name, {}).get("sensor_id", ""))
    station_id = str(station_meta.get("station_id", ""))
    station_name = station_meta.get("name", "Unknown Station")
    val: Optional[float] = None
    try:
        if raw_record.get("value") is not None:
            val = float(raw_record["value"])
    except (ValueError, TypeError):
        val = None

    # Validate record
    eval_rec = {
        "station_id": station_id,
        "sensor_id": sensor_id,
        "parameter": param_name,
        "measurement_time_utc": formatted_ts,
        "value": val,
        "unit": unit,
    }
    val_res = validate_air_quality_record(eval_rec)

    # Compute deterministic record key
    record_key = compute_air_quality_key(sensor_id, param_name, formatted_ts)

    return {
        "record_key": record_key,
        "station_id": station_id,
        "station_name": station_name,
        "latitude": float(lat),
        "longitude": float(lon),
        "sensor_id": sensor_id,
        "parameter": param_name.lower(),
        "value": val,
        "unit": unit,
        "measurement_time_utc": formatted_ts,
        "ingestion_time_utc": ingestion_time_utc,
        "source": "openaq",
        "is_valid": val_res.is_valid,
        "reject_reason": val_res.reject_reason,
    }


def normalize_weather_response(
    raw_response: Dict[str, Any],
    location_id: str,
    ingestion_time_utc: str,
    data_kind: str = "forecast_model",
) -> List[Dict[str, Any]]:
    """Unroll parallel arrays from Open-Meteo response into canonical weather rows."""
    hourly = raw_response.get("hourly", {})
    times = hourly.get("time", [])
    temps = hourly.get("temperature_2m", [])
    humidities = hourly.get("relative_humidity_2m", [])
    wind_speeds = hourly.get("wind_speed_10m", [])
    precips = hourly.get("precipitation", [])

    lat = float(raw_response.get("latitude", 0.0))
    lon = float(raw_response.get("longitude", 0.0))

    rows: List[Dict[str, Any]] = []
    n = len(times)

    for i in range(n):
        ts = _format_utc_timestamp(times[i])
        record_key = compute_weather_key(location_id, ts)

        t_val = float(temps[i]) if i < len(temps) and temps[i] is not None else None
        h_val = float(humidities[i]) if i < len(humidities) and humidities[i] is not None else None
        w_val = float(wind_speeds[i]) if i < len(wind_speeds) and wind_speeds[i] is not None else None
        p_val = float(precips[i]) if i < len(precips) and precips[i] is not None else None

        row = {
            "record_key": record_key,
            "location_id": location_id,
            "latitude": lat,
            "longitude": lon,
            "temperature_c": t_val,
            "humidity_pct": h_val,
            "wind_speed": w_val,
            "wind_unit": "km/h",
            "precipitation_mm": p_val,
            "measurement_time_utc": ts,
            "ingestion_time_utc": ingestion_time_utc,
            "source": "open-meteo",
            "data_kind": data_kind,
        }
        rows.append(row)

    return rows
