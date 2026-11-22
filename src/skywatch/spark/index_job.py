"""Spark analytics job: 24-hour rolling CPCB AQI calculation and suppression gating (SPK-04)."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame

from skywatch.analytics_core.cpcb import classify, load_breakpoints, subindex


def load_stations_metadata(stations_path: Optional[str] = None) -> Dict[str, str]:
    """Load mapping of station_id to station_name from config/stations.json."""
    st_file = Path(stations_path) if stations_path else Path("config/stations.json")
    if not st_file.is_file():
        return {}
    try:
        with open(st_file, "r", encoding="utf-8") as f:
            data = json.load(f)
            stations = data.get("stations", [])
            return {str(s["id"]): str(s.get("name", s["id"])) for s in stations}
    except Exception:
        return {}


def calculate_cpcb_rolling_index_records(
    hourly_records: List[Dict[str, Any]],
    stations_meta: Optional[Dict[str, str]] = None,
    breakpoints: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Pure-Python calculation of 24h rolling CPCB AQI over hourly regularized records.

    Enforces the >=16h valid readings rule, emitting INSUFFICIENT_VALID_HOURS when below threshold.

    Args:
        hourly_records: List of dicts matching hourly_rollup schema.
        stations_meta: Optional station_id -> station_name mapping.
        breakpoints: Optional breakpoint definitions dictionary.

    Returns:
        List of dicts matching cpcb_index schema.
    """
    if breakpoints is None:
        breakpoints = load_breakpoints()
    if stations_meta is None:
        stations_meta = load_stations_metadata()

    # Organize hourly records by station_id, parameter, bucket_dt
    records_by_key: Dict[str, Dict[str, Dict[datetime, float]]] = {}
    all_timestamps: set[datetime] = set()

    for r in hourly_records:
        st_id = str(r["station_id"])
        param = str(r["parameter"]).lower().replace(".", "").replace("-", "")
        if param not in ("pm25", "pm10"):
            continue

        raw_ts = str(r["bucket_start_utc"]).strip()
        if raw_ts.endswith("Z"):
            raw_ts = raw_ts[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(raw_ts)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
        except Exception:
            continue

        val = float(r["average_value"]) if r.get("average_value") is not None else None
        if val is None or val < 0.0:
            continue

        if st_id not in records_by_key:
            records_by_key[st_id] = {"pm25": {}, "pm10": {}}
        records_by_key[st_id][param][dt] = val
        all_timestamps.add(dt)

    output_records: List[Dict[str, Any]] = []

    for st_id, param_dict in sorted(records_by_key.items()):
        st_name = stations_meta.get(st_id, f"Station {st_id}")
        station_times = sorted(set(param_dict["pm25"].keys()) | set(param_dict["pm10"].keys()))

        for end_dt in station_times:
            window_start = end_dt - timedelta(hours=23)
            window_end_str = end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

            # PM2.5 evaluation
            pm25_vals = [
                v for t, v in param_dict["pm25"].items()
                if window_start <= t <= end_dt
            ]
            pm25_count = len(pm25_vals)
            if pm25_count >= 16:
                pm25_avg = round(sum(pm25_vals) / pm25_count, 1)
                pm25_sub = subindex("pm25", pm25_avg, breakpoints)
            else:
                pm25_avg = None
                pm25_sub = None

            # PM10 evaluation
            pm10_vals = [
                v for t, v in param_dict["pm10"].items()
                if window_start <= t <= end_dt
            ]
            pm10_count = len(pm10_vals)
            if pm10_count >= 16:
                pm10_avg = round(sum(pm10_vals) / pm10_count, 1)
                pm10_sub = subindex("pm10", pm10_avg, breakpoints)
            else:
                pm10_avg = None
                pm10_sub = None

            # Determine overall index and suppression
            if pm25_sub is None and pm10_sub is None:
                aqi_val = None
                aqi_cat = None
                dominant = None
                is_partial = False
                suppression = "INSUFFICIENT_VALID_HOURS"
            elif pm25_sub is not None and pm10_sub is None:
                aqi_val = pm25_sub
                aqi_cat = classify(aqi_val, breakpoints)
                dominant = "pm25"
                is_partial = True
                suppression = None
            elif pm25_sub is None and pm10_sub is not None:
                aqi_val = pm10_sub
                aqi_cat = classify(aqi_val, breakpoints)
                dominant = "pm10"
                is_partial = True
                suppression = None
            else:
                assert pm25_sub is not None and pm10_sub is not None
                if pm25_sub >= pm10_sub:
                    aqi_val = pm25_sub
                    dominant = "pm25"
                else:
                    aqi_val = pm10_sub
                    dominant = "pm10"
                aqi_cat = classify(aqi_val, breakpoints)
                is_partial = False
                suppression = None

            output_records.append({
                "station_id": st_id,
                "station_name": st_name,
                "window_end_utc": window_end_str,
                "pm25_24h_avg": pm25_avg,
                "pm25_subindex": pm25_sub,
                "pm10_24h_avg": pm10_avg,
                "pm10_subindex": pm10_sub,
                "aqi_value": aqi_val,
                "aqi_category": aqi_cat,
                "dominant_pollutant": dominant,
                "is_partial": is_partial,
                "suppression_reason": suppression,
            })

    return output_records


def compute_cpcb_rolling_index(
    df_hourly: DataFrame,
    stations_path: Optional[str] = None,
) -> DataFrame:
    """Spark DataFrame implementation of 24h rolling CPCB AQI.

    Args:
        df_hourly: Hourly rollup DataFrame.
        stations_path: Optional stations.json path for metadata enrichment.

    Returns:
        DataFrame conforming to cpcb_index schema.
    """
    bp = load_breakpoints()
    stations_meta = load_stations_metadata(stations_path)

    # Convert DataFrame to Python records for exact calculation parity
    hourly_rows = [r.asDict() for r in df_hourly.collect()]
    index_records = calculate_cpcb_rolling_index_records(hourly_rows, stations_meta, bp)

    spark = df_hourly.sparkSession
    from skywatch.spark.common import build_spark_schema

    schema = build_spark_schema("cpcb_index")
    if not index_records:
        return spark.createDataFrame([], schema)

    return spark.createDataFrame(index_records, schema).orderBy("station_id", "window_end_utc")
