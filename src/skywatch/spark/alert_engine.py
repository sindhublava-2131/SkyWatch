"""Spark analytics job: Health alerts, telemetry monitoring, and spike detection (SPK-05, SPK-08)."""
from __future__ import annotations

import statistics
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from pyspark.sql import DataFrame

from skywatch.analytics_core.freshness import freshness_status


def generate_health_alerts_records(
    cpcb_records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Generate health alerts for Poor, Very Poor, and Severe AQI windows in pure Python.

    Categories:
        - Poor: 201-300 (WARNING)
        - Very Poor: 301-400 (CRITICAL)
        - Severe: 401-500+ (EMERGENCY)
    """
    alerts: List[Dict[str, Any]] = []
    for r in cpcb_records:
        aqi = r.get("aqi_value")
        if aqi is None:
            continue

        cat = str(r.get("aqi_category", "")).strip()
        st_id = str(r["station_id"])
        st_name = str(r.get("station_name", f"Station {st_id}"))
        ts = str(r["window_end_utc"])

        if cat == "Poor" or (200.0 < aqi <= 300.0):
            alert_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"health_{st_id}_{ts}_poor"))
            alerts.append({
                "alert_id": alert_id,
                "alert_type": "category_poor",
                "severity": "WARNING",
                "station_id": st_id,
                "station_name": st_name,
                "timestamp_utc": ts,
                "message": f"Air quality at {st_name} reached Poor category (AQI {aqi:.1f})",
                "metric_value": float(aqi),
            })
        elif cat == "Very Poor" or (300.0 < aqi <= 400.0):
            alert_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"health_{st_id}_{ts}_very_poor"))
            alerts.append({
                "alert_id": alert_id,
                "alert_type": "category_very_poor",
                "severity": "CRITICAL",
                "station_id": st_id,
                "station_name": st_name,
                "timestamp_utc": ts,
                "message": f"Air quality at {st_name} reached Very Poor category (AQI {aqi:.1f})",
                "metric_value": float(aqi),
            })
        elif cat == "Severe" or aqi > 400.0:
            alert_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"health_{st_id}_{ts}_severe"))
            alerts.append({
                "alert_id": alert_id,
                "alert_type": "category_severe",
                "severity": "EMERGENCY",
                "station_id": st_id,
                "station_name": st_name,
                "timestamp_utc": ts,
                "message": f"Air quality emergency at {st_name}: Severe category (AQI {aqi:.1f})",
                "metric_value": float(aqi),
            })

    return alerts


def generate_telemetry_alerts_records(
    station_readings: List[Dict[str, Any]],
    reference_time_utc: Optional[str] = None,
    stations_meta: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    """Generate pipeline telemetry alerts for Stale and Offline sensor stations."""
    if stations_meta is None:
        stations_meta = {}

    latest_by_station: Dict[str, str] = {}
    for r in station_readings:
        st_id = str(r["station_id"])
        ts = str(r.get("measurement_time_utc") or r.get("bucket_start_utc", ""))
        if st_id not in latest_by_station or ts > latest_by_station[st_id]:
            latest_by_station[st_id] = ts

    ref_str = reference_time_utc or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    alerts: List[Dict[str, Any]] = []

    for st_id, latest_ts in sorted(latest_by_station.items()):
        status = freshness_status(latest_ts, ref_str)
        st_name = stations_meta.get(st_id, f"Station {st_id}")

        if status == "Stale":
            alert_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"telemetry_{st_id}_{ref_str}_stale"))
            alerts.append({
                "alert_id": alert_id,
                "alert_type": "stale_station",
                "severity": "WARNING",
                "station_id": st_id,
                "station_name": st_name,
                "timestamp_utc": ref_str,
                "message": f"Telemetry alert: {st_name} data is stale (last reading at {latest_ts})",
                "metric_value": None,
            })
        elif status == "Offline":
            alert_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"telemetry_{st_id}_{ref_str}_offline"))
            alerts.append({
                "alert_id": alert_id,
                "alert_type": "offline_station",
                "severity": "CRITICAL",
                "station_id": st_id,
                "station_name": st_name,
                "timestamp_utc": ref_str,
                "message": f"Telemetry alert: {st_name} is offline (last reading at {latest_ts})",
                "metric_value": None,
            })

    return alerts


def detect_rolling_median_spikes_records(
    hourly_records: List[Dict[str, Any]],
    multiplier: float = 3.0,
    min_history: int = 6,
    stations_meta: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    """Detect rule-based 3x rolling median spikes over trailing 24h window (SPK-08)."""
    if stations_meta is None:
        stations_meta = {}

    # Organize by (station_id, parameter) -> list of (dt, value)
    series: Dict[Tuple[str, str], List[Tuple[datetime, float]]] = {}
    for r in hourly_records:
        st_id = str(r["station_id"])
        param = str(r["parameter"])
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
        if val is None:
            continue

        key = (st_id, param)
        if key not in series:
            series[key] = []
        series[key].append((dt, val))

    alerts: List[Dict[str, Any]] = []

    for (st_id, param), data_points in series.items():
        sorted_points = sorted(data_points, key=lambda x: x[0])
        st_name = stations_meta.get(st_id, f"Station {st_id}")

        for i, (current_dt, current_val) in enumerate(sorted_points):
            window_start = current_dt - timedelta(hours=23)
            trailing_vals = [
                v for t, v in sorted_points
                if window_start <= t < current_dt  # Prior history in window
            ]

            if len(trailing_vals) >= min_history:
                med = statistics.median(trailing_vals)
                if med > 0.0 and current_val > multiplier * med:
                    alert_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"spike_{st_id}_{param}_{current_dt.strftime('%Y%m%d%H')}"))
                    alerts.append({
                        "alert_id": alert_id,
                        "alert_type": "spike_detected",
                        "severity": "WARNING",
                        "station_id": st_id,
                        "station_name": st_name,
                        "timestamp_utc": current_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "message": (
                            f"Pollution spike at {st_name} for {param}: "
                            f"{current_val:.1f} is >{multiplier:.0f}x trailing median ({med:.1f})"
                        ),
                        "metric_value": float(current_val),
                    })

    return alerts


def generate_all_alerts(
    cpcb_records: List[Dict[str, Any]],
    hourly_records: List[Dict[str, Any]],
    clean_readings: Optional[List[Dict[str, Any]]] = None,
    reference_time_utc: Optional[str] = None,
    stations_meta: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    """Aggregate health alerts, telemetry alerts, and spike detection alerts."""
    health = generate_health_alerts_records(cpcb_records)
    readings = clean_readings or hourly_records
    telemetry = generate_telemetry_alerts_records(readings, reference_time_utc, stations_meta)
    spikes = detect_rolling_median_spikes_records(hourly_records, stations_meta=stations_meta)
    return health + telemetry + spikes


def generate_alerts_dataframe(
    spark: Any,
    all_alert_records: List[Dict[str, Any]],
) -> DataFrame:
    """Create PySpark DataFrame conforming to alerts schema."""
    from skywatch.spark.common import build_spark_schema

    schema = build_spark_schema("alerts")
    if not all_alert_records:
        return spark.createDataFrame([], schema)

    return spark.createDataFrame(all_alert_records, schema).orderBy(
        "severity", "timestamp_utc"
    )
