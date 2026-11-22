"""Unit tests for alert engine: health alerts, telemetry alerts, and 3x median spikes."""
from __future__ import annotations

from skywatch.spark.alert_engine import (
    detect_rolling_median_spikes_records,
    generate_health_alerts_records,
    generate_telemetry_alerts_records,
)


def test_generate_health_alerts():
    records = [
        {
            "station_id": "2178",
            "station_name": "BTM Layout",
            "window_end_utc": "2026-11-22T10:00:00Z",
            "aqi_value": 45.0,
            "aqi_category": "Good",
        },
        {
            "station_id": "2178",
            "station_name": "BTM Layout",
            "window_end_utc": "2026-11-22T11:00:00Z",
            "aqi_value": 250.0,
            "aqi_category": "Poor",
        },
        {
            "station_id": "2179",
            "station_name": "Silk Board",
            "window_end_utc": "2026-11-22T12:00:00Z",
            "aqi_value": 350.0,
            "aqi_category": "Very Poor",
        },
        {
            "station_id": "2180",
            "station_name": "Peenya",
            "window_end_utc": "2026-11-22T13:00:00Z",
            "aqi_value": 450.0,
            "aqi_category": "Severe",
        },
    ]

    alerts = generate_health_alerts_records(records)
    assert len(alerts) == 3

    types = [a["alert_type"] for a in alerts]
    assert types == ["category_poor", "category_very_poor", "category_severe"]

    severities = [a["severity"] for a in alerts]
    assert severities == ["WARNING", "CRITICAL", "EMERGENCY"]


def test_generate_telemetry_alerts():
    ref_ts = "2026-11-23T12:00:00Z"
    readings = [
        {"station_id": "2178", "measurement_time_utc": "2026-11-23T11:00:00Z"},  # Fresh (1h)
        {"station_id": "2179", "measurement_time_utc": "2026-11-23T06:00:00Z"},  # Stale (6h)
        {"station_id": "2180", "measurement_time_utc": "2026-11-21T10:00:00Z"},  # Offline (>24h)
    ]
    meta = {"2178": "BTM Layout", "2179": "Silk Board", "2180": "Peenya"}

    alerts = generate_telemetry_alerts_records(readings, ref_ts, meta)
    assert len(alerts) == 2

    stale = next(a for a in alerts if a["alert_type"] == "stale_station")
    assert stale["station_id"] == "2179"
    assert stale["severity"] == "WARNING"

    offline = next(a for a in alerts if a["alert_type"] == "offline_station")
    assert offline["station_id"] == "2180"
    assert offline["severity"] == "CRITICAL"


def test_detect_rolling_median_spikes():
    # Construct 10 hours of steady PM2.5 readings (value=20.0), then a spike to 80.0 (> 3x median of 20)
    hourly = []
    for h in range(10):
        ts = f"2026-11-22T{h:02d}:00:00Z"
        hourly.append({
            "station_id": "2178",
            "parameter": "pm25",
            "bucket_start_utc": ts,
            "average_value": 20.0,
        })
    # Hour 10 spike
    hourly.append({
        "station_id": "2178",
        "parameter": "pm25",
        "bucket_start_utc": "2026-11-22T10:00:00Z",
        "average_value": 85.0,
    })

    spikes = detect_rolling_median_spikes_records(hourly, multiplier=3.0, min_history=6)
    assert len(spikes) == 1
    assert spikes[0]["alert_type"] == "spike_detected"
    assert spikes[0]["severity"] == "WARNING"
    assert spikes[0]["metric_value"] == 85.0
