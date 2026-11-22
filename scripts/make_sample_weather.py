"""Helper script to generate normalized sample weather fixtures for testing."""
from __future__ import annotations

import csv
import math
from datetime import datetime, timedelta, timezone
from pathlib import Path


def generate_sample_weather(output_file: Path) -> int:
    output_file.parent.mkdir(parents=True, exist_ok=True)
    start_dt = datetime(2026, 11, 20, 0, 0, 0, tzinfo=timezone.utc)
    rows = []
    for h in range(72):
        dt = start_dt + timedelta(hours=h)
        dt_str = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        ingest_str = (dt + timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M:%SZ")
        hour_of_day = dt.hour
        temp = round(22.0 + 6.0 * math.sin((hour_of_day - 8) * math.pi / 12), 1)
        humidity = round(65.0 - 15.0 * math.sin((hour_of_day - 8) * math.pi / 12), 1)
        wind = round(8.0 + 4.0 * math.cos(hour_of_day * math.pi / 12), 1)
        precip = 1.2 if hour_of_day == 16 else 0.0
        rec_key = "wth_" + dt.strftime("%Y%m%d%H")
        rows.append({
            "record_key": rec_key,
            "location_id": "bengaluru_central",
            "latitude": 12.9716,
            "longitude": 77.5946,
            "temperature_c": temp,
            "humidity_pct": humidity,
            "wind_speed": wind,
            "wind_unit": "km/h",
            "precipitation_mm": precip,
            "measurement_time_utc": dt_str,
            "ingestion_time_utc": ingest_str,
            "source": "open-meteo",
            "data_kind": "archive",
        })

    fieldnames = [
        "record_key", "location_id", "latitude", "longitude", "temperature_c",
        "humidity_pct", "wind_speed", "wind_unit", "precipitation_mm",
        "measurement_time_utc", "ingestion_time_utc", "source", "data_kind"
    ]
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    return len(rows)


if __name__ == "__main__":
    count = generate_sample_weather(Path("tests/fixtures/sample_weather_normalized.csv"))
    print(f"Generated sample_weather_normalized.csv with {count} rows")
