#!/usr/bin/env python3
"""Generate sample dataset and golden fixtures for Phase 2 Pig ETL."""
from __future__ import annotations

import csv
import hashlib
import shutil
from pathlib import Path
from typing import Dict, List, Tuple

from skywatch.pipeline.pig_runner import generate_sidecar_schema

STATIONS = [
    {"station_id": "2178", "name": "BTM Layout, Bengaluru - CPCB", "lat": 12.9135, "lon": 77.6111, "pm25_sensor": "3917", "pm10_sensor": "3918"},
    {"station_id": "2179", "name": "Silk Board, Bengaluru - CPCB", "lat": 12.9173, "lon": 77.6229, "pm25_sensor": "3919", "pm10_sensor": "3920"},
    {"station_id": "2180", "name": "Peenya, Bengaluru - CPCB", "lat": 13.0274, "lon": 77.5186, "pm25_sensor": "3921", "pm10_sensor": "3922"},
    {"station_id": "2181", "name": "Hebbal, Bengaluru - CPCB", "lat": 13.0359, "lon": 77.5970, "pm25_sensor": "3923", "pm10_sensor": "3924"},
    {"station_id": "2182", "name": "Jayanagar, Bengaluru - CPCB", "lat": 12.9299, "lon": 77.5824, "pm25_sensor": "3925", "pm10_sensor": "3926"},
]

DAYS = ["2026-11-20", "2026-11-21", "2026-11-22"]
HOURS = [f"{h:02d}" for h in range(24)]

def make_record_key(station_id: str, sensor_id: str, timestamp: str) -> str:
    raw = f"{station_id}:{sensor_id}:{timestamp}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]

def main() -> None:
    raw_rows: List[List[str]] = []
    header = [
        "record_key", "station_id", "station_name", "latitude", "longitude",
        "sensor_id", "parameter", "value", "unit", "measurement_time_utc",
        "ingestion_time_utc", "source", "is_valid", "reject_reason"
    ]
    raw_rows.append(header)

    # 1. Generate regular valid rows
    valid_count = 0
    clean_tuples: List[Tuple[str, ...]] = []

    for d_idx, day in enumerate(DAYS):
        for h in range(24):  # Every hour
            time_str = f"{day}T{h:02d}:00:00Z"
            ingest_str = f"{day}T{h:02d}:05:00Z"
            for s_idx, station in enumerate(STATIONS):
                # PM2.5 reading
                pm25_val = round(25.0 + (s_idx * 7.5) + (h * 1.8) + (d_idx * 3.2), 2)
                rk_pm25 = make_record_key(station["station_id"], station["pm25_sensor"], time_str)
                row_pm25 = [
                    rk_pm25, station["station_id"], station["name"], str(station["lat"]), str(station["lon"]),
                    station["pm25_sensor"], "pm25", str(pm25_val), "ug/m3", time_str,
                    ingest_str, "openaq", "true", ""
                ]
                raw_rows.append(row_pm25)
                clean_tuples.append(tuple(row_pm25))
                valid_count += 1

                # PM10 reading
                pm10_val = round(50.0 + (s_idx * 14.0) + (h * 3.1) + (d_idx * 5.0), 2)
                rk_pm10 = make_record_key(station["station_id"], station["pm10_sensor"], time_str)
                row_pm10 = [
                    rk_pm10, station["station_id"], station["name"], str(station["lat"]), str(station["lon"]),
                    station["pm10_sensor"], "pm10", str(pm10_val), "ug/m3", time_str,
                    ingest_str, "openaq", "true", ""
                ]
                raw_rows.append(row_pm10)
                clean_tuples.append(tuple(row_pm10))
                valid_count += 1

    # 2. Inject Duplicates (test safety DISTINCT)
    # Duplicate 15 random valid rows
    for i in range(15):
        raw_rows.append(list(clean_tuples[i * 20]))

    # 3. Inject Edge Cases (Must be filtered out by clean_air_quality.pig)
    # Edge Case A: is_valid is false
    for i in range(10):
        t_str = f"2026-11-20T{i:02d}:30:00Z"
        raw_rows.append([
            f"inv_{i}", "2178", "BTM Layout, Bengaluru - CPCB", "12.9135", "77.6111",
            "3917", "pm25", "42.0", "ug/m3", t_str,
            t_str, "openaq", "false", "spurious_sensor_spike"
        ])

    # Edge Case B: Spike outlier > 1000.0
    for i in range(5):
        t_str = f"2026-11-21T{i:02d}:30:00Z"
        raw_rows.append([
            f"outlier_{i}", "2179", "Silk Board, Bengaluru - CPCB", "12.9173", "77.6229",
            "3919", "pm25", "1250.0", "ug/m3", t_str,
            t_str, "openaq", "true", ""
        ])

    # Edge Case C: Negative value < 0.0
    for i in range(5):
        t_str = f"2026-11-21T{i+10:02d}:30:00Z"
        raw_rows.append([
            f"neg_{i}", "2180", "Peenya, Bengaluru - CPCB", "13.0274", "77.5186",
            "3921", "pm25", "-5.5", "ug/m3", t_str,
            t_str, "openaq", "true", ""
        ])

    # Edge Case D: Missing / empty value
    for i in range(5):
        t_str = f"2026-11-22T{i:02d}:30:00Z"
        raw_rows.append([
            f"null_{i}", "2181", "Hebbal, Bengaluru - CPCB", "13.0359", "77.5970",
            "3923", "pm25", "", "ug/m3", t_str,
            t_str, "openaq", "true", ""
        ])

    print(f"Total raw rows generated: {len(raw_rows)} (including header)")
    print(f"Valid clean records: {len(clean_tuples)}")

    # Write data/sample/sample_air_quality.csv & tests/fixtures/sample_air_quality.csv
    Path("data/sample").mkdir(parents=True, exist_ok=True)
    Path("tests/fixtures/golden").mkdir(parents=True, exist_ok=True)

    for out_path in [Path("data/sample/sample_air_quality.csv"), Path("tests/fixtures/sample_air_quality.csv")]:
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerows(raw_rows)
        print(f"Wrote {out_path} ({len(raw_rows)} lines)")

    # Compute golden_clean_air_quality.csv:
    # Filter header, is_valid==true, value not empty, 0 <= value <= 1000, and DISTINCT
    seen = set()
    distinct_clean: List[List[str]] = []
    for tup in clean_tuples:
        if tup not in seen:
            seen.add(tup)
            distinct_clean.append(list(tup))

    distinct_clean.sort(key=lambda r: (r[1], r[6], r[9]))  # station_id, parameter, measurement_time_utc

    golden_clean_path = Path("tests/fixtures/golden/golden_clean_air_quality.csv")
    with open(golden_clean_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerows(distinct_clean)
    print(f"Wrote {golden_clean_path} ({len(distinct_clean)} lines)")

    # Compute golden_station_stats.csv:
    # Group by (station_id, parameter, date_dt) where date_dt = measurement_time_utc[:10]
    # stats: count, round(avg, 4), min, max
    station_groups: Dict[Tuple[str, str, str], List[float]] = {}
    for r in distinct_clean:
        st_id = r[1]
        param = r[6]
        val = float(r[7])
        dt = r[9][:10]
        key = (st_id, param, dt)
        if key not in station_groups:
            station_groups[key] = []
        station_groups[key].append(val)

    station_stats_rows: List[List[str]] = []
    for key in sorted(station_groups.keys()):
        st_id, param, dt = key
        vals = station_groups[key]
        count = len(vals)
        avg_val = round(sum(vals) / count, 4)
        min_val = min(vals)
        max_val = max(vals)
        station_stats_rows.append([st_id, param, dt, str(count), str(avg_val), str(min_val), str(max_val)])

    golden_stats_path = Path("tests/fixtures/golden/golden_station_stats.csv")
    with open(golden_stats_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerows(station_stats_rows)
    print(f"Wrote {golden_stats_path} ({len(station_stats_rows)} lines)")

    # Compute golden_hourly_rollup.csv:
    # Group by (station_id, parameter, bucket_start_utc) where bucket_start_utc = measurement_time_utc[:13] + ":00:00Z"
    hourly_groups: Dict[Tuple[str, str, str], List[float]] = {}
    for r in distinct_clean:
        st_id = r[1]
        param = r[6]
        val = float(r[7])
        bucket = r[9][:13] + ":00:00Z"
        key = (st_id, param, bucket)
        if key not in hourly_groups:
            hourly_groups[key] = []
        hourly_groups[key].append(val)

    hourly_rollup_rows: List[List[str]] = []
    for key in sorted(hourly_groups.keys()):
        st_id, param, bucket = key
        vals = hourly_groups[key]
        count = len(vals)
        avg_val = round(sum(vals) / count, 4)
        min_val = min(vals)
        max_val = max(vals)
        hourly_rollup_rows.append([st_id, param, bucket, str(count), str(avg_val), str(min_val), str(max_val)])

    golden_hourly_path = Path("tests/fixtures/golden/golden_hourly_rollup.csv")
    with open(golden_hourly_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerows(hourly_rollup_rows)
    print(f"Wrote {golden_hourly_path} ({len(hourly_rollup_rows)} lines)")

    # Golden sidecar schema
    sidecar_path = generate_sidecar_schema("clean_air_quality", "tests/fixtures/golden")
    golden_schema_path = Path("tests/fixtures/golden/golden_clean_air_quality_schema.json")
    shutil.copy(sidecar_path, golden_schema_path)
    Path(sidecar_path).unlink(missing_ok=True)
    print(f"Wrote {golden_schema_path}")

if __name__ == "__main__":
    main()
