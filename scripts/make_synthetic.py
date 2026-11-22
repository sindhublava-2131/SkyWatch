"""Synthetic dataset generator for scale benchmarking with strict isolation tagging (SPK-10)."""
from __future__ import annotations

import argparse
import csv
import json
import math
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

SCALE_ROW_TARGETS: Dict[str, int] = {
    "1x": 50000,
    "10x": 500000,
    "100x": 5000000,
}


def load_active_stations(stations_file: Path = Path("config/stations.json")) -> List[Dict[str, Any]]:
    """Load active Bengaluru monitoring stations from configuration."""
    if not stations_file.is_file():
        # Fallback default stations if stations.json is missing
        return [
            {"id": "2178", "name": "BTM Layout, Bengaluru - CPCB", "latitude": 12.9135, "longitude": 77.6111, "sensors": [{"id": "3917", "parameter": "pm25"}, {"id": "3918", "parameter": "pm10"}]},
            {"id": "2179", "name": "Silk Board, Bengaluru - CPCB", "latitude": 12.9174, "longitude": 77.6228, "sensors": [{"id": "3919", "parameter": "pm25"}, {"id": "3920", "parameter": "pm10"}]},
            {"id": "2180", "name": "Peenya, Bengaluru - CPCB", "latitude": 13.0284, "longitude": 77.5195, "sensors": [{"id": "3921", "parameter": "pm25"}, {"id": "3922", "parameter": "pm10"}]},
            {"id": "2181", "name": "Hebbal, Bengaluru - CPCB", "latitude": 13.0382, "longitude": 77.5919, "sensors": [{"id": "3923", "parameter": "pm25"}, {"id": "3924", "parameter": "pm10"}]},
            {"id": "2182", "name": "Jayanagar, Bengaluru - CPCB", "latitude": 12.9299, "longitude": 77.5824, "sensors": [{"id": "3925", "parameter": "pm25"}, {"id": "3926", "parameter": "pm10"}]},
        ]

    with open(stations_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, list):
        return data
    return data.get("stations", [])


def generate_synthetic_dataset(
    scale: str,
    target_row_count: Optional[int] = None,
    output_dir: Optional[Path] = None,
    stations_file: Path = Path("config/stations.json"),
    seed: int = 42,
    batch_size: int = 5000,
) -> Path:
    """Generate realistic synthetic air quality dataset matching air_quality schema.

    Enforces strict synthetic isolation tagging:
        - record_key prefixed with 'syn_{scale}_'
        - source set to 'synthetic'
        - is_valid set to 'true'
        - isolated in data/synthetic/ to prevent civic reporting pollution.

    Args:
        scale: Scale tier ('1x', '10x', '100x').
        target_row_count: Explicit row count override.
        output_dir: Destination folder.
        stations_file: Path to config/stations.json.
        seed: Random seed for reproducibility.
        batch_size: Number of records per streaming write flush.

    Returns:
        Path to generated CSV file.
    """
    total_rows = target_row_count or SCALE_ROW_TARGETS.get(scale, 50000)
    out_folder = output_dir or Path(f"data/synthetic/scale_{scale}")
    out_folder.mkdir(parents=True, exist_ok=True)
    out_file = out_folder / f"synthetic_air_quality_{scale}.csv"

    stations = load_active_stations(stations_file)
    if not stations:
        raise ValueError("No stations available for synthetic generation")

    rng = random.Random(seed)
    base_dt = datetime(2026, 11, 1, 0, 0, 0, tzinfo=timezone.utc)

    fieldnames = [
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

    with open(out_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        batch: List[Dict[str, Any]] = []
        for idx in range(total_rows):
            station = stations[idx % len(stations)]
            st_id = str(station.get("station_id", station.get("id")))
            st_name = str(station.get("name", f"Station {st_id}"))
            lat = float(station.get("latitude", 12.9716))
            lon = float(station.get("longitude", 77.5946))

            # Alternate between pm25 and pm10
            is_pm25 = (idx % 2 == 0)
            param = "pm25" if is_pm25 else "pm10"
            sensors_obj = station.get("sensors", {})
            if isinstance(sensors_obj, dict) and param in sensors_obj:
                sensor_id = str(sensors_obj[param].get("sensor_id", f"{st_id}_{param}"))
            elif isinstance(sensors_obj, list) and sensors_obj:
                sensor_id = str(sensors_obj[0].get("id", f"{st_id}_{param}"))
            else:
                sensor_id = f"{st_id}_{param}"

            # Temporal simulation: increment hour every N rows
            hours_offset = idx // len(stations)
            meas_dt = base_dt + timedelta(hours=hours_offset)
            ingest_dt = meas_dt + timedelta(minutes=5)

            hour = meas_dt.hour
            # Diurnal curve: peaks in morning (08:00-10:00) and evening (19:00-22:00)
            diurnal_mult = 1.0 + 0.4 * math.sin(2.0 * math.pi * (hour - 3) / 24.0)

            if is_pm25:
                base_conc = 35.0
                noise = rng.gauss(0, 5)
            else:
                base_conc = 75.0
                noise = rng.gauss(0, 10)

            conc = max(1.0, round(base_conc * diurnal_mult + noise, 1))

            record_key = f"syn_{scale}_{idx:08d}"

            batch.append({
                "record_key": record_key,
                "station_id": st_id,
                "station_name": st_name,
                "latitude": lat,
                "longitude": lon,
                "sensor_id": sensor_id,
                "parameter": param,
                "value": conc,
                "unit": "ug/m3",
                "measurement_time_utc": meas_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "ingestion_time_utc": ingest_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "source": "synthetic",
                "is_valid": "true",
                "reject_reason": "",
            })

            if len(batch) >= batch_size:
                writer.writerows(batch)
                batch.clear()

        if batch:
            writer.writerows(batch)
            batch.clear()

    return out_file


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic air quality datasets for scale benchmarking")
    parser.add_argument("--scale", default="1x", choices=["1x", "10x", "100x", "all"], help="Benchmark scale target")
    parser.add_argument("--rows", type=int, default=None, help="Explicit row count override")
    parser.add_argument("--output-dir", default=None, help="Target output folder")
    parser.add_argument("--seed", type=int, default=42, help="Random generator seed")

    args = parser.parse_args()
    scales = ["1x", "10x", "100x"] if args.scale == "all" else [args.scale]

    for s in scales:
        target_rows = args.rows or SCALE_ROW_TARGETS[s]
        out_path = args.output_dir if args.output_dir else Path(f"data/synthetic/scale_{s}")
        print(f"Generating synthetic dataset scale={s} (rows={target_rows})...")
        csv_file = generate_synthetic_dataset(
            scale=s,
            target_row_count=target_rows,
            output_dir=Path(out_path),
            seed=args.seed,
        )
        print(f"Successfully generated: {csv_file}")


if __name__ == "__main__":
    main()
