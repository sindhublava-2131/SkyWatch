"""Station discovery module for OpenAQ monitoring locations in Bengaluru."""
from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from skywatch.collector.openaq_client import OpenAQClient
from skywatch.pipeline.logging import get_logger

logger = get_logger("discovery")

# Bengaluru bounding box: [min_lon, min_lat, max_lon, max_lat]
BENGALURU_BBOX = "77.45,12.83,77.75,13.12"


def discover_stations(
    client: OpenAQClient,
    max_age_hours: int = 72,
    output_file: Optional[str] = "config/stations.json",
) -> List[Dict[str, Any]]:
    """Discover active air quality stations in Bengaluru with PM2.5 or PM10 sensors.

    Filters stations by coordinates, PM parameter availability, and recent activity timestamp.
    Atomically writes output to config/stations.json if specified.
    """
    logger.info(f"Discovering stations in Bengaluru bbox: {BENGALURU_BBOX}")
    payload = client.get_locations(bbox=BENGALURU_BBOX, limit=100)
    raw_locations = payload.get("results", [])

    discovered: List[Dict[str, Any]] = []
    now_utc = datetime.now(timezone.utc)

    for loc in raw_locations:
        loc_id = str(loc.get("id"))
        name = loc.get("name", "Unknown Station")
        coords = loc.get("coordinates") or {}
        lat = coords.get("latitude")
        lon = coords.get("longitude")

        if lat is None or lon is None:
            continue

        # Extract PM sensors
        pm_sensors: Dict[str, Dict[str, str]] = {}
        for sensor in loc.get("sensors", []):
            sensor_name = (sensor.get("name") or "").lower()
            param_obj = sensor.get("parameter") or {}
            param_name = (param_obj.get("name") or sensor_name).lower()
            sensor_id = str(sensor.get("id"))

            if "pm25" in param_name or param_name == "pm2.5":
                pm_sensors["pm25"] = {"sensor_id": sensor_id, "units": "ug/m3"}
            elif "pm10" in param_name:
                pm_sensors["pm10"] = {"sensor_id": sensor_id, "units": "ug/m3"}

        # Require at least one PM parameter
        if not pm_sensors:
            continue

        # Check recency if required and not running in offline mode
        dt_last = loc.get("datetimeLast", {})
        last_utc_str = dt_last.get("utc") if isinstance(dt_last, dict) else None

        if max_age_hours > 0 and not client.offline and last_utc_str:
            try:
                last_dt = datetime.fromisoformat(last_utc_str.replace("Z", "+00:00"))
                age_hours = (now_utc - last_dt).total_seconds() / 3600.0
                if age_hours > max_age_hours:
                    continue
            except (ValueError, TypeError):
                pass

        discovered.append(
            {
                "station_id": loc_id,
                "name": name,
                "latitude": float(lat),
                "longitude": float(lon),
                "last_active_utc": last_utc_str or now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "sensors": pm_sensors,
            }
        )

    logger.info(f"Discovered {len(discovered)} active stations with PM sensors")

    if output_file:
        out_path = Path(output_file)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        # Atomic write via temp file
        with tempfile.NamedTemporaryFile("w", dir=str(out_path.parent), delete=False, encoding="utf-8") as tf:
            json.dump(discovered, tf, indent=2)
            temp_name = tf.name
        os.replace(temp_name, str(out_path))
        logger.info(f"Station list committed to {out_path}")

    return discovered
