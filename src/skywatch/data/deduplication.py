"""Cryptographic deduplication for environmental observation records.

Implements HYG-03: Deduplicate records on composite natural keys via SHA-256 hashes.
"""
from __future__ import annotations

import hashlib
from typing import Any, Dict, List, Set, Tuple


def compute_air_quality_key(
    sensor_id: str,
    parameter: str,
    measurement_time_utc: str,
) -> str:
    """Generate deterministic SHA-256 record key for air quality observation.

    Composite key format: "{sensor_id}|{parameter}|{measurement_time_utc}"
    """
    composite = f"{sensor_id}|{parameter.lower()}|{measurement_time_utc}"
    return hashlib.sha256(composite.encode("utf-8")).hexdigest()


def compute_weather_key(
    location_id: str,
    measurement_time_utc: str,
) -> str:
    """Generate deterministic SHA-256 record key for weather observation.

    Composite key format: "{location_id}|{measurement_time_utc}"
    """
    composite = f"{location_id}|{measurement_time_utc}"
    return hashlib.sha256(composite.encode("utf-8")).hexdigest()


def deduplicate_batch(
    records: List[Dict[str, Any]],
    key_field: str = "record_key",
) -> Tuple[List[Dict[str, Any]], int]:
    """Filter duplicate records from an in-memory batch while preserving original order.

    Returns:
        Tuple of (deduplicated_records_list, duplicate_count)
    """
    seen: Set[str] = set()
    unique_records: List[Dict[str, Any]] = []
    duplicate_count = 0

    for rec in records:
        key = rec.get(key_field)
        if not key:
            # Fallback if key_field is missing or None
            unique_records.append(rec)
            continue

        if key in seen:
            duplicate_count += 1
        else:
            seen.add(key)
            unique_records.append(rec)

    return unique_records, duplicate_count
