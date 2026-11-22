"""Station freshness evaluation based on last measurement timestamp."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional, Union


def freshness_status(
    last_measurement_utc: Optional[str],
    reference_time_utc: Optional[Union[str, datetime]] = None,
    fresh_hours: float = 3.0,
    stale_hours: float = 24.0,
) -> str:
    """Classify station freshness status into 'Fresh', 'Stale', or 'Offline'.

    Rules:
        - delta_hours <= fresh_hours (<=3h): "Fresh"
        - fresh_hours < delta_hours <= stale_hours (3-24h): "Stale"
        - delta_hours > stale_hours (>24h): "Offline"
        - Missing, empty, or unparseable timestamp: "Offline"

    Args:
        last_measurement_utc: ISO 8601 UTC timestamp string of latest reading.
        reference_time_utc: Optional reference timestamp (defaults to current UTC time).
        fresh_hours: Maximum elapsed hours to consider a station Fresh (default 3.0).
        stale_hours: Maximum elapsed hours to consider a station Stale (default 24.0).

    Returns:
        One of 'Fresh', 'Stale', 'Offline'.
    """
    if not last_measurement_utc or not isinstance(last_measurement_utc, str):
        return "Offline"

    cleaned_str = last_measurement_utc.strip()
    if not cleaned_str:
        return "Offline"

    try:
        # Handle 'Z' suffix for Python 3.8-3.10 datetime.fromisoformat
        iso_str = cleaned_str
        if iso_str.endswith("Z"):
            iso_str = iso_str[:-1] + "+00:00"
        last_dt = datetime.fromisoformat(iso_str)
        if last_dt.tzinfo is None:
            last_dt = last_dt.replace(tzinfo=timezone.utc)
    except Exception:
        return "Offline"

    if reference_time_utc is None:
        ref_dt = datetime.now(timezone.utc)
    elif isinstance(reference_time_utc, datetime):
        ref_dt = reference_time_utc
        if ref_dt.tzinfo is None:
            ref_dt = ref_dt.replace(tzinfo=timezone.utc)
    elif isinstance(reference_time_utc, str):
        try:
            ref_iso = reference_time_utc.strip()
            if ref_iso.endswith("Z"):
                ref_iso = ref_iso[:-1] + "+00:00"
            ref_dt = datetime.fromisoformat(ref_iso)
            if ref_dt.tzinfo is None:
                ref_dt = ref_dt.replace(tzinfo=timezone.utc)
        except Exception:
            return "Offline"
    else:
        return "Offline"

    delta_seconds = (ref_dt - last_dt).total_seconds()
    if delta_seconds < 0.0:
        # Timestamp in the future or clock skew; treat as Fresh
        return "Fresh"

    fresh_seconds = fresh_hours * 3600.0
    stale_seconds = stale_hours * 3600.0

    if delta_seconds <= fresh_seconds:
        return "Fresh"
    elif delta_seconds <= stale_seconds:
        return "Stale"
    else:
        return "Offline"
