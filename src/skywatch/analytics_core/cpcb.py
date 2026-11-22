"""CPCB National Air Quality Index (NAQI) sub-index calculation and classification."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

_CACHED_BREAKPOINTS: Optional[Dict[str, Any]] = None


def load_breakpoints(config_path: Optional[str] = None) -> Dict[str, Any]:
    """Load and cache CPCB NAQI breakpoint definitions from breakpoints.yaml."""
    global _CACHED_BREAKPOINTS
    if config_path is None and _CACHED_BREAKPOINTS is not None:
        return _CACHED_BREAKPOINTS

    bp_path = Path(config_path) if config_path else Path("config/breakpoints.yaml")
    if not bp_path.is_file():
        raise FileNotFoundError(f"Breakpoints configuration file not found: {bp_path}")

    with open(bp_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    if config_path is None:
        _CACHED_BREAKPOINTS = data
    return data


def subindex(
    parameter: str,
    concentration: Optional[float],
    breakpoints: Optional[Dict[str, Any]] = None,
) -> Optional[float]:
    """Calculate CPCB sub-index for a given pollutant concentration using linear interpolation.

    Formula:
        I = I_low + ((I_high - I_low) / (B_high - B_low)) * (C - B_low)

    Args:
        parameter: Pollutant parameter name (e.g. 'pm25', 'pm10').
        concentration: 24h rolling average concentration in ug/m3.
        breakpoints: Optional breakpoint configuration dictionary.

    Returns:
        Rounded sub-index (1 decimal place) or None if invalid.
    """
    if concentration is None:
        return None
    try:
        c = float(concentration)
    except (ValueError, TypeError):
        return None

    if c < 0.0:
        return None

    if breakpoints is None:
        breakpoints = load_breakpoints()

    param_key = parameter.lower().replace(".", "").replace("-", "")
    bp_dict = breakpoints.get("breakpoints", {})
    if param_key not in bp_dict:
        return None

    bands: List[Dict[str, Any]] = bp_dict[param_key]
    if not bands:
        return None

    # Check upper ceiling
    max_b_high = float(bands[-1]["b_high"])
    if c > max_b_high:
        return 500.0

    # Locate matching band with continuous upper boundaries
    matched_band: Optional[Dict[str, Any]] = None
    for band in bands:
        b_high = float(band["b_high"])
        if c <= b_high:
            matched_band = band
            break

    if matched_band is None:
        matched_band = bands[-1]

    b_low = float(matched_band["b_low"])
    b_high = float(matched_band["b_high"])
    i_low = float(matched_band["i_low"])
    i_high = float(matched_band["i_high"])

    if b_high <= b_low:
        return i_low

    # Handle boundary transitions between integer gaps (e.g. 30.5 between 30 and 31)
    effective_c = c
    if effective_c < b_low:
        effective_c = b_low

    idx = i_low + ((i_high - i_low) / (b_high - b_low)) * (effective_c - b_low)
    idx = max(0.0, min(500.0, idx))
    return round(idx, 1)


def classify(
    index_value: Optional[float],
    breakpoints: Optional[Dict[str, Any]] = None,
) -> Optional[str]:
    """Classify an AQI sub-index or overall AQI value into official CPCB category.

    Categories:
        - 0-50: Good
        - 51-100: Satisfactory
        - 101-200: Moderate
        - 201-300: Poor
        - 301-400: Very Poor
        - 401-500: Severe
        - >500: Severe
    """
    if index_value is None:
        return None
    try:
        val = float(index_value)
    except (ValueError, TypeError):
        return None

    if val < 0.0:
        return None

    if val > 500.0:
        return "Severe"

    bp = breakpoints if breakpoints is not None else load_breakpoints()
    try:
        categories = bp.get("categories", [])
        for cat in categories:
            i_min = float(cat["index_min"])
            i_max = float(cat["index_max"])
            # Continuous boundary match across fractional gaps
            if i_min <= val <= i_max or (val < i_min and val >= i_min - 1.0):
                return str(cat["name"])
    except Exception:
        pass

    # Standard fallback intervals
    if val <= 50.0:
        return "Good"
    elif val <= 100.0:
        return "Satisfactory"
    elif val <= 200.0:
        return "Moderate"
    elif val <= 300.0:
        return "Poor"
    elif val <= 400.0:
        return "Very Poor"
    else:
        return "Severe"


def is_valid_24h(valid_hours: int, minimum: int = 16) -> bool:
    """Check if the count of valid hourly readings meets the >=16h CPCB rule."""
    return int(valid_hours) >= int(minimum)


def overall_index(
    pm25_sub: Optional[float],
    pm10_sub: Optional[float],
    breakpoints: Optional[Dict[str, Any]] = None,
) -> Tuple[Optional[float], Optional[str], Optional[str], bool]:
    """Calculate overall AQI from PM2.5 and PM10 sub-indices using the max sub-index rule.

    Returns:
        (aqi_value, aqi_category, dominant_pollutant, is_partial)
    """
    if pm25_sub is None and pm10_sub is None:
        return None, None, None, False

    if pm25_sub is not None and pm10_sub is None:
        cat = classify(pm25_sub, breakpoints)
        return pm25_sub, cat, "pm25", True

    if pm25_sub is None and pm10_sub is not None:
        cat = classify(pm10_sub, breakpoints)
        return pm10_sub, cat, "pm10", True

    # Both present
    assert pm25_sub is not None and pm10_sub is not None
    if pm25_sub >= pm10_sub:
        aqi = pm25_sub
        dominant = "pm25"
    else:
        aqi = pm10_sub
        dominant = "pm10"

    cat = classify(aqi, breakpoints)
    return aqi, cat, dominant, False
