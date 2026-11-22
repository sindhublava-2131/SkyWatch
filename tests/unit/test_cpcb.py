"""Unit tests for CPCB NAQI calculation, linear interpolation, and classification."""
from __future__ import annotations

from skywatch.analytics_core.cpcb import (
    classify,
    is_valid_24h,
    load_breakpoints,
    overall_index,
    subindex,
)


def test_load_breakpoints():
    bp = load_breakpoints()
    assert bp["version"] == "2.0"
    assert "categories" in bp
    assert "breakpoints" in bp
    assert "pm25" in bp["breakpoints"]
    assert "pm10" in bp["breakpoints"]


def test_ut14_pm25_satisfactory():
    # UT-14: PM2.5 = 45.5 ug/m3 -> 75.5 (Satisfactory)
    idx = subindex("pm25", 45.5)
    assert idx == 75.5
    cat = classify(idx)
    assert cat == "Satisfactory"


def test_ut15_pm25_moderate():
    # UT-15: PM2.5 = 61.0 ug/m3 -> 101.0 (Moderate)
    idx = subindex("pm25", 61.0)
    assert idx == 101.0
    cat = classify(idx)
    assert cat == "Moderate"


def test_ut16_pm10_satisfactory_and_poor():
    # UT-16: PM10 = 100.0 ug/m3 -> 100.0 (Satisfactory)
    idx100 = subindex("pm10", 100.0)
    assert idx100 == 100.0
    assert classify(idx100) == "Satisfactory"

    # PM10 = 251.0 ug/m3 -> 201.0 (Poor)
    idx251 = subindex("pm10", 251.0)
    assert idx251 == 201.0
    assert classify(idx251) == "Poor"


def test_ut17_invalid_and_negative_concentrations():
    # UT-17: Negative, None, or invalid values return None
    assert subindex("pm25", None) is None
    assert subindex("pm25", -5.0) is None
    assert subindex("pm10", -0.1) is None
    assert subindex("unknown_pollutant", 45.0) is None
    assert classify(None) is None
    assert classify(-1.0) is None


def test_ut18_valid_hours_threshold():
    # UT-18: Valid hours threshold: >= 16 is True, < 16 is False
    assert is_valid_24h(16) is True
    assert is_valid_24h(24) is True
    assert is_valid_24h(17) is True
    assert is_valid_24h(15) is False
    assert is_valid_24h(0) is False
    assert is_valid_24h(12) is False


def test_ut19_partial_index():
    # UT-19: Partial index handling when only PM2.5 or PM10 is available
    aqi, cat, dominant, is_partial = overall_index(75.5, None)
    assert aqi == 75.5
    assert cat == "Satisfactory"
    assert dominant == "pm25"
    assert is_partial is True

    aqi, cat, dominant, is_partial = overall_index(None, 201.0)
    assert aqi == 201.0
    assert cat == "Poor"
    assert dominant == "pm10"
    assert is_partial is True

    aqi, cat, dominant, is_partial = overall_index(None, None)
    assert aqi is None
    assert cat is None
    assert dominant is None
    assert is_partial is False


def test_overall_index_dominant_pollutant():
    # When both present, max sub-index wins
    aqi, cat, dominant, is_partial = overall_index(150.0, 80.0)
    assert aqi == 150.0
    assert cat == "Moderate"
    assert dominant == "pm25"
    assert is_partial is False

    aqi, cat, dominant, is_partial = overall_index(90.0, 250.0)
    assert aqi == 250.0
    assert cat == "Poor"
    assert dominant == "pm10"
    assert is_partial is False


def test_extreme_and_boundary_concentrations():
    # Zero concentration
    idx0 = subindex("pm25", 0.0)
    assert idx0 == 0.0
    assert classify(idx0) == "Good"

    # Upper cap at 500
    idx_high = subindex("pm25", 650.0)
    assert idx_high == 500.0
    assert classify(idx_high) == "Severe"

    # Exact category boundaries
    assert classify(50.0) == "Good"
    assert classify(50.1) == "Satisfactory"
    assert classify(100.0) == "Satisfactory"
    assert classify(200.0) == "Moderate"
    assert classify(300.0) == "Poor"
    assert classify(400.0) == "Very Poor"
    assert classify(401.0) == "Severe"
    assert classify(500.0) == "Severe"
    assert classify(550.0) == "Severe"
