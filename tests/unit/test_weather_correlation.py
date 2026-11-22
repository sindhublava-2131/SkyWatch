"""Unit tests for Pearson correlation and confidence tier calculation."""
from __future__ import annotations

from skywatch.spark.weather_analysis import calculate_pearson_r


def test_calculate_pearson_r_perfect_correlation():
    x = [10.0, 20.0, 30.0, 40.0, 50.0]
    y = [2.0, 4.0, 6.0, 8.0, 10.0]
    r, n, tier = calculate_pearson_r(x, y)
    assert r == 1.0
    assert n == 5
    assert tier == "Low"


def test_calculate_pearson_r_negative_correlation():
    x = [1.0, 2.0, 3.0, 4.0, 5.0]
    y = [10.0, 8.0, 6.0, 4.0, 2.0]
    r, n, tier = calculate_pearson_r(x, y)
    assert r == -1.0
    assert n == 5
    assert tier == "Low"


def test_calculate_pearson_r_confidence_tiers():
    # n = 120 -> High tier
    x_120 = [float(i) for i in range(120)]
    y_120 = [float(i) * 0.5 + 2.0 for i in range(120)]
    r_high, n_high, tier_high = calculate_pearson_r(x_120, y_120)
    assert r_high == 1.0
    assert n_high == 120
    assert tier_high == "High"

    # n = 45 -> Medium tier
    x_45 = [float(i) for i in range(45)]
    y_45 = [float(i) * 1.5 for i in range(45)]
    r_med, n_med, tier_med = calculate_pearson_r(x_45, y_45)
    assert r_med == 1.0
    assert n_med == 45
    assert tier_med == "Medium"

    # n = 15 -> Low tier
    x_15 = [float(i) for i in range(15)]
    y_15 = [float(i) for i in range(15)]
    r_low, n_low, tier_low = calculate_pearson_r(x_15, y_15)
    assert r_low == 1.0
    assert n_low == 15
    assert tier_low == "Low"


def test_calculate_pearson_r_zero_variance_and_insufficient_samples():
    # Zero variance in y
    x = [1.0, 2.0, 3.0, 4.0]
    y_flat = [5.0, 5.0, 5.0, 5.0]
    r, n, tier = calculate_pearson_r(x, y_flat)
    assert r is None
    assert n == 4
    assert tier == "Low"

    # Single sample
    r1, n1, tier1 = calculate_pearson_r([10.0], [20.0])
    assert r1 is None
    assert n1 == 1
    assert tier1 == "Low"

    # Empty list
    r0, n0, tier0 = calculate_pearson_r([], [])
    assert r0 is None
    assert n0 == 0
    assert tier0 == "Low"
