"""Unit tests for record deduplication."""
from __future__ import annotations

from skywatch.data.deduplication import (
    compute_air_quality_key,
    compute_weather_key,
    deduplicate_batch,
)


def test_compute_air_quality_key():
    key1 = compute_air_quality_key("3917", "pm25", "2026-11-22T08:00:00Z")
    key2 = compute_air_quality_key("3917", "PM25", "2026-11-22T08:00:00Z")
    key3 = compute_air_quality_key("3918", "pm25", "2026-11-22T08:00:00Z")

    assert len(key1) == 64
    assert key1 == key2  # Case insensitive parameter normalization
    assert key1 != key3  # Different sensor


def test_compute_weather_key():
    key1 = compute_weather_key("bengaluru_city", "2026-11-22T08:00:00Z")
    key2 = compute_weather_key("bengaluru_city", "2026-11-22T08:00:00Z")
    key3 = compute_weather_key("bengaluru_city", "2026-11-22T09:00:00Z")

    assert len(key1) == 64
    assert key1 == key2
    assert key1 != key3


def test_deduplicate_batch():
    records = [
        {"record_key": "k1", "val": 1},
        {"record_key": "k2", "val": 2},
        {"record_key": "k1", "val": 1},  # duplicate
        {"record_key": "k3", "val": 3},
        {"record_key": "k2", "val": 2},  # duplicate
    ]

    unique, dup_count = deduplicate_batch(records)
    assert dup_count == 2
    assert len(unique) == 3
    assert [r["record_key"] for r in unique] == ["k1", "k2", "k3"]
