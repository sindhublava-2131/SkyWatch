"""Unit tests for station freshness classification across exact 1-second boundaries."""
from __future__ import annotations

from datetime import datetime, timezone

from skywatch.analytics_core.freshness import freshness_status


def test_ut20_freshness_boundaries():
    ref_str = "2026-11-23T12:00:00Z"

    # Exactly 3 hours (10,800 seconds) -> Fresh
    dt_3h = "2026-11-23T09:00:00Z"
    assert freshness_status(dt_3h, ref_str) == "Fresh"

    # 3 hours and 1 second (10,801 seconds) -> Stale
    dt_3h_1s = "2026-11-23T08:59:59Z"
    assert freshness_status(dt_3h_1s, ref_str) == "Stale"

    # Exactly 24 hours (86,400 seconds) -> Stale
    dt_24h = "2026-11-22T12:00:00Z"
    assert freshness_status(dt_24h, ref_str) == "Stale"

    # 24 hours and 1 second (86,401 seconds) -> Offline
    dt_24h_1s = "2026-11-22T11:59:59Z"
    assert freshness_status(dt_24h_1s, ref_str) == "Offline"


def test_freshness_malformed_and_missing():
    # Empty string, None, corrupted string -> Offline
    assert freshness_status(None) == "Offline"
    assert freshness_status("") == "Offline"
    assert freshness_status("   ") == "Offline"
    assert freshness_status("invalid-date-string") == "Offline"


def test_freshness_future_and_zero_delta():
    ref_dt = datetime(2026, 11, 23, 12, 0, 0, tzinfo=timezone.utc)
    # 0 seconds delta
    assert freshness_status("2026-11-23T12:00:00Z", ref_dt) == "Fresh"
    # Future timestamp (clock skew)
    assert freshness_status("2026-11-23T13:00:00Z", ref_dt) == "Fresh"
