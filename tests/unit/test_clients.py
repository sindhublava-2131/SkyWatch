"""Unit tests for OpenAQ and Weather API clients."""
from __future__ import annotations

import pytest
import responses

from skywatch.collector.openaq_client import AuthenticationError, OpenAQClient
from skywatch.collector.weather_client import WeatherClient


def test_client_repr_redaction():
    client = OpenAQClient(api_key="supersecret123", timeout=15)
    repr_str = repr(client)
    assert "supersecret123" not in repr_str
    assert "timeout=15" in repr_str


def test_openaq_offline_mode():
    client = OpenAQClient(offline=True)
    locations = client.get_locations(bbox="77.45,12.83,77.75,13.12")
    assert "results" in locations
    assert len(locations["results"]) >= 5

    latest = client.get_latest(2178)
    assert "results" in latest

    measurements = client.get_measurements(3917, "2026-11-20T00:00:00Z", "2026-11-21T00:00:00Z")
    assert "results" in measurements


def test_weather_offline_mode():
    client = WeatherClient(offline=True)
    forecast = client.get_forecast(12.91, 77.61)
    assert "hourly" in forecast
    assert "temperature_2m" in forecast["hourly"]
    assert len(forecast["hourly"]["time"]) > 0

    archive = client.get_archive(12.91, 77.61, "2026-11-15", "2026-11-16")
    assert "hourly" in archive


@responses.activate
def test_openaq_authentication_failure():
    client = OpenAQClient(api_key="bad_token", max_retries=2)
    responses.add(
        responses.GET,
        "https://api.openaq.org/v3/locations",
        json={"detail": "Unauthorized"},
        status=401,
    )

    with pytest.raises(AuthenticationError) as exc_info:
        client.get_locations("77.45,12.83,77.75,13.12")
    assert "HTTP 401" in str(exc_info.value)


@responses.activate
def test_openaq_rate_limiting_retry():
    client = OpenAQClient(max_retries=3)
    # First response: 429 with Retry-After: 0
    responses.add(
        responses.GET,
        "https://api.openaq.org/v3/locations",
        headers={"Retry-After": "0"},
        status=429,
    )
    # Second response: 200 OK
    responses.add(
        responses.GET,
        "https://api.openaq.org/v3/locations",
        json={"results": [{"id": 1, "name": "Test Station"}]},
        status=200,
    )

    result = client.get_locations("77.45,12.83,77.75,13.12")
    assert len(result["results"]) == 1
    assert result["results"][0]["name"] == "Test Station"
