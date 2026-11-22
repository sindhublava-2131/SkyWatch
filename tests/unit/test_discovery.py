"""Unit tests for station discovery module."""
from __future__ import annotations

import json
from unittest import mock

from skywatch.collector.discovery import discover_stations
from skywatch.collector.openaq_client import OpenAQClient


def test_discover_stations_offline(tmp_path):
    client = OpenAQClient(offline=True)
    out_file = tmp_path / "stations.json"

    stations = discover_stations(client, output_file=str(out_file))

    assert len(stations) >= 5
    assert out_file.is_file()

    with open(out_file, "r", encoding="utf-8") as f:
        loaded = json.load(f)
    assert len(loaded) == len(stations)

    # Validate schema of discovered stations
    station = loaded[0]
    assert "station_id" in station
    assert "name" in station
    assert "latitude" in station
    assert "longitude" in station
    assert "sensors" in station
    assert "pm25" in station["sensors"] or "pm10" in station["sensors"]


def test_discover_stations_filter_missing_pm():
    mock_client = mock.MagicMock(spec=OpenAQClient)
    mock_client.offline = False
    mock_client.get_locations.return_value = {
        "results": [
            {
                "id": 100,
                "name": "Station With No PM",
                "coordinates": {"latitude": 12.9, "longitude": 77.6},
                "sensors": [{"id": 999, "parameter": {"name": "co"}}],
            },
            {
                "id": 101,
                "name": "Station With PM2.5",
                "coordinates": {"latitude": 12.95, "longitude": 77.65},
                "sensors": [{"id": 888, "parameter": {"name": "pm25"}}],
            },
        ]
    }

    stations = discover_stations(mock_client, output_file=None)
    assert len(stations) == 1
    assert stations[0]["station_id"] == "101"
    assert "pm25" in stations[0]["sensors"]
