"""Unit tests for SkyWatch REST API layer and static JSON export."""
from __future__ import annotations

import json
from pathlib import Path

from api.index import app
from skywatch.spark.export import compute_station_freshness, export_static_api_endpoints


def test_compute_station_freshness():
    stations = [
        {"station_id": "2178", "name": "BTM", "latitude": 12.9, "longitude": 77.6, "sensors": {"pm25": {"id": "1"}}},
        {"station_id": "9999", "name": "Missing Station", "latitude": 13.0, "longitude": 77.5},
    ]
    clean_records = [
        {"station_id": "2178", "measurement_time_utc": "2026-11-23T18:00:00Z", "parameter": "pm25", "value": 45.0},
        {"station_id": "2178", "measurement_time_utc": "2026-11-23T19:00:00Z", "parameter": "pm25", "value": 50.0},
    ]

    # Reference time 1h after latest measurement -> Fresh
    res_fresh = compute_station_freshness(stations, clean_records, reference_time_utc="2026-11-23T20:00:00Z")
    assert len(res_fresh) == 2
    st2178 = next(s for s in res_fresh if s["station_id"] == "2178")
    assert st2178["last_measurement_time_utc"] == "2026-11-23T19:00:00Z"
    assert st2178["freshness_status"] == "Fresh"

    st9999 = next(s for s in res_fresh if s["station_id"] == "9999")
    assert st9999["last_measurement_time_utc"] is None
    assert st9999["freshness_status"] == "Offline"

    # Reference time 10h after latest measurement -> Stale
    res_stale = compute_station_freshness(stations, clean_records, reference_time_utc="2026-11-24T05:00:00Z")
    st_stale = next(s for s in res_stale if s["station_id"] == "2178")
    assert st_stale["freshness_status"] == "Stale"

    # Reference time 30h after latest measurement -> Offline
    res_off = compute_station_freshness(stations, clean_records, reference_time_utc="2026-11-25T01:00:00Z")
    st_off = next(s for s in res_off if s["station_id"] == "2178")
    assert st_off["freshness_status"] == "Offline"


def test_export_static_api_endpoints(tmp_path: Path):
    pub_dir = tmp_path / "published"
    pub_dir.mkdir()

    # Create dummy cpcb_index and trends_hourly csv files
    cpcb_dir = pub_dir / "cpcb_index" / "csv"
    cpcb_dir.mkdir(parents=True)
    (cpcb_dir / "part-00000.csv").write_text(
        "station_id,station_name,window_end_utc,pm25_24h_avg,pm25_subindex,pm10_24h_avg,pm10_subindex,aqi_value,aqi_category,dominant_pollutant,is_partial,suppression_reason\n"
        "2178,BTM,2026-11-23T18:00:00Z,65.0,115.0,120.0,110.0,115.0,Moderate,pm25,false,\n",
        encoding="utf-8",
    )

    trends_dir = pub_dir / "trends_hourly" / "csv"
    trends_dir.mkdir(parents=True)
    (trends_dir / "part-00000.csv").write_text(
        "station_id,parameter,hour_of_day,average_value,reading_count,is_peak_hour\n"
        "2178,pm25,3,45.2,12,true\n",
        encoding="utf-8",
    )

    api_out = export_static_api_endpoints(
        base_output_dir=pub_dir,
        run_id="test_run_123",
        profile_name="modern",
        engine_name="APACHE_SPARK",
        generated_at_utc="2026-11-23T19:00:00Z",
        clean_records=[{"station_id": "2178", "measurement_time_utc": "2026-11-23T18:00:00Z"}],
    )

    assert api_out.is_dir()
    meta_file = api_out / "meta.json"
    assert meta_file.is_file()
    with open(meta_file, "r", encoding="utf-8") as f:
        meta = json.load(f)
    assert meta["run_id"] == "test_run_123"
    assert meta["engine"] == "APACHE_SPARK"
    assert meta["is_simulation"] is False

    stations_file = api_out / "stations.json"
    assert stations_file.is_file()
    with open(stations_file, "r", encoding="utf-8") as f:
        stations = json.load(f)
    assert isinstance(stations, list)
    if stations:
        assert "last_measurement_time_utc" in stations[0]
        assert "freshness_status" in stations[0]

    trends_file = api_out / "trends_hourly.json"
    assert trends_file.is_file()
    with open(trends_file, "r", encoding="utf-8") as f:
        trends = json.load(f)
    assert len(trends) == 1
    assert trends[0]["hour_of_day"] == 3
    assert trends[0]["ist_time_range"] == "08:30 - 09:30 IST"


def test_api_wsgi_router():
    captured_status = []
    captured_headers = []

    def start_response(status, headers):
        captured_status.append(status)
        captured_headers.extend(headers)

    # 1. /api/v1/meta
    res_bytes = app({"PATH_INFO": "/api/v1/meta", "REQUEST_METHOD": "GET"}, start_response)
    assert captured_status[-1] == "200 OK"
    meta = json.loads(b"".join(res_bytes).decode("utf-8"))
    assert "run_id" in meta
    assert "engine" in meta

    # 2. Unknown route -> 404
    app({"PATH_INFO": "/api/v1/unknown", "REQUEST_METHOD": "GET"}, start_response)
    assert captured_status[-1] == "404 Not Found"
