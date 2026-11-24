"""Full-stack end-to-end integration test suite (OPS-01, OPS-05, OPS-06, OPS-07).

Validates complete analytical flow from raw inputs through Pig ETL, Spark analytics,
static API lake export, and REST API WSGI routing against frontend contracts.
"""
from __future__ import annotations

import io
import json
from pathlib import Path
from typing import Any, Dict, List, Tuple

from api.index import app
from skywatch.pipeline.pipeline_runner import run_pipeline


def _make_wsgi_request(environ_overrides: Dict[str, Any]) -> Tuple[int, Dict[str, str], Any]:
    """Helper to dispatch requests directly to api/index.py WSGI application."""
    status_code = [200]
    headers_dict = {}

    def start_response(status: str, headers: List[Tuple[str, str]]):
        status_code[0] = int(status.split()[0])
        headers_dict.update(dict(headers))

    environ = {
        "REQUEST_METHOD": "GET",
        "PATH_INFO": "/api/v1/meta",
        "QUERY_STRING": "",
        "SERVER_NAME": "localhost",
        "SERVER_PORT": "80",
        "wsgi.version": (1, 0),
        "wsgi.url_scheme": "http",
        "wsgi.input": io.BytesIO(b""),
        "wsgi.errors": io.StringIO(),
        "wsgi.multithread": False,
        "wsgi.multiprocess": False,
        "wsgi.run_once": False,
    }
    environ.update(environ_overrides)

    response_iter = app(environ, start_response)
    response_body = b"".join(response_iter)
    parsed = json.loads(response_body.decode("utf-8"))
    return status_code[0], headers_dict, parsed


def test_full_stack_e2e_pipeline_and_api(tmp_path: Path, monkeypatch):
    """Verify complete end-to-end data pipeline from raw fixtures to REST API JSON delivery."""
    clean_dir = tmp_path / "clean"
    published_dir = tmp_path / "published"
    air_input = "tests/fixtures/sample_air_quality.csv"
    weather_input = "tests/fixtures/sample_weather_normalized.csv"

    # Step 1-4: Run end-to-end multi-stage pipeline
    summary = run_pipeline(
        profile_name="modern",
        offline=True,
        simulate=True,
        require_real_engine=False,
        clean_dir=clean_dir,
        published_dir=published_dir,
        sample_air_input=air_input,
        sample_weather_input=weather_input,
        run_id="full_stack_e2e_run",
    )

    assert summary.success is True, f"Pipeline run failed: {summary}"
    assert len(summary.stages) == 4
    for stage in summary.stages:
        assert stage.success is True, f"Stage {stage.stage_name} failed: {stage.error_message}"

    api_dir = published_dir / "api" / "v1"
    assert api_dir.is_dir()

    # Step 5: Assert all 9 JSON endpoints are generated and conform to schema contracts
    endpoints = [
        "meta.json",
        "stations.json",
        "cpcb_index.json",
        "station_statistics.json",
        "trends_hourly.json",
        "trends_daily.json",
        "weather_correlation.json",
        "alerts.json",
        "quality_report.json",
    ]

    for ep in endpoints:
        ep_file = api_dir / ep
        assert ep_file.is_file(), f"Missing static lake endpoint: {ep}"
        with open(ep_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        assert data is not None, f"Endpoint {ep} is null"

    # Specific field validations matching frontend/lib/schemas.ts
    with open(api_dir / "meta.json", "r", encoding="utf-8") as f:
        meta = json.load(f)
    assert meta["run_id"] == "full_stack_e2e_run"
    assert "profile" in meta
    assert "engine" in meta
    assert "is_simulation" in meta
    assert isinstance(meta["is_simulation"], bool)

    with open(api_dir / "stations.json", "r", encoding="utf-8") as f:
        stations = json.load(f)
    assert len(stations) > 0
    st0 = stations[0]
    assert "station_id" in st0 or "id" in st0
    assert "name" in st0
    assert "freshness_status" in st0
    assert st0["freshness_status"].lower() in ("fresh", "stale", "offline")
    assert "last_measurement_time_utc" in st0

    with open(api_dir / "trends_hourly.json", "r", encoding="utf-8") as f:
        trends = json.load(f)
    assert len(trends) > 0
    t0 = trends[0]
    assert "hour_of_day" in t0
    assert "hour_of_day_utc" in t0
    assert "ist_time_range" in t0
    assert "ist_label" in t0
    assert "IST" in t0["ist_time_range"]

    with open(api_dir / "cpcb_index.json", "r", encoding="utf-8") as f:
        cpcb = json.load(f)
    assert len(cpcb) > 0
    c0 = cpcb[0]
    assert "aqi_value" in c0
    assert "aqi_category" in c0
    assert "dominant_pollutant" in c0

    # Step 6: Verify REST API routing via WSGI application
    monkeypatch.setenv("SKYWATCH_PUBLISHED_DIR", str(published_dir))

    test_routes = [
        ("/api/v1/meta", lambda d: d["run_id"] == "full_stack_e2e_run"),
        ("/api/v1/stations", lambda d: isinstance(d, list) and len(d) > 0),
        ("/api/v1/aqi/cpcb-index", lambda d: isinstance(d, list) and len(d) > 0),
        ("/api/v1/analytics/station-statistics", lambda d: isinstance(d, list) and len(d) > 0),
        ("/api/v1/analytics/trends/hourly", lambda d: isinstance(d, list) and len(d) > 0),
        ("/api/v1/analytics/trends/daily", lambda d: isinstance(d, list) and len(d) > 0),
        ("/api/v1/analytics/weather-correlation", lambda d: isinstance(d, list) and len(d) > 0),
        ("/api/v1/alerts", lambda d: isinstance(d, list)),
        ("/api/v1/audit/quality-report", lambda d: isinstance(d, list)),
    ]

    for path, validator in test_routes:
        status, headers, body = _make_wsgi_request({"PATH_INFO": path})
        assert status == 200, f"Route {path} returned status {status}"
        assert "application/json" in headers.get("Content-Type", "")
        assert validator(body), f"Route {path} returned unexpected payload: {body}"
