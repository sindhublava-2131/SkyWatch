"""Vercel Serverless REST API entrypoint for SkyWatch Environmental Intelligence."""
from __future__ import annotations

import csv
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs

# Base data directory lookup
PROJECT_ROOT = Path(__file__).resolve().parent.parent


def get_published_dir() -> Path:
    return Path(os.environ.get("SKYWATCH_PUBLISHED_DIR", str(PROJECT_ROOT / "output" / "published")))


def get_static_api_dir() -> Path:
    return get_published_dir() / "api" / "v1"


PUBLISHED_DIR = get_published_dir()
STATIC_API_DIR = get_static_api_dir()


def _load_dataset(name: str) -> List[Dict[str, Any]]:
    """Load dataset from static JSON cache or parse published CSV on the fly."""
    json_path = get_static_api_dir() / f"{name}.json"
    if json_path.is_file():
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass

    csv_path = get_published_dir() / name / "csv" / "part-00000.csv"
    if not csv_path.is_file():
        return []

    rows: List[Dict[str, Any]] = []
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            typed: Dict[str, Any] = {}
            for k, v in r.items():
                if v is None or v == "":
                    typed[k] = None
                elif v.lower() == "true":
                    typed[k] = True
                elif v.lower() == "false":
                    typed[k] = False
                else:
                    try:
                        if "." in v:
                            typed[k] = float(v)
                        else:
                            typed[k] = int(v)
                    except ValueError:
                        typed[k] = v
            rows.append(typed)
    return rows


def get_meta() -> Dict[str, Any]:
    """Return pipeline execution metadata, profile, and engine."""
    meta_path = get_static_api_dir() / "meta.json"
    if meta_path.is_file():
        try:
            with open(meta_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {
        "run_id": "default",
        "generated_at_utc": "2026-11-23T00:00:00Z",
        "profile": os.environ.get("SKYWATCH_PROFILE", "modern"),
        "engine": "STANDALONE_PYTHON_SIMULATION",
        "is_simulation": True,
    }


def get_stations(status: Optional[str] = None, freshness: Optional[str] = None) -> List[Dict[str, Any]]:
    """Return CAAQMS stations enriched with reading timestamp and freshness status."""
    stations = _load_dataset("stations")
    if not stations:
        # Fallback to config/stations.json
        cfg_stations = PROJECT_ROOT / "config" / "stations.json"
        if cfg_stations.is_file():
            try:
                from skywatch.spark.export import compute_station_freshness
                with open(cfg_stations, "r", encoding="utf-8") as f:
                    raw_st = json.load(f)
                stations = compute_station_freshness(raw_st)
            except Exception:
                stations = []

    res = stations
    if freshness:
        res = [s for s in res if str(s.get("freshness_status", "")).lower() == freshness.lower()]
    return res


def get_cpcb_index(station_id: Optional[str] = None, aqi_category: Optional[str] = None, limit: Optional[int] = None) -> List[Dict[str, Any]]:
    """Return 24-hour rolling CPCB AQI records."""
    rows = _load_dataset("cpcb_index")
    if station_id:
        rows = [r for r in rows if str(r.get("station_id")) == str(station_id)]
    if aqi_category:
        rows = [r for r in rows if str(r.get("aqi_category", "")).lower() == aqi_category.lower()]
    if limit is not None and limit > 0:
        rows = rows[:limit]
    return rows


def get_station_statistics(parameter: Optional[str] = None) -> List[Dict[str, Any]]:
    """Return station summary metrics and pollution ranks."""
    rows = _load_dataset("station_statistics")
    if parameter:
        rows = [r for r in rows if str(r.get("parameter", "")).lower() == parameter.lower()]
    return rows


def get_trends_hourly(station_id: Optional[str] = None, parameter: Optional[str] = None) -> List[Dict[str, Any]]:
    """Return diurnal 24-hour trends with IST ranges."""
    rows = _load_dataset("trends_hourly")
    if station_id:
        rows = [r for r in rows if str(r.get("station_id")) == str(station_id)]
    if parameter:
        rows = [r for r in rows if str(r.get("parameter", "")).lower() == parameter.lower()]
    return rows


def get_trends_daily(station_id: Optional[str] = None, parameter: Optional[str] = None) -> List[Dict[str, Any]]:
    """Return day-of-week trends."""
    rows = _load_dataset("trends_daily")
    if station_id:
        rows = [r for r in rows if str(r.get("station_id")) == str(station_id)]
    if parameter:
        rows = [r for r in rows if str(r.get("parameter", "")).lower() == parameter.lower()]
    return rows


def get_weather_correlation(station_id: Optional[str] = None, pollutant: Optional[str] = None) -> List[Dict[str, Any]]:
    """Return Pearson r correlation matrix."""
    rows = _load_dataset("weather_correlation")
    if station_id:
        rows = [r for r in rows if str(r.get("station_id")) == str(station_id)]
    if pollutant:
        rows = [r for r in rows if str(r.get("pollutant", "")).lower() == pollutant.lower()]
    return rows


def get_alerts(severity: Optional[str] = None, station_id: Optional[str] = None) -> List[Dict[str, Any]]:
    """Return active health, telemetry, and spike alerts."""
    rows = _load_dataset("alerts")
    if severity:
        rows = [r for r in rows if str(r.get("severity", "")).upper() == severity.upper()]
    if station_id:
        rows = [r for r in rows if str(r.get("station_id")) == str(station_id)]
    return rows


def get_quality_report(run_id: Optional[str] = None, stage: Optional[str] = None) -> List[Dict[str, Any]]:
    """Return pipeline quality audit report records."""
    rows = _load_dataset("quality_report")
    if run_id:
        rows = [r for r in rows if str(r.get("run_id")) == str(run_id)]
    if stage:
        rows = [r for r in rows if str(r.get("stage", "")).lower() == stage.lower()]
    return rows


# ==============================================================================
# FastAPI / WSGI Serverless Router Definition
# ==============================================================================

try:
    from fastapi import FastAPI, Query
    from fastapi.middleware.cors import CORSMiddleware

    app = FastAPI(
        title="SkyWatch Environmental Intelligence API",
        description="REST API interface exposing Apache Spark published analytical datasets for Bengaluru air quality and weather intelligence.",
        version="2.0.0",
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.get("/api/v1/meta")
    def api_meta() -> Dict[str, Any]:
        return get_meta()

    @app.get("/api/v1/stations")
    def api_stations(
        status: Optional[str] = Query(None, description="Filter by status (active/all)"),
        freshness: Optional[str] = Query(None, description="Filter by freshness (Fresh, Stale, Offline)"),
    ) -> List[Dict[str, Any]]:
        return get_stations(status=status, freshness=freshness)

    @app.get("/api/v1/aqi/cpcb-index")
    def api_cpcb_index(
        station_id: Optional[str] = Query(None, description="Station identifier"),
        aqi_category: Optional[str] = Query(None, description="AQI category (Good, Satisfactory, Moderate, Poor, Very Poor, Severe)"),
        limit: Optional[int] = Query(None, description="Maximum records to return"),
    ) -> List[Dict[str, Any]]:
        return get_cpcb_index(station_id=station_id, aqi_category=aqi_category, limit=limit)

    @app.get("/api/v1/analytics/station-statistics")
    def api_station_statistics(
        parameter: Optional[str] = Query(None, description="Pollutant parameter (pm25, pm10)"),
    ) -> List[Dict[str, Any]]:
        return get_station_statistics(parameter=parameter)

    @app.get("/api/v1/analytics/trends/hourly")
    def api_trends_hourly(
        station_id: Optional[str] = Query(None, description="Station identifier"),
        parameter: Optional[str] = Query(None, description="Pollutant parameter (pm25, pm10)"),
    ) -> List[Dict[str, Any]]:
        return get_trends_hourly(station_id=station_id, parameter=parameter)

    @app.get("/api/v1/analytics/trends/daily")
    def api_trends_daily(
        station_id: Optional[str] = Query(None, description="Station identifier"),
        parameter: Optional[str] = Query(None, description="Pollutant parameter (pm25, pm10)"),
    ) -> List[Dict[str, Any]]:
        return get_trends_daily(station_id=station_id, parameter=parameter)

    @app.get("/api/v1/analytics/weather-correlation")
    def api_weather_correlation(
        station_id: Optional[str] = Query(None, description="Station identifier"),
        pollutant: Optional[str] = Query(None, description="Pollutant (pm25, pm10)"),
    ) -> List[Dict[str, Any]]:
        return get_weather_correlation(station_id=station_id, pollutant=pollutant)

    @app.get("/api/v1/alerts")
    def api_alerts(
        severity: Optional[str] = Query(None, description="Severity (WARNING, CRITICAL, EMERGENCY)"),
        station_id: Optional[str] = Query(None, description="Station identifier"),
    ) -> List[Dict[str, Any]]:
        return get_alerts(severity=severity, station_id=station_id)

    @app.get("/api/v1/audit/quality-report")
    def api_quality_report(
        run_id: Optional[str] = Query(None, description="Pipeline run ID"),
        stage: Optional[str] = Query(None, description="Pipeline stage (INGESTION, PIG_ETL, SPARK_ANALYTICS)"),
    ) -> List[Dict[str, Any]]:
        return get_quality_report(run_id=run_id, stage=stage)

except ImportError:
    # Standard library WSGI application fallback for minimal Python runtimes
    def app(environ: Dict[str, Any], start_response: Any) -> List[bytes]:
        path = environ.get("PATH_INFO", "")
        query = parse_qs(environ.get("QUERY_STRING", ""))

        status = "200 OK"
        headers = [
            ("Content-Type", "application/json; charset=utf-8"),
            ("Access-Control-Allow-Origin", "*"),
            ("Access-Control-Allow-Methods", "GET, OPTIONS"),
            ("Access-Control-Allow-Headers", "*"),
        ]

        def q_first(key: str) -> Optional[str]:
            vals = query.get(key)
            return vals[0] if vals else None

        if environ.get("REQUEST_METHOD") == "OPTIONS":
            start_response("204 No Content", headers)
            return []

        if path == "/api/v1/meta":
            data: Any = get_meta()
        elif path == "/api/v1/stations":
            data = get_stations(status=q_first("status"), freshness=q_first("freshness"))
        elif path == "/api/v1/aqi/cpcb-index":
            lim = int(q_first("limit")) if q_first("limit") and q_first("limit").isdigit() else None
            data = get_cpcb_index(station_id=q_first("station_id"), aqi_category=q_first("aqi_category"), limit=lim)
        elif path == "/api/v1/analytics/station-statistics":
            data = get_station_statistics(parameter=q_first("parameter"))
        elif path == "/api/v1/analytics/trends/hourly":
            data = get_trends_hourly(station_id=q_first("station_id"), parameter=q_first("parameter"))
        elif path == "/api/v1/analytics/trends/daily":
            data = get_trends_daily(station_id=q_first("station_id"), parameter=q_first("parameter"))
        elif path == "/api/v1/analytics/weather-correlation":
            data = get_weather_correlation(station_id=q_first("station_id"), pollutant=q_first("pollutant"))
        elif path == "/api/v1/alerts":
            data = get_alerts(severity=q_first("severity"), station_id=q_first("station_id"))
        elif path == "/api/v1/audit/quality-report":
            data = get_quality_report(run_id=q_first("run_id"), stage=q_first("stage"))
        else:
            status = "404 Not Found"
            data = {"error": f"Endpoint not found: {path}"}

        start_response(status, headers)
        return [json.dumps(data, indent=2).encode("utf-8")]
