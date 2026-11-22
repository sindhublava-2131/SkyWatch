"""Open-Meteo weather client supporting hourly forecasts and historical archive data."""
from __future__ import annotations

import json
import os
import random
import time
from pathlib import Path
from typing import Any, Dict, Optional

import requests

from skywatch.pipeline.logging import get_logger

logger = get_logger("weather_client")


class WeatherClient:
    """Client for Open-Meteo forecast and archive APIs with UTC normalization and fixture fallback."""

    FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
    ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

    def __init__(
        self,
        timeout: int = 20,
        max_retries: int = 5,
        offline: bool = False,
        fixtures_dir: Optional[str] = None,
    ) -> None:
        self.timeout = timeout
        self.max_retries = max_retries
        self.offline = offline or bool(os.environ.get("OPENAQ_OFFLINE", ""))
        self.fixtures_dir = Path(fixtures_dir) if fixtures_dir else Path("tests/fixtures")

    def __repr__(self) -> str:
        return f"<WeatherClient offline={self.offline} timeout={self.timeout}>"

    def _load_fixture(self, filename: str) -> Dict[str, Any]:
        fixture_path = self.fixtures_dir / filename
        if not fixture_path.is_file():
            raise FileNotFoundError(f"Offline fixture not found: {fixture_path}")
        with open(fixture_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def _request_with_retry(self, base_url: str, params: Dict[str, Any]) -> Dict[str, Any]:
        for attempt in range(self.max_retries):
            try:
                response = requests.get(base_url, params=params, timeout=self.timeout)

                if response.status_code == 429:
                    sleep_time = min(30.0, 1.5 * (2**attempt)) + random.uniform(0.1, 1.0)
                    logger.warning(f"Open-Meteo 429. Retrying in {sleep_time:.2f}s")
                    time.sleep(sleep_time)
                    continue

                if response.status_code >= 500:
                    sleep_time = min(30.0, 1.0 * (2**attempt)) + random.uniform(0.1, 0.5)
                    logger.warning(f"Open-Meteo server error {response.status_code}. Retrying in {sleep_time:.2f}s")
                    time.sleep(sleep_time)
                    continue

                response.raise_for_status()
                return response.json()

            except (requests.ConnectionError, requests.Timeout) as exc:
                if attempt == self.max_retries - 1:
                    raise exc
                sleep_time = min(30.0, 1.0 * (2**attempt)) + random.uniform(0.1, 0.5)
                time.sleep(sleep_time)

        raise requests.HTTPError(f"Exceeded max retries ({self.max_retries}) for {base_url}")

    def get_forecast(
        self,
        latitude: float,
        longitude: float,
        hourly: str = "temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation",
    ) -> Dict[str, Any]:
        """Fetch 7-day hourly forecast from Open-Meteo in UTC."""
        if self.offline:
            return self._load_fixture("sample_weather_forecast.json")

        params = {
            "latitude": latitude,
            "longitude": longitude,
            "hourly": hourly,
            "timezone": "UTC",
        }
        return self._request_with_retry(self.FORECAST_URL, params)

    def get_archive(
        self,
        latitude: float,
        longitude: float,
        start_date: str,
        end_date: str,
        hourly: str = "temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation",
    ) -> Dict[str, Any]:
        """Fetch historical hourly weather data from Open-Meteo Archive API in UTC."""
        if self.offline:
            return self._load_fixture("sample_weather_archive.json")

        params = {
            "latitude": latitude,
            "longitude": longitude,
            "start_date": start_date,
            "end_date": end_date,
            "hourly": hourly,
            "timezone": "UTC",
        }
        return self._request_with_retry(self.ARCHIVE_URL, params)
