"""OpenAQ API v3 client with rate-limiting backoff and offline fixture fallback."""
from __future__ import annotations

import json
import os
import random
import time
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import urlencode

import requests

from skywatch.pipeline.logging import get_logger

logger = get_logger("openaq_client")


class AuthenticationError(Exception):
    """Raised when API credentials are rejected (HTTP 401/403)."""


class OpenAQClient:
    """Client for OpenAQ API v3 supporting rate-limit backoff and offline fixtures."""

    BASE_URL = "https://api.openaq.org/v3"

    def __init__(
        self,
        api_key: Optional[str] = None,
        timeout: int = 20,
        max_retries: int = 5,
        offline: bool = False,
        fixtures_dir: Optional[str] = None,
    ) -> None:
        self._api_key = api_key or os.environ.get("OPENAQ_API_KEY")
        self.timeout = timeout
        self.max_retries = max_retries
        self.offline = offline or bool(os.environ.get("OPENAQ_OFFLINE", ""))
        self.fixtures_dir = Path(fixtures_dir) if fixtures_dir else Path("tests/fixtures")
        self.session = requests.Session()

    def __repr__(self) -> str:
        # Prevent API key leakage in string representations
        return f"<OpenAQClient offline={self.offline} timeout={self.timeout}>"

    def _get_headers(self) -> Dict[str, str]:
        headers = {"Accept": "application/json"}
        if self._api_key:
            headers["X-API-Key"] = self._api_key
        return headers

    def _load_fixture(self, filename: str) -> Dict[str, Any]:
        fixture_path = self.fixtures_dir / filename
        if not fixture_path.is_file():
            raise FileNotFoundError(f"Offline fixture not found: {fixture_path}")
        with open(fixture_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def _request_with_retry(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = f"{self.BASE_URL}{endpoint}"
        query_string = f"?{urlencode(params)}" if params else ""
        full_url = f"{url}{query_string}"

        for attempt in range(self.max_retries):
            try:
                response = self.session.get(
                    url,
                    headers=self._get_headers(),
                    params=params,
                    timeout=self.timeout,
                )

                if response.status_code in (401, 403):
                    raise AuthenticationError(
                        f"Authentication failed for OpenAQ API (HTTP {response.status_code})"
                    )

                if response.status_code == 429:
                    retry_after = response.headers.get("Retry-After")
                    if retry_after and retry_after.isdigit():
                        sleep_time = int(retry_after) + random.uniform(0.1, 1.0)
                    else:
                        sleep_time = min(60.0, 1.5 * (2**attempt)) + random.uniform(0.1, 1.0)
                    logger.warning(
                        f"Rate limit HTTP 429 received from {endpoint}. Retrying in {sleep_time:.2f}s (attempt {attempt + 1}/{self.max_retries})"
                    )
                    time.sleep(sleep_time)
                    continue

                if response.status_code >= 500:
                    sleep_time = min(30.0, 1.0 * (2**attempt)) + random.uniform(0.1, 0.5)
                    logger.warning(
                        f"Server error HTTP {response.status_code} from {endpoint}. Retrying in {sleep_time:.2f}s"
                    )
                    time.sleep(sleep_time)
                    continue

                response.raise_for_status()
                try:
                    return response.json()
                except (ValueError, json.JSONDecodeError) as json_err:
                    if attempt == self.max_retries - 1:
                        raise requests.HTTPError(f"Invalid JSON response from {endpoint}: {json_err}")
                    sleep_time = min(30.0, 1.0 * (2**attempt)) + random.uniform(0.1, 0.5)
                    time.sleep(sleep_time)
                    continue

            except (requests.ConnectionError, requests.Timeout, requests.RequestException) as exc:
                if attempt == self.max_retries - 1:
                    raise exc
                sleep_time = min(30.0, 1.0 * (2**attempt)) + random.uniform(0.1, 0.5)
                logger.warning(f"Connection error to {endpoint}: {exc}. Retrying in {sleep_time:.2f}s")
                time.sleep(sleep_time)

        raise requests.HTTPError(f"Exceeded max retries ({self.max_retries}) for {full_url}")

    def get_locations(self, bbox: str, limit: int = 100, page: int = 1) -> Dict[str, Any]:
        """Query /v3/locations with bounding box coordinates."""
        if self.offline:
            return self._load_fixture("sample_openaq_locations.json")
        params = {"bbox": bbox, "limit": limit, "page": page}
        return self._request_with_retry("/locations", params=params)

    def get_latest(self, location_id: int) -> Dict[str, Any]:
        """Query /v3/locations/{id}/latest."""
        if self.offline:
            return self._load_fixture("sample_openaq_latest.json")
        return self._request_with_retry(f"/locations/{location_id}/latest")

    def get_measurements(
        self,
        sensor_id: int,
        datetime_from: str,
        datetime_to: str,
        limit: int = 100,
        page: int = 1,
    ) -> Dict[str, Any]:
        """Query /v3/sensors/{id}/measurements for timeseries observations."""
        if self.offline:
            return self._load_fixture("sample_openaq_measurements.json")
        params = {
            "datetime_from": datetime_from,
            "datetime_to": datetime_to,
            "limit": limit,
            "page": page,
        }
        return self._request_with_retry(f"/sensors/{sensor_id}/measurements", params=params)

    def get_hours(
        self,
        sensor_id: int,
        datetime_from: str,
        datetime_to: str,
    ) -> Dict[str, Any]:
        """Query precomputed hourly aggregates /v3/sensors/{id}/hours."""
        if self.offline:
            return self._load_fixture("sample_openaq_measurements.json")
        params = {
            "datetime_from": datetime_from,
            "datetime_to": datetime_to,
            "limit": 100,
        }
        return self._request_with_retry(f"/sensors/{sensor_id}/hours", params=params)
