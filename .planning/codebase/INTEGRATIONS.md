---
last_mapped_commit: 6b4cc6dd01b87e70230978c94258f457e4b5fcb4
last_mapped_at: 2026-11-26
---
# External Integrations

**Analysis Date:** 2026-11-26

## External APIs & Services

**Air Quality Telemetry:**
- OpenAQ API v3 — Station discovery, real-time measurements, and historical sensor readings for Bengaluru.
  - Base URL: `https://api.openaq.org/v3` (`src/skywatch/collector/openaq_client.py:24`)
  - Client: Custom HTTP client wrapping `requests.Session` with retry loops and exponential backoff (`src/skywatch/collector/openaq_client.py:27`).
  - Authentication: Custom `X-API-Key` header populated from `OPENAQ_API_KEY` environment variable. HTTP 401/403 triggers non-retryable `AuthenticationError`.
  - Endpoints:
    - `GET /locations?bbox=77.45,12.83,77.75,13.12&limit=100&page=1` — Discover CAAQMS monitoring stations in the Bengaluru metropolitan bounding box (`src/skywatch/collector/discovery.py:17`).
    - `GET /locations/{id}/latest` — Fetch latest snapshot readings per location (`src/skywatch/collector/openaq_client.py:72`).
    - `GET /sensors/{id}/measurements?datetime_from=...&datetime_to=...&limit=100&page=1` — Incremental collection and chunked historical backfill per sensor (`src/skywatch/collector/openaq_client.py:86`).
  - Retry Strategy: Exponential backoff with random jitter up to `max_retries` (default 5). Honors HTTP 429 `Retry-After` headers; retries HTTP 5xx errors and network timeouts.
  - Offline Mode: Setting `offline=True` or `OPENAQ_OFFLINE=1` diverts requests to frozen test fixtures in `tests/fixtures/sample_openaq_*.json`.
  - Security Boundary: Direct browser calls to OpenAQ v3 are strictly excluded from the frontend contract to prevent client-side leakage of `X-API-Key`. All consumption by the UI is routed through SkyWatch static or REST API layers.

**Meteorological Data:**
- Open-Meteo APIs — Free-tier meteorological forecasts and historical weather archives for Bengaluru.
  - Forecast Endpoint: `https://api.open-meteo.com/v1/forecast` (`src/skywatch/collector/weather_client.py:21`)
  - Archive Endpoint: `https://archive-api.open-meteo.com/v1/archive` (`src/skywatch/collector/weather_client.py:22`)
  - Authentication: None required.
  - Parameters: Centered at Bengaluru coordinates `(12.91, 77.61)` requesting `hourly=temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation` in `UTC` timezone.
  - Offline Mode: Supported via `tests/fixtures/sample_weather_forecast.json` and `sample_weather_archive.json`.

**Map Tiles & Geospatial Services:**
- MapLibre GL (`maplibre-gl>=6.19.0`, `react-map-gl>=8.1.3` in `frontend/`):
  - Used for vector map rendering in the station explorer view (`frontend/app/stations/page.tsx`).
  - Consumes open vector tile styles (e.g. OpenStreetMap / Protomaps / Carto Positron) without proprietary API keys.

## Internal Storage & Big Data Integrations

**Local Partitioned Filesystem:**
- Directory tree: `data/`
  - `data/raw/` — Verbatim JSON API responses accompanied by `_manifest.csv` audit trails.
  - `data/normalized/` — Ingestion-date partitioned CSVs (`ingest_dt=YYYY-MM-DD`) validated against `config/schemas.yaml`.
  - `data/clean/` — Event-date partitioned headerless CSVs (`event_dt=YYYY-MM-DD`) produced by Pig ETL.
  - `data/analytics/` — Pig daily and hourly rollup datasets (`data/analytics/station_stats/`, `data/analytics/hourly_rollup/`).
  - `output/published/` — Dual Parquet and CSV analytical datasets produced by Spark jobs.
  - `output/published/api/v1/` — Static JSON REST lake generated during Spark execution for frontend consumption.
  - `data/synthetic/` — Deterministic synthetic benchmarking datasets for 1x, 10x, and 100x scale stress testing.

**Hadoop Distributed File System (HDFS):**
- Module: `src/skywatch/storage/hdfs_storage.py` (`HDFSStorageAdapter`)
- Interacts with Apache Hadoop HDFS via `hdfs dfs` subprocess execution, with automatic environment detection and local directory fallback (`data/local_hdfs/`).

**State & Watermark Tracking:**
- High-Water Marks: `data/.watermarks.json` managed by `WatermarkManager` (`src/skywatch/storage/watermark.py`). Tracks ISO-8601 UTC timestamps per sensor to ensure idempotent incremental ingestion.
- Backfill Checkpoints: `data/.backfill_checkpoint.json` records completed date chunks to support resumption after failure.

## Frontend to API Integration

**Data Fetching & Validation Layer (`frontend/lib/api.ts`):**
- Configuration: Controlled by environment variables `NEXT_PUBLIC_API_BASE_URL` and `NEXT_PUBLIC_USE_MOCKS`.
- Fallback Fixtures: When `NEXT_PUBLIC_USE_MOCKS` is enabled (default in local development), queries resolve to validated fixtures in `frontend/lib/mocks/index.ts` containing genuine published datasets from the analytical lake (zero synthetic placeholders).
- Schema Guard: Responses are verified at runtime via Zod schemas (`frontend/lib/schemas.ts`) enforcing ISO-8601 UTC formats, enumeration bounds, and automatic station ID string coercion.
- Complete 9 Endpoints Consumed:
  - `GET /api/v1/meta` (`loadMeta`) — Engine verification and profile detection.
  - `GET /api/v1/stations` (`loadStations`) — Station metadata with measurement freshness status and coordinates.
  - `GET /api/v1/aqi/cpcb-index` (`loadIndexes`) — 24-hour rolling CPCB AQI values and category ratings.
  - `GET /api/v1/analytics/trends/hourly` (`loadHourlyTrends`) — Diurnal 24-hour trends with UTC and IST mapping.
  - `GET /api/v1/alerts` (`loadAlerts`) — Real-time telemetry, health, and spike alerts.
  - `GET /api/v1/analytics/station-statistics` (`loadStationStatistics`) — Multi-metric summary statistics and rankings.
  - `GET /api/v1/analytics/trends/daily` (`loadDailyTrends`) — Day-of-week relative aggregation and variance profiles.
  - `GET /api/v1/analytics/weather-correlation` (`loadWeatherCorrelation`) — Air-weather equijoins and Pearson $r$ correlations.
  - `GET /api/v1/audit/quality-report` (`loadQualityReport`) — Multi-stage record reconciliation and audit diagnostics.
