# Phase 1 Research: Foundation, Data Ingestion & Storage Architecture

**Author:** GSD Phase Researcher  
**Phase:** 1 — Foundation, Data Ingestion & Storage Architecture  
**Date:** 2026-11-22  
**Status:** Complete & Ready for Planning  
**Traceability:** ING-01, ING-02, ING-03, ING-04, ING-05, ING-06, HYG-01, HYG-02, HYG-03, HYG-04, HYG-05, HYG-06, HYG-07  

---

## 1. Executive Summary & Phase Scope

Phase 1 establishes the production backbone of SkyWatch: project directory scaffolding, dual-profile environment configurations (`modern.env` and `lab.env`), canonical data contracts in `config/schemas.yaml`, resilient data ingestion clients for OpenAQ API v3 and Open-Meteo, data hygiene filtering and deduplication, atomic local and HDFS storage handlers, incremental per-sensor watermarking, chunked backfilling, offline mock fixtures, and automated collection scheduling [CITED].

Every architectural pattern in Phase 1 is designed to support downstream Apache Pig ETL (Phase 2) and Apache Spark analytics (Phase 3) while adhering strictly to single-machine developer constraints (≤4GB RAM budget, localhost bindings) and the academic evaluation mandate of the Big Data Lab (ISL74) [CITED].

---

## 2. Requirements Traceability Matrix

| Requirement ID | Specification Anchor | Focus Area | Technical Mechanism | Status |
| :--- | :--- | :--- | :--- | :--- |
| **ING-01** | FR-01 | OpenAQ Collection | Query `/v3/sensors/{id}/measurements` & `/v3/locations/{id}/latest` for PM2.5 and PM10 | Researched [VERIFIED] |
| **ING-02** | FR-02 | Weather Ingestion | Query Open-Meteo Forecast & Archive APIs for hourly temp, humidity, wind, rain in UTC | Researched [VERIFIED] |
| **ING-03** | FR-03 | Station Discovery | Query `/v3/locations` with Bengaluru bounding box (`77.45,12.83,77.75,13.12`) to `config/stations.json` | Researched [VERIFIED] |
| **ING-04** | FR-04 | Raw Data Preservation | Save untouched raw JSON payloads with audit metadata (`_manifest.csv`) before any parsing | Researched [CITED] |
| **ING-05** | FR-11, FR-12 | Watermark & Backfill | Incremental collection via `data/.watermarks.json` committed atomically with renames; chunked backfilling | Researched [CITED] |
| **ING-06** | FR-13 | Automated Scheduling | Dual automation: local shell script with lockfile & GitHub Actions workflow (`scheduled-collect.yml`) | Researched [CITED] |
| **HYG-01** | FR-05 | Schema Normalization | Normalize raw API feeds to canonical schemas defined in `config/schemas.yaml` | Researched [CITED] |
| **HYG-02** | FR-06 | Explicit Timestamps | Preserve explicit UTC measurement and ingestion timestamps on every record | Researched [CITED] |
| **HYG-03** | FR-07 | Ingestion Deduplication | Deduplicate on composite key `(sensor_id, parameter, measurement_time_utc)` via SHA-256 hash | Researched [CITED] |
| **HYG-04** | FR-08 | Validation & Hygiene | Validate required fields, numeric bounds, units (`µg/m³`), and plausible ceiling (≤ 1000 µg/m³) | Researched [CITED] |
| **HYG-05** | FR-09, FR-10 | Resilience & Atomicity | Exponential backoff with jitter on HTTP 429 (`Retry-After`), atomic write via temp file -> rename | Researched [CITED] |
| **HYG-06** | FR-14, FR-15 | Partitioned Storage | Local & HDFS storage: `/raw/` and `/normalized/` with `ingest_dt=YYYY-MM-DD`; idempotent uploads | Researched [CITED] |
| **HYG-07** | FR-16 | Offline Execution | Seamless fallback to local mock fixtures (`tests/fixtures/`) and cached datasets when offline | Researched [CITED] |

---

## 3. External API Specifications & Ingestion Protocols

### 3.1 OpenAQ API v3

* **Base URL**: `https://api.openaq.org/v3` [VERIFIED]
* **Authentication**: HTTP Header `X-API-Key: <key>` on every request [VERIFIED].
  * Security Rule: Zero key logging. 401/403 responses must trigger immediate failure without retry and redact keys from exception messages [CITED].
* **Rate Limits**:
  * Free Tier: 60 requests/minute and 2,000 requests/hour [CITED].
  * Rate-limit headers returned: `x-ratelimit-limit`, `x-ratelimit-remaining`, `x-ratelimit-reset`.
  * HTTP 429 handling: Inspect `Retry-After` header. If present, sleep for the specified integer seconds plus 1.0s jitter. If missing, apply exponential backoff `min(60.0, backoff_factor * (2 ** attempt)) + random.uniform(0.1, 1.0)` [CITED].

#### Key Endpoints & Query Shapes

1. **Station Discovery (`ING-03`)**:
   * Endpoint: `GET /v3/locations`
   * Query Parameters:
     * `bbox`: `77.45,12.83,77.75,13.12` (Bengaluru metropolitan bounding box) [VERIFIED]
     * `limit`: `100` (max 1000)
     * `page`: `1`
   * Response Envelope:
     ```json
     {
       "meta": {"name": "openaq-api", "page": 1, "limit": 100, "found": 8},
       "results": [
         {
           "id": 2178,
           "name": "BTM Layout, Bengaluru - CPCB",
           "coordinates": {"latitude": 12.9135, "longitude": 77.6111},
           "datetimeLast": {"utc": "2026-11-22T08:00:00Z"},
           "sensors": [
             {"id": 3917, "name": "pm25", "parameter": {"id": 2, "name": "pm25", "units": "µg/m³"}},
             {"id": 3918, "name": "pm10", "parameter": {"id": 1, "name": "pm10", "units": "µg/m³"}}
           ]
         }
       ]
     }
     ```
   * Discovery Filtering Logic: Filter stations that have active sensors for `pm25` or `pm10` and where `datetimeLast.utc` is within the last 72 hours (configurable via `max_age_hours`). Output written to `config/stations.json` [CITED].

2. **Sensor Measurements Timeseries (`ING-01`, `ING-05`)**:
   * Endpoint: `GET /v3/sensors/{sensors_id}/measurements`
   * Query Parameters:
     * `datetime_from`: ISO 8601 UTC timestamp (`YYYY-MM-DDTHH:MM:SSZ`)
     * `datetime_to`: ISO 8601 UTC timestamp
     * `limit`: `100`
     * `page`: `1`
   * Response Structure:
     ```json
     {
       "meta": {"page": 1, "limit": 100, "found": 1},
       "results": [
         {
           "value": 42.8,
           "parameter": {"id": 2, "name": "pm25", "units": "µg/m³"},
           "period": {
             "datetimeFrom": {"utc": "2026-11-22T07:00:00Z"},
             "datetimeTo": {"utc": "2026-11-22T08:00:00Z"}
           }
         }
       ]
     }
     ```
   * Measurement Timestamp Rule: SkyWatch uses `period.datetimeTo.utc` (or `datetime.utc` on latest endpoints) as the canonical `measurement_time_utc` [VERIFIED].

3. **Latest Station Reading (`ING-01`)**:
   * Endpoint: `GET /v3/locations/{locations_id}/latest`
   * Used for fast incremental polling during recurring cron executions [CITED].

4. **Precomputed Hourly Aggregations for Fast Backfill (`ING-05`)**:
   * Endpoint: `GET /v3/sensors/{sensors_id}/hours`
   * Used to pull historical 24h/multi-day series without paginating across thousands of discrete sub-hourly raw readings [VERIFIED].

---

### 3.2 Open-Meteo API (Weather Context)

* **Authentication**: None required for free non-commercial tier [VERIFIED].
* **Coordinate Targeting**: Query at each station's verified coordinates (`latitude`, `longitude`) [CITED].
* **Hourly Weather Forecast & Recent Live Data (`ING-02`)**:
  * Endpoint: `https://api.open-meteo.com/v1/forecast`
  * Query Parameters:
    * `latitude`: Station latitude (e.g., `12.9135`)
    * `longitude`: Station longitude (e.g., `77.6111`)
    * `hourly`: `temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation` [VERIFIED]
    * `past_days`: `2` (pulls recent 48 hours for alignment with air quality readings)
    * `timezone`: `UTC` (ensures clock-hour alignment without timezone offsets) [VERIFIED]
* **Historical Weather Archive (`ING-02`, `ING-05`)**:
  * Endpoint: `https://archive-api.open-meteo.com/v1/archive`
  * Query Parameters:
    * `latitude`, `longitude`, `hourly`, `timezone=UTC`
    * `start_date`: `YYYY-MM-DD`
    * `end_date`: `YYYY-MM-DD`
* **Response Structure & Normalization**:
  * Open-Meteo returns parallel arrays inside the `hourly` object:
    ```json
    {
      "latitude": 12.91,
      "longitude": 77.61,
      "hourly_units": {
        "time": "iso8601",
        "temperature_2m": "°C",
        "relative_humidity_2m": "%",
        "wind_speed_10m": "km/h",
        "precipitation": "mm"
      },
      "hourly": {
        "time": ["2026-11-22T00:00", "2026-11-22T01:00"],
        "temperature_2m": [22.4, 21.8],
        "relative_humidity_2m": [78, 81],
        "wind_speed_10m": [8.5, 7.2],
        "precipitation": [0.0, 0.2]
      }
    }
    ```
  * Transformation: Normalizer iterates index $i \in [0, N-1]$ across arrays to construct discrete row records formatted to `schemas.yaml`, appending `:00Z` to format timestamps to standard ISO 8601 UTC [VERIFIED].

---

## 4. Canonical Data Schemas & Validation Hygiene

### 4.1 Schema Definition: `config/schemas.yaml`

Single source of truth governing Python ingestion, Pig schemas, and Spark `StructType` [CITED]:

```yaml
version: "2.0"

air_quality:
  partition_key: "ingest_dt"
  delimiter: ","
  has_header: true
  fields:
    - name: "record_key"
      type: "string"
      nullable: false
      description: "SHA-256 hash of sensor_id|parameter|measurement_time_utc"
    - name: "station_id"
      type: "string"
      nullable: false
      description: "OpenAQ location identifier"
    - name: "station_name"
      type: "string"
      nullable: false
      description: "Human-readable station name"
    - name: "latitude"
      type: "double"
      nullable: false
      description: "WGS84 latitude"
    - name: "longitude"
      type: "double"
      nullable: false
      description: "WGS84 longitude"
    - name: "sensor_id"
      type: "string"
      nullable: false
      description: "OpenAQ sensor identifier"
    - name: "parameter"
      type: "string"
      nullable: false
      description: "Observed pollutant: pm25, pm10, no2, o3"
    - name: "value"
      type: "double"
      nullable: true
      description: "Observed concentration in ug/m3"
    - name: "unit"
      type: "string"
      nullable: false
      description: "Canonical concentration unit (ug/m3)"
    - name: "measurement_time_utc"
      type: "string"
      nullable: false
      description: "Observation timestamp in ISO-8601 UTC"
    - name: "ingestion_time_utc"
      type: "string"
      nullable: false
      description: "Pipeline ingestion timestamp in ISO-8601 UTC"
    - name: "source"
      type: "string"
      nullable: false
      description: "Source identifier: openaq_v3"
    - name: "is_valid"
      type: "boolean"
      nullable: false
      description: "Validation pass flag"
    - name: "reject_reason"
      type: "string"
      nullable: true
      description: "Failure reason code if is_valid is false"

weather:
  partition_key: "ingest_dt"
  delimiter: ","
  has_header: true
  fields:
    - name: "record_key"
      type: "string"
      nullable: false
      description: "SHA-256 hash of location_id|measurement_time_utc"
    - name: "location_id"
      type: "string"
      nullable: false
      description: "Station ID or geohash coordinate identifier"
    - name: "latitude"
      type: "double"
      nullable: false
      description: "WGS84 latitude"
    - name: "longitude"
      type: "double"
      nullable: false
      description: "WGS84 longitude"
    - name: "temperature_c"
      type: "double"
      nullable: true
      description: "Air temperature at 2m in Celsius"
    - name: "humidity_pct"
      type: "double"
      nullable: true
      description: "Relative humidity percentage (0-100)"
    - name: "wind_speed"
      type: "double"
      nullable: true
      description: "Wind speed at 10m"
    - name: "wind_unit"
      type: "string"
      nullable: false
      description: "Wind speed unit (km/h)"
    - name: "precipitation_mm"
      type: "double"
      nullable: true
      description: "Precipitation in mm"
    - name: "measurement_time_utc"
      type: "string"
      nullable: false
      description: "Observation clock hour in ISO-8601 UTC"
    - name: "ingestion_time_utc"
      type: "string"
      nullable: false
      description: "Pipeline ingestion timestamp in ISO-8601 UTC"
    - name: "source"
      type: "string"
      nullable: false
      description: "Source identifier: open_meteo"
    - name: "data_kind"
      type: "string"
      nullable: false
      description: "Model classification: forecast_model or archive_model"
```

### 4.2 Data Hygiene Validation Rules (`HYG-04`)

Each raw record is passed through an extensible chain of validators in `data/validation.py` [CITED]:

1. **`check_required_fields`**:
   * Asserts presence and non-null values for `station_id`, `sensor_id`, `parameter`, and `measurement_time_utc`.
   * Violation code: `missing_field`.
2. **`check_numeric_range`**:
   * Asserts `value` can be parsed as float. If null, flagged as `null_value`.
   * Asserts `value >= 0.0`. Negative readings rejected with code `negative_value` [CITED].
3. **`check_plausible_ceiling`**:
   * For particulate matter (`pm25`, `pm10`), checks `value <= 1000.0` µg/m³.
   * Sensor readings above 1000 µg/m³ are physically implausible ambient levels in Bengaluru and indicate optical chamber saturation or dust bursts. Flagged with code `implausible_spike` [CITED].
4. **`check_unit`**:
   * Normalizes unit strings (`µg/m³`, `ug/m3`, `ug/m^3` -> `ug/m3`).
   * Rejects non-concentration units (e.g. `ppm`, `ppb` for particulates) with code `invalid_unit` [CITED].
5. **`check_timestamp`**:
   * Parses timestamp with ISO 8601 regex/datetime parser. Rejects invalid formats with `invalid_timestamp`.
   * Asserts timestamp is not in the future (`measurement_time_utc <= now_utc + 3600s`). Violation code: `future_timestamp` [CITED].

### 4.3 Deduplication Strategy (`HYG-03`)

* **Composite Unique Key**:
  $$\text{record\_key} = \text{SHA256}(\text{sensor\_id} \parallel \text{"|"} \parallel \text{parameter} \parallel \text{"|"} \parallel \text{measurement\_time\_utc})$$
* **Deduplication Engine**:
  * In-memory batch deduplication using an ordered dictionary / set lookup to ensure first-seen preservation.
  * Cross-run deduplication: When merging new ingest records with existing partition data, records with existing `record_key` values are discarded, preventing duplicate accumulation across recurring cron jobs [NFR-02, FR-07].

---

## 5. Storage Architecture & Partition Layout

### 5.1 Partition Hierarchy (`HYG-06`)

To align with big data ingestion standards and prevent partition re-computation overhead, storage is strictly bifurcated [CITED]:

* **Ingestion Time Partitioning (`ingest_dt=YYYY-MM-DD`)**:
  Used exclusively for raw and normalized layers:
  * `/skywatch/raw/openaq/ingest_dt=YYYY-MM-DD/openaq_{run_id}.json`
  * `/skywatch/raw/weather/ingest_dt=YYYY-MM-DD/weather_{run_id}.json`
  * `/skywatch/normalized/air_quality/ingest_dt=YYYY-MM-DD/air_quality_{run_id}.csv`
  * `/skywatch/normalized/weather/ingest_dt=YYYY-MM-DD/weather_{run_id}.csv`
  * *Rationale:* External data arrives by collection time; partitioning by collection date guarantees that write operations only ever touch today's partition directory, eliminating random seeks or file rewrites into older directories.
* **Event Time Partitioning (`event_dt=YYYY-MM-DD`)**:
  Used downstream for Apache Pig and Apache Spark outputs:
  * `/skywatch/clean/air_quality/event_dt=YYYY-MM-DD/`
  * `/skywatch/analytics/{metric}/event_dt=YYYY-MM-DD/`
  * *Rationale:* Analytics queries (hotspots, 24-hour CPCB AQI, hourly trends) filter by observation date, enabling partition pruning during MapReduce and Spark execution.

### 5.2 Raw Response Audit Ledger: `data/raw/_manifest.csv` (`ING-04`)

Every raw API response is written unmodified to disk and recorded in an append-only audit manifest [CITED]:

| Column Name | Type | Description |
| :--- | :--- | :--- |
| `run_id` | UUID string | Unique pipeline invocation identifier |
| `file_path` | String | Relative path to stored raw JSON file |
| `source` | String | `openaq_v3` or `open_meteo` |
| `endpoint_url` | String | Sanitized API endpoint URL queried |
| `http_status` | Integer | HTTP response status code (e.g. 200, 429) |
| `sha256` | Hex string | Cryptographic hash of the raw payload file |
| `record_count` | Integer | Number of measurement records extracted |
| `collected_at_utc` | String | ISO 8601 UTC collection completion timestamp |

### 5.3 Atomic File Write Protocol (`HYG-05`, `NFR-01`)

To guarantee that pipeline interruptions or network drops never corrupt existing datasets:

1. **Local Filesystem**:
   * Write data to temporary staging file in the target directory: `{filename}.tmp.{run_id}`.
   * Call `os.flush()` and `os.fsync(f.fileno())` to guarantee hardware persistence.
   * Execute atomic swap: `os.replace(temp_path, target_path)`. On POSIX and modern Windows systems, `os.replace` is an atomic directory rename [VERIFIED].
2. **HDFS Storage Adapter (`storage/hdfs_storage.py`)**:
   * Wraps `hdfs dfs` via Python `subprocess.run` [CITED].
   * Pre-check NameNode safemode: `hdfs dfsadmin -safemode get`. If safemode is `ON`, raise warning and enter offline fallback [CITED].
   * Staging upload: `hdfs dfs -put -f {local_tmp_path} /skywatch/_staging/{run_id}_{filename}`.
   * Atomic move: `hdfs dfs -mv /skywatch/_staging/{run_id}_{filename} {target_hdfs_path}`.
   * Idempotency guarantee (`FR-15`): If target partition exists, either stage and replace atomically or write partitioned chunk with unique `run_id` suffix [CITED].

---

## 6. Incremental State & Watermarking Architecture

### 6.1 Per-Sensor Watermark Ledger (`ING-05`)

State is persisted in `data/.watermarks.json`:

```json
{
  "updated_at_utc": "2026-11-22T08:30:00Z",
  "sensors": {
    "3917": {
      "parameter": "pm25",
      "station_id": "2178",
      "last_measurement_utc": "2026-11-22T08:00:00Z",
      "last_run_id": "9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d",
      "records_ingested": 142
    },
    "3918": {
      "parameter": "pm10",
      "station_id": "2178",
      "last_measurement_utc": "2026-11-22T08:00:00Z",
      "last_run_id": "9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d",
      "records_ingested": 139
    }
  }
}
```

### 6.2 Two-Phase Atomic Watermark Commit (`FR-11`)

To eliminate duplicate ingestion and prevent phantom watermark advancement during failures:
1. **Fetch**: Query OpenAQ using `datetime_from = sensor_watermark.last_measurement_utc`.
2. **Process**: Validate, normalize, and deduplicate records.
3. **Write Partition**: Write normalized records to storage via atomic swap.
4. **Commit Watermark**: Write updated watermark state to `data/.watermarks.json.tmp.{run_id}` and atomically rename to `data/.watermarks.json`.
5. *Crash Recovery Guarantee:* If process fails before step 4, the old watermark remains active. On restart, re-ingested overlapping records will be safely discarded by the `record_key` deduplication filter [CITED].

### 6.3 Chunked Historical Backfilling (`ING-05`, `FR-12`)

* When populating multi-month or annual history, querying large date ranges triggers gateway timeouts (HTTP 504) or rate limit exhaustions.
* **Chunking Algorithm**:
  * Divide requested window `[start_date, end_date]` into 7-day increments.
  * For each chunk, query `/v3/sensors/{id}/hours` or `/v3/sensors/{id}/measurements`.
  * Write chunk partition and flush a checkpoint marker `data/.backfill_checkpoint.json`.
  * Resumable: If chunk $k$ fails, re-running `skywatch backfill` reads checkpoint and resumes from chunk $k$ without repeating chunks $1 \dots k-1$ [CITED].

---

## 7. Dual-Profile Environment Architecture

SkyWatch supports two execution profiles via zero version hardcoding [CITED]:
1. `modern`: Default modern stack (OpenJDK 17/21 for Spark, OpenJDK 8 pinned for Apache Pig 0.18, Hadoop 3.4.3, Spark 4.3.0, Python 3.11+).
2. `lab`: Rollback university lab cluster stack (OpenJDK 8 for everything, Hadoop 3.2.2, Spark 3.2.2, Python 3.8/3.9).

### 7.1 Environment Profiles Configuration

* **`profiles/modern.env`**:
  ```bash
  export SKYWATCH_PROFILE="modern"
  export HADOOP_HOME="/opt/bigdata/hadoop-3.4.3"
  export PIG_HOME="/opt/bigdata/pig-0.18.0"
  export SPARK_HOME="/opt/bigdata/spark-4.3.0-bin-hadoop3"
  export JAVA_HOME_PIG="/usr/lib/jvm/java-8-openjdk-amd64"
  export JAVA_HOME_SPARK="/usr/lib/jvm/java-17-openjdk-amd64"
  export JAVA_HOME="$JAVA_HOME_PIG"
  export HDFS_ROOT="/skywatch"
  export SKYWATCH_HTTP_TIMEOUT=20
  export SKYWATCH_MAX_RETRIES=5
  export SKYWATCH_FRESH_HOURS=3
  export SKYWATCH_STALE_HOURS=24
  ```
* **`profiles/lab.env`**:
  ```bash
  export SKYWATCH_PROFILE="lab"
  export HADOOP_HOME="/opt/bigdata/hadoop-3.2.2"
  export PIG_HOME="/opt/bigdata/pig-0.18.0"
  export SPARK_HOME="/opt/bigdata/spark-3.2.2-bin-hadoop3.2"
  export JAVA_HOME_PIG="/usr/lib/jvm/java-8-openjdk-amd64"
  export JAVA_HOME_SPARK="/usr/lib/jvm/java-8-openjdk-amd64"
  export JAVA_HOME="$JAVA_HOME_PIG"
  export HDFS_ROOT="/skywatch"
  export SKYWATCH_HTTP_TIMEOUT=30
  export SKYWATCH_MAX_RETRIES=5
  export SKYWATCH_FRESH_HOURS=3
  export SKYWATCH_STALE_HOURS=24
  ```

### 7.2 Prerequisite Validation Module (`src/skywatch/pipeline/profile.py`)

A standalone validator verifies all required binaries and environment variables before any pipeline stage runs:
* Verifies `JAVA_HOME_PIG` points to JDK 8 (verifying `java -version` returns `1.8.*`). This prevents fatal `InaccessibleObjectException` crashes caused by JDK 17+ module encapsulation on unmaintained Pig internals [CITED].
* Verifies `JAVA_HOME_SPARK` points to JDK 17+ on `modern` or JDK 8 on `lab`.
* Verifies `HADOOP_HOME` and tests HDFS reachability via `hdfs dfsadmin -safemode get`.
* Python Syntax Guard: Enforces Python 3.8 syntax floor (`from __future__ import annotations`, `typing.Optional`, no `match/case` syntax) to guarantee execution under older lab Python runtimes [CITED].

---

## 8. Mock Fixtures & Fully Offline Execution Mode

### 8.1 Mock Fixture Suite (`tests/fixtures/`)

To guarantee that unit tests, contract tests, and offline runs execute without requiring live API keys or active internet connections (`HYG-07`):

1. `tests/fixtures/sample_openaq_locations.json`: Full Bengaluru `/v3/locations` API response containing 8 stations with active PM2.5/PM10 sensors [VERIFIED].
2. `tests/fixtures/sample_openaq_latest.json`: Multi-station `/v3/locations/{id}/latest` response containing:
   * Valid PM2.5 and PM10 measurements.
   * Injected edge cases: missing station ID, negative value (`-5.0`), implausible spike (`1250.0 µg/m³`), future timestamp, invalid unit (`ppm`).
3. `tests/fixtures/sample_openaq_measurements.json`: Multi-hour time-series data for BTM Layout station.
4. `tests/fixtures/sample_weather_forecast.json`: Open-Meteo forecast response with 48 hours of temperature, humidity, wind speed, and precipitation for Bengaluru.
5. `tests/fixtures/sample_weather_archive.json`: Historical archive payload covering 30 days.

### 8.2 Offline Mode Execution (`--offline`)

* When the `--offline` CLI flag is set or when API calls fail repeatedly:
  * Collectors load data directly from `tests/fixtures/` or local cache in `data/sample/`.
  * HDFS Storage adapter detects daemon unavailability and writes directly to local directory `data/local_hdfs/` without throwing exceptions.
  * Pipeline completes full normalization and validation cycle deterministically, enabling complete testing in air-gapped lab or CI environments [CITED].

---

## 9. Automation & Scheduling Architecture

### 9.1 Local Cron Script (`scripts/cron_collect.sh`)

* Invoked hourly via system crontab: `0 * * * * /path/to/SkyWatch/scripts/cron_collect.sh >> /var/log/skywatch_cron.log 2>&1`.
* Features lockfile handling (`flock -n /tmp/skywatch_collect.lock`) to prevent overlapping executions if network latency extends collection time [CITED].
* Loads appropriate profile (`source profiles/modern.env`), runs `skywatch collect --mode=incremental`, and uploads new partitions to HDFS if available.

### 9.2 GitHub Actions Cron Workflow (`.github/workflows/scheduled-collect.yml`)

* Runs every 30 minutes (`schedule: - cron: '*/30 * * * *'`).
* Ingests OpenAQ readings using `secrets.OPENAQ_API_KEY`.
* Commits raw JSON responses and updated manifest to a dedicated `data` branch.
* The local big data pipeline pulls from this branch to populate HDFS, ensuring continuous data accumulation even when developer laptops are powered off [CITED].

---

## 10. Phase 1 Implementation Plan Decomposition

Phase 1 is decomposed into three focused, dependency-ordered plans:

### Plan 01-01: Foundation, Profiles, Schemas, Scaffolding & Contracts
* **Requirements:** `HYG-01`, `HYG-02`, `OPS-06`, `OPS-07`
* **Artifacts Created:**
  * Directory scaffolding: `src/skywatch/`, `profiles/`, `config/`, `data/`, `tests/`
  * Dual-profile environment files: `profiles/modern.env` and `profiles/lab.env`
  * Profile switching and prerequisite validation: `scripts/use_profile.sh`, `src/skywatch/pipeline/profile.py`
  * Canonical schema definition: `config/schemas.yaml`
  * Schema contract reader and validator: `src/skywatch/data/schemas.py`
  * Structured run logging module with `run_id`: `src/skywatch/pipeline/logging.py`
  * Contract tests: `tests/contract/test_schemas.py`

### Plan 01-02: Collectors, API Clients, Validation & Normalization
* **Requirements:** `ING-01`, `ING-02`, `ING-03`, `ING-04`, `HYG-03`, `HYG-04`, `HYG-05`
* **Artifacts Created:**
  * Mock test fixtures: `tests/fixtures/sample_openaq_*.json`, `tests/fixtures/sample_weather_*.json`
  * Resilient API clients: `src/skywatch/collector/openaq_client.py`, `src/skywatch/collector/weather_client.py` (with backoff, 429 `Retry-After`, auth protection)
  * Station discovery module: `src/skywatch/collector/discovery.py` -> `config/stations.json`
  * Data hygiene & validation engine: `src/skywatch/data/validation.py` (range, unit, ceiling, reject reasons)
  * Feed normalizers: `src/skywatch/data/normalization.py` (UTC enforcement, unit canonicalization)
  * Ingestion deduplicator: `src/skywatch/data/deduplication.py` (`record_key` hashing)
  * Raw preservation handler and manifest ledger: `src/skywatch/storage/raw_manifest.py`
  * Comprehensive unit tests: `tests/unit/test_validation.py`, `test_normalization.py`, `test_deduplication.py`, `test_clients.py`

### Plan 01-03: Storage Architecture, Watermarking, Backfill & Automation
* **Requirements:** `ING-05`, `ING-06`, `HYG-06`, `HYG-07`
* **Artifacts Created:**
  * Local atomic storage handler: `src/skywatch/storage/local_storage.py` (atomic temp write & replace)
  * HDFS storage wrapper: `src/skywatch/storage/hdfs_storage.py` (`hdfs dfs` CLI wrapper, safemode check, idempotent upload)
  * State & watermark manager: `src/skywatch/storage/watermark.py` (per-sensor incremental state)
  * Ingestion orchestrator & backfill engine: `src/skywatch/collector/collector.py` (chunked backfills, offline fallback)
  * CLI entrypoint commands: `src/skywatch/cli.py` (`discover`, `collect`, `backfill`)
  * Automation scripts: `scripts/cron_collect.sh`, `.github/workflows/scheduled-collect.yml`
  * Storage and backfill integration tests: `tests/integration/test_storage.py`, `test_watermark.py`

---

## 11. Verification Checklist for Phase 1

1. **Station Discovery Test**: Running `python -m skywatch.cli discover` successfully queries OpenAQ v3 (or verified fixture) and outputs ≥5 valid Bengaluru stations to `config/stations.json`.
2. **Hygiene Validation Test**: All test cases in `UT-01` through `UT-12` pass (rejecting negative values, missing fields, implausible spikes >1000 µg/m³, and non-UTC timestamps).
3. **Atomic Write Test**: Simulated crash or interruption during write leaves prior files byte-identical without partial `.tmp` file corruption (`UT-21`).
4. **Watermark Idempotency Test**: Ingesting the same time window twice produces zero duplicate rows and updates watermarks without regression.
5. **Dual Profile Verification**: Running prerequisite checks across both `profiles/modern.env` and `profiles/lab.env` passes without syntax or version errors.
6. **Offline Mode Test**: Ingesting with network disabled and `--offline` flag executes entirely from fixtures, producing valid normalized CSV files in `data/normalized/`.
