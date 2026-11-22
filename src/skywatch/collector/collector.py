"""Ingestion orchestrator and chunked backfill engine for SkyWatch.

Implements ING-01, ING-02, ING-04, ING-05, HYG-06, HYG-07:
Coordinates API clients, raw preservation, validation, normalization,
deduplication, atomic partition storage, watermark commits, and chunked backfills.
"""
from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from skywatch.collector.discovery import discover_stations
from skywatch.collector.openaq_client import OpenAQClient
from skywatch.collector.weather_client import WeatherClient
from skywatch.data.deduplication import deduplicate_batch
from skywatch.data.normalization import (
    normalize_openaq_record,
    normalize_weather_response,
)
from skywatch.pipeline.logging import get_logger, run_context
from skywatch.storage.hdfs_storage import HDFSStorageAdapter
from skywatch.storage.local_storage import write_normalized_csv
from skywatch.storage.raw_manifest import save_raw_response
from skywatch.storage.watermark import WatermarkManager

logger = get_logger("orchestrator")

BENGALURU_CENTRAL_COORDINATES = (12.91, 77.61)


def _safe_id(val: Any) -> Any:
    """Cast to int if all digits, else preserve original string."""
    s = str(val).strip()
    return int(s) if s.isdigit() else s


class IngestionOrchestrator:
    """End-to-end ingestion orchestrator for environmental feeds."""

    def __init__(
        self,
        openaq_client: Optional[OpenAQClient] = None,
        weather_client: Optional[WeatherClient] = None,
        watermark_manager: Optional[WatermarkManager] = None,
        hdfs_adapter: Optional[HDFSStorageAdapter] = None,
        stations_path: str = "config/stations.json",
        raw_dir: str = "data/raw",
        normalized_dir: str = "data/normalized",
        checkpoint_path: str = "data/.backfill_checkpoint.json",
        offline: bool = False,
    ) -> None:
        self.offline = offline
        self.openaq = openaq_client or OpenAQClient(offline=offline)
        self.weather = weather_client or WeatherClient(offline=offline)
        self.watermark_mgr = watermark_manager or WatermarkManager()
        self.hdfs = hdfs_adapter or HDFSStorageAdapter()
        self.stations_path = Path(stations_path)
        self.raw_dir = raw_dir
        self.normalized_dir = normalized_dir
        self.checkpoint_path = Path(checkpoint_path)

    def _ensure_stations(self) -> List[Dict[str, Any]]:
        """Load stations from disk or run discovery if file does not exist."""
        if self.stations_path.is_file():
            try:
                with open(self.stations_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as exc:
                logger.warning(f"Failed to read {self.stations_path}: {exc}. Re-discovering.")
        return discover_stations(self.openaq, output_file=str(self.stations_path))

    def run_incremental_collection(
        self,
        run_id: Optional[str] = None,
        mode: str = "incremental",
    ) -> Dict[str, Any]:
        """Execute complete collection cycle across all stations."""
        with run_context(run_id) as rid:
            logger.info(f"Starting collection cycle in mode '{mode}' (run_id: {rid})")
            stations = self._ensure_stations()

            now_utc = datetime.now(timezone.utc)
            ingest_dt = now_utc.strftime("%Y-%m-%d")
            ingestion_time_utc = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

            raw_records: List[Dict[str, Any]] = []
            normalized_aq_records: List[Dict[str, Any]] = []
            normalized_weather_records: List[Dict[str, Any]] = []

            # 1. Collect Air Quality Data
            for station in stations:
                station_id = station["station_id"]
                try:
                    if mode == "latest":
                        endpoint = f"/locations/{station_id}/latest"
                        payload = self.openaq.get_latest(_safe_id(station_id))
                        save_raw_response(
                            raw_data=payload,
                            source="openaq",
                            endpoint_url=f"https://api.openaq.org/v3{endpoint}",
                            http_status=200,
                            run_id=rid,
                            base_dir=self.raw_dir,
                            ingest_dt=ingest_dt,
                        )
                        results = payload.get("results", [])
                        raw_records.extend(results)

                        for rec in results:
                            norm = normalize_openaq_record(rec, station, ingestion_time_utc)
                            normalized_aq_records.append(norm)

                            # Update watermark only if record is valid and newer (WR-06)
                            if norm.get("is_valid"):
                                sensor_id = norm["sensor_id"]
                                meas_time = norm["measurement_time_utc"]
                                current_wm = self.watermark_mgr.get_watermark(sensor_id)
                                if not current_wm or meas_time > current_wm:
                                    self.watermark_mgr.update_watermark(
                                        sensor_id=sensor_id,
                                        station_id=station_id,
                                        parameter=norm["parameter"],
                                        last_measurement_utc=meas_time,
                                        records_ingested=1,
                                        run_id=rid,
                                    )

                    else:  # incremental mode
                        for param, sinfo in station.get("sensors", {}).items():
                            sensor_id = sinfo["sensor_id"]
                            wm = self.watermark_mgr.get_watermark(sensor_id)

                            if wm:
                                dt_from = wm
                            else:
                                # Default to 24 hours ago if no previous watermark exists
                                dt_from = (now_utc - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ")
                            dt_to = ingestion_time_utc

                            endpoint = f"/sensors/{sensor_id}/measurements"
                            payload = self.openaq.get_measurements(
                                sensor_id=_safe_id(sensor_id),
                                datetime_from=dt_from,
                                datetime_to=dt_to,
                            )
                            save_raw_response(
                                raw_data=payload,
                                source="openaq",
                                endpoint_url=f"https://api.openaq.org/v3{endpoint}",
                                http_status=200,
                                run_id=rid,
                                base_dir=self.raw_dir,
                                ingest_dt=ingest_dt,
                            )
                            results = payload.get("results", [])
                            raw_records.extend(results)

                            newest_ts = None
                            for rec in results:
                                norm = normalize_openaq_record(rec, station, ingestion_time_utc)
                                normalized_aq_records.append(norm)
                                meas_ts = norm["measurement_time_utc"]
                                if norm.get("is_valid") and (not newest_ts or meas_ts > newest_ts):
                                    newest_ts = meas_ts

                            if newest_ts and (not wm or newest_ts > wm) and len(results) > 0:
                                self.watermark_mgr.update_watermark(
                                    sensor_id=sensor_id,
                                    station_id=station_id,
                                    parameter=param,
                                    last_measurement_utc=newest_ts,
                                    records_ingested=len(results),
                                    run_id=rid,
                                )
                except Exception as exc:
                    logger.warning(f"Error querying station {station_id}: {exc}. Continuing with next station.")

            # 2. Collect Weather Data for Bengaluru central reference
            ref_lat, ref_lon = BENGALURU_CENTRAL_COORDINATES
            weather_payload = self.weather.get_forecast(ref_lat, ref_lon)
            save_raw_response(
                raw_data=weather_payload,
                source="open-meteo",
                endpoint_url="https://api.open-meteo.com/v1/forecast",
                http_status=200,
                run_id=rid,
                base_dir=self.raw_dir,
                ingest_dt=ingest_dt,
            )
            weather_rows = normalize_weather_response(
                weather_payload,
                location_id="bengaluru_central",
                ingestion_time_utc=ingestion_time_utc,
            )
            normalized_weather_records.extend(weather_rows)

            # 3. Deduplication
            unique_aq, aq_dups = deduplicate_batch(normalized_aq_records)
            unique_weather, weather_dups = deduplicate_batch(normalized_weather_records)

            # 4. Write Normalized Partitions
            aq_csv = write_normalized_csv(
                records=unique_aq,
                stream_name="air_quality",
                ingest_dt=ingest_dt,
                run_id=rid,
                base_dir=self.normalized_dir,
            )

            weather_csv = write_normalized_csv(
                records=unique_weather,
                stream_name="weather",
                ingest_dt=ingest_dt,
                run_id=rid,
                base_dir=self.normalized_dir,
            )

            # 5. HDFS Upload / Cache
            self.hdfs.upload_partition(aq_csv, f"normalized/air_quality/ingest_dt={ingest_dt}", rid)
            self.hdfs.upload_partition(weather_csv, f"normalized/weather/ingest_dt={ingest_dt}", rid)

            # 6. Commit Watermarks (Two-Phase Commit)
            self.watermark_mgr.commit(run_id=rid)

            summary = {
                "run_id": rid,
                "stations_polled": len(stations),
                "air_quality_records": len(unique_aq),
                "air_quality_duplicates": aq_dups,
                "weather_records": len(unique_weather),
                "weather_duplicates": weather_dups,
                "air_quality_csv": aq_csv,
                "weather_csv": weather_csv,
            }
            logger.info(f"Collection complete: {summary}")
            return summary

    def run_chunked_backfill(
        self,
        start_date: str,
        end_date: str,
        chunk_days: int = 7,
        run_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Perform chunked historical backfilling with persistent checkpoint resumption."""
        with run_context(run_id) as rid:
            logger.info(f"Initiating chunked backfill: {start_date} to {end_date} (chunk size: {chunk_days}d)")
            dt_start = datetime.strptime(start_date, "%Y-%m-%d")
            dt_end = datetime.strptime(end_date, "%Y-%m-%d")

            # Load checkpoint
            completed_chunks: List[str] = []
            if self.checkpoint_path.is_file():
                try:
                    with open(self.checkpoint_path, "r", encoding="utf-8") as f:
                        data = json.load(f)
                        completed_chunks = data.get("completed_chunks", [])
                except Exception:
                    completed_chunks = []

            curr = dt_start
            processed_chunks = 0
            stations = self._ensure_stations()

            while curr < dt_end:
                chunk_end = min(curr + timedelta(days=chunk_days), dt_end)
                chunk_key = f"{curr.strftime('%Y-%m-%d')}_{chunk_end.strftime('%Y-%m-%d')}"

                if chunk_key in completed_chunks:
                    logger.info(f"Skipping already completed chunk: {chunk_key}")
                    curr = chunk_end
                    continue

                logger.info(f"Processing chunk {chunk_key}...")
                chunk_ingest_dt = chunk_end.strftime("%Y-%m-%d")
                now_str = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

                chunk_aq: List[Dict[str, Any]] = []
                for station in stations:
                    for _param, sinfo in station.get("sensors", {}).items():
                        sensor_id = sinfo["sensor_id"]
                        dt_from_str = curr.strftime("%Y-%m-%dT00:00:00Z")
                        dt_to_str = chunk_end.strftime("%Y-%m-%dT00:00:00Z")
                        payload = self.openaq.get_measurements(
                            sensor_id=_safe_id(sensor_id),
                            datetime_from=dt_from_str,
                            datetime_to=dt_to_str,
                        )
                        # Preserve raw payload during backfill (CR-03)
                        save_raw_response(
                            raw_data=payload,
                            source="openaq",
                            endpoint_url=f"https://api.openaq.org/v3/sensors/{sensor_id}/measurements",
                            http_status=200,
                            run_id=rid,
                            base_dir=self.raw_dir,
                            ingest_dt=chunk_ingest_dt,
                        )
                        for r in payload.get("results", []):
                            chunk_aq.append(normalize_openaq_record(r, station, now_str))

                # Fetch archive weather for chunk
                ref_lat, ref_lon = BENGALURU_CENTRAL_COORDINATES
                weather_payload = self.weather.get_archive(
                    latitude=ref_lat,
                    longitude=ref_lon,
                    start_date=curr.strftime("%Y-%m-%d"),
                    end_date=chunk_end.strftime("%Y-%m-%d"),
                )
                # Preserve raw archive weather response during backfill (CR-03)
                save_raw_response(
                    raw_data=weather_payload,
                    source="open-meteo",
                    endpoint_url="https://archive-api.open-meteo.com/v1/archive",
                    http_status=200,
                    run_id=rid,
                    base_dir=self.raw_dir,
                    ingest_dt=chunk_ingest_dt,
                )
                chunk_weather = normalize_weather_response(
                    weather_payload,
                    location_id="bengaluru_central",
                    ingestion_time_utc=now_str,
                    data_kind="archive_reanalysis",
                )

                unique_aq, _ = deduplicate_batch(chunk_aq)
                unique_w, _ = deduplicate_batch(chunk_weather)

                # Write partitions
                aq_path = write_normalized_csv(unique_aq, "air_quality", chunk_ingest_dt, rid, self.normalized_dir)
                w_path = write_normalized_csv(unique_w, "weather", chunk_ingest_dt, rid, self.normalized_dir)

                self.hdfs.upload_partition(aq_path, f"normalized/air_quality/ingest_dt={chunk_ingest_dt}", rid)
                self.hdfs.upload_partition(w_path, f"normalized/weather/ingest_dt={chunk_ingest_dt}", rid)

                # Update and persist checkpoint with cleanup on failure (WR-10)
                completed_chunks.append(chunk_key)
                self.checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
                tf = tempfile.NamedTemporaryFile("w", dir=str(self.checkpoint_path.parent), delete=False, encoding="utf-8")
                tmp_cp = tf.name
                try:
                    json.dump({"completed_chunks": completed_chunks, "last_updated": now_str}, tf, indent=2)
                    tf.flush()
                    os.fsync(tf.fileno())
                    tf.close()
                    os.replace(tmp_cp, str(self.checkpoint_path))
                except Exception:
                    if os.path.exists(tmp_cp):
                        os.unlink(tmp_cp)
                    raise

                processed_chunks += 1
                curr = chunk_end

            return {
                "run_id": rid,
                "start_date": start_date,
                "end_date": end_date,
                "chunks_processed": processed_chunks,
                "total_completed_chunks": len(completed_chunks),
            }
