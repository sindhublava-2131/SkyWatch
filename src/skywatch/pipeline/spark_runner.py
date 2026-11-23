"""Master Spark analytics orchestrator and standalone simulation fallback runner (SPK-01 to SPK-08)."""
from __future__ import annotations

import argparse
import csv
import os
import shutil
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

from skywatch.analytics_core.cpcb import load_breakpoints
from skywatch.data.schemas import get_column_names
from skywatch.pipeline.logging import generate_run_id, get_logger
from skywatch.pipeline.profile import ProfileConfig, _check_java_version, load_profile
from skywatch.spark.alert_engine import generate_all_alerts
from skywatch.spark.export import export_published_dataset
from skywatch.spark.index_job import (
    calculate_cpcb_rolling_index_records,
    load_stations_metadata,
)
from skywatch.spark.quality_report import build_quality_report_records
from skywatch.spark.weather_analysis import calculate_pearson_r

logger = get_logger("spark_runner")


@dataclass
class SparkRunResult:
    """Encapsulates execution result of a Spark analytical pipeline stage."""
    job_name: str
    returncode: int
    duration_seconds: float
    output_dir: str
    records_processed: int
    success: bool
    error_message: Optional[str] = None
    engine: str = "APACHE_SPARK"


def is_spark_available(spark_home: Optional[str] = None) -> bool:
    """Check if Apache Spark / PySpark runtime and valid Java environment are accessible."""
    try:
        import pyspark  # noqa: F401
    except ImportError:
        return False

    java_home = os.environ.get("JAVA_HOME")
    if not java_home:
        # Check if java is on PATH
        java_cmd = shutil.which("java")
        return java_cmd is not None

    version_str = _check_java_version(java_home)
    return version_str is not None


def _load_clean_air_records(input_path: Path) -> List[Dict[str, Any]]:
    """Load clean air quality CSV records handling single files or directory globbing."""
    files: List[Path] = []
    if input_path.is_file():
        files.append(input_path)
    elif input_path.is_dir():
        for p in input_path.glob("**/*"):
            if p.is_file() and not p.name.startswith((".", "_")) and p.suffix.lower() in (".csv", ""):
                files.append(p)

    if not files:
        return []

    clean_cols = get_column_names("clean_air_quality")
    raw_cols = get_column_names("air_quality")
    records: List[Dict[str, Any]] = []

    for f in files:
        with open(f, "r", encoding="utf-8") as fp:
            reader = csv.reader(fp)
            for row in reader:
                if not row:
                    continue
                # Check for header row
                if row[0] == "record_key":
                    continue

                if len(row) >= len(clean_cols):
                    rec = {col_name: row[i] for i, col_name in enumerate(clean_cols)}
                elif len(row) >= len(raw_cols):
                    rec = {col_name: row[i] for i, col_name in enumerate(raw_cols)}
                else:
                    continue

                # Cast numeric fields
                try:
                    val_str = rec.get("value", "")
                    if val_str is not None and val_str != "":
                        rec["value"] = float(val_str)
                    else:
                        rec["value"] = None
                except (ValueError, TypeError):
                    rec["value"] = None

                is_valid_str = str(rec.get("is_valid", "true")).lower()
                rec["is_valid"] = is_valid_str in ("true", "1")

                records.append(rec)

    return records


def _load_weather_records(input_path: Path) -> List[Dict[str, Any]]:
    """Load normalized weather records from CSV file or folder."""
    files: List[Path] = []
    if input_path.is_file():
        files.append(input_path)
    elif input_path.is_dir():
        for p in input_path.glob("**/*"):
            if p.is_file() and not p.name.startswith((".", "_")) and p.suffix.lower() in (".csv", ""):
                files.append(p)

    if not files:
        return []

    records: List[Dict[str, Any]] = []
    for f in files:
        with open(f, "r", encoding="utf-8") as fp:
            reader = csv.DictReader(fp)
            for row in reader:
                # Cast numeric fields
                for num_col in ("temperature_c", "humidity_pct", "wind_speed", "precipitation_mm"):
                    val = row.get(num_col)
                    if val is not None and val != "":
                        try:
                            row[num_col] = float(val)
                        except (ValueError, TypeError):
                            row[num_col] = None
                    else:
                        row[num_col] = None
                records.append(row)

    return records


def _run_standalone_spark_fallback(
    input_clean_dir: Path,
    input_weather_dir: Path,
    output_published_dir: Path,
    run_id: str,
    generated_at_utc: Optional[str] = None,
    stations_path: Optional[str] = None,
) -> List[SparkRunResult]:
    """Pure-Python standalone simulation fallback for Spark analytics jobs."""
    start_time = time.time()
    ts = generated_at_utc or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    stations_meta = load_stations_metadata(stations_path)
    breakpoints = load_breakpoints()

    logger.info("Starting standalone Spark analytics simulation fallback (run_id=%s)", run_id)

    # 1. Load inputs
    clean_records = _load_clean_air_records(input_clean_dir)
    weather_records = _load_weather_records(input_weather_dir)

    valid_clean = [
        r for r in clean_records
        if r.get("value") is not None and r.get("is_valid") is True
    ]

    results: List[SparkRunResult] = []

    # 2. Compute station statistics & rankings (SPK-01)
    stat_groups: Dict[Tuple[str, str, str], List[float]] = {}
    for r in valid_clean:
        st_id = str(r["station_id"])
        st_name = str(r.get("station_name") or stations_meta.get(st_id, f"Station {st_id}"))
        param = str(r["parameter"])
        val = float(r["value"])
        key = (st_id, st_name, param)
        if key not in stat_groups:
            stat_groups[key] = []
        stat_groups[key].append(val)

    stats_list: List[Dict[str, Any]] = []
    for (st_id, st_name, param), values in stat_groups.items():
        stats_list.append({
            "station_id": st_id,
            "station_name": st_name,
            "parameter": param,
            "reading_count": len(values),
            "average_value": round(sum(values) / len(values), 2),
            "minimum_value": round(min(values), 2),
            "maximum_value": round(max(values), 2),
        })

    # Compute dense pollution rankings per parameter
    by_param: Dict[str, List[Dict[str, Any]]] = {}
    for s in stats_list:
        p = s["parameter"]
        if p not in by_param:
            by_param[p] = []
        by_param[p].append(s)

    ranked_stats: List[Dict[str, Any]] = []
    for p, items in by_param.items():
        # Sort descending by average_value, then ascending station_id
        items.sort(key=lambda x: (-x["average_value"], x["station_id"]))
        current_rank = 1
        for i, item in enumerate(items):
            if i > 0 and item["average_value"] < items[i - 1]["average_value"]:
                current_rank += 1
            item["pollution_rank"] = current_rank
            item["generated_at_utc"] = ts
            ranked_stats.append(item)

    ranked_stats.sort(key=lambda x: (x["parameter"], x["pollution_rank"], x["station_id"]))
    path_stats = export_published_dataset(ranked_stats, "station_statistics", output_published_dir, run_id)
    results.append(SparkRunResult(
        job_name="station_statistics",
        returncode=0,
        duration_seconds=round(time.time() - start_time, 2),
        output_dir=str(path_stats),
        records_processed=len(ranked_stats),
        success=True,
    ))

    # 3. Compute temporal trends (hourly & daily) (SPK-02)
    hourly_groups: Dict[Tuple[str, str, int], List[float]] = {}
    daily_groups: Dict[Tuple[str, str, str], List[float]] = {}
    hourly_rollup_groups: Dict[Tuple[str, str, str], List[float]] = {}

    for r in valid_clean:
        st_id = str(r["station_id"])
        param = str(r["parameter"])
        val = float(r["value"])
        raw_ts = str(r["measurement_time_utc"]).strip()
        if raw_ts.endswith("Z"):
            raw_ts = raw_ts[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(raw_ts)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
        except Exception:
            continue

        h = dt.hour
        day_str = dt.strftime("%A")
        bucket_utc = dt.strftime("%Y-%m-%dT%H:00:00Z")

        hk = (st_id, param, h)
        if hk not in hourly_groups:
            hourly_groups[hk] = []
        hourly_groups[hk].append(val)

        dk = (st_id, param, day_str)
        if dk not in daily_groups:
            daily_groups[dk] = []
        daily_groups[dk].append(val)

        hrk = (st_id, param, bucket_utc)
        if hrk not in hourly_rollup_groups:
            hourly_rollup_groups[hrk] = []
        hourly_rollup_groups[hrk].append(val)

    # Hourly trends
    raw_hourly_trends: List[Dict[str, Any]] = []
    for (st_id, param, h), vals in hourly_groups.items():
        raw_hourly_trends.append({
            "station_id": st_id,
            "parameter": param,
            "hour_of_day": h,
            "average_value": round(sum(vals) / len(vals), 2),
            "reading_count": len(vals),
        })

    # Flag peak hour per (station_id, parameter)
    max_by_station_param: Dict[Tuple[str, str], float] = {}
    for item in raw_hourly_trends:
        k = (item["station_id"], item["parameter"])
        if k not in max_by_station_param or item["average_value"] > max_by_station_param[k]:
            max_by_station_param[k] = item["average_value"]

    trends_hourly_final: List[Dict[str, Any]] = []
    for item in raw_hourly_trends:
        k = (item["station_id"], item["parameter"])
        is_peak = (item["average_value"] == max_by_station_param[k])
        item["is_peak_hour"] = is_peak
        trends_hourly_final.append(item)

    trends_hourly_final.sort(key=lambda x: (x["station_id"], x["parameter"], x["hour_of_day"]))
    path_th = export_published_dataset(trends_hourly_final, "trends_hourly", output_published_dir, run_id)
    results.append(SparkRunResult(
        job_name="trends_hourly",
        returncode=0,
        duration_seconds=round(time.time() - start_time, 2),
        output_dir=str(path_th),
        records_processed=len(trends_hourly_final),
        success=True,
    ))

    # Daily trends
    trends_daily_final: List[Dict[str, Any]] = []
    for (st_id, param, day_str), vals in daily_groups.items():
        trends_daily_final.append({
            "station_id": st_id,
            "parameter": param,
            "day_of_week": day_str,
            "average_value": round(sum(vals) / len(vals), 2),
            "reading_count": len(vals),
        })
    trends_daily_final.sort(key=lambda x: (x["station_id"], x["parameter"], x["day_of_week"]))
    path_td = export_published_dataset(trends_daily_final, "trends_daily", output_published_dir, run_id)
    results.append(SparkRunResult(
        job_name="trends_daily",
        returncode=0,
        duration_seconds=round(time.time() - start_time, 2),
        output_dir=str(path_td),
        records_processed=len(trends_daily_final),
        success=True,
    ))

    # Build regularized hourly records for downstream weather join and CPCB
    hourly_records: List[Dict[str, Any]] = []
    for (st_id, param, bucket_utc), vals in hourly_rollup_groups.items():
        hourly_records.append({
            "station_id": st_id,
            "parameter": param,
            "bucket_start_utc": bucket_utc,
            "reading_count": len(vals),
            "average_value": round(sum(vals) / len(vals), 1),
            "minimum_value": min(vals),
            "maximum_value": max(vals),
        })

    # 4. Weather correlation (SPK-03)
    weather_by_hour: Dict[str, Dict[str, float]] = {}
    for wr in weather_records:
        w_ts = str(wr.get("measurement_time_utc", "")).strip()[:13] + ":00:00Z"
        weather_by_hour[w_ts] = {
            "temperature_c": wr.get("temperature_c"),
            "humidity_pct": wr.get("humidity_pct"),
            "wind_speed": wr.get("wind_speed"),
            "precipitation_mm": wr.get("precipitation_mm"),
        }

    # Group paired observations by (station_id, parameter, weather_var)
    weather_vars = ["temperature_c", "humidity_pct", "wind_speed", "precipitation_mm"]
    corr_pairs: Dict[Tuple[str, str, str], Tuple[List[float], List[float]]] = {}

    for hr in hourly_records:
        st_id = hr["station_id"]
        param = hr["parameter"]
        hour_key = hr["bucket_start_utc"]
        air_val = hr["average_value"]

        if hour_key in weather_by_hour:
            w_data = weather_by_hour[hour_key]
            for var in weather_vars:
                w_val = w_data.get(var)
                if w_val is not None and air_val is not None:
                    cp_key = (st_id, param, var)
                    if cp_key not in corr_pairs:
                        corr_pairs[cp_key] = ([], [])
                    corr_pairs[cp_key][0].append(air_val)
                    corr_pairs[cp_key][1].append(w_val)

    corr_records: List[Dict[str, Any]] = []
    # Ensure all (station_id, parameter, var) combinations are covered
    distinct_st_params = sorted(set((hr["station_id"], hr["parameter"]) for hr in hourly_records))
    for st_id, param in distinct_st_params:
        for var in weather_vars:
            cp_key = (st_id, param, var)
            if cp_key in corr_pairs:
                x_vals, y_vals = corr_pairs[cp_key]
                r, n, tier = calculate_pearson_r(x_vals, y_vals)
            else:
                r, n, tier = None, 0, "Low"

            corr_records.append({
                "station_id": st_id,
                "pollutant": param,
                "weather_variable": var,
                "pearson_r": r,
                "sample_count": n,
                "confidence_tier": tier,
            })

    corr_records.sort(key=lambda x: (x["station_id"], x["pollutant"], x["weather_variable"]))
    path_wc = export_published_dataset(corr_records, "weather_correlation", output_published_dir, run_id)
    results.append(SparkRunResult(
        job_name="weather_correlation",
        returncode=0,
        duration_seconds=round(time.time() - start_time, 2),
        output_dir=str(path_wc),
        records_processed=len(corr_records),
        success=True,
    ))

    # 5. 24h rolling CPCB AQI (SPK-04)
    cpcb_records = calculate_cpcb_rolling_index_records(hourly_records, stations_meta, breakpoints)
    path_cpcb = export_published_dataset(cpcb_records, "cpcb_index", output_published_dir, run_id)
    results.append(SparkRunResult(
        job_name="cpcb_index",
        returncode=0,
        duration_seconds=round(time.time() - start_time, 2),
        output_dir=str(path_cpcb),
        records_processed=len(cpcb_records),
        success=True,
    ))

    # 6. Alerts & Spike detection (SPK-05, SPK-08)
    alert_records = generate_all_alerts(
        cpcb_records,
        hourly_records,
        clean_records,
        reference_time_utc=ts,
        stations_meta=stations_meta,
    )
    alert_records.sort(key=lambda x: (x["severity"], x["timestamp_utc"]))
    path_alerts = export_published_dataset(alert_records, "alerts", output_published_dir, run_id)
    results.append(SparkRunResult(
        job_name="alerts",
        returncode=0,
        duration_seconds=round(time.time() - start_time, 2),
        output_dir=str(path_alerts),
        records_processed=len(alert_records),
        success=True,
    ))

    # 7. Quality audit report (SPK-07)
    stage_summaries = [
        {
            "stage": "ingestion",
            "accepted_count": len(clean_records),
            "rejected_count": 0,
            "duplicate_count": 0,
            "reason_counts": {},
        },
        {
            "stage": "pig_etl",
            "accepted_count": len(valid_clean),
            "rejected_count": len(clean_records) - len(valid_clean),
            "duplicate_count": 0,
            "reason_counts": {"INVALID_FILTERED": len(clean_records) - len(valid_clean)},
        },
        {
            "stage": "spark_analytics",
            "accepted_count": len(cpcb_records),
            "rejected_count": 0,
            "duplicate_count": 0,
            "reason_counts": {},
        },
    ]
    report_records = build_quality_report_records(run_id, stage_summaries, ts)
    path_qr = export_published_dataset(report_records, "quality_report", output_published_dir, run_id)
    results.append(SparkRunResult(
        job_name="quality_report",
        returncode=0,
        duration_seconds=round(time.time() - start_time, 2),
        output_dir=str(path_qr),
        records_processed=len(report_records),
        success=True,
    ))

    total_duration = round(time.time() - start_time, 2)
    logger.info("Completed Spark analytics simulation fallback in %.2fs (7 datasets published)", total_duration)
    for r in results:
        r.engine = "STANDALONE_PYTHON_SIMULATION"
    return results


def run_spark_analytics(
    input_clean_dir: Union[str, Path] = Path("output/clean_air_quality"),
    input_weather_dir: Union[str, Path] = Path("output/weather"),
    output_published_dir: Union[str, Path] = Path("output/published"),
    profile: Optional[ProfileConfig] = None,
    force_simulation: bool = False,
    run_id: Optional[str] = None,
    generated_at_utc: Optional[str] = None,
    require_real_engine: bool = False,
) -> List[SparkRunResult]:
    """Execute end-to-end Spark analytical jobs with automatic simulation fallback.

    Args:
        input_clean_dir: Path to clean air quality data.
        input_weather_dir: Path to normalized weather data.
        output_published_dir: Base output directory for published tables.
        profile: Optional active ProfileConfig.
        force_simulation: If True, bypass Spark JVM and run pure-Python fallback.
        run_id: Optional pipeline execution identifier.
        generated_at_utc: Optional fixed timestamp for reproducible runs and tests.
        require_real_engine: If True, fail if native Apache Spark is unavailable.

    Returns:
        List of SparkRunResult objects.
    """
    active_profile = profile if profile is not None else load_profile()
    active_run_id = run_id or generate_run_id()

    clean_path = Path(input_clean_dir)
    weather_path = Path(input_weather_dir)
    out_path = Path(output_published_dir)

    require_real = (
        require_real_engine
        or os.environ.get("SKYWATCH_REQUIRE_REAL_ENGINE", "").lower() in ("1", "true", "yes")
    )

    # Use pure-Python simulation fallback if requested or if Spark JVM is unavailable
    if force_simulation:
        if require_real:
            logger.error("[ENGINE: APACHE_SPARK] Simulation fallback requested but real engine is required")
            raise RuntimeError("Cannot force simulation fallback when real Spark engine is required (SKYWATCH_REQUIRE_REAL_ENGINE=1).")
        logger.warning("[ENGINE: STANDALONE_PYTHON_SIMULATION] Running via standalone simulation fallback (force_simulation=True)")
        print("[ENGINE: STANDALONE_PYTHON_SIMULATION] Executing via standalone Python simulation (force_simulation=True)")
        res = _run_standalone_spark_fallback(
            clean_path,
            weather_path,
            out_path,
            active_run_id,
            generated_at_utc=generated_at_utc,
        )
        for r in res:
            r.engine = "STANDALONE_PYTHON_SIMULATION"
        return res

    if not is_spark_available(active_profile.spark_home):
        if require_real:
            logger.error(
                "[ENGINE: APACHE_SPARK] Apache Spark runtime not available and real engine is required",
                extra={"spark_home": active_profile.spark_home},
            )
            raise RuntimeError(
                f"Apache Spark runtime not available (SPARK_HOME='{active_profile.spark_home}') and real engine is required."
            )
        logger.warning(
            "[ENGINE: STANDALONE_PYTHON_SIMULATION] Apache Spark runtime not found; executing via standalone Python fallback",
            extra={"spark_home": active_profile.spark_home},
        )
        print("[ENGINE: STANDALONE_PYTHON_SIMULATION] Executing via standalone Python simulation (Spark runtime not found)")
        res = _run_standalone_spark_fallback(
            clean_path,
            weather_path,
            out_path,
            active_run_id,
            generated_at_utc=generated_at_utc,
        )
        for r in res:
            r.engine = "STANDALONE_PYTHON_SIMULATION"
        return res

    # Distributed Spark Execution
    logger.info("[ENGINE: APACHE_SPARK] Starting native PySpark execution pipeline (run_id=%s)", active_run_id)
    print(f"[ENGINE: APACHE_SPARK] Starting native PySpark execution pipeline (run_id={active_run_id})")
    try:
        from skywatch.spark.alert_engine import generate_alerts_dataframe, generate_all_alerts
        from skywatch.spark.common import get_spark, read_clean_dataset
        from skywatch.spark.index_job import compute_cpcb_rolling_index
        from skywatch.spark.quality_report import build_quality_report
        from skywatch.spark.station_statistics import compute_station_statistics
        from skywatch.spark.trend_analysis import compute_temporal_trends
        from skywatch.spark.weather_analysis import compute_weather_correlation

        start_time = time.time()
        spark = get_spark(app_name="SkyWatchAnalytics", profile=active_profile)

        # 1. Ingest clean air quality and weather
        df_clean = read_clean_dataset(spark, str(clean_path), "clean_air_quality")
        df_weather = read_clean_dataset(spark, str(weather_path), "weather")

        results: List[SparkRunResult] = []

        # 2. Station statistics
        df_stats = compute_station_statistics(df_clean, generated_at_utc=generated_at_utc)
        path_stats = export_published_dataset(df_stats, "station_statistics", out_path, active_run_id)
        results.append(SparkRunResult("station_statistics", 0, round(time.time() - start_time, 2), str(path_stats), df_stats.count(), True, engine="APACHE_SPARK"))

        # 3. Trends hourly & daily
        df_hourly, df_daily = compute_temporal_trends(df_clean)
        path_th = export_published_dataset(df_hourly, "trends_hourly", out_path, active_run_id)
        path_td = export_published_dataset(df_daily, "trends_daily", out_path, active_run_id)
        results.append(SparkRunResult("trends_hourly", 0, round(time.time() - start_time, 2), str(path_th), df_hourly.count(), True, engine="APACHE_SPARK"))
        results.append(SparkRunResult("trends_daily", 0, round(time.time() - start_time, 2), str(path_td), df_daily.count(), True, engine="APACHE_SPARK"))

        # 4. Weather correlation
        df_corr = compute_weather_correlation(df_hourly, df_weather)
        path_corr = export_published_dataset(df_corr, "weather_correlation", out_path, active_run_id)
        results.append(SparkRunResult("weather_correlation", 0, round(time.time() - start_time, 2), str(path_corr), df_corr.count(), True, engine="APACHE_SPARK"))

        # 5. CPCB rolling index
        df_cpcb = compute_cpcb_rolling_index(df_hourly)
        path_cpcb = export_published_dataset(df_cpcb, "cpcb_index", out_path, active_run_id)
        results.append(SparkRunResult("cpcb_index", 0, round(time.time() - start_time, 2), str(path_cpcb), df_cpcb.count(), True, engine="APACHE_SPARK"))

        # 6. Alerts
        clean_rows = [r.asDict() for r in df_clean.collect()]
        cpcb_rows = [r.asDict() for r in df_cpcb.collect()]
        hourly_rows = [r.asDict() for r in df_hourly.collect()]
        alert_rows = generate_all_alerts(cpcb_rows, hourly_rows, clean_rows, reference_time_utc=generated_at_utc)
        df_alerts = generate_alerts_dataframe(spark, alert_rows)
        path_alerts = export_published_dataset(df_alerts, "alerts", out_path, active_run_id)
        results.append(SparkRunResult("alerts", 0, round(time.time() - start_time, 2), str(path_alerts), len(alert_rows), True, engine="APACHE_SPARK"))

        # 7. Quality report
        stage_summaries = [
            {"stage": "ingestion", "accepted_count": len(clean_rows), "rejected_count": 0, "duplicate_count": 0, "reason_counts": {}},
            {"stage": "pig_etl", "accepted_count": len(clean_rows), "rejected_count": 0, "duplicate_count": 0, "reason_counts": {}},
            {"stage": "spark_analytics", "accepted_count": len(cpcb_rows), "rejected_count": 0, "duplicate_count": 0, "reason_counts": {}},
        ]
        df_qr = build_quality_report(spark, active_run_id, stage_summaries, generated_at_utc=generated_at_utc)
        path_qr = export_published_dataset(df_qr, "quality_report", out_path, active_run_id)
        results.append(SparkRunResult("quality_report", 0, round(time.time() - start_time, 2), str(path_qr), df_qr.count(), True, engine="APACHE_SPARK"))

        return results

    except Exception as e:
        if require_real:
            logger.error("[ENGINE: APACHE_SPARK] Native PySpark execution failed and real engine is required: %s", str(e))
            raise RuntimeError(f"Native PySpark execution failed and real engine is required: {e}") from e

        logger.warning("[ENGINE: STANDALONE_PYTHON_SIMULATION] Native PySpark execution failed (%s); falling back to standalone simulation", str(e))
        print(f"[ENGINE: STANDALONE_PYTHON_SIMULATION] Native PySpark execution failed ({e}); falling back to standalone simulation")
        res = _run_standalone_spark_fallback(
            clean_path,
            weather_path,
            out_path,
            active_run_id,
            generated_at_utc=generated_at_utc,
        )
        for r in res:
            r.engine = "STANDALONE_PYTHON_SIMULATION"
        return res


def main() -> None:
    parser = argparse.ArgumentParser(description="SkyWatch Spark Analytics Engine")
    parser.add_argument("--input-clean", default="output/clean_air_quality", help="Path to clean air quality data")
    parser.add_argument("--input-weather", default="output/weather", help="Path to normalized weather data")
    parser.add_argument("--output", default="output/published", help="Output directory for published datasets")
    parser.add_argument("--profile", default=None, help="Active profile name (modern/lab)")
    parser.add_argument("--simulate", "--offline", action="store_true", dest="simulate", help="Force standalone simulation")
    parser.add_argument("--require-real-engine", action="store_true", help="Fail if Apache Spark runtime is unavailable")
    parser.add_argument("--run-id", default=None, help="Explicit run ID")

    args = parser.parse_args()
    results = run_spark_analytics(
        input_clean_dir=args.input_clean,
        input_weather_dir=args.input_weather,
        output_published_dir=args.output,
        profile=load_profile(args.profile),
        force_simulation=args.simulate,
        run_id=args.run_id,
        require_real_engine=args.require_real_engine,
    )

    all_success = all(r.success for r in results)
    engine_name = results[0].engine if results else "UNKNOWN"
    print(f"Spark Analytics completed: {len(results)} stages, success={all_success}, engine={engine_name}")
    for r in results:
        print(f" - [{r.job_name}] engine={r.engine} records={r.records_processed} duration={r.duration_seconds}s dir={r.output_dir}")


if __name__ == "__main__":
    main()
