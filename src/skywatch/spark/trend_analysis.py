"""Spark analytics job: Hourly and daily temporal trend analysis and peak-hour detection (SPK-02)."""
from __future__ import annotations

from typing import Tuple

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    avg,
    col,
    count,
    date_format,
    hour,
    to_timestamp,
    when,
)
from pyspark.sql.functions import (
    max as spark_max,
)
from pyspark.sql.functions import (
    round as spark_round,
)


def compute_temporal_trends(df_clean: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """Analyze temporal profiles and identify peak pollution hours per station.

    Args:
        df_clean: DataFrame conforming to clean_air_quality schema.

    Returns:
        Tuple of (trends_hourly_df, trends_daily_df).
    """
    df_valid = df_clean.filter(col("value").isNotNull() & (col("is_valid") == True))  # noqa: E712

    # Parse UTC measurement timestamp
    df_parsed = df_valid.withColumn(
        "ts",
        to_timestamp(col("measurement_time_utc")),
    ).withColumn(
        "hour_of_day",
        hour("ts").cast("integer"),
    ).withColumn(
        "day_of_week",
        date_format("ts", "EEEE"),
    )

    # 1. Hourly trends
    df_hourly = df_parsed.groupBy("station_id", "parameter", "hour_of_day").agg(
        spark_round(avg("value"), 2).alias("average_value"),
        count("value").cast("integer").alias("reading_count"),
    )

    # Peak hour detection: maximum average_value per station and parameter
    peak_window = Window.partitionBy("station_id", "parameter")
    df_hourly_peaks = df_hourly.withColumn(
        "max_hourly_avg",
        spark_max("average_value").over(peak_window),
    ).withColumn(
        "is_peak_hour",
        when(col("average_value") == col("max_hourly_avg"), True).otherwise(False),
    ).drop("max_hourly_avg")

    df_trends_hourly = df_hourly_peaks.select(
        "station_id",
        "parameter",
        "hour_of_day",
        "average_value",
        "reading_count",
        "is_peak_hour",
    ).orderBy("station_id", "parameter", "hour_of_day")

    # 2. Daily trends
    df_daily = df_parsed.groupBy("station_id", "parameter", "day_of_week").agg(
        spark_round(avg("value"), 2).alias("average_value"),
        count("value").cast("integer").alias("reading_count"),
    )

    df_trends_daily = df_daily.select(
        "station_id",
        "parameter",
        "day_of_week",
        "average_value",
        "reading_count",
    ).orderBy("station_id", "parameter", "day_of_week")

    return df_trends_hourly, df_trends_daily
