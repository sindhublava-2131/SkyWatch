"""Spark analytics job: Hourly weather join and Pearson correlation analysis (SPK-03)."""
from __future__ import annotations

import math
from typing import Any, List, Optional, Tuple

try:
    from pyspark.sql import DataFrame
    from pyspark.sql.functions import (
        col,
        corr,
        count,
        date_trunc,
        isnan,
        lit,
        to_timestamp,
        when,
    )
    from pyspark.sql.functions import (
        round as spark_round,
    )
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False
    DataFrame = Any  # type: ignore[misc,assignment]


def calculate_pearson_r(
    x_vals: List[float],
    y_vals: List[float],
) -> Tuple[Optional[float], int, str]:
    """Calculate Pearson correlation r, sample count n, and confidence tier in pure Python.

    Tiers:
        - n >= 100: "High"
        - 30 <= n < 100: "Medium"
        - n < 30: "Low"

    Returns:
        Tuple of (pearson_r, sample_count, confidence_tier).
    """
    paired = [
        (float(x), float(y))
        for x, y in zip(x_vals, y_vals)
        if x is not None and y is not None and not math.isnan(x) and not math.isnan(y)
    ]
    n = len(paired)

    if n < 2:
        return None, n, "Low"

    mean_x = sum(p[0] for p in paired) / n
    mean_y = sum(p[1] for p in paired) / n

    num = sum((p[0] - mean_x) * (p[1] - mean_y) for p in paired)
    den_x = sum((p[0] - mean_x) ** 2 for p in paired)
    den_y = sum((p[1] - mean_y) ** 2 for p in paired)

    if den_x <= 0.0 or den_y <= 0.0:
        tier = "High" if n >= 100 else ("Medium" if n >= 30 else "Low")
        return None, n, tier

    r = num / math.sqrt(den_x * den_y)
    r_clamped = max(-1.0, min(1.0, r))
    r_rounded = round(r_clamped, 4)

    if n >= 100:
        tier = "High"
    elif n >= 30:
        tier = "Medium"
    else:
        tier = "Low"

    return r_rounded, n, tier


def compute_weather_correlation(
    df_air_hourly: DataFrame,
    df_weather: DataFrame,
) -> DataFrame:
    """Join hourly air quality with weather observations and compute Pearson correlation.

    Args:
        df_air_hourly: Hourly rollup DataFrame (station_id, parameter, bucket_start_utc, average_value).
        df_weather: Normalized weather DataFrame (measurement_time_utc, temperature_c, humidity_pct, wind_speed, precipitation_mm).

    Returns:
        DataFrame conforming to weather_correlation schema.
    """
    # Truncate hourly timestamps for equi-join
    df_air = df_air_hourly.withColumn(
        "join_hour",
        date_trunc("hour", to_timestamp(col("bucket_start_utc"))),
    )
    df_w = df_weather.withColumn(
        "join_hour",
        date_trunc("hour", to_timestamp(col("measurement_time_utc"))),
    )

    df_joined = df_air.join(df_w, on="join_hour", how="inner")

    weather_vars = ["temperature_c", "humidity_pct", "wind_speed", "precipitation_mm"]
    dfs: List[DataFrame] = []

    for var in weather_vars:
        df_var = (
            df_joined.filter(col("average_value").isNotNull() & col(var).isNotNull())
            .groupBy(col("station_id"), col("parameter").alias("pollutant"))
            .agg(
                spark_round(corr("average_value", var), 4).alias("raw_r"),
                count("average_value").cast("integer").alias("sample_count"),
            )
            .withColumn("weather_variable", lit(var))
            .withColumn(
                "pearson_r",
                when(col("sample_count") < 2, lit(None).cast("double"))
                .when(isnan(col("raw_r")), lit(None).cast("double"))
                .otherwise(col("raw_r")),
            )
            .withColumn(
                "confidence_tier",
                when(col("sample_count") >= 100, lit("High"))
                .when(col("sample_count") >= 30, lit("Medium"))
                .otherwise(lit("Low")),
            )
            .select(
                "station_id",
                "pollutant",
                "weather_variable",
                "pearson_r",
                "sample_count",
                "confidence_tier",
            )
        )
        dfs.append(df_var)

    union_df = dfs[0]
    for other in dfs[1:]:
        union_df = union_df.unionByName(other)

    return union_df.orderBy("station_id", "pollutant", "weather_variable")
