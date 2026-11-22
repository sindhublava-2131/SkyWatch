"""Spark analytics job: Station summary statistics and pollution rankings (SPK-01)."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import avg, col, count, dense_rank, lit
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import min as spark_min
from pyspark.sql.functions import round as spark_round


def compute_station_statistics(
    df_clean: DataFrame,
    generated_at_utc: Optional[str] = None,
) -> DataFrame:
    """Calculate station summary statistics and dense pollution rankings.

    Schema:
        station_id, station_name, parameter, reading_count, average_value,
        minimum_value, maximum_value, pollution_rank, generated_at_utc

    Args:
        df_clean: DataFrame conforming to clean_air_quality schema.
        generated_at_utc: Optional ISO UTC timestamp string for reproducibility.

    Returns:
        DataFrame conforming to station_statistics schema.
    """
    if generated_at_utc is None:
        timestamp_lit = lit(datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
    else:
        timestamp_lit = lit(generated_at_utc)

    # Filter valid non-null values
    df_valid = df_clean.filter(col("value").isNotNull() & (col("is_valid") == lit(True)))

    # Aggregations per station and parameter
    df_stats = df_valid.groupBy("station_id", "station_name", "parameter").agg(
        count("value").cast("integer").alias("reading_count"),
        spark_round(avg("value"), 2).alias("average_value"),
        spark_round(spark_min("value"), 2).alias("minimum_value"),
        spark_round(spark_max("value"), 2).alias("maximum_value"),
    )

    # Ranking window: per parameter, order by average_value DESC, station_id ASC
    ranking_window = Window.partitionBy("parameter").orderBy(
        col("average_value").desc(),
        col("station_id").asc(),
    )

    df_ranked = df_stats.withColumn(
        "pollution_rank",
        dense_rank().over(ranking_window).cast("integer"),
    ).withColumn(
        "generated_at_utc",
        timestamp_lit,
    )

    # Reorder columns matching canonical schema
    return df_ranked.select(
        "station_id",
        "station_name",
        "parameter",
        "reading_count",
        "average_value",
        "minimum_value",
        "maximum_value",
        "pollution_rank",
        "generated_at_utc",
    )
