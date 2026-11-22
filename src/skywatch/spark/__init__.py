"""Apache Spark analytics package for SkyWatch."""
from __future__ import annotations

from skywatch.spark.common import (
    build_spark_schema,
    get_spark,
    read_clean_dataset,
)

__all__ = [
    "get_spark",
    "build_spark_schema",
    "read_clean_dataset",
]
