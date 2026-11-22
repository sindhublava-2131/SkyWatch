"""Common PySpark configuration, session initialization, and schema mapping."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, Optional

from skywatch.data.schemas import get_schema
from skywatch.pipeline.profile import ProfileConfig, load_profile

try:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.types import (
        BooleanType,
        DataType,
        DoubleType,
        FloatType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
    )
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False

    class DataType:  # type: ignore[no-redef]
        def __repr__(self) -> str:
            return self.__class__.__name__

        def __eq__(self, other: Any) -> bool:
            return isinstance(other, self.__class__)

    class StringType(DataType):  # type: ignore[no-redef]
        pass

    class DoubleType(DataType):  # type: ignore[no-redef]
        pass

    class FloatType(DataType):  # type: ignore[no-redef]
        pass

    class IntegerType(DataType):  # type: ignore[no-redef]
        pass

    class LongType(DataType):  # type: ignore[no-redef]
        pass

    class BooleanType(DataType):  # type: ignore[no-redef]
        pass

    class StructField:  # type: ignore[no-redef]
        def __init__(self, name: str, dataType: DataType, nullable: bool = True):
            self.name = name
            self.dataType = dataType
            self.nullable = nullable

        def __repr__(self) -> str:
            return f"StructField('{self.name}', {self.dataType}, {self.nullable})"

        def __eq__(self, other: Any) -> bool:
            if not isinstance(other, StructField):
                return False
            return (
                self.name == other.name
                and self.dataType == other.dataType
                and self.nullable == other.nullable
            )

    class StructType:  # type: ignore[no-redef]
        def __init__(self, fields: Optional[list] = None):
            self.fields = list(fields or [])

        def add(self, field: Any) -> StructType:
            self.fields.append(field)
            return self

        def __iter__(self):
            return iter(self.fields)

        def __len__(self) -> int:
            return len(self.fields)

        def __getitem__(self, idx: Any) -> Any:
            return self.fields[idx]

        def __repr__(self) -> str:
            return f"StructType({self.fields})"

        def __eq__(self, other: Any) -> bool:
            if not isinstance(other, StructType):
                return False
            return self.fields == other.fields

    class SparkSession:  # type: ignore[no-redef]
        pass

    class DataFrame:  # type: ignore[no-redef]
        pass


TYPE_MAP: Dict[str, DataType] = {
    "string": StringType(),
    "double": DoubleType(),
    "float": DoubleType(),
    "integer": IntegerType(),
    "int": IntegerType(),
    "long": LongType(),
    "boolean": BooleanType(),
}


def build_spark_schema(
    entity_name: str,
    config_path: Optional[str] = None,
) -> StructType:
    """Build a PySpark StructType schema dynamically from canonical config/schemas.yaml.

    Disallows inferSchema by constructing an explicit typed StructType matching the contract.

    Args:
        entity_name: Name of schema entity defined in config/schemas.yaml.
        config_path: Optional path to schemas.yaml file.

    Returns:
        Explicit PySpark StructType.
    """
    schema_def = get_schema(entity_name, config_path)
    columns = schema_def.get("columns", [])

    fields = []
    for col in columns:
        col_name = str(col["name"])
        raw_type = str(col.get("type", "string")).lower()
        nullable = bool(col.get("nullable", True))

        spark_type = TYPE_MAP.get(raw_type, StringType())
        fields.append(StructField(col_name, spark_type, nullable))

    return StructType(fields)


def get_spark(
    app_name: str = "SkyWatchAnalytics",
    profile: Optional[ProfileConfig] = None,
    master: str = "local[2]",
) -> SparkSession:
    """Initialize a deterministic, low-memory SparkSession.

    Configuration constraints:
        - master("local[2]")
        - spark.driver.memory = "2g"
        - spark.sql.shuffle.partitions = "2"
        - spark.sql.session.timeZone = "UTC"
        - spark.sql.ansi.enabled = "false"
        - spark.ui.enabled = "false"

    Args:
        app_name: Name of the Spark application.
        profile: Optional active ProfileConfig instance.
        master: Spark master URL (default 'local[2]').

    Returns:
        SparkSession instance.
    """
    if not PYSPARK_AVAILABLE:
        raise RuntimeError(
            "PySpark is not installed in the active Python environment. "
            "Use standalone simulation fallback or install pyspark."
        )

    active_profile = profile if profile is not None else load_profile()

    # Bind JAVA_HOME for Spark execution if configured in active profile
    if active_profile.java_home_spark:
        java_dir = Path(active_profile.java_home_spark)
        if java_dir.is_dir():
            os.environ["JAVA_HOME"] = str(java_dir)

    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.ui.enabled", "false")
    )
    return builder.getOrCreate()


def read_clean_dataset(
    spark: SparkSession,
    dataset_path: str,
    entity_name: str = "clean_air_quality",
    config_path: Optional[str] = None,
) -> DataFrame:
    """Read a clean CSV dataset using explicit StructType from config/schemas.yaml.

    Never uses inferSchema=true.

    Args:
        spark: Active SparkSession.
        dataset_path: Path to dataset file or folder.
        entity_name: Canonical schema entity name (default 'clean_air_quality').
        config_path: Optional path to schemas.yaml.

    Returns:
        PySpark DataFrame adhering strictly to canonical schema.
    """
    schema = build_spark_schema(entity_name, config_path)
    schema_def = get_schema(entity_name, config_path)
    has_header = str(schema_def.get("has_header", False)).lower()
    delimiter = str(schema_def.get("delimiter", ","))

    return (
        spark.read.format("csv")
        .schema(schema)
        .option("header", has_header)
        .option("delimiter", delimiter)
        .load(str(dataset_path))
    )
