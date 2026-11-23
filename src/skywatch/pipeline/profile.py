"""Profile loader and environment validation for SkyWatch.

Supports switching between 'modern' (2026 stack) and 'lab' (Hadoop 3.2.2 / Spark 3.2.2) profiles.
"""
from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional

try:
    import dotenv
except ImportError:
    dotenv = None


@dataclass
class ProfileConfig:
    profile_name: str
    hadoop_home: str
    pig_home: str
    spark_home: str
    java_home_pig: str
    java_home_spark: str
    hdfs_root: str
    http_timeout: int = 20
    max_retries: int = 5
    fresh_hours: int = 3
    stale_hours: int = 24


def load_profile(
    profile_name: Optional[str] = None,
    profiles_dir: Optional[str] = None,
) -> ProfileConfig:
    """Load configuration for the requested profile name, falling back to active env or 'modern'."""
    raw_name = (
        profile_name
        or os.environ.get("SKYWATCH_PROFILE")
        or "modern"
    ).lower()

    # Enforce safe profile name to prevent path traversal (WR-18)
    clean_profile = "".join(c for c in raw_name if c.isalnum() or c in ("_", "-"))
    if not clean_profile:
        clean_profile = "modern"

    base_dir = Path(profiles_dir) if profiles_dir else Path("profiles")
    profile_file = base_dir / f"{clean_profile}.env"

    file_values: Dict[str, str] = {}
    if profile_file.is_file():
        if dotenv is not None:
            file_values = dotenv.dotenv_values(str(profile_file))
        else:
            with open(profile_file, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    k, v = line.split("=", 1)
                    file_values[k.strip()] = v.strip().strip("'\"")

    def get_val(key: str, default: str) -> str:
        # Prioritize file values if profile was explicitly requested (CR-05), otherwise ambient os.environ
        if profile_name is not None and key in file_values:
            return file_values[key]
        return os.environ.get(key) or file_values.get(key) or default

    return ProfileConfig(
        profile_name=clean_profile,
        hadoop_home=get_val("HADOOP_HOME", "/opt/bigdata/hadoop-3.4.3"),
        pig_home=get_val("PIG_HOME", "/opt/bigdata/pig-0.18.0"),
        spark_home=get_val("SPARK_HOME", "/opt/bigdata/spark-4.3.0-bin-hadoop3"),
        java_home_pig=get_val("JAVA_HOME_PIG", "/usr/lib/jvm/java-8-openjdk-amd64"),
        java_home_spark=get_val("JAVA_HOME_SPARK", "/usr/lib/jvm/java-17-openjdk-amd64"),
        hdfs_root=get_val("HDFS_ROOT", "/skywatch"),
        http_timeout=int(get_val("SKYWATCH_HTTP_TIMEOUT", "20")),
        max_retries=int(get_val("SKYWATCH_MAX_RETRIES", "5")),
        fresh_hours=int(get_val("SKYWATCH_FRESH_HOURS", "3")),
        stale_hours=int(get_val("SKYWATCH_STALE_HOURS", "24")),
    )


def _check_java_version(java_home: str) -> Optional[str]:
    """Execute java -version and return version string or None if unexecutable."""
    if not java_home:
        return None
    java_bin = Path(java_home) / "bin" / ("java.exe" if os.name == "nt" else "java")
    if not java_bin.is_file():
        return None
    try:
        proc = subprocess.run(
            [str(java_bin), "-version"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
        return proc.stderr or proc.stdout
    except Exception:
        return None


def validate_environment(config: ProfileConfig) -> Dict[str, bool]:
    """Validate active profile environment directories and runtimes."""
    results: Dict[str, bool] = {
        "hadoop_home_exists": Path(config.hadoop_home).is_dir(),
        "pig_home_exists": Path(config.pig_home).is_dir(),
        "spark_home_exists": Path(config.spark_home).is_dir(),
        "java_home_pig_exists": Path(config.java_home_pig).is_dir(),
        "java_home_spark_exists": Path(config.java_home_spark).is_dir(),
    }

    pig_java_out = _check_java_version(config.java_home_pig)
    if pig_java_out is not None:
        results["java_pig_is_jdk8"] = "1.8." in pig_java_out or "8" in pig_java_out
    else:
        results["java_pig_is_jdk8"] = False

    spark_java_out = _check_java_version(config.java_home_spark)
    if spark_java_out is not None:
        if config.profile_name == "lab":
            results["java_spark_version_ok"] = "1.8." in spark_java_out or "8" in spark_java_out
        else:
            results["java_spark_version_ok"] = any(
                v in spark_java_out for v in ["17.", "21.", "17", "21"]
            )
    else:
        results["java_spark_version_ok"] = False

    return results
