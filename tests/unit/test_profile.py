"""Unit tests for profile loading and environment validation."""
from __future__ import annotations

import os
from unittest import mock

from skywatch.pipeline.profile import (
    ProfileConfig,
    load_profile,
    validate_environment,
)


def test_load_profile_modern(tmp_path):
    profiles_dir = tmp_path / "profiles"
    profiles_dir.mkdir()
    (profiles_dir / "modern.env").write_text(
        'SKYWATCH_PROFILE="modern"\n'
        'HADOOP_HOME="/custom/hadoop"\n'
        'PIG_HOME="/custom/pig"\n'
        'SPARK_HOME="/custom/spark"\n'
        'JAVA_HOME_PIG="/custom/java8"\n'
        'JAVA_HOME_SPARK="/custom/java17"\n'
        'HDFS_ROOT="/skywatch_test"\n'
        'SKYWATCH_HTTP_TIMEOUT=25\n'
        'SKYWATCH_MAX_RETRIES=4\n',
        encoding="utf-8",
    )

    with mock.patch.dict(os.environ, {}, clear=True):
        config = load_profile("modern", profiles_dir=str(profiles_dir))

    assert config.profile_name == "modern"
    assert config.hadoop_home == "/custom/hadoop"
    assert config.pig_home == "/custom/pig"
    assert config.spark_home == "/custom/spark"
    assert config.java_home_pig == "/custom/java8"
    assert config.java_home_spark == "/custom/java17"
    assert config.hdfs_root == "/skywatch_test"
    assert config.http_timeout == 25
    assert config.max_retries == 4


def test_load_profile_lab_defaults(tmp_path):
    profiles_dir = tmp_path / "profiles"
    profiles_dir.mkdir()
    (profiles_dir / "lab.env").write_text(
        'SKYWATCH_PROFILE="lab"\n'
        'HADOOP_HOME="/opt/hadoop-3.2.2"\n'
        'PIG_HOME="/opt/pig-0.18.0"\n'
        'SPARK_HOME="/opt/spark-3.2.2"\n'
        'JAVA_HOME_PIG="/opt/jdk8"\n'
        'JAVA_HOME_SPARK="/opt/jdk8"\n'
        'HDFS_ROOT="/skywatch"\n',
        encoding="utf-8",
    )

    with mock.patch.dict(os.environ, {}, clear=True):
        config = load_profile("lab", profiles_dir=str(profiles_dir))

    assert config.profile_name == "lab"
    assert config.hadoop_home == "/opt/hadoop-3.2.2"
    assert config.java_home_spark == "/opt/jdk8"


def test_load_profile_prioritizes_file_over_ambient_env(tmp_path):
    # CR-05: Ambient env vars should not overwrite values when a profile is explicitly loaded
    profiles_dir = tmp_path / "profiles"
    profiles_dir.mkdir()
    (profiles_dir / "lab.env").write_text(
        'HADOOP_HOME="/opt/lab-hadoop"\n',
        encoding="utf-8",
    )
    with mock.patch.dict(os.environ, {"HADOOP_HOME": "/opt/ambient-hadoop"}):
        config = load_profile("lab", profiles_dir=str(profiles_dir))
        assert config.hadoop_home == "/opt/lab-hadoop"


def test_validate_environment_nonexistent_paths():
    config = ProfileConfig(
        profile_name="modern",
        hadoop_home="/nonexistent/hadoop",
        pig_home="/nonexistent/pig",
        spark_home="/nonexistent/spark",
        java_home_pig="/nonexistent/java8",
        java_home_spark="/nonexistent/java17",
        hdfs_root="/skywatch",
    )
    results = validate_environment(config)
    assert results["hadoop_home_exists"] is False
    assert results["pig_home_exists"] is False
    assert results["spark_home_exists"] is False
    assert results["java_pig_is_jdk8"] is False
    assert results["java_spark_version_ok"] is False


@mock.patch("skywatch.pipeline.profile._check_java_version")
def test_validate_environment_mocked_java(mock_check_java, tmp_path):
    hadoop_dir = tmp_path / "hadoop"
    hadoop_dir.mkdir()
    pig_dir = tmp_path / "pig"
    pig_dir.mkdir()
    spark_dir = tmp_path / "spark"
    spark_dir.mkdir()
    java_pig = tmp_path / "java8"
    java_pig.mkdir()
    java_spark = tmp_path / "java17"
    java_spark.mkdir()

    config = ProfileConfig(
        profile_name="modern",
        hadoop_home=str(hadoop_dir),
        pig_home=str(pig_dir),
        spark_home=str(spark_dir),
        java_home_pig=str(java_pig),
        java_home_spark=str(java_spark),
        hdfs_root="/skywatch",
    )

    def side_effect(path):
        if "java8" in path:
            return 'openjdk version "1.8.0_382"'
        if "java17" in path:
            return 'openjdk version "17.0.8" 2023-07-18'
        return None

    mock_check_java.side_effect = side_effect
    results = validate_environment(config)

    assert results["hadoop_home_exists"] is True
    assert results["pig_home_exists"] is True
    assert results["spark_home_exists"] is True
    assert results["java_pig_is_jdk8"] is True
    assert results["java_spark_version_ok"] is True
