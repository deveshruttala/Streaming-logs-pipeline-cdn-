"""Pytest fixtures for transform unit tests (no Spark cluster required for logic checks)."""

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("cdn-logs-pipeline-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()
