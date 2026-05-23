"""Unit tests for aggregation and enrichment logic."""

import sys

import pytest

pytestmark = pytest.mark.skipif(
    sys.version_info >= (3, 14),
    reason="PySpark is not compatible with Python 3.14+ for executor serialization",
)

from pyspark.sql import Row

from cdn_logs_pipeline.config import MetricsConfig
from cdn_logs_pipeline.transforms import aggregate_by_region, enrich_with_region_metadata


def _metrics_config() -> MetricsConfig:
    return MetricsConfig(error_status_threshold=400, cache_hit_values=["HIT", "TCP_HIT"])


def test_aggregate_by_region_cache_and_errors(spark):
    rows = [
        Row(
            timestamp="2024-06-01T12:00:00Z",
            edge_region="us-east-1",
            dt="2024-06-01",
            status=200,
            bytes=1000,
            time_taken_ms=50,
            cache_status="HIT",
        ),
        Row(
            timestamp="2024-06-01T12:01:00Z",
            edge_region="us-east-1",
            dt="2024-06-01",
            status=404,
            bytes=0,
            time_taken_ms=120,
            cache_status="MISS",
        ),
        Row(
            timestamp="2024-06-01T12:02:00Z",
            edge_region="eu-west-1",
            dt="2024-06-01",
            status=500,
            bytes=100,
            time_taken_ms=900,
            cache_status="MISS",
        ),
    ]
    logs = spark.createDataFrame(rows)
    result = aggregate_by_region(logs, _metrics_config()).collect()

    by_region = {r.edge_region: r for r in result}
    us = by_region["us-east-1"]
    assert us.request_count == 2
    assert us.cache_hit_count == 1
    assert abs(us.cache_hit_ratio - 0.5) < 0.001
    assert us.error_count == 1
    assert abs(us.error_rate - 0.5) < 0.001

    eu = by_region["eu-west-1"]
    assert eu.error_count == 1
    assert eu.cache_hit_count == 0


def test_enrich_broadcast_join(spark):
    agg = spark.createDataFrame(
        [Row(edge_region="us-east-1", dt="2024-06-01", request_count=10, cache_hit_ratio=0.9)]
    )
    dim = spark.createDataFrame(
        [Row(edge_region="us-east-1", region_tier="primary", region_display_name="US East")]
    )
    enriched = enrich_with_region_metadata(agg, dim, spark).collect()[0]
    assert enriched.region_tier == "primary"
    assert enriched.region_display_name == "US East"
