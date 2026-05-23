"""Aggregations, joins, and partition strategies for CDN log analytics."""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from cdn_logs_pipeline.config import MetricsConfig, PipelineConfig
from cdn_logs_pipeline.schema import EDGE_REGION_DIMENSION_SCHEMA

logger = logging.getLogger(__name__)


def _is_cache_hit(cache_status_col: F.Column, hit_values: list[str]) -> F.Column:
    normalized = F.upper(F.trim(cache_status_col))
    return normalized.isin([v.upper() for v in hit_values])


def aggregate_by_region(logs: DataFrame, metrics: MetricsConfig) -> DataFrame:
    """
    Compute per-region, per-day metrics used for capacity and QoE dashboards.

    Metrics:
      - cache_hit_ratio: share of requests served from edge cache
      - latency percentiles: p50 / p95 / p99 of time_taken_ms
      - error_count / error_rate: HTTP status >= threshold (default 400)
    """
    is_hit = _is_cache_hit(F.col("cache_status"), metrics.cache_hit_values)
    is_error = F.col("status") >= metrics.error_status_threshold

    return (
        logs.groupBy("edge_region", "dt")
        .agg(
            F.count(F.lit(1)).alias("request_count"),
            F.sum("bytes").alias("bytes_served"),
            F.sum(F.when(is_hit, 1).otherwise(0)).alias("cache_hit_count"),
            F.sum(F.when(is_error, 1).otherwise(0)).alias("error_count"),
            F.expr("percentile_approx(time_taken_ms, 0.5)").alias("latency_p50_ms"),
            F.expr("percentile_approx(time_taken_ms, 0.95)").alias("latency_p95_ms"),
            F.expr("percentile_approx(time_taken_ms, 0.99)").alias("latency_p99_ms"),
            F.avg("time_taken_ms").alias("latency_avg_ms"),
        )
        .withColumn(
            "cache_hit_ratio",
            F.when(F.col("request_count") > 0, F.col("cache_hit_count") / F.col("request_count")).otherwise(
                F.lit(0.0)
            ),
        )
        .withColumn(
            "error_rate",
            F.when(F.col("request_count") > 0, F.col("error_count") / F.col("request_count")).otherwise(F.lit(0.0)),
        )
    )


def read_edge_region_dimension(spark, path: str) -> DataFrame:
    """Load small region metadata used for enrichment joins."""
    if not path or path.startswith("${"):
        logger.warning("Dimension path not configured; skipping region enrichment")
        return None

    return (
        spark.read.format("json")
        .schema(EDGE_REGION_DIMENSION_SCHEMA)
        .load(path)
        .dropDuplicates(["edge_region"])
    )


def enrich_with_region_metadata(
    aggregates: DataFrame,
    dimension: DataFrame | None,
    spark,
) -> DataFrame:
    """
    Join aggregates with edge-region metadata.

    Uses a broadcast join when the dimension is small (typical for region tables).
    For large dimensions, remove broadcast hint and rely on AQE shuffle join.
    """
    if dimension is None:
        return aggregates.withColumn("region_tier", F.lit(None).cast("string")).withColumn(
            "region_display_name", F.lit(None).cast("string")
        )

    from pyspark.sql.functions import broadcast

    dim_count = dimension.count()
    logger.info("Joining aggregates with edge_region dimension (%d rows)", dim_count)

    if dim_count < 10_000:
        joined = aggregates.join(broadcast(dimension), on="edge_region", how="left")
    else:
        # Shuffle join — partition both sides on join key for even skew handling
        agg_part = aggregates.repartition("edge_region")
        dim_part = dimension.repartition("edge_region")
        joined = agg_part.join(dim_part, on="edge_region", how="left")

    return joined


def repartition_for_write(df: DataFrame, partition_columns: list[str], target_files: int) -> DataFrame:
    """
    Align output file layout with query patterns (filter by dt, then region).

    partitionBy on write creates Hive directories; repartition before write
    controls file count and avoids tiny files on high-cardinality keys.
    """
    if not partition_columns:
        return df.coalesce(target_files)

    # Repartition on partition keys so each task writes one directory subtree
    return df.repartition(target_files, *partition_columns)
