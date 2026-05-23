"""Read CDN access logs from S3 or local paths."""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from cdn_logs_pipeline.config import PipelineConfig
from cdn_logs_pipeline.schema import CDN_ACCESS_LOG_SCHEMA

logger = logging.getLogger(__name__)


def read_access_logs(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    """
    Load raw CDN logs and normalize columns for downstream aggregation.

    Supports Hive-style date partitions in the input path (dt=YYYY-MM-DD).
    Repartitioning by dt before heavy shuffles keeps regional aggregates local
    when input layout matches output partitions.
    """
    path = config.input.path
    logger.info("Reading CDN access logs from %s (format=%s)", path, config.input.format)

    reader = (
        spark.read.format(config.input.format)
        .schema(CDN_ACCESS_LOG_SCHEMA)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
    )

    # partitionDiscovery works for s3a://bucket/prefix/dt=2024-01-01/
    if "dt=" in path or path.endswith("/"):
        reader = reader.option("basePath", path.rstrip("/"))

    df = reader.load(path)

    # PERMISSIVE mode adds _corrupt_record only when malformed lines exist
    if "_corrupt_record" in df.columns:
        df = df.filter(F.col("_corrupt_record").isNull()).drop("_corrupt_record")

    df = (
        df.withColumn("event_ts", F.to_timestamp("timestamp"))
        .withColumn("dt", F.coalesce(F.col("dt"), F.date_format("event_ts", "yyyy-MM-dd")))
        .withColumn("status", F.col("status").cast("int"))
        .withColumn("time_taken_ms", F.col("time_taken_ms").cast("int"))
        .withColumn("bytes", F.coalesce(F.col("bytes"), F.lit(0)).cast("long"))
        .filter(F.col("edge_region").isNotNull())
    )

    return df
