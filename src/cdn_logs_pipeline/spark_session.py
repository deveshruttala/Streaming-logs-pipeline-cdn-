"""Spark session factory with S3-friendly defaults."""

from __future__ import annotations

from pyspark.sql import SparkSession

from cdn_logs_pipeline.config import PipelineConfig


def build_spark(config: PipelineConfig) -> SparkSession:
    """
    Create a SparkSession tuned for S3 reads/writes and Parquet analytics.

    Uses the Hadoop AWS bundle for s3a:// paths. On EMR or managed Spark,
    many of these settings are already applied by the platform.
    """
    builder = (
        SparkSession.builder.appName(config.spark.app_name)
        .config("spark.sql.shuffle.partitions", str(config.spark.shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        # Parquet: push filters into file reads for partition-pruned queries
        .config("spark.sql.parquet.filterPushdown", "true")
        .config("spark.sql.sources.partitionColumnTypeInference.enabled", "true")
    )

    input_path = config.input.path
    if input_path.startswith("s3a://") or input_path.startswith("s3://"):
        builder = (
            builder.config(
                "spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem",
            )
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
            # Multipart upload tuning for large Parquet writes
            .config("spark.hadoop.fs.s3a.fast.upload", "true")
        )

    return builder.getOrCreate()
