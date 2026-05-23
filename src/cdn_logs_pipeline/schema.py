"""Spark schemas for CDN access logs and aggregated outputs."""

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Raw CDN access log (JSON lines). Field names align with common edge/CDN exports.
CDN_ACCESS_LOG_SCHEMA = StructType(
    [
        StructField("timestamp", StringType(), nullable=False),
        StructField("client_ip", StringType(), True),
        StructField("method", StringType(), True),
        StructField("uri", StringType(), True),
        StructField("status", IntegerType(), False),
        StructField("bytes", LongType(), True),
        StructField("time_taken_ms", IntegerType(), True),
        StructField("cache_status", StringType(), True),
        StructField("edge_region", StringType(), False),
        # Optional request id for dedup / tracing in downstream systems
        StructField("request_id", StringType(), True),
    ]
)

REGION_AGGREGATE_SCHEMA = StructType(
    [
        StructField("edge_region", StringType(), False),
        StructField("dt", StringType(), False),
        StructField("request_count", LongType(), False),
        StructField("bytes_served", LongType(), False),
        StructField("cache_hit_count", LongType(), False),
        StructField("cache_hit_ratio", DoubleType(), False),
        StructField("error_count", LongType(), False),
        StructField("error_rate", DoubleType(), False),
        StructField("latency_p50_ms", DoubleType(), True),
        StructField("latency_p95_ms", DoubleType(), True),
        StructField("latency_p99_ms", DoubleType(), True),
        StructField("latency_avg_ms", DoubleType(), True),
        # Enriched from dimension join (when available)
        StructField("region_tier", StringType(), True),
        StructField("region_display_name", StringType(), True),
    ]
)

EDGE_REGION_DIMENSION_SCHEMA = StructType(
    [
        StructField("edge_region", StringType(), False),
        StructField("region_tier", StringType(), True),
        StructField("region_display_name", StringType(), True),
    ]
)
