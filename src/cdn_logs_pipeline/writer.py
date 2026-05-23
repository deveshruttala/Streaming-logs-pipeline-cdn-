"""Write aggregated metrics to Parquet with partition layout."""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame

from cdn_logs_pipeline.config import PipelineConfig
from cdn_logs_pipeline.transforms import repartition_for_write

logger = logging.getLogger(__name__)


def write_aggregates(df: DataFrame, config: PipelineConfig) -> str:
    """Persist regional aggregates as Parquet under dt/edge_region partitions."""
    out = config.output
    path = out.path

    prepared = repartition_for_write(
        df,
        partition_columns=out.partition_columns,
        target_files=config.spark.coalesce_on_write,
    )

    logger.info(
        "Writing aggregates to %s (format=%s, partitions=%s)",
        path,
        out.format,
        out.partition_columns,
    )

    writer = prepared.write.mode(out.mode).format(out.format)
    if out.partition_columns:
        writer = writer.partitionBy(*out.partition_columns)
    writer.save(path)

    return path
