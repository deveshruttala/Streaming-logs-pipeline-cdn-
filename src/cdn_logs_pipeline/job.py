"""Main pipeline orchestration: read → aggregate → enrich → write → observability."""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timezone

from pyspark.sql import functions as F

from cdn_logs_pipeline.config import load_config
from cdn_logs_pipeline.schema import CDN_ACCESS_LOG_SCHEMA
from cdn_logs_pipeline.observability import (
    collect_pipeline_metrics,
    new_run_id,
    persist_job_metrics,
)
from cdn_logs_pipeline.reader import read_access_logs
from cdn_logs_pipeline.spark_session import build_spark
from cdn_logs_pipeline.transforms import (
    aggregate_by_region,
    enrich_with_region_metadata,
    read_edge_region_dimension,
)
from cdn_logs_pipeline.writer import write_aggregates

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


def run(config_path: str | None = None) -> int:
    """Execute the CDN log analytics batch job."""
    run_id = new_run_id()
    started_at = datetime.now(timezone.utc)
    config = load_config(config_path)

    if not config.input.path or config.input.path.startswith("${"):
        logger.error("INPUT_PATH is not set. Copy .env.example to .env or export INPUT_PATH.")
        return 1

    spark = build_spark(config)
    spark.sparkContext.setLogLevel("WARN")

    try:
        logger.info("Starting pipeline run_id=%s app=%s", run_id, config.spark.app_name)

        logs = read_access_logs(spark, config)
        # Re-read once for corrupt tally (cheap on sample/local volumes)
        corrupt_df = (
            spark.read.format(config.input.format)
            .schema(CDN_ACCESS_LOG_SCHEMA)
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .load(config.input.path)
        )
        corrupt_count = (
            corrupt_df.filter(F.col("_corrupt_record").isNotNull()).count()
            if "_corrupt_record" in corrupt_df.columns
            else 0
        )
        logs.cache()

        aggregates = aggregate_by_region(logs, config.metrics)
        dimension = read_edge_region_dimension(spark, config.dimensions_edge_regions_path)
        enriched = enrich_with_region_metadata(aggregates, dimension, spark)

        output_path = write_aggregates(enriched, config)

        finished_at = datetime.now(timezone.utc)
        job_metrics = collect_pipeline_metrics(
            raw_logs=logs,
            aggregates=enriched,
            metrics_cfg=config.metrics,
            run_id=run_id,
            started_at=started_at,
            finished_at=finished_at,
            config=config,
            spark_app_id=spark.sparkContext.applicationId,
            corrupt_count=corrupt_count,
        )
        metrics_path = persist_job_metrics(job_metrics, config)

        logger.info(
            "Pipeline complete run_id=%s output=%s observability=%s rows_read=%d",
            run_id,
            output_path,
            metrics_path,
            job_metrics.rows_read,
        )
        return 0
    except Exception:
        logger.exception("Pipeline failed run_id=%s", run_id)
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(run())
