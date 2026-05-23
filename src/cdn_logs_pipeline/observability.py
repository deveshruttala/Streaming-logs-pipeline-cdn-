"""Job-level metrics for pipeline health alongside analytical outputs."""

from __future__ import annotations

import json
import logging
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from cdn_logs_pipeline.config import MetricsConfig, PipelineConfig

logger = logging.getLogger(__name__)


@dataclass
class JobMetrics:
    """Snapshot written next to Parquet results for alerting and SLO tracking."""

    run_id: str
    started_at: str
    finished_at: str
    duration_seconds: float
    input_path: str
    output_path: str
    rows_read: int
    rows_written: int
    corrupt_rows: int
    distinct_regions: int
    global_cache_hit_ratio: float
    global_error_rate: float
    global_latency_p95_ms: float
    status: str
    spark_app_id: str | None = None
    extra: dict[str, Any] | None = None


def collect_pipeline_metrics(
    raw_logs: DataFrame,
    aggregates: DataFrame,
    metrics_cfg: MetricsConfig,
    run_id: str,
    started_at: datetime,
    finished_at: datetime,
    config: PipelineConfig,
    spark_app_id: str | None,
    corrupt_count: int = 0,
) -> JobMetrics:
    """Compute rollups used by on-call dashboards and data-quality checks."""
    from cdn_logs_pipeline.transforms import _is_cache_hit

    is_hit = _is_cache_hit(F.col("cache_status"), metrics_cfg.cache_hit_values)
    is_error = F.col("status") >= metrics_cfg.error_status_threshold

    global_stats = raw_logs.agg(
        F.count(F.lit(1)).alias("rows"),
        F.sum(F.when(is_hit, 1).otherwise(0)).alias("hits"),
        F.sum(F.when(is_error, 1).otherwise(0)).alias("errors"),
        F.expr("percentile_approx(time_taken_ms, 0.95)").alias("p95"),
    ).collect()[0]

    rows_read = int(global_stats["rows"] or 0)
    hits = int(global_stats["hits"] or 0)
    errors = int(global_stats["errors"] or 0)
    p95 = float(global_stats["p95"] or 0.0)

    agg_stats = aggregates.agg(
        F.sum("request_count").alias("written_requests"),
        F.countDistinct("edge_region").alias("regions"),
    ).collect()[0]

    rows_written = int(agg_stats["written_requests"] or 0)
    regions = int(agg_stats["regions"] or 0)

    duration = (finished_at - started_at).total_seconds()

    return JobMetrics(
        run_id=run_id,
        started_at=started_at.replace(tzinfo=timezone.utc).isoformat(),
        finished_at=finished_at.replace(tzinfo=timezone.utc).isoformat(),
        duration_seconds=round(duration, 3),
        input_path=config.input.path,
        output_path=config.output.path,
        rows_read=rows_read,
        rows_written=rows_written,
        corrupt_rows=corrupt_count,
        distinct_regions=regions,
        global_cache_hit_ratio=round(hits / rows_read, 4) if rows_read else 0.0,
        global_error_rate=round(errors / rows_read, 4) if rows_read else 0.0,
        global_latency_p95_ms=round(p95, 2),
        status="success",
        spark_app_id=spark_app_id,
    )


def persist_job_metrics(metrics: JobMetrics, config: PipelineConfig) -> str:
    """Write metrics JSON to observability path (local or S3)."""
    if not config.observability.enabled:
        logger.info("Observability disabled; skipping metrics write")
        return ""

    payload = asdict(metrics)
    body = json.dumps(payload, indent=2)
    dest_root = config.observability.path.rstrip("/")
    rel_path = f"run_id={metrics.run_id}/metrics.json"
    full_uri = f"{dest_root}/{rel_path}"

    scheme = urlparse(dest_root).scheme
    if scheme in ("", "file"):
        local_root = dest_root.replace("file://", "")
        out_file = Path(local_root) / rel_path
        out_file.parent.mkdir(parents=True, exist_ok=True)
        out_file.write_text(body, encoding="utf-8")
        logger.info("Wrote job metrics to %s", out_file)
        return str(out_file)

    # s3a:// — use Hadoop FS via Spark JVM
    logger.info("Wrote job metrics to %s (use aws s3 cp or console to fetch)", full_uri)
    _write_via_hadoop(full_uri, body, config)
    return full_uri


def _write_via_hadoop(uri: str, body: str, config: PipelineConfig) -> None:
    """Write observability JSON to S3 using the active Spark Hadoop configuration."""
    from pyspark.sql import SparkSession

    spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError("No active Spark session for S3 observability write")

    jvm = spark._jvm
    conf = spark._jsc.hadoopConfiguration()
    path = jvm.org.apache.hadoop.fs.Path(uri)
    fs = path.getFileSystem(conf)
    out = fs.create(path, True)
    out.write(bytearray(body.encode("utf-8")))
    out.close()


def new_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
