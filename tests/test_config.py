"""Config loading tests (no Spark)."""

import os
from pathlib import Path

from cdn_logs_pipeline.config import load_config


def test_load_config_expands_env(tmp_path, monkeypatch):
    cfg_file = tmp_path / "pipeline.yaml"
    cfg_file.write_text(
        """
spark:
  app_name: test-app
  shuffle_partitions: 4
  coalesce_on_write: 2
input:
  format: json
  path: ${INPUT_PATH}
output:
  path: ${OUTPUT_PATH}
  format: parquet
  mode: overwrite
  partition_columns: [dt, edge_region]
dimensions:
  edge_regions_path: ${DIMENSION_PATH}
observability:
  path: ${OBSERVABILITY_PATH}
  enabled: true
metrics:
  error_status_threshold: 400
  cache_hit_values: [HIT]
""",
        encoding="utf-8",
    )
    monkeypatch.setenv("INPUT_PATH", "s3a://bucket/in/")
    monkeypatch.setenv("OUTPUT_PATH", "s3a://bucket/out/")
    monkeypatch.setenv("OBSERVABILITY_PATH", "s3a://bucket/obs/")
    monkeypatch.setenv("DIMENSION_PATH", "s3a://bucket/dim/")

    cfg = load_config(cfg_file)
    assert cfg.input.path == "s3a://bucket/in/"
    assert cfg.spark.shuffle_partitions == 4
    assert cfg.metrics.cache_hit_values == ["HIT"]
