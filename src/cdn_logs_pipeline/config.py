"""Load pipeline configuration from YAML and environment variables."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

_ENV_PATTERN = re.compile(r"\$\{([A-Z0-9_]+)\}")


def _expand_env(value: Any) -> Any:
    """Replace ${VAR} placeholders with environment values."""
    if isinstance(value, str):

        def repl(match: re.Match[str]) -> str:
            key = match.group(1)
            return os.environ.get(key, match.group(0))

        return _ENV_PATTERN.sub(repl, value)
    if isinstance(value, dict):
        return {k: _expand_env(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_expand_env(v) for v in value]
    return value


@dataclass(frozen=True)
class SparkConfig:
    app_name: str
    shuffle_partitions: int
    coalesce_on_write: int


@dataclass(frozen=True)
class InputConfig:
    format: str
    path: str


@dataclass(frozen=True)
class OutputConfig:
    path: str
    format: str
    mode: str
    partition_columns: list[str]


@dataclass(frozen=True)
class MetricsConfig:
    error_status_threshold: int
    cache_hit_values: list[str]


@dataclass(frozen=True)
class ObservabilityConfig:
    path: str
    enabled: bool


@dataclass(frozen=True)
class PipelineConfig:
    spark: SparkConfig
    input: InputConfig
    output: OutputConfig
    dimensions_edge_regions_path: str
    observability: ObservabilityConfig
    metrics: MetricsConfig
    raw: dict[str, Any] = field(repr=False, default_factory=dict)


def load_config(config_path: str | Path | None = None) -> PipelineConfig:
    """Load and validate pipeline configuration."""
    path = Path(config_path or os.environ.get("PIPELINE_CONFIG", "config/pipeline.yaml"))
    if not path.is_file():
        raise FileNotFoundError(f"Pipeline config not found: {path}")

    with path.open(encoding="utf-8") as fh:
        data = _expand_env(yaml.safe_load(fh))

    spark = data["spark"]
    inp = data["input"]
    out = data["output"]
    metrics = data["metrics"]
    obs = data["observability"]

    return PipelineConfig(
        spark=SparkConfig(
            app_name=spark["app_name"],
            shuffle_partitions=int(spark["shuffle_partitions"]),
            coalesce_on_write=int(spark["coalesce_on_write"]),
        ),
        input=InputConfig(format=inp["format"], path=inp["path"]),
        output=OutputConfig(
            path=out["path"],
            format=out["format"],
            mode=out["mode"],
            partition_columns=list(out["partition_columns"]),
        ),
        dimensions_edge_regions_path=data["dimensions"]["edge_regions_path"],
        observability=ObservabilityConfig(
            path=obs["path"],
            enabled=bool(obs["enabled"]),
        ),
        metrics=MetricsConfig(
            error_status_threshold=int(metrics["error_status_threshold"]),
            cache_hit_values=[str(v) for v in metrics["cache_hit_values"]],
        ),
        raw=data,
    )
