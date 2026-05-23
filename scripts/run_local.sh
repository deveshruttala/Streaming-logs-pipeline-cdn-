#!/usr/bin/env bash
# Run the pipeline locally against generated sample data.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

python3 scripts/generate_sample_logs.py --rows 1000

if [[ ! -d "${ROOT}/.venv" ]]; then
  python3 -m venv "${ROOT}/.venv"
fi
# shellcheck source=/dev/null
source "${ROOT}/.venv/bin/activate"
pip install -q -e ".[dev]"

export INPUT_PATH="file://${ROOT}/data/sample/"
export OUTPUT_PATH="file://${ROOT}/data/output/aggregated/"
export OBSERVABILITY_PATH="file://${ROOT}/data/output/observability/"
export DIMENSION_PATH="file://${ROOT}/data/dimensions/edge_regions/"
export PIPELINE_CONFIG="${ROOT}/config/pipeline.yaml"

python -m cdn_logs_pipeline

echo ""
echo "Aggregates:    ${OUTPUT_PATH}"
echo "Observability: ${OBSERVABILITY_PATH}"
