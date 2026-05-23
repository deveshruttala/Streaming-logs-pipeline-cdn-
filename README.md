# Streaming CDN Log Analytics (PySpark)

Batch pipeline that ingests CDN access logs from S3, aggregates QoE and cache metrics by edge region, and writes Parquet for Athena/Spark SQL. Job-level observability metrics are stored alongside outputs for pipeline health checks.

## Features

- Read JSON CDN access logs from **S3** (`s3a://`) or local paths
- **Regional aggregates**: request volume, bytes served, cache-hit ratio, error rate, latency (p50 / p95 / p99, avg)
- **Parquet output** with `dt` and `edge_region` partitions for efficient downstream queries
- **Join optimization**: broadcast join for small region dimension tables; repartitioned shuffle join when the dimension grows
- **Partition tuning**: repartition on write keys to control file size and avoid small files
- **Observability**: per-run JSON metrics (rows in/out, global cache-hit ratio, error rate, p95 latency, corrupt rows)

## Project layout

```
config/pipeline.yaml          # Defaults (paths from env)
src/cdn_logs_pipeline/        # Pipeline code
scripts/run_local.sh          # Local run with sample data
data/sample/                  # Generated test logs
data/output/                  # Local Parquet + metrics (gitignored)
tests/                        # PySpark unit tests
```

## Setup

**Requirements:** Python 3.10–3.13, Java 11+ (for Spark), AWS credentials when using S3.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

Copy environment template and set paths:

```bash
cp .env.example .env
# Edit INPUT_PATH, OUTPUT_PATH, OBSERVABILITY_PATH, DIMENSION_PATH
```

**Local run (no AWS):**

```bash
chmod +x scripts/run_local.sh
./scripts/run_local.sh
```

**Production (EMR / Spark on EKS / Databricks):**

```bash
export INPUT_PATH=s3a://your-bucket/cdn-logs/raw/
export OUTPUT_PATH=s3a://your-bucket/cdn-logs/aggregated/
export OBSERVABILITY_PATH=s3a://your-bucket/cdn-logs/observability/
export DIMENSION_PATH=s3a://your-bucket/cdn-logs/dimensions/edge_regions/

spark-submit \
  --packages org.apache.hadoop:hadoop-aws:3.3.4 \
  src/cdn_logs_pipeline/job.py
```

Or: `python -m cdn_logs_pipeline`

**Tests:**

```bash
pytest
```

## Input format

JSON lines per request (one object per line):

```json
{
  "timestamp": "2024-06-01T12:00:00Z",
  "client_ip": "10.0.1.2",
  "method": "GET",
  "uri": "/vod/segment.ts",
  "status": 200,
  "bytes": 1048576,
  "time_taken_ms": 42,
  "cache_status": "HIT",
  "edge_region": "us-east-1",
  "request_id": "uuid"
}
```

Optional Hive layout: `s3a://bucket/prefix/dt=2024-06-01/*.jsonl`

## Solution

1. **Ingest** — Spark reads logs with a fixed schema; corrupt lines are counted for data-quality alerts.
2. **Transform** — Group by `edge_region` and `dt`; compute cache-hit ratio, HTTP error rate, and latency percentiles.
3. **Enrich** — Left join a small edge-region dimension table (broadcast when &lt; 10k rows).
4. **Serve** — Write Parquet partitioned by `dt` and `edge_region` for partition-pruned queries in Athena or Spark.
5. **Observe** — Emit `metrics.json` per run under `observability/run_id=…/` with throughput, error rate, and latency SLO inputs.

Example query on output (Spark SQL):

```sql
SELECT edge_region, dt, cache_hit_ratio, error_rate, latency_p95_ms
FROM parquet.`s3a://your-bucket/cdn-logs/aggregated/`
WHERE dt = '2024-06-01'
ORDER BY error_rate DESC;
```
