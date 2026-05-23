#!/usr/bin/env python3
"""Generate sample CDN access logs for local pipeline runs."""

from __future__ import annotations

import argparse
import json
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

REGIONS = ["us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"]
METHODS = ["GET", "GET", "GET", "HEAD"]
CACHE_STATUSES = ["HIT", "MISS", "HIT", "TCP_HIT", "TCP_MISS"]
URIS = ["/live/stream.m3u8", "/vod/segment.ts", "/static/logo.png", "/api/health"]


def _random_row(dt: str) -> dict:
    status = random.choices([200, 206, 301, 404, 500], weights=[85, 5, 2, 6, 2])[0]
    return {
        "timestamp": (
            datetime.now(timezone.utc) - timedelta(seconds=random.randint(0, 86_400))
        ).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "client_ip": f"10.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 254)}",
        "method": random.choice(METHODS),
        "uri": random.choice(URIS),
        "status": status,
        "bytes": random.randint(512, 5_000_000),
        "time_taken_ms": random.randint(5, 2500 if status >= 500 else 800),
        "cache_status": random.choice(CACHE_STATUSES),
        "edge_region": random.choice(REGIONS),
        "request_id": str(uuid.uuid4()),
        "dt": dt,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path, default=Path("data/sample"))
    parser.add_argument("--rows", type=int, default=500)
    parser.add_argument("--date", default=datetime.now(timezone.utc).strftime("%Y-%m-%d"))
    args = parser.parse_args()

    out_dir = args.output_dir / f"dt={args.date}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "cdn_access.jsonl"

    with out_file.open("w", encoding="utf-8") as fh:
        for _ in range(args.rows):
            fh.write(json.dumps(_random_row(args.date)) + "\n")

    dim_dir = args.output_dir.parent / "dimensions" / "edge_regions"
    dim_dir.mkdir(parents=True, exist_ok=True)
    dim_file = dim_dir / "regions.jsonl"
    if not dim_file.exists():
        with dim_file.open("w", encoding="utf-8") as fh:
            for region in REGIONS:
                fh.write(
                    json.dumps(
                        {
                            "edge_region": region,
                            "region_tier": "primary" if "us-" in region else "secondary",
                            "region_display_name": region.replace("-", " ").title(),
                        }
                    )
                    + "\n"
                )

    print(f"Wrote {args.rows} rows to {out_file}")
    print(f"Dimension table at {dim_file}")


if __name__ == "__main__":
    main()
