"""Allow: python -m cdn_logs_pipeline"""

import sys

from cdn_logs_pipeline.job import run

if __name__ == "__main__":
    config_arg = sys.argv[1] if len(sys.argv) > 1 else None
    sys.exit(run(config_arg))
