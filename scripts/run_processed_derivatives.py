import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.core.fetch_config import FetchConfig
from src.data.processed_derivatives_builder import ProcessedDerivativesBuilder


def parse_args():
    parser = argparse.ArgumentParser(description="Processed Derivatives Builder")
    parser.add_argument(
        "--mode",
        choices=["full", "incremental"],
        required=True,
        help="Run mode: full rebuild or incremental append"
    )
    return parser.parse_args()


def main():
    args = parse_args()
    config = FetchConfig(base_dir=Path(__file__).resolve().parents[1])
    ProcessedDerivativesBuilder(config)
    logger = logging.getLogger("Processed_Derivatives")
    logger.info("Starting ProcessedDerivativesBuilder | mode=%s", args.mode)
    ProcessedDerivativesBuilder(config).run(args.mode)
    logger.info("Done.")


if __name__ == "__main__":
    main()

# run
"""
python scripts/run_processed_derivatives.py --mode full
python scripts/run_processed_derivatives.py --mode incremental
"""
