import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.core.fetch_config import FetchConfig
from src.data.processed_gbond_builder import ProcessedGBondBuilder


def parse_args():
    parser = argparse.ArgumentParser(description="Processed GBond Builder")
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
    builder = ProcessedGBondBuilder(config)
    logger = logging.getLogger("Processed_GBond")
    logger.info("Starting ProcessedGBondBuilder | mode=%s", args.mode)
    builder.run(args.mode)
    logger.info("Done.")


if __name__ == "__main__":
    main()

# run
"""
python scripts/run_processed_gbond.py --mode full
python scripts/run_processed_gbond.py --mode incremental
"""
