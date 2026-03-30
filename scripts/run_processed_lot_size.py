import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.core.fetch_config import FetchConfig
from src.data.processed_lot_size_builder import ProcessedLotSizeBuilder


def parse_args():
    parser = argparse.ArgumentParser(description="Processed Lot Size Builder")
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
    builder = ProcessedLotSizeBuilder(config)
    logger = logging.getLogger("Processed_LotSize")
    logger.info("Starting ProcessedLotSizeBuilder | mode=%s", args.mode)
    builder.run(args.mode)
    logger.info("Done.")


if __name__ == "__main__":
    main()

# run
"""
python scripts/run_processed_lot_size.py --mode full
python scripts/run_processed_lot_size.py --mode incremental
"""
