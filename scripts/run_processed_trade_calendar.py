import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.core.fetch_config import FetchConfig
from src.data.processed_trade_calendar_builder import ProcessedTradeCalendarBuilder


def parse_args():
    parser = argparse.ArgumentParser(description="Processed Trade Calendar Builder")
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
    builder = ProcessedTradeCalendarBuilder(config)
    logger = logging.getLogger("Processed_TradeCalendar")
    logger.info("Starting ProcessedTradeCalendarBuilder | mode=%s", args.mode)
    builder.run(args.mode)
    logger.info("Done.")


if __name__ == "__main__":
    main()

# run
"""
python scripts/run_processed_trade_calendar.py --mode full
python scripts/run_processed_trade_calendar.py --mode incremental
"""
