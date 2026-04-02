import sys
import logging
import argparse
from datetime import date, datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(BASE_DIR))

logging.getLogger("numexpr").setLevel(logging.WARNING)

from src.core.fetch_config import FetchConfig
from src.data.sync_checker import SyncChecker


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Check sync status across ingest, processed, and curated layers."
    )
    parser.add_argument(
        "--from", dest="from_date", type=str, default=None,
        help="Start date (YYYY-MM-DD). Optional.",
    )
    parser.add_argument(
        "--to", dest="to_date", type=str, default=None,
        help="End date (YYYY-MM-DD). Optional.",
    )
    return parser.parse_args()


def parse_date(val: str) -> date:
    try:
        return datetime.strptime(val, "%Y-%m-%d").date()
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Invalid date format: '{val}'. Expected YYYY-MM-DD."
        )


def main():
    args = parse_args()

    from_date = parse_date(args.from_date) if args.from_date else None
    to_date = parse_date(args.to_date) if args.to_date else None

    config = FetchConfig(BASE_DIR, use_year_partition=True)
    checker = SyncChecker(config)
    checker.run(mode="full", from_date=from_date, to_date=to_date)

    print("Sync check complete. See logs/data_pipeline_fetch.log for results.")


if __name__ == "__main__":
    main()

#run
"""
python -m scripts.check_sync_status
python -m scripts.check_sync_status --from 2026-03-01 --to 2026-03-01
"""
