import argparse
from datetime import datetime
from pathlib import Path
import sys

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(BASE_DIR))

from src.core.fetch_config import FetchConfig
from src.data.master_index_fetch import MasterIndexFetcher


def parse_date(date_str: str):
    return datetime.strptime(date_str, "%Y-%m-%d").date()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True, help="Start date in YYYY-MM-DD format")
    parser.add_argument("--end", required=True, help="End date in YYYY-MM-DD format")

    args = parser.parse_args()

    config = FetchConfig(BASE_DIR)

    fetcher = MasterIndexFetcher(config=config)

    fetcher.run(
        start_date=parse_date(args.start),
        end_date=parse_date(args.end)
    )


if __name__ == "__main__":
    main()


# Run Script
"""
--> python -m scripts.run_index_fetch_cli --start 2019-01-01 --end 2026-02-12
python -m scripts.run_index_fetch_cli --start 2025-12-01 --end 2026-02-12

"""
