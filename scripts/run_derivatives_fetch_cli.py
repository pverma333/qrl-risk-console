import argparse
from datetime import datetime
from pathlib import Path
import sys

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(BASE_DIR))

from src.core.fetch_config import FetchConfig
from src.data.master_derivatives_fetch import DerivativesFetcher


def parse_date(date_str):
    return datetime.strptime(date_str, "%Y-%m-%d").date()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--rebuild", action="store_true")

    args = parser.parse_args()

    config = FetchConfig(BASE_DIR, use_year_partition=True)
    fetcher = DerivativesFetcher(config=config,rebuild=args.rebuild)

    fetcher.run(
        start_date=parse_date(args.start),
        end_date=parse_date(args.end)
    )

if __name__ == "__main__":
    main()

#Run script
"""
python -m scripts.run_derivatives_fetch_cli --start 2019-01-01 --end 2026-02-12 --rebuild
python -m scripts.run_derivatives_fetch_cli --start 2024-05-01 --end 2026-02-12 --rebuild
"""

