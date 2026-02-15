import argparse
from datetime import datetime
from pathlib import Path
import sys

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(BASE_DIR))

from src.data.master_index_yield_fetch import MasterIndexYieldFetcher


def parse_date(date_str: str):
    return datetime.strptime(date_str, "%Y-%m-%d").date()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="End date YYYY-MM-DD")

    args = parser.parse_args()

    start_date = parse_date(args.start)
    end_date = parse_date(args.end)

    fetcher = MasterIndexYieldFetcher(base_dir=BASE_DIR)
    fetcher.run(start_date=start_date, end_date=end_date)


if __name__ == "__main__":
    main()

#run script
"""
python -m scripts.run_index_yield_cli --start 2019-01-01 --end 2026-02-12
"""
