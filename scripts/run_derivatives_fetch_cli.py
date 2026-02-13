import argparse
from datetime import datetime
from pathlib import Path
import sys

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(BASE_DIR))

from src.data.master_derivatives_fetch import DerivativesFetcher


def parse_date(date_str):
    return datetime.strptime(date_str, "%Y-%m-%d").date()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)

    args = parser.parse_args()

    fetcher = DerivativesFetcher(base_dir=BASE_DIR)
    fetcher.run(
        start_date=parse_date(args.start),
        end_date=parse_date(args.end)
    )


if __name__ == "__main__":
    main()
#Run script
"""
python -m scripts.run_derivatives_fetch_cli --start 2019-01-01 --end 2026-02-12
"""
