import argparse
from datetime import datetime
from pathlib import Path
import sys

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(BASE_DIR))

from src.core.fetch_config import FetchConfig
from src.data.master_derivatives_fetch import DerivativesFetcher
from src.data.master_index_fetch import MasterIndexFetcher


def parse_date(date_str: str):
    return datetime.strptime(date_str, "%Y-%m-%d").date()


def main():

    parser = argparse.ArgumentParser()

    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)

    parser.add_argument("--derivatives-only", action="store_true")
    parser.add_argument("--index-only", action="store_true")

    parser.add_argument("--rebuild", action="store_true")

    args = parser.parse_args()

    start_date = parse_date(args.start)
    end_date = parse_date(args.end)

    config = FetchConfig(BASE_DIR, use_year_partition=True)

    # Safety: cannot select both flags
    if args.derivatives_only and args.index_only:
        raise ValueError("Cannot use both --derivatives-only and --index-only")

    # Run Derivatives
    if not args.index_only:
        derivatives_fetcher = DerivativesFetcher(
            config=config,
            rebuild=args.rebuild
        )

        derivatives_fetcher.run(
            start_date=start_date,
            end_date=end_date
        )

    # Run Index
    if not args.derivatives_only:
        index_fetcher = MasterIndexFetcher(config=config)

        index_fetcher.run(
            start_date=start_date,
            end_date=end_date
        )


if __name__ == "__main__":
    main()

#run script
"""
python -m scripts.run_data_pipeline --start 2019-01-01 --end 2026-02-12 --rebuild
"""
#run only derivatives
"""
python -m scripts.run_data_pipeline --start 2024-01-01 --end 2026-02-12 --derivatives-only --rebuild
"""
#run index
"""
python -m scripts.run_data_pipeline --start 2019-01-01 --end 2026-02-12 --index-only

"""
