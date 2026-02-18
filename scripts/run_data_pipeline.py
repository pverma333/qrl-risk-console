import argparse
from datetime import datetime
from pathlib import Path
import sys

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(BASE_DIR))

from src.core.fetch_config import FetchConfig
from src.data.master_derivatives_fetch import DerivativesFetcher
from src.data.master_index_fetch import MasterIndexFetcher
from src.data.master_index_yield_fetch import MasterIndexYieldFetcher
from src.data.master_combined_gbond import GbondProcessor

def parse_date(date_str: str):
    return datetime.strptime(date_str, "%Y-%m-%d").date()

def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)

    parser.add_argument("--derivatives-only", action="store_true")
    parser.add_argument("--index-only", action="store_true")
    parser.add_argument("--yield-only", action="store_true")
    parser.add_argument("--bond-only", action="store_true")

    parser.add_argument("--rebuild", action="store_true")

    args = parser.parse_args()

    start_date = parse_date(args.start)
    end_date = parse_date(args.end)

    config = FetchConfig(BASE_DIR, use_year_partition=True)

    # If no specific flag → run everything
    run_all = not (
        args.derivatives_only or
        args.index_only or
        args.yield_only or
        args.bond_only
    )

    # Derivatives
    if run_all or args.derivatives_only:
        derivatives_fetcher = DerivativesFetcher(
            config=config,
            rebuild=args.rebuild
        )

        derivatives_fetcher.run(
            start_date=start_date,
            end_date=end_date
        )

    # Index Price
    if run_all or args.index_only:
        index_fetcher = MasterIndexFetcher(config=config)

        index_fetcher.run(
            start_date=start_date,
            end_date=end_date
        )

    # Index Yield
    if run_all or args.yield_only:
        yield_fetcher = MasterIndexYieldFetcher(config=config)

        yield_fetcher.run(
            start_date=start_date,
            end_date=end_date
        )

    # Gbond Combine   ← Added
    if run_all or args.bond_only:
        bond_processor = GbondProcessor(
            config=config,
            rebuild=args.rebuild
        )

        bond_processor.build_combined_gbond()


if __name__ == "__main__":
    main()

#run script
"""
python -m scripts.run_data_pipeline --start 2019-01-01 --end 2026-02-17 --rebuild
python -m scripts.run_data_pipeline --start 2025-12-12 --end 2026-01-12 --rebuild
python -m scripts.run_data_pipeline --start 2025-12-12 --end 2026-01-17

"""
#run only derivatives
"""
python -m scripts.run_data_pipeline --start 2024-01-01 --end 2026-02-12 --derivatives-only --rebuild
"""
#run index
"""
python -m scripts.run_data_pipeline --start 2019-01-01 --end 2026-02-12 --index-only
"""
#run index yield only
"""
python -m scripts.run_data_pipeline --start 2019-01-01 --end 2026-02-12 --index-only
"""
#run bond only
"""
python -m scripts.run_data_pipeline --start 2019-01-01 --end 2026-02-17 --bond-only
"""
