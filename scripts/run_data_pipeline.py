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
from src.data.master_lot_size_map import LotSizeMapStore
from src.data.master_trade_calendar_writer import TradeCalendarWriter

def parse_date(date_str: str):
    return datetime.strptime(date_str, "%Y-%m-%d").date()

def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--start")
    parser.add_argument("--end")

    parser.add_argument("--derivatives-only", action="store_true")
    parser.add_argument("--index-only", action="store_true")
    parser.add_argument("--yield-only", action="store_true")
    parser.add_argument("--bond-only", action="store_true")
    parser.add_argument("--lot-size-only", action="store_true")
    parser.add_argument("--rebuild", action="store_true")

    args = parser.parse_args()
    # Validate start/end unless lot-size-only
    if not args.lot_size_only:
        if not args.start or not args.end:
            parser.error("--start and --end are required unless --lot-size-only is used.")

    start_date = parse_date(args.start) if args.start else None
    end_date = parse_date(args.end) if args.start else None

    config = FetchConfig(BASE_DIR, use_year_partition=True)

    if args.lot_size_only:
        lot_store = LotSizeMapStore(config=config)
        lot_store.build()
        return

    # If no specific flag → run everything
    run_all = not (
        args.derivatives_only or
        args.index_only or
        args.yield_only or
        args.bond_only or
        args.lot_size_only
    )

    if run_all and args.rebuild:
        lot_store = LotSizeMapStore(config=config)
        lot_store.build()

    # Derivatives
    if run_all or args.derivatives_only:
        derivatives_fetcher = DerivativesFetcher(config=config,rebuild=args.rebuild)
        derivatives_fetcher.run(start_date=start_date,end_date=end_date)
        calendar_writer = TradeCalendarWriter(config=config,rebuild=args.rebuild)
        calendar_writer.run()

    # Index Price
    if run_all or args.index_only:
        index_fetcher = MasterIndexFetcher(config=config)
        index_fetcher.run(start_date=start_date,end_date=end_date)

    # Index Yield
    if run_all or args.yield_only:
        yield_fetcher = MasterIndexYieldFetcher(config=config)
        yield_fetcher.run(start_date=start_date,end_date=end_date)

    # Gbond Combine
    if run_all or args.bond_only:
        bond_processor = GbondProcessor(config=config,rebuild=args.rebuild)
        bond_processor.build_combined_gbond()



if __name__ == "__main__":
    main()

#run script
"""
python -m scripts.run_data_pipeline --start 2019-01-01 --end 2026-02-18 --rebuild
python -m scripts.run_data_pipeline --start 2025-12-12 --end 2026-01-12 --rebuild
python -m scripts.run_data_pipeline --start 2025-12-12 --end 2026-01-09

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
#run lot size only
"""
python -m scripts.run_data_pipeline --lot-size-only --rebuild
"""
#daily fetch
"""
python -m scripts.run_data_pipeline --start ... --end ...

→ derivatives append
→ trade calendar merge + dedup
"""
