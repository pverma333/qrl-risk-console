import sys
import logging
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(BASE_DIR))
logging.getLogger("numexpr").setLevel(logging.WARNING)

from src.core.fetch_config import FetchConfig
from src.data.master_derivatives_fetch import DerivativesFetcher
from src.data.master_index_fetch import MasterIndexFetcher
from src.data.master_index_yield_fetch import MasterIndexYieldFetcher
from src.data.master_trade_calendar_writer import TradeCalendarWriter
from src.data.daily_fetch_gbond import GbondDailyFetch
from src.data.processed_trade_calendar_builder import ProcessedTradeCalendarBuilder
from src.data.processed_lot_size_builder import ProcessedLotSizeBuilder
from src.data.processed_derivatives_builder import ProcessedDerivativesBuilder
from src.data.processed_index_spot_builder import ProcessedIndexSpotBuilder
from src.data.processed_vix_builder import ProcessedVIXBuilder
from src.data.processed_index_yield_builder import ProcessedIndexYieldBuilder
from src.data.processed_gbond_builder import ProcessedGBondBuilder
from src.data.curated_option_chain_builder import CuratedOptionChainBuilder
from src.data.sync_checker import SyncChecker
from src.data.curated_futures_builder import CuratedFuturesBuilder


def main():


    today = datetime(2026, 5, 25).date()
    #today = datetime.today().date()
    config = FetchConfig(BASE_DIR, use_year_partition=True)

    # Ingest Layer
    DerivativesFetcher(config=config, rebuild=False).run(start_date=today, end_date=today)
    TradeCalendarWriter(config=config, rebuild=False).run()
    MasterIndexFetcher(config=config, rebuild=False).run(start_date=today, end_date=today)
    MasterIndexYieldFetcher(config=config, rebuild=False).run(start_date=today, end_date=today)
    GbondDailyFetch(config=config).run()

    # Processed Layer
    ProcessedTradeCalendarBuilder(config).run("incremental")
    ProcessedLotSizeBuilder(config).run("incremental")
    ProcessedDerivativesBuilder(config).run("incremental")
    ProcessedIndexSpotBuilder(config).run("incremental")
    ProcessedVIXBuilder(config).run("incremental")
    ProcessedIndexYieldBuilder(config).run("incremental")
    ProcessedGBondBuilder(config).run("incremental")

    # Curated Layer
    CuratedOptionChainBuilder(config).run("incremental")
    CuratedFuturesBuilder(config).run("incremental")

    #Sync Checker
    SyncChecker(config).run(mode="daily")

if __name__ == "__main__":
    main()

#run script
"""
python -m scripts.run_daily_fetch
"""
