import sys
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(BASE_DIR))

from src.core.fetch_config import FetchConfig
from src.data.master_derivatives_fetch import DerivativesFetcher
from src.data.master_index_fetch import MasterIndexFetcher
from src.data.master_index_yield_fetch import MasterIndexYieldFetcher
from src.data.master_trade_calendar_writer import TradeCalendarWriter
from src.data.daily_fetch_gbond import GbondDailyFetch

def main():

    today = datetime.today().date()
    config = FetchConfig(BASE_DIR, use_year_partition=True)

    # Derivatives
    derivatives_fetcher = DerivativesFetcher(config=config, rebuild=False)
    derivatives_fetcher.run(start_date=today, end_date=today)

    # Trade Calendar
    calendar_writer = TradeCalendarWriter(config=config, rebuild=False)
    calendar_writer.run()

    # Index Price
    index_fetcher = MasterIndexFetcher(config=config)
    index_fetcher.run(start_date=today, end_date=today)

    # Index Yield
    yield_fetcher = MasterIndexYieldFetcher(config=config)
    yield_fetcher.run(start_date=today, end_date=today)

    # Gbond
    gbond_fetcher = GbondDailyFetch(config=config)
    gbond_fetcher.run()

if __name__ == "__main__":
    main()

#run script
"""
python -m scripts.run_daily_fetch
"""
