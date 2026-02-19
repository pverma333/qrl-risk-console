import argparse
from pathlib import Path
import sys

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(BASE_DIR))

from src.core.fetch_config import FetchConfig
from src.data.master_trade_calendar_writer import TradeCalendarWriter

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rebuild", action="store_true")
    args = parser.parse_args()

    config = FetchConfig(BASE_DIR)

    writer = TradeCalendarWriter(
        config=config,
        rebuild=args.rebuild
    )

    writer.run()


if __name__ == "__main__":
    main()

#run script
"""
python -m scripts.run_trade_calendar_cli --rebuild
"""
#run incremental updates
"""
python -m scripts.run_trade_calendar

"""
