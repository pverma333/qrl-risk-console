import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(BASE_DIR))

from src.core.fetch_config import FetchConfig
from src.data.daily_fetch_gbond import GbondDailyFetch


def main():
    config = FetchConfig(BASE_DIR, use_year_partition=True)
    runner = GbondDailyFetch(config)
    df = runner.run()
    if df is None:
        print("No update performed.")
    else:
        print("Updated successfully. Rows:", len(df))

if __name__ == "__main__":
    main()

#run script
"""
python -m scripts.run_daily_gbond_cli
"""
