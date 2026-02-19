import argparse
from pathlib import Path
import sys

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(BASE_DIR))

from src.core.fetch_config import FetchConfig
from src.data.master_lot_size_map import LotSizeMapStore


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rebuild", action="store_true")
    args = parser.parse_args()

    config = FetchConfig(BASE_DIR)

    if args.rebuild:
        store = LotSizeMapStore(config=config)
        store.build()
    else:
        print("Lot size map only runs in --rebuild mode.")


if __name__ == "__main__":
    main()

#Run script
"""
python -m scripts.run_lot_size_map --rebuild
"""
