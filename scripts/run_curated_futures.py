import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.core.fetch_config import FetchConfig
from src.data.curated_futures_builder import CuratedFuturesBuilder


def parse_args():
    parser = argparse.ArgumentParser(description="Curated Futures Builder")
    parser.add_argument(
        "--mode",
        choices=["full", "incremental"],
        required=True,
        help="Run mode: full rebuild or incremental append",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    config = FetchConfig(base_dir=Path(__file__).resolve().parents[1])
    CuratedFuturesBuilder(config).run(args.mode)


if __name__ == "__main__":
    main()


# run
"""
python scripts/run_curated_futures.py --mode full
python scripts/run_curated_futures.py --mode incremental
"""
