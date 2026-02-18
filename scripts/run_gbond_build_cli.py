import argparse
from pathlib import Path
import sys

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(BASE_DIR))

from src.core.fetch_config import FetchConfig
from src.data.master_combined_gbond import GbondProcessor


def main():
    parser = argparse.ArgumentParser(
        description="Build or update combined Gbond parquet file"
    )
    parser.add_argument(
        "--rebuild",
        action="store_true",
        help="Delete existing combined file and rebuild from scratch"
    )

    args = parser.parse_args()

    config = FetchConfig(BASE_DIR, use_year_partition=False)

    processor = GbondProcessor(
        config=config,
        rebuild=args.rebuild
    )

    processor.build_combined_gbond()


if __name__ == "__main__":
    main()

#run script
"""
python -m scripts.run_gbond_build_cli --rebuild
python -m scripts.run_gbond_build_cli

"""
