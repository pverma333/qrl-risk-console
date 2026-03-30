import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.core.fetch_config import FetchConfig
from src.data.processed_trade_calendar_builder import ProcessedTradeCalendarBuilder
from src.data.processed_lot_size_builder import ProcessedLotSizeBuilder
from src.data.processed_derivatives_builder import ProcessedDerivativesBuilder
from src.data.processed_index_spot_builder import ProcessedIndexSpotBuilder
from src.data.processed_vix_builder import ProcessedVIXBuilder
from src.data.processed_index_yield_builder import ProcessedIndexYieldBuilder
from src.data.processed_gbond_builder import ProcessedGBondBuilder


def parse_args():
    parser = argparse.ArgumentParser(description="Processed Layer Builder — runs all processors")
    parser.add_argument(
        "--mode",
        choices=["full", "incremental"],
        required=True,
        help="Run mode: full rebuild or incremental append"
    )
    parser.add_argument("--calendar-only", action="store_true")
    parser.add_argument("--lot-size-only", action="store_true")
    parser.add_argument("--derivatives-only", action="store_true")
    parser.add_argument("--index-only", action="store_true")
    parser.add_argument("--vix-only", action="store_true")
    parser.add_argument("--yield-only", action="store_true")
    parser.add_argument("--gbond-only", action="store_true")
    return parser.parse_args()


PROCESSORS = [
    ("trade_calendar", ProcessedTradeCalendarBuilder),
    ("lot_size",       ProcessedLotSizeBuilder),
    ("derivatives",    ProcessedDerivativesBuilder),
    ("index_spot",     ProcessedIndexSpotBuilder),
    ("vix",            ProcessedVIXBuilder),
    ("index_yield",    ProcessedIndexYieldBuilder),
    ("gbond",          ProcessedGBondBuilder),
]

FLAG_MAP = {
    "calendar_only":    "trade_calendar",
    "lot_size_only":    "lot_size",
    "derivatives_only": "derivatives",
    "index_only":       "index_spot",
    "vix_only":         "vix",
    "yield_only":       "index_yield",
    "gbond_only":       "gbond",
}


def main():
    args = parse_args()
    config = FetchConfig(base_dir=Path(__file__).resolve().parents[1])

    selected = {
        name for flag, name in FLAG_MAP.items()
        if getattr(args, flag, False)
    }
    run_all = len(selected) == 0

    for name, BuilderClass in PROCESSORS:
        if not run_all and name not in selected:
            continue
        BuilderClass(config).run(args.mode)


if __name__ == "__main__":
    main()

# run
"""
python scripts/run_processed_builder.py --mode full
python scripts/run_processed_builder.py --mode incremental
python scripts/run_processed_builder.py --mode incremental --derivatives-only
python scripts/run_processed_builder.py --mode incremental --gbond-only
"""
