"""Curated builders also need Bug 8's backfill fix.

After a Nov 3 fetch failure followed by a Nov 4 success:
  - _get_latest_trade_date(year) → Nov 4
  - _query_year SQL: WHERE ... AND trade_date > 'Nov 4'
  - Re-fetched Nov 3 (now in processed) is silently excluded from curated

Expected fix: replace `> latest` SQL clause with `NOT IN (existing_dates)`.

Tests 1 and 3 fail today (backfill dates silently excluded from curated).
Tests 2 and 4 pass today (forward dates work, dedup still works).
"""
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from src.core.fetch_config import FetchConfig
from src.data.curated_option_chain_builder import CuratedOptionChainBuilder
from src.data.curated_futures_builder import CuratedFuturesBuilder

_NOV3  = date(2026, 11, 3)
_NOV4  = date(2026, 11, 4)
_NOV5  = date(2026, 11, 5)
_EXPIRY = date(2026, 11, 27)

# ---------------------------------------------------------------------------
# helpers — write minimal processed parquet files so DuckDB views register
# ---------------------------------------------------------------------------

def _w(path: Path, rows: list[dict]):
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(path, index=False)


def _processed_opt_row(d: date) -> dict:
    return dict(
        trade_date=d, symbol="NIFTY", expiry_date=_EXPIRY,
        strike=20000.0, option_type="CE",
        open=150.0, high=160.0, low=140.0, close=150.0, settle=150.0,
        contracts=10, open_interest=100, chg_in_oi=0, dte=24,
    )


def _processed_spot_row(d: date) -> dict:
    return dict(trade_date=d, symbol="NIFTY", close=20200.0)


def _processed_yield_row(d: date) -> dict:
    return dict(trade_date=d, symbol="NIFTY", div_yield=1.2)


def _processed_gbond_row(d: date, tenor: str) -> dict:
    return dict(trade_date=d, tenor=tenor, yield_pct=6.5)


def _setup_processed(config: FetchConfig, dates: list[date]):
    """Write all processed source files for the given dates."""
    opt_path = config.processed_dir / "options" / "2026" / "processed_options_2026.parquet"
    spot_path = config.processed_dir / "index_spot" / "2026" / "processed_index_spot_2026.parquet"
    yld_path  = config.processed_dir / "index_yield" / "2026" / "processed_index_yield_2026.parquet"
    gbond_path = config.processed_dir / "gbond" / "2026" / "processed_gbond_2026.parquet"

    _w(opt_path,  [_processed_opt_row(d) for d in dates])
    _w(spot_path, [_processed_spot_row(d) for d in dates])
    _w(yld_path,  [_processed_yield_row(d) for d in dates])
    gbond_rows = [_processed_gbond_row(d, t) for d in dates for t in ("3m", "6m", "1y")]
    _w(gbond_path, gbond_rows)

    # trade_calendar required so the registry doesn't skip that folder
    tc_path = config.processed_dir / "trade_calendar" / "trade_calendar.parquet"
    _w(tc_path, [{"trade_date": d, "symbol": "NIFTY"} for d in dates])


def _setup_processed_futures(config: FetchConfig, dates: list[date]):
    """Write processed futures file (in addition to the shared sources above)."""
    fut_path = config.processed_dir / "futures" / "2026" / "processed_futures_2026.parquet"
    _w(fut_path, [{
        "trade_date": d, "symbol": "NIFTY", "expiry_date": _EXPIRY,
        "open": 20150.0, "high": 20250.0, "low": 20100.0,
        "close": 20200.0, "settle": 20200.0,
        "contracts": 5, "open_interest": 50, "chg_in_oi": 0, "dte": 24,
    } for d in dates])


def _curated_opt_rows(dates: list[date]) -> list[dict]:
    return [{
        "trade_date": d, "symbol": "NIFTY", "expiry_date": _EXPIRY,
        "strike": 20000.0, "option_type": "CE",
        "open": 150.0, "high": 160.0, "low": 140.0, "close": 150.0, "settle": 150.0,
        "contracts": 10, "open_interest": 100, "chg_in_oi": 0, "dte": 24,
        "spot": 20200.0, "div_yield": 1.2, "rate": 0.065,
        "iv": 0.15, "delta": 0.5, "gamma": 0.001, "vega": 10.0,
        "theta": -1.5, "rho": 0.5,
    } for d in dates]


def _curated_fut_rows(dates: list[date]) -> list[dict]:
    return [{
        "trade_date": d, "symbol": "NIFTY", "expiry_date": _EXPIRY,
        "open": 20150.0, "high": 20250.0, "low": 20100.0,
        "close": 20200.0, "settle": 20200.0,
        "contracts": 5, "open_interest": 50, "chg_in_oi": 0, "dte": 24,
        "spot": 20200.0, "div_yield": 1.2, "rate": 0.065,
    } for d in dates]


def _dates_in_parquet(path: Path) -> set[date]:
    if not path.exists():
        return set()
    df = pd.read_parquet(path, columns=["trade_date"])
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    return set(df["trade_date"])


# ---------------------------------------------------------------------------
# 1. CuratedOptionChainBuilder — backfill scenario
# ---------------------------------------------------------------------------

class TestCuratedOptionChainBackfill:

    def test_backfill_date_added_when_missing_from_curated(self, tmp_path):
        """Nov3 missing from curated; it is now in processed.
        Incremental must include it."""
        config = FetchConfig(base_dir=tmp_path, use_year_partition=True)
        out = config.curated_dir / "option_chain" / "2026" / "curated_options_2026.parquet"

        _setup_processed(config, [_NOV3, _NOV4])
        _w(out, _curated_opt_rows([_NOV4]))   # curated only has Nov4

        CuratedOptionChainBuilder(config).run("incremental")

        dates = _dates_in_parquet(out)
        assert _NOV3 in dates, "Nov3 must appear in curated/option_chain after backfill"
        assert _NOV4 in dates

    def test_forward_date_still_processed_normally(self, tmp_path):
        """Normal forward case: Nov5 arrives; it must be curated."""
        config = FetchConfig(base_dir=tmp_path, use_year_partition=True)
        out = config.curated_dir / "option_chain" / "2026" / "curated_options_2026.parquet"

        _setup_processed(config, [_NOV3, _NOV4, _NOV5])
        _w(out, _curated_opt_rows([_NOV3, _NOV4]))

        CuratedOptionChainBuilder(config).run("incremental")

        assert _NOV5 in _dates_in_parquet(out)

    def test_no_duplicate_on_re_run(self, tmp_path):
        """Re-running incremental on a fully-synced dataset must not duplicate rows."""
        config = FetchConfig(base_dir=tmp_path, use_year_partition=True)
        out = config.curated_dir / "option_chain" / "2026" / "curated_options_2026.parquet"

        _setup_processed(config, [_NOV3, _NOV4])
        _w(out, _curated_opt_rows([_NOV3, _NOV4]))

        before = len(pd.read_parquet(out))
        CuratedOptionChainBuilder(config).run("incremental")
        assert len(pd.read_parquet(out)) == before

    def test_bug_demonstration_latest_filter_drops_backfill(self, tmp_path):
        """Documents bug: _get_latest_trade_date returns Nov4; `> Nov4` drops Nov3."""
        config = FetchConfig(base_dir=tmp_path, use_year_partition=True)
        out = config.curated_dir / "option_chain" / "2026" / "curated_options_2026.parquet"
        _setup_processed(config, [_NOV3, _NOV4])
        _w(out, _curated_opt_rows([_NOV4]))

        b = CuratedOptionChainBuilder(config)
        latest = b._get_latest_trade_date(2026)
        assert latest == _NOV4

        # Old SQL logic would exclude Nov3
        assert str(_NOV3) < str(latest), "Confirms: Nov3 string < Nov4 → old > filter would drop it"


# ---------------------------------------------------------------------------
# 2. CuratedFuturesBuilder — same backfill scenario
# ---------------------------------------------------------------------------

class TestCuratedFuturesBackfill:

    def test_backfill_date_added_when_missing_from_curated(self, tmp_path):
        """Nov3 missing from curated/futures; now in processed. Must be backfilled."""
        config = FetchConfig(base_dir=tmp_path, use_year_partition=True)
        out = config.curated_dir / "futures" / "2026" / "curated_futures_2026.parquet"

        _setup_processed(config, [_NOV3, _NOV4])
        _setup_processed_futures(config, [_NOV3, _NOV4])
        _w(out, _curated_fut_rows([_NOV4]))

        CuratedFuturesBuilder(config).run("incremental")

        dates = _dates_in_parquet(out)
        assert _NOV3 in dates, "Nov3 must appear in curated/futures after backfill"
        assert _NOV4 in dates
