"""Bug 5: Curated builders drop rows silently via INNER JOINs.

Both CuratedOptionChainBuilder and CuratedFuturesBuilder use five INNER JOINs
(spot, yield, rate_3m, rate_6m, rate_1y).  When any input is missing for a
trade_date/symbol, all rows for that date are silently lost.

Fix: switch to LEFT JOINs + add _check_join_completeness that:
  - identifies rows where any join partner was absent (NULL in join columns)
  - logs a rejection report (date, symbol, missing input name)
  - returns (complete_df, rejected_df)

These tests fail today with AttributeError because the method doesn't exist.
"""
import logging
import pandas as pd
import pytest
from datetime import date

from src.data.curated_option_chain_builder import CuratedOptionChainBuilder
from src.data.curated_futures_builder import CuratedFuturesBuilder


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _opt_builder() -> CuratedOptionChainBuilder:
    b = CuratedOptionChainBuilder.__new__(CuratedOptionChainBuilder)
    b.logger = logging.getLogger("test_curated_opt")
    return b


def _fut_builder() -> CuratedFuturesBuilder:
    b = CuratedFuturesBuilder.__new__(CuratedFuturesBuilder)
    b.logger = logging.getLogger("test_curated_fut")
    return b


def _base_row(**overrides) -> dict:
    """A fully complete row (all join columns present)."""
    row = {
        "trade_date": date(2026, 10, 27),
        "symbol":     "NIFTY",
        "expiry_date": date(2026, 11, 27),
        "strike":     24000.0,
        "option_type": "CE",
        "dte":        31,
        "spot":       24001.26,
        "div_yield":  1.52,
        "rate_3m":    6.5,
        "rate_6m":    6.6,
        "rate_1y":    6.7,
        # option-only columns (ignored by futures builder)
        "open": 100.0, "high": 110.0, "low": 90.0, "close": 105.0,
        "settle": 105.0, "contracts": 1000, "open_interest": 50000, "chg_in_oi": 200,
    }
    row.update(overrides)
    return row


def _df(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# 1. method exists on both builders
# ---------------------------------------------------------------------------

class TestMethodExists:

    def test_option_builder_has_method(self):
        assert hasattr(_opt_builder(), "_check_join_completeness")

    def test_futures_builder_has_method(self):
        assert hasattr(_fut_builder(), "_check_join_completeness")


# ---------------------------------------------------------------------------
# 2. complete rows pass through unchanged
# ---------------------------------------------------------------------------

class TestCompleteRowsPassThrough:

    def test_no_nulls_all_complete(self):
        df = _df([_base_row(), _base_row(trade_date=date(2026, 10, 28))])
        complete, rejected = _opt_builder()._check_join_completeness(df)
        assert len(complete)  == 2
        assert len(rejected)  == 0

    def test_futures_no_nulls_all_complete(self):
        df = _df([_base_row(), _base_row(symbol="BANKNIFTY")])
        complete, rejected = _fut_builder()._check_join_completeness(df)
        assert len(complete)  == 2
        assert len(rejected)  == 0


# ---------------------------------------------------------------------------
# 3. missing spot → row rejected
# ---------------------------------------------------------------------------

class TestMissingSpot:

    def test_null_spot_row_rejected(self):
        df = _df([_base_row(spot=None)])
        complete, rejected = _opt_builder()._check_join_completeness(df)
        assert len(complete) == 0
        assert len(rejected) == 1

    def test_other_rows_kept_when_one_has_null_spot(self):
        df = _df([_base_row(spot=None), _base_row(trade_date=date(2026, 10, 28))])
        complete, rejected = _opt_builder()._check_join_completeness(df)
        assert len(complete) == 1
        assert len(rejected) == 1


# ---------------------------------------------------------------------------
# 4. missing div_yield → row rejected
# ---------------------------------------------------------------------------

class TestMissingDivYield:

    def test_null_div_yield_row_rejected(self):
        df = _df([_base_row(div_yield=None)])
        complete, rejected = _opt_builder()._check_join_completeness(df)
        assert len(rejected) == 1

    def test_midcpnifty_missing_yield_rows_rejected(self):
        # Mirrors 29 Oct: all MIDCPNIFTY rows lose div_yield → all rejected
        rows = [
            _base_row(symbol="MIDCPNIFTY", div_yield=None, strike=float(k))
            for k in [24000, 24100, 24200]
        ]
        df = _df(rows)
        complete, rejected = _opt_builder()._check_join_completeness(df)
        assert len(rejected) == 3
        assert len(complete) == 0


# ---------------------------------------------------------------------------
# 5. missing g-bond tenor → row rejected (mirrors 27 Oct)
# ---------------------------------------------------------------------------

class TestMissingGBondTenor:

    def test_null_rate_3m_row_rejected(self):
        df = _df([_base_row(rate_3m=None)])
        complete, rejected = _opt_builder()._check_join_completeness(df)
        assert len(rejected) == 1

    def test_null_rate_6m_row_rejected(self):
        df = _df([_base_row(rate_6m=None)])
        complete, rejected = _opt_builder()._check_join_completeness(df)
        assert len(rejected) == 1

    def test_null_rate_1y_row_rejected(self):
        df = _df([_base_row(rate_1y=None)])
        complete, rejected = _opt_builder()._check_join_completeness(df)
        assert len(rejected) == 1

    def test_all_rows_lost_when_gbond_missing_for_date(self):
        # Mirrors 27 Oct: all rows for the date lose the 1y tenor
        rows = [
            _base_row(rate_1y=None, symbol="NIFTY",    strike=float(k))
            for k in [23900, 24000, 24100]
        ] + [
            _base_row(rate_1y=None, symbol="BANKNIFTY", strike=float(k))
            for k in [52000, 52100]
        ]
        df = _df(rows)
        complete, rejected = _opt_builder()._check_join_completeness(df)
        assert len(rejected) == 5
        assert len(complete) == 0


# ---------------------------------------------------------------------------
# 6. rejection DataFrame includes date/symbol for the report
# ---------------------------------------------------------------------------

class TestRejectedRowsHaveContext:

    def test_rejected_has_trade_date(self):
        df = _df([_base_row(trade_date=date(2026, 10, 27), spot=None)])
        _, rejected = _opt_builder()._check_join_completeness(df)
        assert rejected.iloc[0]["trade_date"] == date(2026, 10, 27)

    def test_rejected_has_symbol(self):
        df = _df([_base_row(symbol="FINNIFTY", div_yield=None)])
        _, rejected = _opt_builder()._check_join_completeness(df)
        assert rejected.iloc[0]["symbol"] == "FINNIFTY"
