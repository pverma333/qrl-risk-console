"""Bug 4: blank NSE div_yield filled with 0.0 instead of forward-filled.

_handle_null_yield currently calls fillna(0.0).
Expected behaviour:
  - Forward-fill the last valid value per symbol.
  - Add a div_yield_imputed bool column (True where filled, False elsewhere).
  - If a symbol's first row is null (no prior valid value), leave it null so
    _validate_schema raises and the build aborts cleanly.
  - 0.0 must never appear as a fill value where the last known yield was > 0.

Tests fail today because:
  - fillna(0.0) produces 0.0, not the last valid value.
  - No div_yield_imputed column exists → KeyError.
"""
import pandas as pd
import pytest
from datetime import date

from src.data.processed_index_yield_builder import ProcessedIndexYieldBuilder


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _builder() -> ProcessedIndexYieldBuilder:
    """Return a builder with all file I/O disabled (we test _handle_null_yield directly)."""
    b = ProcessedIndexYieldBuilder.__new__(ProcessedIndexYieldBuilder)
    import logging
    b.logger = logging.getLogger("test_yield_builder")
    return b


def _df(rows: list[dict]) -> pd.DataFrame:
    """rows: list of {trade_date, symbol, div_yield} dicts. div_yield=None → NaN."""
    df = pd.DataFrame(rows)
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    df["div_yield"]  = pd.to_numeric(df["div_yield"], errors="coerce")
    return df


# ---------------------------------------------------------------------------
# 1. forward-fill value, not 0.0
# ---------------------------------------------------------------------------

class TestForwardFill:

    def test_null_filled_with_prior_value_not_zero(self):
        # BANKNIFTY: good yield on day 1, blank on day 2 → day 2 should get day 1's value
        df = _df([
            {"trade_date": "2026-11-04", "symbol": "BANKNIFTY", "div_yield": 1.52},
            {"trade_date": "2026-11-05", "symbol": "BANKNIFTY", "div_yield": None},
        ])
        result = _builder()._handle_null_yield(df)
        assert result.loc[1, "div_yield"] == pytest.approx(1.52, abs=1e-9)

    def test_null_not_filled_with_zero(self):
        df = _df([
            {"trade_date": "2026-11-04", "symbol": "BANKNIFTY", "div_yield": 1.52},
            {"trade_date": "2026-11-05", "symbol": "BANKNIFTY", "div_yield": None},
        ])
        result = _builder()._handle_null_yield(df)
        assert result.loc[1, "div_yield"] != pytest.approx(0.0, abs=1e-9)

    def test_multiple_consecutive_nulls_all_filled(self):
        # Three blank days in a row → all get the last valid value
        df = _df([
            {"trade_date": "2026-11-03", "symbol": "BANKNIFTY", "div_yield": 1.52},
            {"trade_date": "2026-11-04", "symbol": "BANKNIFTY", "div_yield": None},
            {"trade_date": "2026-11-05", "symbol": "BANKNIFTY", "div_yield": None},
            {"trade_date": "2026-11-06", "symbol": "BANKNIFTY", "div_yield": None},
        ])
        result = _builder()._handle_null_yield(df)
        assert result.loc[1, "div_yield"] == pytest.approx(1.52, abs=1e-9)
        assert result.loc[2, "div_yield"] == pytest.approx(1.52, abs=1e-9)
        assert result.loc[3, "div_yield"] == pytest.approx(1.52, abs=1e-9)

    def test_forward_fill_per_symbol_independent(self):
        # NIFTY and BANKNIFTY have different last-valid values; each fills independently
        df = _df([
            {"trade_date": "2026-11-04", "symbol": "NIFTY",     "div_yield": 1.20},
            {"trade_date": "2026-11-04", "symbol": "BANKNIFTY", "div_yield": 1.52},
            {"trade_date": "2026-11-05", "symbol": "NIFTY",     "div_yield": None},
            {"trade_date": "2026-11-05", "symbol": "BANKNIFTY", "div_yield": None},
        ])
        result = _builder()._handle_null_yield(df)
        nifty_filled    = result[(result.symbol == "NIFTY")    & (result.trade_date == date(2026, 11, 5))].iloc[0]
        banknifty_filled = result[(result.symbol == "BANKNIFTY") & (result.trade_date == date(2026, 11, 5))].iloc[0]
        assert nifty_filled["div_yield"]     == pytest.approx(1.20, abs=1e-9)
        assert banknifty_filled["div_yield"] == pytest.approx(1.52, abs=1e-9)

    def test_no_nulls_data_unchanged(self):
        df = _df([
            {"trade_date": "2026-11-04", "symbol": "NIFTY", "div_yield": 1.20},
            {"trade_date": "2026-11-05", "symbol": "NIFTY", "div_yield": 1.21},
        ])
        result = _builder()._handle_null_yield(df)
        assert result.loc[0, "div_yield"] == pytest.approx(1.20, abs=1e-9)
        assert result.loc[1, "div_yield"] == pytest.approx(1.21, abs=1e-9)


# ---------------------------------------------------------------------------
# 2. div_yield_imputed flag column
# ---------------------------------------------------------------------------

class TestImputedFlag:

    def test_imputed_column_exists(self):
        df = _df([
            {"trade_date": "2026-11-04", "symbol": "BANKNIFTY", "div_yield": 1.52},
            {"trade_date": "2026-11-05", "symbol": "BANKNIFTY", "div_yield": None},
        ])
        result = _builder()._handle_null_yield(df)
        assert "div_yield_imputed" in result.columns

    def test_imputed_true_where_filled(self):
        df = _df([
            {"trade_date": "2026-11-04", "symbol": "BANKNIFTY", "div_yield": 1.52},
            {"trade_date": "2026-11-05", "symbol": "BANKNIFTY", "div_yield": None},
        ])
        result = _builder()._handle_null_yield(df)
        assert result.loc[1, "div_yield_imputed"] == True

    def test_imputed_false_where_original_valid(self):
        df = _df([
            {"trade_date": "2026-11-04", "symbol": "BANKNIFTY", "div_yield": 1.52},
            {"trade_date": "2026-11-05", "symbol": "BANKNIFTY", "div_yield": None},
        ])
        result = _builder()._handle_null_yield(df)
        assert result.loc[0, "div_yield_imputed"] == False

    def test_no_nulls_all_imputed_false(self):
        df = _df([
            {"trade_date": "2026-11-04", "symbol": "NIFTY", "div_yield": 1.20},
            {"trade_date": "2026-11-05", "symbol": "NIFTY", "div_yield": 1.21},
        ])
        result = _builder()._handle_null_yield(df)
        assert "div_yield_imputed" in result.columns
        assert (result["div_yield_imputed"] == False).all()


# ---------------------------------------------------------------------------
# 3. edge case: first row null — no prior value → stays null → build should abort
# ---------------------------------------------------------------------------

class TestFirstRowNull:

    def test_first_row_null_stays_null(self):
        # Symbol's very first row is blank — cannot forward-fill; must leave null
        # so _validate_schema raises and the caller knows something is wrong.
        df = _df([
            {"trade_date": "2026-11-04", "symbol": "FINNIFTY", "div_yield": None},
            {"trade_date": "2026-11-05", "symbol": "FINNIFTY", "div_yield": 0.95},
        ])
        result = _builder()._handle_null_yield(df)
        assert pd.isna(result.loc[0, "div_yield"])

    def test_first_row_null_does_not_fill_zero(self):
        df = _df([
            {"trade_date": "2026-11-04", "symbol": "FINNIFTY", "div_yield": None},
            {"trade_date": "2026-11-05", "symbol": "FINNIFTY", "div_yield": 0.95},
        ])
        result = _builder()._handle_null_yield(df)
        # 0.0 is explicitly wrong — it's the old bug
        assert result.loc[0, "div_yield"] != pytest.approx(0.0, abs=1e-9)
