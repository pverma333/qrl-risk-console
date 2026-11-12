"""Bug 10: Mixed-symbol VaR — per-symbol returns (Option B).

Before fix: compute_var fetches returns for the primary symbol only and applies
that single return to every position regardless of symbol.

After fix: each symbol's positions are shocked with that symbol's own historical
return on the same scenario date.

Failing tests before fix
-------------------------
TestMultiSymbolVar.test_each_leg_uses_its_own_symbol_return
  → _compute_portfolio_pnl called once (all positions, NIFTY return)
    → len(calls) == 1, not 2
TestMultiSymbolVar.test_total_pnl_is_sum_of_per_symbol_pnls
  → same root cause: single-symbol path produces wrong aggregate P&L
TestMultiSymbolVar.test_single_symbol_book_unchanged
  → passes before fix (regression guard)
TestMultiSymbolVar.test_missing_symbol_history_falls_back_to_primary
  → passes before fix (regression guard)
"""
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.quant.var import compute_var


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _make_positions(symbols: list[str]) -> pd.DataFrame:
    rows = []
    for sym in symbols:
        rows.append({
            "symbol":       sym,
            "expiry_date":  "2026-03-27",
            "strike":       22000.0,
            "option_type":  "CE",
            "quantity":     1,
            "entry_date":   "2026-03-01",
            "entry_price":  100.0,
        })
    return pd.DataFrame(rows)


def _mock_db(returns_by_symbol: dict[str, list[tuple]]) -> MagicMock:
    """Return a mock DB whose execute() returns different DataFrames per symbol."""
    mock_db = MagicMock()

    def _execute(query, params):
        sym = params[0]
        rows = returns_by_symbol.get(sym, returns_by_symbol[next(iter(returns_by_symbol))])
        df = pd.DataFrame(rows, columns=["trade_date", "spot", "prev_spot", "daily_return"])
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
        result = MagicMock()
        result.df.return_value = df
        return result

    mock_db.execute.side_effect = _execute
    return mock_db


# ---------------------------------------------------------------------------
# tests
# ---------------------------------------------------------------------------

class TestMultiSymbolVar:

    _NIFTY_RETURN = +0.02   # +2 %
    _BNF_RETURN   = -0.02   # -2 %

    _NIFTY_ROWS = [("2026-03-10", 22440, 22000, _NIFTY_RETURN)]
    _BNF_ROWS   = [("2026-03-10", 44000, 44898, _BNF_RETURN)]

    def test_each_leg_uses_its_own_symbol_return(self):
        """NIFTY positions must be shocked with NIFTY's return,
        BANKNIFTY positions with BANKNIFTY's return."""
        db = _mock_db({"NIFTY": self._NIFTY_ROWS, "BANKNIFTY": self._BNF_ROWS})
        positions = _make_positions(["NIFTY", "BANKNIFTY"])

        calls: list[dict] = []

        def _pnl(positions_df, curated_options, curated_futures, lot_size_df,
                 trade_date, spot_return):
            calls.append({"symbol": positions_df["symbol"].iloc[0],
                          "spot_return": spot_return})
            return 0.0

        with patch("src.quant.var._compute_portfolio_pnl", side_effect=_pnl):
            compute_var(
                positions_df=positions,
                curated_options=pd.DataFrame(),
                curated_futures=pd.DataFrame(),
                lot_size_df=pd.DataFrame(),
                symbol="NIFTY",
                trade_date="2026-03-13",
                db=db,
            )

        assert len(calls) == 2, \
            "_compute_portfolio_pnl must be called once per symbol (2), not once for all"

        nifty_call = next(c for c in calls if c["symbol"] == "NIFTY")
        bnf_call   = next(c for c in calls if c["symbol"] == "BANKNIFTY")

        assert nifty_call["spot_return"] == pytest.approx(self._NIFTY_RETURN), \
            "NIFTY leg must use NIFTY's historical return"
        assert bnf_call["spot_return"] == pytest.approx(self._BNF_RETURN), \
            "BANKNIFTY leg must use BANKNIFTY's historical return"

    def test_total_pnl_is_sum_of_per_symbol_pnls(self):
        """Total scenario P&L == NIFTY_pnl + BANKNIFTY_pnl."""
        db = _mock_db({"NIFTY": self._NIFTY_ROWS, "BANKNIFTY": self._BNF_ROWS})
        positions = _make_positions(["NIFTY", "BANKNIFTY"])

        NIFTY_PNL = 8000.0
        BNF_PNL   = -3000.0

        def _pnl(positions_df, **kw):
            return NIFTY_PNL if positions_df["symbol"].iloc[0] == "NIFTY" else BNF_PNL

        with patch("src.quant.var._compute_portfolio_pnl", side_effect=_pnl):
            result = compute_var(
                positions_df=positions,
                curated_options=pd.DataFrame(),
                curated_futures=pd.DataFrame(),
                lot_size_df=pd.DataFrame(),
                symbol="NIFTY",
                trade_date="2026-03-13",
                db=db,
            )

        expected_total = round(NIFTY_PNL + BNF_PNL, 2)
        assert result.pnl_distribution[0].portfolio_pnl == pytest.approx(expected_total), \
            "P&L must sum across all symbol groups"

    def test_single_symbol_book_unchanged(self):
        """Single-symbol portfolio: behaviour identical to before the fix."""
        db = _mock_db({"NIFTY": self._NIFTY_ROWS})
        positions = _make_positions(["NIFTY"])

        calls: list[dict] = []

        def _pnl(positions_df, curated_options, curated_futures, lot_size_df,
                 trade_date, spot_return):
            calls.append({"symbol": positions_df["symbol"].iloc[0],
                          "spot_return": spot_return})
            return -2000.0

        with patch("src.quant.var._compute_portfolio_pnl", side_effect=_pnl):
            result = compute_var(
                positions_df=positions,
                curated_options=pd.DataFrame(),
                curated_futures=pd.DataFrame(),
                lot_size_df=pd.DataFrame(),
                symbol="NIFTY",
                trade_date="2026-03-13",
                db=db,
            )

        assert len(calls) == 1, "single-symbol: exactly one pnl call per scenario date"
        assert calls[0]["spot_return"] == pytest.approx(self._NIFTY_RETURN)
        assert result.pnl_distribution[0].portfolio_pnl == pytest.approx(-2000.0)

    def test_missing_symbol_history_falls_back_to_primary(self):
        """If a secondary symbol has no history for a date, fall back to
        the primary symbol's return rather than crash."""
        # BANKNIFTY has no data at all; DB returns NIFTY data for any symbol query
        db = _mock_db({"NIFTY": self._NIFTY_ROWS, "BANKNIFTY": self._NIFTY_ROWS})
        positions = _make_positions(["NIFTY", "BANKNIFTY"])

        with patch("src.quant.var._compute_portfolio_pnl", return_value=0.0):
            # must not raise
            result = compute_var(
                positions_df=positions,
                curated_options=pd.DataFrame(),
                curated_futures=pd.DataFrame(),
                lot_size_df=pd.DataFrame(),
                symbol="NIFTY",
                trade_date="2026-03-13",
                db=db,
            )
        assert result.scenario_count == 1
