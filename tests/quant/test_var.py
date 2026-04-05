import numpy as np
import pandas as pd
import pytest
from unittest.mock import MagicMock, patch
from src.quant.var import compute_var, VaRResult, _fetch_historical_returns


def make_positions_df():
    return pd.DataFrame([{
        "symbol": "NIFTY",
        "expiry_date": "2026-03-24",
        "strike": 22500.0,
        "option_type": "CE",
        "quantity": 2,
        "entry_date": "2026-03-10",
        "entry_price": 120.50,
    }])


def make_mock_db(returns: list[tuple]):
    mock_db = MagicMock()
    df = pd.DataFrame(returns, columns=["trade_date", "spot", "prev_spot", "daily_return"])
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    mock_result = MagicMock()
    mock_result.df.return_value = df
    mock_db.execute.return_value = mock_result
    return mock_db


class TestFetchHistoricalReturns:

    def test_returns_dataframe_with_correct_columns(self):
        returns = [
            ("2026-03-10", 22000, 21800, 0.00917),
            ("2026-03-11", 21800, 22000, -0.00909),
        ]
        db = make_mock_db(returns)
        df = _fetch_historical_returns(db, "NIFTY", "2026-03-13", 252)
        assert "daily_return" in df.columns
        assert "trade_date" in df.columns
        assert len(df) == 2

    def test_empty_returns_raises(self):
        db = make_mock_db([])
        with pytest.raises(ValueError, match="No historical returns found"):
            from src.quant.var import compute_var
            compute_var(
                positions_df=make_positions_df(),
                curated_options=pd.DataFrame(),
                curated_futures=pd.DataFrame(),
                lot_size_df=pd.DataFrame(),
                symbol="NIFTY",
                trade_date="2026-03-13",
                db=db,
                lookback_days=252,
            )


class TestComputeVar:

    def _make_pnl_sequence(self, n=252, seed=42):
        rng = np.random.default_rng(seed)
        return rng.normal(loc=0, scale=10000, size=n).tolist()

    def test_var_99_greater_than_var_95(self):
        pnl = self._make_pnl_sequence()
        pnl_array = np.array(pnl)
        var_95 = float(-np.percentile(pnl_array, 5))
        var_99 = float(-np.percentile(pnl_array, 1))
        assert var_99 >= var_95

    def test_cvar_95_greater_than_var_95(self):
        pnl = self._make_pnl_sequence()
        pnl_array = np.array(pnl)
        var_95  = float(-np.percentile(pnl_array, 5))
        cvar_95 = float(-pnl_array[pnl_array < -var_95].mean())
        assert cvar_95 >= var_95

    def test_cvar_99_greater_than_var_99(self):
        pnl = self._make_pnl_sequence()
        pnl_array = np.array(pnl)
        var_99  = float(-np.percentile(pnl_array, 1))
        cvar_99 = float(-pnl_array[pnl_array < -var_99].mean())
        assert cvar_99 >= var_99

    def test_all_positive_pnl_gives_zero_var(self):
        pnl_array = np.array([1000.0] * 252)
        var_95 = float(-np.percentile(pnl_array, 5))
        assert var_95 <= 0

    def test_scenario_count_matches_lookback(self):
        returns = [
            ("2026-03-{:02d}".format(i), 22000 + i*10,
             22000 + (i-1)*10, 0.001) for i in range(1, 11)
        ]
        db = make_mock_db(returns)
        with patch("src.quant.var._compute_portfolio_pnl", return_value=-5000.0):
            result = compute_var(
                positions_df=make_positions_df(),
                curated_options=pd.DataFrame(),
                curated_futures=pd.DataFrame(),
                lot_size_df=pd.DataFrame(),
                symbol="NIFTY",
                trade_date="2026-03-13",
                db=db,
                lookback_days=252,
            )
        assert result.scenario_count == 10

    def test_var_result_fields_present(self):
        returns = [
            ("2026-03-{:02d}".format(i), 22000, 21900, 0.001 * (i - 5))
            for i in range(1, 11)
        ]
        db = make_mock_db(returns)
        with patch("src.quant.var._compute_portfolio_pnl", return_value=-5000.0):
            result = compute_var(
                positions_df=make_positions_df(),
                curated_options=pd.DataFrame(),
                curated_futures=pd.DataFrame(),
                lot_size_df=pd.DataFrame(),
                symbol="NIFTY",
                trade_date="2026-03-13",
                db=db,
                lookback_days=252,
            )
        assert hasattr(result, "var_95")
        assert hasattr(result, "var_99")
        assert hasattr(result, "cvar_95")
        assert hasattr(result, "cvar_99")
        assert hasattr(result, "pnl_distribution")
        assert isinstance(result.pnl_distribution, list)
