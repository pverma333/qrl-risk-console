import numpy as np
import pandas as pd
import duckdb
from dataclasses import dataclass
from typing import Optional

from src.quant.scenario_engine import Shock
from src.quant.portfolio import run_portfolio


@dataclass
class ScenarioPnLPoint:
    date:           str
    spot_return_pct: float
    portfolio_pnl:  float


@dataclass
class VaRResult:
    symbol:             str
    trade_date:         str
    lookback_days:      int
    scenario_count:     int
    var_95:             float
    var_99:             float
    cvar_95:            float
    cvar_99:            float
    mean_pnl:           float
    min_pnl:            float
    max_pnl:            float
    pnl_distribution:   list[ScenarioPnLPoint]


def _fetch_historical_returns(
    db: duckdb.DuckDBPyConnection,
    symbol: str,
    trade_date: str,
    lookback_days: int,
) -> pd.DataFrame:
    query = """
        WITH spot_series AS (
            SELECT
                CAST(trade_date AS DATE) AS trade_date,
                close AS spot
            FROM v_processed_index_spot
            WHERE symbol = ?
              AND CAST(trade_date AS DATE) <= ?
            ORDER BY trade_date DESC
            LIMIT ?
        ),
        with_lag AS (
            SELECT
                trade_date,
                spot,
                LAG(spot) OVER (ORDER BY trade_date ASC) AS prev_spot
            FROM spot_series
        )
        SELECT
            trade_date,
            spot,
            prev_spot,
            (spot - prev_spot) / prev_spot AS daily_return
        FROM with_lag
        WHERE prev_spot IS NOT NULL
        ORDER BY trade_date ASC
    """
    df = db.execute(query, [symbol, trade_date, lookback_days + 1]).df()
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    return df


def _compute_portfolio_pnl(
    positions_df: pd.DataFrame,
    curated_options: pd.DataFrame,
    curated_futures: pd.DataFrame,
    lot_size_df: pd.DataFrame,
    trade_date: str,
    spot_return: float,
) -> float:
    shock = Shock(
        spot_shock_pct=spot_return * 100.0,
        vol_shock_abs=0.0,
        rate_shock_bps=0.0,
    )
    result = run_portfolio(
        positions_df=positions_df.copy(),
        curated_options=curated_options,
        curated_futures=curated_futures,
        lot_size_df=lot_size_df,
        shock=shock,
        trade_date=trade_date,
    )
    return result.summary.total_scenario_pnl


def compute_var(
    positions_df: pd.DataFrame,
    curated_options: pd.DataFrame,
    curated_futures: pd.DataFrame,
    lot_size_df: pd.DataFrame,
    symbol: str,
    trade_date: str,
    db: duckdb.DuckDBPyConnection,
    lookback_days: int = 252,
) -> VaRResult:
    returns_df = _fetch_historical_returns(
        db=db,
        symbol=symbol,
        trade_date=trade_date,
        lookback_days=lookback_days,
    )

    if returns_df.empty:
        raise ValueError(
            f"No historical returns found for {symbol} up to {trade_date}. "
            f"Check that processed index spot data exists."
        )

    scenarios: list[ScenarioPnLPoint] = []

    for _, row in returns_df.iterrows():
        spot_return  = float(row["daily_return"])
        scenario_date = str(row["trade_date"])

        pnl = _compute_portfolio_pnl(
            positions_df=positions_df,
            curated_options=curated_options,
            curated_futures=curated_futures,
            lot_size_df=lot_size_df,
            trade_date=trade_date,
            spot_return=spot_return,
        )

        scenarios.append(ScenarioPnLPoint(
            date=scenario_date,
            spot_return_pct=round(spot_return * 100, 4),
            portfolio_pnl=round(pnl, 2),
        ))

    pnl_array = np.array([s.portfolio_pnl for s in scenarios])

    var_95  = float(-np.percentile(pnl_array, 5))
    var_99  = float(-np.percentile(pnl_array, 1))
    cvar_95 = float(-pnl_array[pnl_array < -var_95].mean()) if (pnl_array < -var_95).any() else var_95
    cvar_99 = float(-pnl_array[pnl_array < -var_99].mean()) if (pnl_array < -var_99).any() else var_99

    return VaRResult(
        symbol=symbol,
        trade_date=trade_date,
        lookback_days=lookback_days,
        scenario_count=len(scenarios),
        var_95=round(var_95, 2),
        var_99=round(var_99, 2),
        cvar_95=round(cvar_95, 2),
        cvar_99=round(cvar_99, 2),
        mean_pnl=round(float(pnl_array.mean()), 2),
        min_pnl=round(float(pnl_array.min()), 2),
        max_pnl=round(float(pnl_array.max()), 2),
        pnl_distribution=scenarios,
    )
