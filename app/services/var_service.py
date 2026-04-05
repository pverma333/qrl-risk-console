import io
import duckdb
import pandas as pd
from datetime import date

from app.schemas.var import VaRResponse, VaRSummary, ScenarioPnLPoint
from src.quant.var import compute_var


def _query_curated_options(db: duckdb.DuckDBPyConnection, trade_date: date) -> pd.DataFrame:
    query = """
        SELECT
            CAST(trade_date AS DATE)  AS trade_date,
            symbol,
            CAST(expiry_date AS DATE) AS expiry_date,
            strike,
            option_type,
            dte,
            spot,
            div_yield,
            rate,
            iv,
            delta, gamma, vega, theta, rho
        FROM v_curated_option_chain
        WHERE CAST(trade_date AS DATE) = ?
    """
    df = db.execute(query, [trade_date]).df()
    df["trade_date"]  = pd.to_datetime(df["trade_date"]).dt.date
    df["expiry_date"] = pd.to_datetime(df["expiry_date"]).dt.date
    return df


def _query_curated_futures(db: duckdb.DuckDBPyConnection, trade_date: date) -> pd.DataFrame:
    query = """
        SELECT
            CAST(trade_date AS DATE)  AS trade_date,
            symbol,
            CAST(expiry_date AS DATE) AS expiry_date,
            dte,
            spot,
            div_yield,
            rate,
            settle
        FROM v_curated_futures
        WHERE CAST(trade_date AS DATE) = ?
    """
    df = db.execute(query, [trade_date]).df()
    df["trade_date"]  = pd.to_datetime(df["trade_date"]).dt.date
    df["expiry_date"] = pd.to_datetime(df["expiry_date"]).dt.date
    return df


def _query_lot_size(db: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    query = """
        SELECT
            symbol,
            CAST(start_date AS DATE) AS start_date,
            CAST(end_date   AS DATE) AS end_date,
            lot_size
        FROM v_processed_lot_size
    """
    df = db.execute(query).df()
    df["start_date"] = pd.to_datetime(df["start_date"], errors="coerce").dt.date
    df["end_date"]   = pd.to_datetime(df["end_date"],   errors="coerce").dt.date
    return df


def _parse_csv(file_bytes: bytes) -> pd.DataFrame:
    try:
        return pd.read_csv(io.BytesIO(file_bytes))
    except Exception as e:
        raise ValueError(f"Failed to parse CSV: {e}")


def _to_response(result) -> VaRResponse:
    summary = VaRSummary(
        symbol=result.symbol,
        trade_date=result.trade_date,
        lookback_days=result.lookback_days,
        scenario_count=result.scenario_count,
        var_95=result.var_95,
        var_99=result.var_99,
        cvar_95=result.cvar_95,
        cvar_99=result.cvar_99,
        mean_pnl=result.mean_pnl,
        min_pnl=result.min_pnl,
        max_pnl=result.max_pnl,
    )
    distribution = [
        ScenarioPnLPoint(
            date=s.date,
            spot_return_pct=s.spot_return_pct,
            portfolio_pnl=s.portfolio_pnl,
        )
        for s in result.pnl_distribution
    ]
    return VaRResponse(summary=summary, pnl_distribution=distribution)


def analyze_var(
    file_bytes: bytes,
    symbol: str,
    trade_date: date,
    lookback_days: int,
    db: duckdb.DuckDBPyConnection,
) -> VaRResponse:
    positions_df    = _parse_csv(file_bytes)
    curated_options = _query_curated_options(db, trade_date)
    curated_futures = _query_curated_futures(db, trade_date)
    lot_size_df     = _query_lot_size(db)

    if curated_options.empty and curated_futures.empty:
        raise ValueError(
            f"No curated data found for trade_date={trade_date}. "
            f"Check that the pipeline has run for this date."
        )

    result = compute_var(
        positions_df=positions_df,
        curated_options=curated_options,
        curated_futures=curated_futures,
        lot_size_df=lot_size_df,
        symbol=symbol,
        trade_date=str(trade_date),
        db=db,
        lookback_days=lookback_days,
    )

    return _to_response(result)
