import io
import pandas as pd
import duckdb
from datetime import date
from app.schemas.portfolio import (PortfolioResponse, PositionResult, PortfolioSummary)
from src.quant.scenario_engine import Shock
from src.quant.portfolio import run_portfolio

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
    df["start_date"] = pd.to_datetime(df["start_date"]).dt.date
    df["end_date"]   = pd.to_datetime(df["end_date"]).dt.date
    return df


def _parse_csv(file_bytes: bytes) -> pd.DataFrame:
    try:
        df = pd.read_csv(io.BytesIO(file_bytes))
    except Exception as e:
        raise ValueError(f"Failed to parse CSV: {e}")
    return df


def _to_response(result) -> PortfolioResponse:
    positions = [
        PositionResult(
            symbol=p.symbol,
            expiry_date=p.expiry_date,
            strike=p.strike,
            option_type=p.option_type,
            quantity=p.quantity,
            lot_size=p.lot_size,
            entry_date=p.entry_date,
            entry_price=p.entry_price,
            current_price=p.current_price,
            mtm_pnl=p.mtm_pnl,
            scenario_pnl=p.scenario_pnl,
            total_pnl=p.total_pnl,
            method=p.method,
            delta=p.delta,
            gamma=p.gamma,
            vega=p.vega,
            theta=p.theta,
            rho=p.rho,
        )
        for p in result.positions
    ]

    summary = PortfolioSummary(
        total_mtm_pnl=result.summary.total_mtm_pnl,
        total_scenario_pnl=result.summary.total_scenario_pnl,
        total_pnl=result.summary.total_pnl,
        net_delta=result.summary.net_delta,
        net_gamma=result.summary.net_gamma,
        net_vega=result.summary.net_vega,
        net_theta=result.summary.net_theta,
        net_rho=result.summary.net_rho,
    )

    return PortfolioResponse(
        trade_date=result.trade_date,
        positions=positions,
        summary=summary,
    )


def analyze_portfolio(
    file_bytes: bytes,
    trade_date: date,
    shock: Shock,
    db: duckdb.DuckDBPyConnection,
) -> PortfolioResponse:
    positions_df     = _parse_csv(file_bytes)
    curated_options  = _query_curated_options(db, trade_date)
    curated_futures  = _query_curated_futures(db, trade_date)
    lot_size_df      = _query_lot_size(db)

    if curated_options.empty and curated_futures.empty:
        raise ValueError(
            f"No curated data found for trade_date={trade_date}. "
            f"Check that the pipeline has run for this date."
        )

    result = run_portfolio(
        positions_df=positions_df,
        curated_options=curated_options,
        curated_futures=curated_futures,
        lot_size_df=lot_size_df,
        shock=shock,
        trade_date=str(trade_date),
    )

    return _to_response(result)
