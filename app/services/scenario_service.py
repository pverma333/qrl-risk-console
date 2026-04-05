import duckdb
import pandas as pd
from datetime import date
from fastapi import HTTPException

from app.schemas.scenario import ScenarioRequest, ScenarioResponse
from src.quant.scenario_engine import (
    MarketSnapshot, Shock, OptionContract, FuturesContract,
    scenario_option, scenario_futures,
)


def _query_option_row(
    db: duckdb.DuckDBPyConnection,
    symbol: str,
    trade_date: date,
    expiry_date: date,
    strike: float,
    option_type: str,
) -> pd.Series | None:
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
        WHERE symbol      = ?
          AND CAST(trade_date AS DATE)  = ?
          AND CAST(expiry_date AS DATE) = ?
          AND strike      = ?
          AND option_type = ?
        LIMIT 1
    """
    df = db.execute(query, [symbol, trade_date, expiry_date, strike, option_type]).df()
    if df.empty:
        return None
    return df.iloc[0]


def _query_futures_row(
    db: duckdb.DuckDBPyConnection,
    symbol: str,
    trade_date: date,
    expiry_date: date,
) -> pd.Series | None:
    query = """
        SELECT
            CAST(trade_date AS DATE)  AS trade_date,
            symbol,
            CAST(expiry_date AS DATE) AS expiry_date,
            dte,
            spot,
            div_yield,
            rate
        FROM v_curated_futures
        WHERE symbol      = ?
          AND CAST(trade_date AS DATE)  = ?
          AND CAST(expiry_date AS DATE) = ?
        LIMIT 1
    """
    df = db.execute(query, [symbol, trade_date, expiry_date]).df()
    if df.empty:
        return None
    return df.iloc[0]


def _query_lot_size(
    db: duckdb.DuckDBPyConnection,
    symbol: str,
    trade_date: date,
) -> int:
    query = """
        SELECT lot_size, start_date, end_date
        FROM v_processed_lot_size
        WHERE symbol = ?
    """
    df = db.execute(query, [symbol]).df()
    if df.empty:
        return 1

    df["start_date"] = pd.to_datetime(df["start_date"], errors="coerce").dt.date
    df["end_date"]   = pd.to_datetime(df["end_date"],   errors="coerce").dt.date

    trade_date_as_date = pd.Timestamp(trade_date).date()

    lot_rows = df[
        (df["start_date"] <= trade_date_as_date) &
        (df["end_date"].isna() | (df["end_date"] >= trade_date_as_date))
    ]
    return int(lot_rows.iloc[0]["lot_size"]) if not lot_rows.empty else 1


def _build_snapshot_from_option(row: pd.Series) -> MarketSnapshot:
    return MarketSnapshot(
        spot=float(row["spot"]),
        iv=float(row["iv"])       if pd.notna(row["iv"])    else None,
        rate=float(row["rate"]),
        div_yield=float(row["div_yield"]),
        dte=int(row["dte"]),
        delta=float(row["delta"]) if pd.notna(row["delta"]) else None,
        gamma=float(row["gamma"]) if pd.notna(row["gamma"]) else None,
        vega=float(row["vega"])   if pd.notna(row["vega"])  else None,
        theta=float(row["theta"]) if pd.notna(row["theta"]) else None,
        rho=float(row["rho"])     if pd.notna(row["rho"])   else None,
    )


def _build_snapshot_from_futures(row: pd.Series) -> MarketSnapshot:
    return MarketSnapshot(
        spot=float(row["spot"]),
        iv=None,
        rate=float(row["rate"]),
        div_yield=float(row["div_yield"]),
        dte=int(row["dte"]),
        delta=1.0, gamma=0.0, vega=0.0, theta=0.0, rho=0.0,
    )


def run_scenario(
    req: ScenarioRequest,
    db: duckdb.DuckDBPyConnection,
) -> ScenarioResponse:
    symbol      = req.symbol.upper()
    lot_size    = _query_lot_size(db, symbol, req.trade_date)

    shock = Shock(
        spot_shock_pct=req.spot_shock_pct,
        vol_shock_abs=req.vol_shock_abs,
        rate_shock_bps=req.rate_shock_bps,
    )

    if req.option_type == "XX":
        row = _query_futures_row(db, symbol, req.trade_date, req.expiry_date)
        if row is None:
            raise HTTPException(
                status_code=404,
                detail=f"No futures data found for {symbol} "
                       f"trade_date={req.trade_date} expiry={req.expiry_date}."
            )

        snapshot = _build_snapshot_from_futures(row)
        contract = FuturesContract(quantity=req.quantity, lot_size=lot_size)
        result   = scenario_futures(snapshot, contract, shock)

        return ScenarioResponse(
            symbol=symbol,
            trade_date=req.trade_date,
            expiry_date=req.expiry_date,
            strike=req.strike,
            option_type=req.option_type,
            quantity=req.quantity,
            lot_size=lot_size,
            base_price=result.base_price,
            shocked_price=result.shocked_price,
            mtm_pnl=result.mtm_pnl,
            scenario_pnl=result.pnl_total,
            total_pnl=result.pnl_total,
            method=result.method,
            delta=1.0, gamma=0.0, vega=0.0, theta=0.0, rho=0.0,
        )

    else:
        row = _query_option_row(
            db, symbol, req.trade_date, req.expiry_date,
            req.strike, req.option_type,
        )
        if row is None:
            raise HTTPException(
                status_code=404,
                detail=f"No option data found for {symbol} "
                       f"strike={req.strike} {req.option_type} "
                       f"trade_date={req.trade_date} expiry={req.expiry_date}."
            )

        snapshot = _build_snapshot_from_option(row)
        contract = OptionContract(
            strike=req.strike,
            option_type=req.option_type,
            quantity=req.quantity,
            lot_size=lot_size,
        )
        result = scenario_option(snapshot, contract, shock)

        return ScenarioResponse(
            symbol=symbol,
            trade_date=req.trade_date,
            expiry_date=req.expiry_date,
            strike=req.strike,
            option_type=req.option_type,
            quantity=req.quantity,
            lot_size=lot_size,
            base_price=result.base_price,
            shocked_price=result.shocked_price,
            mtm_pnl=result.mtm_pnl,
            scenario_pnl=result.pnl_total,
            total_pnl=result.pnl_total,
            method=result.method,
            delta=snapshot.delta,
            gamma=snapshot.gamma,
            vega=snapshot.vega,
            theta=snapshot.theta,
            rho=snapshot.rho,
        )
