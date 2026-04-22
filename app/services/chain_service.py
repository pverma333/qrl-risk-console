import duckdb
import pandas as pd
from datetime import date

from app.schemas.chain import ChainRow, ChainResponse


def get_option_chain(
    symbol: str,
    trade_date: date,
    expiry_date: date,
    db: duckdb.DuckDBPyConnection,
) -> ChainResponse:
    query = """
        SELECT
            CAST(trade_date AS DATE)    AS trade_date,
            symbol,
            CAST(expiry_date AS DATE)   AS expiry_date,
            strike,
            option_type,
            open, high, low, close, settle,
            contracts, open_interest, chg_in_oi, dte,
            spot, div_yield, rate,
            iv, delta, gamma, vega, theta, rho
        FROM v_curated_option_chain
        WHERE symbol      = ?
          AND CAST(trade_date AS DATE)   = ?
          AND CAST(expiry_date AS DATE)  = ?
        ORDER BY strike ASC, option_type ASC
    """
    df: pd.DataFrame = db.execute(query, [symbol, trade_date, expiry_date]).df()

    if df.empty:
        return ChainResponse(
            symbol=symbol,
            trade_date=trade_date,
            expiry_date=expiry_date,
            row_count=0,
            iv_computed_count=0,
            iv_avg=0,
            rows=[],
        )

    iv_computed = int(df["iv"].notna().sum())
    iv_values = df[df["iv"].notna()]["iv"].values
    iv_avg = float(iv_values.mean()) if len(iv_values) > 0 else None

    rows = [ChainRow(**row) for row in df.to_dict(orient="records")]

    return ChainResponse(
        symbol=symbol,
        trade_date=trade_date,
        expiry_date=expiry_date,
        row_count=len(rows),
        iv_computed_count=iv_computed,
        iv_avg=iv_avg,
        rows=rows,
    )
