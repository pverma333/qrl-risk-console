import duckdb
from datetime import date
from fastapi import HTTPException

from app.schemas.vix import VIXResponse


def get_vix(trade_date: date, db: duckdb.DuckDBPyConnection) -> VIXResponse:
    query = """
        SELECT
            CAST(trade_date AS DATE) AS trade_date,
            close
        FROM v_processed_vix
        WHERE CAST(trade_date AS DATE) = ?
    """
    result = db.execute(query, [trade_date]).fetchone()

    if result is None:
        raise HTTPException(
            status_code=404,
            detail=f"No VIX data found for {trade_date}. Check that it is a valid NSE trading day."
        )

    return VIXResponse(trade_date=result[0], close=result[1])
