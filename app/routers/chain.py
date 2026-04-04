import duckdb
from datetime import date
from fastapi import APIRouter, Depends, HTTPException

from app.dependencies import get_db
from app.schemas.chain import ChainResponse
from app.services.chain_service import get_option_chain

router = APIRouter(prefix="/chain", tags=["chain"])

VALID_SYMBOLS = {"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"}


@router.get("/{symbol}/{trade_date}/{expiry_date}", response_model=ChainResponse)
def chain_endpoint(
    symbol: str,
    trade_date: date,
    expiry_date: date,
    db: duckdb.DuckDBPyConnection = Depends(get_db),
):
    symbol = symbol.upper()

    if symbol not in VALID_SYMBOLS:
        raise HTTPException(status_code=400, detail=f"Unknown symbol: {symbol}. Valid: {VALID_SYMBOLS}")

    if expiry_date < trade_date:
        raise HTTPException(status_code=400, detail="expiry_date must be >= trade_date")

    return get_option_chain(symbol, trade_date, expiry_date, db)
