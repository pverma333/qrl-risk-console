import duckdb
from datetime import date
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form

from app.dependencies import get_db
from app.schemas.var import VaRResponse
from app.services.var_service import analyze_var

router = APIRouter(prefix="/var", tags=["var"])

VALID_SYMBOLS = {"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"}


@router.post("/analyze", response_model=VaRResponse)
def var_endpoint(
    symbol:        str        = Form(...),
    trade_date:    date       = Form(...),
    lookback_days: int        = Form(default=252),
    file:          UploadFile = File(...),
    db:            duckdb.DuckDBPyConnection = Depends(get_db),
):
    symbol = symbol.upper()

    if symbol not in VALID_SYMBOLS:
        raise HTTPException(status_code=400, detail=f"Unknown symbol: {symbol}")

    if lookback_days < 10 or lookback_days > 1260:
        raise HTTPException(
            status_code=400,
            detail="lookback_days must be between 10 and 1260."
        )

    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Uploaded file must be a CSV.")

    file_bytes = file.file.read()
    if len(file_bytes) == 0:
        raise HTTPException(status_code=400, detail="Uploaded CSV is empty.")

    try:
        return analyze_var(
            file_bytes=file_bytes,
            symbol=symbol,
            trade_date=trade_date,
            lookback_days=lookback_days,
            db=db,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
