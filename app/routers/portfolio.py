import duckdb
from datetime import date
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form

from app.dependencies import get_db
from app.schemas.portfolio import PortfolioResponse, ShockInput
from app.services.portfolio_service import analyze_portfolio
from src.quant.scenario_engine import Shock

router = APIRouter(prefix="/portfolio", tags=["portfolio"])

VALID_SYMBOLS = {"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"}


@router.post("/analyze", response_model=PortfolioResponse)
def portfolio_endpoint(
    trade_date:      date        = Form(...),
    spot_shock_pct:  float       = Form(...),
    vol_shock_abs:   float       = Form(...),
    rate_shock_bps:  float       = Form(...),
    file:            UploadFile  = File(...),
    db:              duckdb.DuckDBPyConnection = Depends(get_db),
):
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Uploaded file must be a CSV.")

    file_bytes = file.file.read()
    if len(file_bytes) == 0:
        raise HTTPException(status_code=400, detail="Uploaded CSV is empty.")

    shock = Shock(
        spot_shock_pct=spot_shock_pct,
        vol_shock_abs=vol_shock_abs,
        rate_shock_bps=rate_shock_bps,
    )

    try:
        return analyze_portfolio(
            file_bytes=file_bytes,
            trade_date=trade_date,
            shock=shock,
            db=db,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
