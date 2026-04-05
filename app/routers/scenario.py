import duckdb
from fastapi import APIRouter, Depends, HTTPException

from app.dependencies import get_db
from app.schemas.scenario import ScenarioRequest, ScenarioResponse
from app.services.scenario_service import run_scenario

router = APIRouter(prefix="/scenario", tags=["scenario"])

VALID_SYMBOLS   = {"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"}
VALID_OPT_TYPES = {"CE", "PE", "XX"}


@router.post("/", response_model=ScenarioResponse)
def scenario_endpoint(
    req: ScenarioRequest,
    db: duckdb.DuckDBPyConnection = Depends(get_db),
):
    if req.symbol.upper() not in VALID_SYMBOLS:
        raise HTTPException(status_code=400, detail=f"Unknown symbol: {req.symbol}")

    if req.option_type.upper() not in VALID_OPT_TYPES:
        raise HTTPException(status_code=400, detail=f"Unknown option_type: {req.option_type}")

    if req.expiry_date < req.trade_date:
        raise HTTPException(status_code=400, detail="expiry_date must be >= trade_date")

    if req.quantity == 0:
        raise HTTPException(status_code=400, detail="quantity cannot be zero")

    try:
        return run_scenario(req=req, db=db)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
