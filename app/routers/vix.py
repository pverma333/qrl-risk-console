import duckdb
from datetime import date
from fastapi import APIRouter, Depends

from app.dependencies import get_db
from app.schemas.vix import VIXResponse
from app.services.vix_service import get_vix

router = APIRouter(prefix="/vix", tags=["vix"])


@router.get("/{trade_date}", response_model=VIXResponse)
def vix_endpoint(
    trade_date: date,
    db: duckdb.DuckDBPyConnection = Depends(get_db),
):
    return get_vix(trade_date, db)
