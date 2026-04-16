from fastapi import APIRouter, Depends, HTTPException
from datetime import date
import duckdb

from app.dependencies import get_db
from app.schemas.market import MarketSummaryResponse, VIXData, YieldData
from app.services.market_service import get_market_summary


router = APIRouter(prefix="/market", tags=["market"])


@router.get("/summary/{trade_date}", response_model=MarketSummaryResponse)
def fetch_market_summary(
    trade_date: date,
    conn: duckdb.DuckDBPyConnection = Depends(get_db)
):
    try:
        result = get_market_summary(conn, trade_date)

        return MarketSummaryResponse(
            indices=result['indices'],
            vix=VIXData(value=result['vix']['value']),
            yields=YieldData(
                rate_3m=result['yields']['3M'],
                rate_6m=result['yields']['6M'],
                rate_1y=result['yields']['1Y']
            ),
            chart_data=result['chart_data']
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch market summary: {str(e)}")
