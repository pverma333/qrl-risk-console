from pydantic import BaseModel
from typing import List, Optional


class IndexData(BaseModel):
    symbol: str
    display_name: str
    open: float
    high: float
    low: float
    close: float
    change: float
    change_pct: float


class VIXData(BaseModel):
    value: Optional[float] = None


class YieldData(BaseModel):
    rate_3m: Optional[float] = None
    rate_6m: Optional[float] = None
    rate_1y: Optional[float] = None


class ChartDataPoint(BaseModel):
    date: str
    symbol: str
    close: float


class MarketSummaryResponse(BaseModel):
    indices: List[IndexData]
    vix: VIXData
    yields: YieldData
    chart_data: List[ChartDataPoint]
