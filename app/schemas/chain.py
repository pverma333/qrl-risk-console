from pydantic import BaseModel
from datetime import date
from typing import Optional


class ChainRow(BaseModel):
    trade_date: date
    symbol: str
    expiry_date: date
    strike: float
    option_type: str
    open: float
    high: float
    low: float
    close: float
    settle: float
    contracts: int
    open_interest: int
    chg_in_oi: int
    dte: int
    spot: float
    div_yield: float
    rate: float
    iv: Optional[float] = None
    delta: Optional[float] = None
    gamma: Optional[float] = None
    vega: Optional[float] = None
    theta: Optional[float] = None
    rho: Optional[float] = None


class ChainResponse(BaseModel):
    symbol: str
    trade_date: date
    expiry_date: date
    row_count: int
    iv_computed_count: int
    iv_avg: float | None = None
    rows: list[ChainRow]
