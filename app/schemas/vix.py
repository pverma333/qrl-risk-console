from pydantic import BaseModel
from datetime import date


class VIXRow(BaseModel):
    trade_date: date
    close: float


class VIXResponse(BaseModel):
    trade_date: date
    close: float
