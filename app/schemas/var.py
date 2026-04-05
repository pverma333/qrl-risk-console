from pydantic import BaseModel, Field
from datetime import date
from typing import Optional


class ScenarioPnLPoint(BaseModel):
    date:            str
    spot_return_pct: float
    portfolio_pnl:   float


class VaRRequest(BaseModel):
    symbol:        str
    trade_date:    date
    lookback_days: int = Field(default=252, ge=10, le=1260)


class VaRSummary(BaseModel):
    symbol:         str
    trade_date:     date
    lookback_days:  int
    scenario_count: int
    var_95:         float
    var_99:         float
    cvar_95:        float
    cvar_99:        float
    mean_pnl:       float
    min_pnl:        float
    max_pnl:        float


class VaRResponse(BaseModel):
    summary:          VaRSummary
    pnl_distribution: list[ScenarioPnLPoint]
