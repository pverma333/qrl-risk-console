from pydantic import BaseModel, Field
from datetime import date
from typing import Optional


class PositionInput(BaseModel):
    symbol:       str
    expiry_date:  date
    strike:       float
    option_type:  str
    quantity:     int
    entry_date:   date
    entry_price:  float


class ShockInput(BaseModel):
    spot_shock_pct:  float = Field(..., description="Percentage. -1.5 means spot drops 1.5%.")
    vol_shock_abs:   float = Field(..., description="Absolute vol points. +2.0 means IV rises 2 points.")
    rate_shock_bps:  float = Field(..., description="Basis points. +20 means rate rises 20bps.")


class PortfolioRequest(BaseModel):
    trade_date:  date
    shock:       ShockInput


class PositionResult(BaseModel):
    symbol:         str
    expiry_date:    date
    strike:         float
    option_type:    str
    quantity:       int
    lot_size:       int
    entry_date:     date
    entry_price:    float
    current_price:  Optional[float] = None
    mtm_pnl:        float
    scenario_pnl:   float
    total_pnl:      float
    method:         str
    delta:          Optional[float] = None
    gamma:          Optional[float] = None
    vega:           Optional[float] = None
    theta:          Optional[float] = None
    rho:            Optional[float] = None


class PortfolioSummary(BaseModel):
    total_mtm_pnl:      float
    total_scenario_pnl: float
    total_pnl:          float
    net_delta:          float
    net_gamma:          float
    net_vega:           float
    net_theta:          float
    net_rho:            float


class PortfolioResponse(BaseModel):
    trade_date:  date
    positions:   list[PositionResult]
    summary:     PortfolioSummary
