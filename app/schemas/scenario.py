from pydantic import BaseModel, Field
from datetime import date
from typing import Optional


class ScenarioRequest(BaseModel):
    symbol:        str
    trade_date:    date
    expiry_date:   date
    strike:        float
    option_type:   str
    quantity:      int   = Field(default=1, description="Positive=long, negative=short.")
    spot_shock_pct: float = Field(..., description="Percentage. -1.5 means spot drops 1.5%.")
    vol_shock_abs:  float = Field(..., description="Absolute vol points. +2.0 means IV rises 2 points.")
    rate_shock_bps: float = Field(..., description="Basis points. +20 means rate rises 20bps.")


class ScenarioResponse(BaseModel):
    symbol:        str
    trade_date:    date
    expiry_date:   date
    strike:        float
    option_type:   str
    quantity:      int
    lot_size:      int
    base_price:    Optional[float] = None
    shocked_price: Optional[float] = None
    mtm_pnl:       float
    scenario_pnl:  float
    total_pnl:     float
    method:        str
    delta:         Optional[float] = None
    gamma:         Optional[float] = None
    vega:          Optional[float] = None
    theta:         Optional[float] = None
    rho:           Optional[float] = None
