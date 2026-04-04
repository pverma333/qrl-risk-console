from pydantic import BaseModel, Field
from datetime import date


class ScenarioParameters(BaseModel):
    symbol: str
    trade_date: date
    expiry_date: date
    spot_shock_pct: float = Field(..., description="Percentage. -1.5 means spot drops 1.5%.")
    vol_shock_abs: float = Field(..., description="Absolute vol points. +2.0 means IV rises 2 points.")
    rate_shock_bps: float = Field(..., description="Basis points. +20 means rate rises 20bps.")


class ScenarioResponse(BaseModel):
    status: str = "stub"
    message: str = "Scenario engine not yet implemented. Returns in Milestone 2."
    parameters_received: ScenarioParameters
