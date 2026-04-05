import math
from dataclasses import dataclass
from typing import Optional

from src.quant.black_scholes import _bs_price, _bs_greeks, _time_to_expiry
from src.quant.yield_curve import TenorRates, interpolate_rate


@dataclass
class MarketSnapshot:
    spot:          float
    iv:            Optional[float]
    rate:          float
    div_yield:     float
    dte:           int
    delta:         Optional[float]
    gamma:         Optional[float]
    vega:          Optional[float]
    theta:         Optional[float]
    rho:           Optional[float]


@dataclass
class Shock:
    spot_shock_pct:  float
    vol_shock_abs:   float
    rate_shock_bps:  float


@dataclass
class OptionContract:
    strike:      float
    option_type: str
    quantity:    int
    lot_size:    int


@dataclass
class FuturesContract:
    quantity:  int
    lot_size:  int


@dataclass
class ScenarioPnL:
    base_price:     Optional[float]
    shocked_price:  Optional[float]
    mtm_pnl:        float
    pnl_per_lot:    float
    pnl_total:      float
    method:         str


def _apply_shock_to_market(snapshot: MarketSnapshot, shock: Shock) -> tuple[float, float, float]:
    S_shocked = snapshot.spot * (1.0 + shock.spot_shock_pct / 100.0)
    r_shocked = snapshot.rate + shock.rate_shock_bps / 10000.0
    σ_shocked = (snapshot.iv or 0.0) + shock.vol_shock_abs / 100.0
    return S_shocked, r_shocked, σ_shocked


def price_option(snapshot: MarketSnapshot, contract: OptionContract) -> ScenarioPnL:
    if snapshot.dte <= 0:
        return ScenarioPnL(
            base_price=None, shocked_price=None,
            mtm_pnl=0.0, pnl_per_lot=0.0,
            pnl_total=0.0, method="expired"
        )

    T = _time_to_expiry(snapshot.dte)
    if snapshot.iv is not None and snapshot.iv > 0:
        base_price = _bs_price(
            snapshot.spot, contract.strike, T,
            snapshot.rate, snapshot.div_yield,
            snapshot.iv, contract.option_type
        )
    else:
        base_price = None

    return ScenarioPnL(
        base_price=base_price,
        shocked_price=None,
        mtm_pnl=0.0,
        pnl_per_lot=0.0,
        pnl_total=0.0,
        method="base_price_only"
    )


def scenario_option(
    snapshot: MarketSnapshot,
    contract: OptionContract,
    shock: Shock,
) -> ScenarioPnL:
    if snapshot.dte <= 0:
        return ScenarioPnL(
            base_price=None, shocked_price=None,
            mtm_pnl=0.0, pnl_per_lot=0.0,
            pnl_total=0.0, method="expired"
        )

    T             = _time_to_expiry(snapshot.dte)
    S_shocked, r_shocked, σ_shocked = _apply_shock_to_market(snapshot, shock)
    σ_shocked     = max(σ_shocked, 1e-4)
    multiplier    = contract.quantity * contract.lot_size

    if snapshot.iv is not None and snapshot.iv > 0:
        base_price = _bs_price(
            snapshot.spot, contract.strike, T,
            snapshot.rate, snapshot.div_yield,
            snapshot.iv, contract.option_type
        )
        shocked_price = _bs_price(
            S_shocked, contract.strike, T,
            r_shocked, snapshot.div_yield,
            σ_shocked, contract.option_type
        )
        pnl_per_lot = shocked_price - base_price
        pnl_total   = pnl_per_lot * multiplier
        method      = "full_reprice"

    elif all(g is not None for g in [
        snapshot.delta, snapshot.gamma, snapshot.vega, snapshot.rho
    ]):
        ΔS          = S_shocked - snapshot.spot
        Δσ_pts      = shock.vol_shock_abs
        Δr_pts      = shock.rate_shock_bps / 100.0

        pnl_per_lot = (
            snapshot.delta * ΔS
            + 0.5 * snapshot.gamma * ΔS ** 2
            + snapshot.vega * Δσ_pts
            + snapshot.rho  * Δr_pts
        )
        pnl_total   = pnl_per_lot * multiplier
        base_price  = None
        shocked_price = None
        method      = "greeks_approx"

    else:
        return ScenarioPnL(
            base_price=None, shocked_price=None,
            mtm_pnl=0.0, pnl_per_lot=0.0,
            pnl_total=0.0, method="no_data"
        )

    return ScenarioPnL(
        base_price=base_price,
        shocked_price=shocked_price,
        mtm_pnl=0.0,
        pnl_per_lot=pnl_per_lot,
        pnl_total=pnl_total,
        method=method,
    )


def scenario_futures(
    snapshot: MarketSnapshot,
    contract: FuturesContract,
    shock: Shock,
) -> ScenarioPnL:
    S_shocked   = snapshot.spot * (1.0 + shock.spot_shock_pct / 100.0)
    ΔS          = S_shocked - snapshot.spot
    multiplier  = contract.quantity * contract.lot_size
    pnl_per_lot = ΔS
    pnl_total   = ΔS * multiplier

    return ScenarioPnL(
        base_price=snapshot.spot,
        shocked_price=S_shocked,
        mtm_pnl=0.0,
        pnl_per_lot=pnl_per_lot,
        pnl_total=pnl_total,
        method="futures_linear",
    )
