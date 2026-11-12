"""Bug 1: curated div_yield is a PERCENT (1.30 == 1.30%); pricing needs a decimal.

bs_vectorized.compute_batch divides by 100. The repricing paths (portfolio and
scenario_service snapshot builders) must do the same, otherwise q = 130%.
"""
import pandas as pd
import pytest

from app.services import scenario_service
from src.quant import portfolio
from src.quant.black_scholes import _bs_price, _time_to_expiry
from src.quant.scenario_engine import OptionContract, Shock, scenario_option

# Curated units: rate/iv decimal, div_yield percent.
SPOT = 30000.0
RATE = 0.065
DIV_YIELD_PCT = 1.30
IV = 0.15
DTE = 30
LOT = 65
QTY = 2

CASES = [
    pytest.param(30000.0, "CE", id="ATM-CE"),
    pytest.param(29000.0, "CE", id="ITM-CE"),
    pytest.param(31000.0, "CE", id="OTM-CE"),
    pytest.param(30000.0, "PE", id="ATM-PE"),
    pytest.param(31000.0, "PE", id="ITM-PE"),
    pytest.param(29000.0, "PE", id="OTM-PE"),
]


def _exact_settle(strike: float, option_type: str, spot: float = SPOT) -> float:
    """Exact BS price with the CORRECT decimal q, no tick rounding."""
    return _bs_price(
        spot, strike, _time_to_expiry(DTE),
        RATE, DIV_YIELD_PCT / 100.0, IV, option_type,
    )


def _curated_row(strike: float, option_type: str) -> pd.Series:
    return pd.Series({
        "spot": SPOT, "strike": strike, "option_type": option_type,
        "settle": _exact_settle(strike, option_type),
        "iv": IV, "rate": RATE, "div_yield": DIV_YIELD_PCT, "dte": DTE,
        "delta": 0.5, "gamma": 1e-4, "vega": 10.0, "theta": -5.0, "rho": 3.0,
    })


@pytest.mark.parametrize("strike,option_type", CASES)
def test_portfolio_reprice_at_curated_iv_matches_settle(strike, option_type):
    row = _curated_row(strike, option_type)
    snapshot = portfolio._build_snapshot(row)
    contract = OptionContract(strike=strike, option_type=option_type,
                              quantity=QTY, lot_size=LOT)

    price, mtm = portfolio._compute_mtm_option(snapshot, contract, entry_price=row["settle"])

    # settle is exact BS (no tick rounding) -> tight tolerance
    assert price == pytest.approx(row["settle"], rel=1e-6, abs=1e-4)
    assert mtm == pytest.approx(0.0, abs=1e-3)


@pytest.mark.parametrize("strike,option_type", CASES)
def test_scenario_zero_shock_base_price_matches_settle(strike, option_type):
    row = _curated_row(strike, option_type)
    snapshot = scenario_service._build_snapshot_from_option(row)
    contract = OptionContract(strike=strike, option_type=option_type,
                              quantity=QTY, lot_size=LOT)

    result = scenario_option(snapshot, contract, Shock(0.0, 0.0, 0.0))

    # pnl == 0 holds even with the bug (same wrong price both sides);
    # the base_price assertion is what catches it.
    assert result.pnl_total == pytest.approx(0.0, abs=1e-9)
    assert result.base_price == pytest.approx(row["settle"], rel=1e-6, abs=1e-4)


@pytest.mark.parametrize("strike,option_type", CASES)
def test_scenario_spot_shock_pnl_uses_decimal_q(strike, option_type):
    row = _curated_row(strike, option_type)
    snapshot = scenario_service._build_snapshot_from_option(row)
    contract = OptionContract(strike=strike, option_type=option_type,
                              quantity=QTY, lot_size=LOT)

    result = scenario_option(snapshot, contract, Shock(-1.0, 0.0, 0.0))

    expected = (
        _exact_settle(strike, option_type, spot=SPOT * 0.99)
        - _exact_settle(strike, option_type)
    ) * QTY * LOT
    assert result.pnl_total == pytest.approx(expected, rel=1e-6, abs=1e-3)
