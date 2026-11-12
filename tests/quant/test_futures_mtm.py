"""Bug 3: Futures MTM uses index spot instead of futures settle.

_compute_mtm_futures currently returns (snapshot.spot, (spot - entry)*mult).
It should return (futures_settle, (settle - entry)*mult).
Scenario P&L stays delta-1 on the spot move — unchanged.

The fix: add `futures_settle: float` as a new required parameter.
These tests fail today with TypeError (too many args).
"""
import pytest
from src.quant.portfolio import _compute_mtm_futures
from src.quant.scenario_engine import (
    MarketSnapshot, Shock, FuturesContract, scenario_futures,
)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _snap(spot=30001.26, rate=0.065, div_yield=0.013):
    return MarketSnapshot(
        spot=spot, iv=None, rate=rate, div_yield=div_yield, dte=20,
        delta=1.0, gamma=0.0, vega=0.0, theta=0.0, rho=0.0,
    )


def _contract(quantity=1, lot_size=65):
    return FuturesContract(quantity=quantity, lot_size=lot_size)


# ---------------------------------------------------------------------------
# 1. current_price is settle, not spot
# ---------------------------------------------------------------------------

class TestFuturesMtmUsesSettle:

    def test_current_price_is_settle_not_spot(self):
        # settle > spot — current_price must be settle
        snap     = _snap(spot=30001.26)
        contract = _contract()
        price, _mtm = _compute_mtm_futures(snap, contract, entry_price=29800.0,
                                           futures_settle=30045.45)
        assert price == pytest.approx(30045.45, abs=1e-6)

    def test_current_price_is_not_spot_when_basis_nonzero(self):
        snap     = _snap(spot=30001.26)
        contract = _contract()
        price, _mtm = _compute_mtm_futures(snap, contract, entry_price=29800.0,
                                           futures_settle=30045.45)
        # spot is 30001.26 — price must NOT equal spot
        assert price != pytest.approx(30001.26, abs=1e-6)

    def test_mtm_pnl_uses_settle(self):
        # (30045.45 - 29800.0) * 1 * 65 = 245.45 * 65 = 15954.25
        snap     = _snap(spot=30001.26)
        contract = _contract(quantity=1, lot_size=65)
        _price, mtm = _compute_mtm_futures(snap, contract, entry_price=29800.0,
                                           futures_settle=30045.45)
        assert mtm == pytest.approx(15954.25, abs=1e-6)

    def test_mtm_pnl_wrong_if_using_spot(self):
        # sanity: settle-based MTM ≠ spot-based MTM when basis ≠ 0
        snap     = _snap(spot=30001.26)
        contract = _contract(quantity=1, lot_size=65)
        _price, mtm = _compute_mtm_futures(snap, contract, entry_price=29800.0,
                                           futures_settle=30045.45)
        spot_based_mtm = (30001.26 - 29800.0) * 1 * 65  # 13081.90
        assert mtm != pytest.approx(spot_based_mtm, abs=1e-6)

    def test_zero_basis_unchanged(self):
        # when settle == spot the result is identical to the old path
        spot = 30001.26
        snap = _snap(spot=spot)
        contract = _contract(quantity=1, lot_size=65)
        price, mtm = _compute_mtm_futures(snap, contract, entry_price=29800.0,
                                          futures_settle=spot)
        assert price == pytest.approx(spot, abs=1e-6)
        assert mtm   == pytest.approx((spot - 29800.0) * 65, abs=1e-6)


# ---------------------------------------------------------------------------
# 2. seeded case from the bug report
#    spot=30001.26, settle=30045.45, entry=29800.0, lot=65, qty=1
#    MTM difference vs old (spot-based) = (44.19) * 65 = 2872.35
# ---------------------------------------------------------------------------

class TestFuturesMtmSeeded:

    SPOT   = 30001.26
    SETTLE = 30045.45
    ENTRY  = 29800.0
    LOT    = 65
    QTY    = 1

    def test_seeded_current_price(self):
        snap     = _snap(spot=self.SPOT)
        contract = _contract(quantity=self.QTY, lot_size=self.LOT)
        price, _mtm = _compute_mtm_futures(snap, contract, entry_price=self.ENTRY,
                                           futures_settle=self.SETTLE)
        assert price == pytest.approx(self.SETTLE, abs=1e-6)

    def test_seeded_mtm(self):
        expected = (self.SETTLE - self.ENTRY) * self.QTY * self.LOT  # 15954.25
        snap     = _snap(spot=self.SPOT)
        contract = _contract(quantity=self.QTY, lot_size=self.LOT)
        _price, mtm = _compute_mtm_futures(snap, contract, entry_price=self.ENTRY,
                                           futures_settle=self.SETTLE)
        assert mtm == pytest.approx(expected, abs=1e-6)

    def test_seeded_mtm_error_vs_spot_path(self):
        # the spot-based path would have been off by exactly 2872.35
        expected_settle_mtm = (self.SETTLE - self.ENTRY) * self.QTY * self.LOT
        expected_spot_mtm   = (self.SPOT   - self.ENTRY) * self.QTY * self.LOT
        assert abs(expected_settle_mtm - expected_spot_mtm) == pytest.approx(2872.35, abs=0.01)


# ---------------------------------------------------------------------------
# 3. short positions
# ---------------------------------------------------------------------------

class TestFuturesMtmShort:

    def test_short_position_settle_above_entry_negative_pnl(self):
        # short 2 lots, settle > entry → negative MTM
        snap     = _snap(spot=30001.26)
        contract = _contract(quantity=-2, lot_size=65)
        _price, mtm = _compute_mtm_futures(snap, contract, entry_price=29800.0,
                                           futures_settle=30045.45)
        expected = (30045.45 - 29800.0) * (-2) * 65  # -31908.50
        assert mtm == pytest.approx(expected, abs=1e-6)

    def test_short_position_settle_below_entry_positive_pnl(self):
        # short 1 lot, settle < entry → positive MTM
        snap     = _snap(spot=30001.26)
        contract = _contract(quantity=-1, lot_size=65)
        _price, mtm = _compute_mtm_futures(snap, contract, entry_price=30200.0,
                                           futures_settle=30045.45)
        expected = (30045.45 - 30200.0) * (-1) * 65  # 10,044.75
        assert mtm == pytest.approx(expected, abs=1e-6)


# ---------------------------------------------------------------------------
# 4. scenario P&L stays spot-based (no change to scenario_futures)
# ---------------------------------------------------------------------------

class TestFuturesScenarioStillSpotBased:

    def test_scenario_pnl_uses_spot_not_settle(self):
        # -1% spot shock on spot=30000 → ΔS = -300
        # scenario pnl = -300 * 1 * 65 = -19500
        snap     = _snap(spot=30000.0)
        contract = _contract(quantity=1, lot_size=65)
        shock    = Shock(spot_shock_pct=-1.0, vol_shock_abs=0.0, rate_shock_bps=0.0)
        result   = scenario_futures(snap, contract, shock)
        assert result.pnl_total == pytest.approx(-300.0 * 65, abs=1e-6)

    def test_scenario_base_price_is_spot(self):
        snap   = _snap(spot=30000.0)
        contract = _contract()
        shock  = Shock(spot_shock_pct=0.0, vol_shock_abs=0.0, rate_shock_bps=0.0)
        result = scenario_futures(snap, contract, shock)
        assert result.base_price == pytest.approx(30000.0, abs=1e-6)
