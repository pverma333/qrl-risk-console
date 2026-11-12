"""Bug 2: expiry-day positions (dte == 0).

Price should be intrinsic value; MTM and scenario P&L must use it.
Negative dte (already expired) keeps returning 0.0 / "expired".
"""
import pytest
from src.quant.scenario_engine import (
    MarketSnapshot, Shock, OptionContract,
    scenario_option,
)
from src.quant.portfolio import _compute_mtm_option


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _snap(spot, dte, iv=0.18, rate=0.065, div_yield=0.012):
    return MarketSnapshot(
        spot=spot, iv=iv, rate=rate, div_yield=div_yield, dte=dte,
        delta=None, gamma=None, vega=None, theta=None, rho=None,
    )


def _contract(strike, option_type, quantity=1, lot_size=75):
    return OptionContract(
        strike=strike, option_type=option_type,
        quantity=quantity, lot_size=lot_size,
    )


def _zero_shock():
    return Shock(spot_shock_pct=0.0, vol_shock_abs=0.0, rate_shock_bps=0.0)


def _spot_shock(pct):
    return Shock(spot_shock_pct=pct, vol_shock_abs=0.0, rate_shock_bps=0.0)


# ---------------------------------------------------------------------------
# 1. price and MTM P&L on expiry day
# ---------------------------------------------------------------------------

class TestExpiryDayMtm:

    def test_itm_call_price_is_intrinsic(self):
        # S=22100, K=22000 → intrinsic = 100
        snap     = _snap(spot=22100.0, dte=0)
        contract = _contract(strike=22000.0, option_type="CE")
        price, _mtm = _compute_mtm_option(snap, contract, entry_price=80.0)
        assert price == pytest.approx(100.0, abs=1e-9)

    def test_itm_call_mtm_pnl(self):
        # intrinsic=100, entry=80, qty=1, lot=75 → MTM = 20*75 = 1500
        snap     = _snap(spot=22100.0, dte=0)
        contract = _contract(strike=22000.0, option_type="CE")
        _price, mtm = _compute_mtm_option(snap, contract, entry_price=80.0)
        assert mtm == pytest.approx(1500.0, abs=1e-6)

    def test_itm_put_price_is_intrinsic(self):
        # S=21900, K=22000 → intrinsic = 100
        snap     = _snap(spot=21900.0, dte=0)
        contract = _contract(strike=22000.0, option_type="PE")
        price, _mtm = _compute_mtm_option(snap, contract, entry_price=80.0)
        assert price == pytest.approx(100.0, abs=1e-9)

    def test_itm_put_mtm_pnl(self):
        snap     = _snap(spot=21900.0, dte=0)
        contract = _contract(strike=22000.0, option_type="PE")
        _price, mtm = _compute_mtm_option(snap, contract, entry_price=80.0)
        assert mtm == pytest.approx(1500.0, abs=1e-6)

    def test_otm_option_price_is_zero(self):
        # S=22100, K=22200, CE → intrinsic = 0 (OTM, expires worthless)
        snap     = _snap(spot=22100.0, dte=0)
        contract = _contract(strike=22200.0, option_type="CE")
        price, _mtm = _compute_mtm_option(snap, contract, entry_price=50.0)
        assert price == pytest.approx(0.0, abs=1e-9)

    def test_otm_option_mtm_is_full_loss_not_zero(self):
        # entry=50, intrinsic=0 → MTM = (0-50)*1*75 = -3750, NOT 0.0
        snap     = _snap(spot=22100.0, dte=0)
        contract = _contract(strike=22200.0, option_type="CE")
        _price, mtm = _compute_mtm_option(snap, contract, entry_price=50.0)
        assert mtm == pytest.approx(-3750.0, abs=1e-6)

    def test_iv_is_ignored_at_expiry_day(self):
        # With positive IV the BS formula is undefined at T=0; must NOT raise
        snap1 = _snap(spot=22100.0, dte=0, iv=0.20)
        snap2 = _snap(spot=22100.0, dte=0, iv=None)
        contract = _contract(strike=22000.0, option_type="CE")
        p1, m1 = _compute_mtm_option(snap1, contract, entry_price=80.0)
        p2, m2 = _compute_mtm_option(snap2, contract, entry_price=80.0)
        assert p1 == pytest.approx(p2, abs=1e-9)
        assert m1 == pytest.approx(m2, abs=1e-9)


# ---------------------------------------------------------------------------
# 2. short positions (negative quantity)
# ---------------------------------------------------------------------------

class TestExpiryDayShortPositions:

    def test_short_itm_call_mtm_pnl_negative(self):
        # intrinsic=100, entry=80, qty=-2, lot=75 → MTM = 20*(-2)*75 = -3000
        snap     = _snap(spot=22100.0, dte=0)
        contract = _contract(strike=22000.0, option_type="CE", quantity=-2, lot_size=75)
        _price, mtm = _compute_mtm_option(snap, contract, entry_price=80.0)
        assert mtm == pytest.approx(-3000.0, abs=1e-6)

    def test_short_otm_call_mtm_positive(self):
        # entry=50, intrinsic=0, qty=-1, lot=75 → MTM = (0-50)*(-1)*75 = 3750
        snap     = _snap(spot=22100.0, dte=0)
        contract = _contract(strike=22200.0, option_type="CE", quantity=-1, lot_size=75)
        _price, mtm = _compute_mtm_option(snap, contract, entry_price=50.0)
        assert mtm == pytest.approx(3750.0, abs=1e-6)


# ---------------------------------------------------------------------------
# 3. scenario P&L at expiry (vol/rate shocks irrelevant; only spot matters)
# ---------------------------------------------------------------------------

class TestExpiryDayScenario:

    def test_itm_call_spot_down_1pct(self):
        # S=22000, K=21800, CE → base intrinsic=200
        # S_shocked=22000*0.99=21780 → shocked intrinsic=max(21780-21800,0)=0
        # scenario P&L = (0-200)*1*75 = -15000
        snap     = _snap(spot=22000.0, dte=0)
        contract = _contract(strike=21800.0, option_type="CE")
        result   = scenario_option(snap, contract, _spot_shock(-1.0))
        assert result.pnl_total == pytest.approx(-15000.0, abs=1e-6)
        assert result.method != "expired"

    def test_otm_put_spot_down_1pct_becomes_itm(self):
        # S=22000, K=21900, PE → base intrinsic=max(21900-22000,0)=0
        # S_shocked=22000*0.99=21780 → shocked intrinsic=max(21900-21780,0)=120
        # scenario P&L = (120-0)*1*75 = 9000  (non-zero!)
        snap     = _snap(spot=22000.0, dte=0)
        contract = _contract(strike=21900.0, option_type="PE")
        result   = scenario_option(snap, contract, _spot_shock(-1.0))
        assert result.pnl_total == pytest.approx(9000.0, abs=1e-6)
        assert result.method != "expired"

    def test_vol_and_rate_shocks_ignored_at_expiry(self):
        # Same result regardless of vol/rate shock
        snap     = _snap(spot=22100.0, dte=0)
        contract = _contract(strike=22000.0, option_type="CE")
        r_no_shocks = scenario_option(snap, contract, Shock(0.0,  0.0,   0.0))
        r_all_shocks = scenario_option(snap, contract, Shock(0.0, 10.0, 200.0))
        assert r_no_shocks.pnl_total == pytest.approx(r_all_shocks.pnl_total, abs=1e-9)

    def test_zero_shock_scenario_pnl_is_zero(self):
        snap     = _snap(spot=22100.0, dte=0)
        contract = _contract(strike=22000.0, option_type="CE")
        result   = scenario_option(snap, contract, _zero_shock())
        assert result.pnl_total == pytest.approx(0.0, abs=1e-9)


# ---------------------------------------------------------------------------
# 4. seeded baseline case
#    intrinsic=301.70, entry=137.45, lot=65, qty=1 → MTM=10,676.25
# ---------------------------------------------------------------------------

class TestExpiryDayBaseline:

    def test_seeded_mtm(self):
        # S=24301.70, K=24000.0, CE → intrinsic = 301.70
        S        = 24301.70
        K        = 24000.0
        entry    = 137.45
        lot      = 65
        qty      = 1

        expected_intrinsic = S - K        # 301.70
        expected_mtm       = (expected_intrinsic - entry) * qty * lot  # 10_676.25

        snap     = _snap(spot=S, dte=0)
        contract = _contract(strike=K, option_type="CE", quantity=qty, lot_size=lot)
        price, mtm = _compute_mtm_option(snap, contract, entry_price=entry)

        assert price == pytest.approx(expected_intrinsic, abs=1e-6)
        assert mtm   == pytest.approx(expected_mtm,       abs=1e-6)


# ---------------------------------------------------------------------------
# 5. negative dte (already expired) keeps existing behaviour
# ---------------------------------------------------------------------------

class TestNegativeDtePreserved:

    def test_negative_dte_mtm_returns_none_zero(self):
        snap     = _snap(spot=22100.0, dte=-1)
        contract = _contract(strike=22000.0, option_type="CE")
        price, mtm = _compute_mtm_option(snap, contract, entry_price=80.0)
        # already expired: no price, no P&L
        assert price is None
        assert mtm == pytest.approx(0.0, abs=1e-9)

    def test_negative_dte_scenario_method_expired(self):
        snap     = _snap(spot=22100.0, dte=-1)
        contract = _contract(strike=22000.0, option_type="CE")
        result   = scenario_option(snap, contract, _spot_shock(-1.0))
        assert result.pnl_total == pytest.approx(0.0, abs=1e-9)
        assert result.method == "expired"
