import math
import pytest
from src.quant.scenario_engine import (
    MarketSnapshot, Shock, OptionContract, FuturesContract,
    scenario_option, scenario_futures, ScenarioPnL,
)


def make_snapshot(iv=0.18, delta=0.5, gamma=0.002, vega=8.5, rho=3.2):
    return MarketSnapshot(
        spot=22000.0, iv=iv, rate=0.065, div_yield=0.012,
        dte=30, delta=delta, gamma=gamma, vega=vega,
        theta=-5.0, rho=rho,
    )


def make_contract():
    return OptionContract(strike=22000.0, option_type="CE", quantity=1, lot_size=75)


def make_shock(spot=0.0, vol=0.0, rate=0.0):
    return Shock(spot_shock_pct=spot, vol_shock_abs=vol, rate_shock_bps=rate)


class TestScenarioOptionFullReprice:

    def test_zero_shock_zero_pnl(self):
        result = scenario_option(make_snapshot(), make_contract(), make_shock())
        assert abs(result.pnl_per_lot) < 1e-6
        assert result.method == "full_reprice"

    def test_spot_up_call_positive_pnl(self):
        result = scenario_option(make_snapshot(), make_contract(), make_shock(spot=2.0))
        assert result.pnl_per_lot > 0
        assert result.method == "full_reprice"

    def test_spot_down_call_negative_pnl(self):
        result = scenario_option(make_snapshot(), make_contract(), make_shock(spot=-2.0))
        assert result.pnl_per_lot < 0
        assert result.method == "full_reprice"

    def test_vol_up_call_positive_pnl(self):
        result = scenario_option(make_snapshot(), make_contract(), make_shock(vol=2.0))
        assert result.pnl_per_lot > 0
        assert result.method == "full_reprice"

    def test_pnl_total_equals_per_lot_times_multiplier(self):
        contract = make_contract()
        result = scenario_option(make_snapshot(), make_contract(), make_shock(spot=1.0))
        assert abs(result.pnl_total - result.pnl_per_lot * contract.quantity * contract.lot_size) < 1e-6

    def test_put_spot_down_positive_pnl(self):
        contract = OptionContract(strike=22000.0, option_type="PE", quantity=1, lot_size=75)
        result = scenario_option(make_snapshot(), contract, make_shock(spot=-2.0))
        assert result.pnl_per_lot > 0
        assert result.method == "full_reprice"

    def test_expired_position_returns_zero(self):
        snap = MarketSnapshot(
            spot=22000.0, iv=0.18, rate=0.065, div_yield=0.012,
            dte=0, delta=0.5, gamma=0.002, vega=8.5, theta=-5.0, rho=3.2,
        )
        result = scenario_option(snap, make_contract(), make_shock(spot=2.0))
        assert result.pnl_total == 0.0
        assert result.method == "expired"


class TestScenarioOptionGreeksApprox:

    def test_falls_back_to_greeks_when_iv_null(self):
        snap = make_snapshot(iv=None)
        result = scenario_option(snap, make_contract(), make_shock(spot=1.0))
        assert result.method == "greeks_approx"

    def test_no_data_when_iv_null_and_greeks_null(self):
        snap = MarketSnapshot(
            spot=22000.0, iv=None, rate=0.065, div_yield=0.012,
            dte=30, delta=None, gamma=None, vega=None, theta=None, rho=None,
        )
        result = scenario_option(snap, make_contract(), make_shock(spot=1.0))
        assert result.method == "no_data"
        assert result.pnl_total == 0.0

    def test_greeks_approx_spot_up_positive_delta(self):
        snap = make_snapshot(iv=None, delta=0.5, gamma=0.002, vega=8.5, rho=3.2)
        result = scenario_option(snap, make_contract(), make_shock(spot=1.0))
        assert result.pnl_per_lot > 0


class TestScenarioFutures:

    def test_spot_up_positive_pnl(self):
        snap = make_snapshot()
        contract = FuturesContract(quantity=1, lot_size=75)
        result = scenario_futures(snap, contract, make_shock(spot=1.0))
        assert result.pnl_per_lot > 0
        assert result.method == "futures_linear"

    def test_spot_down_negative_pnl(self):
        snap = make_snapshot()
        contract = FuturesContract(quantity=1, lot_size=75)
        result = scenario_futures(snap, contract, make_shock(spot=-1.0))
        assert result.pnl_per_lot < 0

    def test_zero_shock_zero_pnl(self):
        snap = make_snapshot()
        contract = FuturesContract(quantity=1, lot_size=75)
        result = scenario_futures(snap, contract, make_shock())
        assert abs(result.pnl_total) < 1e-6

    def test_pnl_total_equals_per_lot_times_multiplier(self):
        snap = make_snapshot()
        contract = FuturesContract(quantity=2, lot_size=75)
        result = scenario_futures(snap, contract, make_shock(spot=1.0))
        assert abs(result.pnl_total - result.pnl_per_lot * 2 * 75) < 1e-6

    def test_vol_shock_no_effect_on_futures(self):
        snap = make_snapshot()
        contract = FuturesContract(quantity=1, lot_size=75)
        result_no_vol = scenario_futures(snap, contract, make_shock(spot=1.0, vol=0.0))
        result_vol    = scenario_futures(snap, contract, make_shock(spot=1.0, vol=5.0))
        assert abs(result_no_vol.pnl_total - result_vol.pnl_total) < 1e-6
