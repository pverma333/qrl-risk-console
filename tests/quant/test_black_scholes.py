import pytest
import math
from src.quant.black_scholes import (BSMInputs,BSMResult,compute,_bs_price,_norm_cdf,_invert_iv,IV_LOWER_BOUND,IV_UPPER_BOUND,TOLERANCE,)

@pytest.fixture
def atm_call():
    return BSMInputs(spot=22000.0,strike=22000.0,dte=30,rate=0.0650,div_yield=0.0123,option_type="CE",)

@pytest.fixture
def atm_put():
    return BSMInputs(spot=22000.0,strike=22000.0,dte=30,rate=0.0650,div_yield=0.0123,option_type="PE",)

@pytest.fixture
def itm_call():
    return BSMInputs(spot=22000.0,strike=21000.0,dte=30,rate=0.0650,div_yield=0.0123,option_type="CE",)

@pytest.fixture
def otm_call():
    return BSMInputs(spot=22000.0,strike=23000.0,dte=30,rate=0.0650,div_yield=0.0123,option_type="CE",)

@pytest.fixture
def itm_put():
    return BSMInputs(spot=22000.0,strike=23000.0,dte=30,rate=0.0650,div_yield=0.0123,option_type="PE",)

@pytest.fixture
def otm_put():
    return BSMInputs(spot=22000.0,strike=21000.0,dte=30,rate=0.0650,div_yield=0.0123,option_type="PE",)

@pytest.fixture
def long_dte_call():
    return BSMInputs(spot=22000.0,strike=22000.0,dte=180,rate=0.0650,div_yield=0.0123,option_type="CE",)

# Input Validation Tests

class TestInputGuards:

    def test_zero_dte_returns_null(self, atm_call):
        atm_call.dte = 0
        result = compute(atm_call, 300.0)
        assert result.iv is None
        assert result.delta is None

    def test_negative_dte_returns_null(self, atm_call):
        atm_call.dte = -5
        result = compute(atm_call, 300.0)
        assert result.iv is None

    def test_zero_market_price_returns_null(self, atm_call):
        result = compute(atm_call, 0.0)
        assert result.iv is None

    def test_negative_market_price_returns_null(self, atm_call):
        result = compute(atm_call, -10.0)
        assert result.iv is None

    def test_zero_spot_returns_null(self, atm_call):
        atm_call.spot = 0.0
        result = compute(atm_call, 300.0)
        assert result.iv is None

    def test_negative_spot_returns_null(self, atm_call):
        atm_call.spot = -100.0
        result = compute(atm_call, 300.0)
        assert result.iv is None

    def test_zero_strike_returns_null(self, atm_call):
        atm_call.strike = 0.0
        result = compute(atm_call, 300.0)
        assert result.iv is None

    def test_negative_strike_returns_null(self, atm_call):
        atm_call.strike = -100.0
        result = compute(atm_call, 300.0)
        assert result.iv is None

    def test_invalid_option_type_returns_null(self, atm_call):
        atm_call.option_type = "XX"
        result = compute(atm_call, 300.0)
        assert result.iv is None

    def test_futures_option_type_returns_null(self, atm_call):
        atm_call.option_type = "FUT"
        result = compute(atm_call, 300.0)
        assert result.iv is None

    def test_below_intrinsic_call_returns_null(self, itm_call):
        # ITM call intrinsic ≈ 1000, price of 1 violates no-arbitrage
        result = compute(itm_call, 1.0)
        assert result.iv is None

    def test_below_intrinsic_put_returns_null(self, itm_put):
        # ITM put intrinsic ≈ 1000, price of 1 violates no-arbitrage
        result = compute(itm_put, 1.0)
        assert result.iv is None


# IV Inversion Tests

class TestIVInversion:

    def test_atm_call_iv_round_trip(self, atm_call):
        # compute BS price at known vol, then invert back
        known_vol = 0.18
        T = 30 / 365.0
        theoretical_price = _bs_price(
            atm_call.spot, atm_call.strike, T,
            atm_call.rate, atm_call.div_yield, known_vol, "CE"
        )
        result = compute(atm_call, theoretical_price)
        assert result.iv is not None
        assert result.iv == pytest.approx(known_vol, abs=1e-4)

    def test_atm_put_iv_round_trip(self, atm_put):
        known_vol = 0.18
        T = 30 / 365.0
        theoretical_price = _bs_price(
            atm_put.spot, atm_put.strike, T,
            atm_put.rate, atm_put.div_yield, known_vol, "PE"
        )
        result = compute(atm_put, theoretical_price)
        assert result.iv is not None
        assert result.iv == pytest.approx(known_vol, abs=1e-4)

    def test_itm_call_iv_round_trip(self, itm_call):
        known_vol = 0.15
        T = 30 / 365.0
        theoretical_price = _bs_price(
            itm_call.spot, itm_call.strike, T,
            itm_call.rate, itm_call.div_yield, known_vol, "CE"
        )
        result = compute(itm_call, theoretical_price)
        assert result.iv is not None
        assert result.iv == pytest.approx(known_vol, abs=1e-4)

    def test_otm_call_iv_round_trip(self, otm_call):
        known_vol = 0.20
        T = 30 / 365.0
        theoretical_price = _bs_price(
            otm_call.spot, otm_call.strike, T,
            otm_call.rate, otm_call.div_yield, known_vol, "CE"
        )
        result = compute(otm_call, theoretical_price)
        assert result.iv is not None
        assert result.iv == pytest.approx(known_vol, abs=1e-4)

    def test_long_dte_iv_round_trip(self, long_dte_call):
        known_vol = 0.22
        T = 180 / 365.0
        theoretical_price = _bs_price(
            long_dte_call.spot, long_dte_call.strike, T,
            long_dte_call.rate, long_dte_call.div_yield, known_vol, "CE"
        )
        result = compute(long_dte_call, theoretical_price)
        assert result.iv is not None
        assert result.iv == pytest.approx(known_vol, abs=1e-4)

    def test_iv_within_bounds(self, atm_call):
        result = compute(atm_call, 300.0)
        if result.iv is not None:
            assert IV_LOWER_BOUND <= result.iv <= IV_UPPER_BOUND

    def test_extremely_high_price_returns_null(self, atm_call):
        # price higher than any BS model can produce within vol bounds
        result = compute(atm_call, 99999.0)
        assert result.iv is None

    def test_iv_inversion_tolerance(self, atm_call):
        known_vol = 0.18
        T = 30 / 365.0
        theoretical_price = _bs_price(
            atm_call.spot, atm_call.strike, T,
            atm_call.rate, atm_call.div_yield, known_vol, "CE"
        )
        result = compute(atm_call, theoretical_price)
        # verify recovered price matches market price within tolerance
        if result.iv is not None:
            recovered_price = _bs_price(
                atm_call.spot, atm_call.strike, T,
                atm_call.rate, atm_call.div_yield, result.iv, "CE"
            )
            assert abs(recovered_price - theoretical_price) < 0.01


# Put-Call Parity Tests

class TestPutCallParity:

    def test_put_call_parity_atm(self, atm_call, atm_put):
        # C - P = S*e^(-qT) - K*e^(-rT)
        known_vol = 0.18
        T = 30 / 365.0
        S = atm_call.spot
        K = atm_call.strike
        r = atm_call.rate
        q = atm_call.div_yield

        call_price = _bs_price(S, K, T, r, q, known_vol, "CE")
        put_price  = _bs_price(S, K, T, r, q, known_vol, "PE")

        lhs = call_price - put_price
        rhs = S * math.exp(-q * T) - K * math.exp(-r * T)

        assert lhs == pytest.approx(rhs, abs=1e-6)

    def test_put_call_parity_itm(self):
        S, K, T = 22000.0, 21000.0, 30 / 365.0
        r, q, vol = 0.065, 0.0123, 0.18

        call_price = _bs_price(S, K, T, r, q, vol, "CE")
        put_price  = _bs_price(S, K, T, r, q, vol, "PE")

        lhs = call_price - put_price
        rhs = S * math.exp(-q * T) - K * math.exp(-r * T)

        assert lhs == pytest.approx(rhs, abs=1e-6)

    def test_call_put_same_iv_atm(self, atm_call, atm_put):
        # ATM call and put with same market price should give same IV
        known_vol = 0.18
        T = 30 / 365.0
        call_price = _bs_price(
            atm_call.spot, atm_call.strike, T,
            atm_call.rate, atm_call.div_yield, known_vol, "CE"
        )
        put_price = _bs_price(
            atm_put.spot, atm_put.strike, T,
            atm_put.rate, atm_put.div_yield, known_vol, "PE"
        )
        call_result = compute(atm_call, call_price)
        put_result  = compute(atm_put, put_price)

        assert call_result.iv == pytest.approx(put_result.iv, abs=1e-4)


# Greeks Tests

class TestGreeksDirection:

    def test_call_delta_positive(self, atm_call):
        result = compute(atm_call, 300.0)
        assert result.delta is not None
        assert result.delta > 0

    def test_put_delta_negative(self, atm_put):
        result = compute(atm_put, 300.0)
        assert result.delta is not None
        assert result.delta < 0

    def test_call_delta_between_0_and_1(self, atm_call):
        result = compute(atm_call, 300.0)
        assert result.delta is not None
        assert 0.0 < result.delta < 1.0

    def test_put_delta_between_minus1_and_0(self, atm_put):
        result = compute(atm_put, 300.0)
        assert result.delta is not None
        assert -1.0 < result.delta < 0.0

    def test_itm_call_delta_greater_than_otm_call(self, itm_call, otm_call):
        itm_result = compute(itm_call, 1050.0)
        otm_result = compute(otm_call, 50.0)
        if itm_result.delta is not None and otm_result.delta is not None:
            assert itm_result.delta > otm_result.delta

    def test_gamma_positive_for_call(self, atm_call):
        result = compute(atm_call, 300.0)
        assert result.gamma is not None
        assert result.gamma > 0

    def test_gamma_positive_for_put(self, atm_put):
        result = compute(atm_put, 300.0)
        assert result.gamma is not None
        assert result.gamma > 0

    def test_vega_positive_for_call(self, atm_call):
        result = compute(atm_call, 300.0)
        assert result.vega is not None
        assert result.vega > 0

    def test_vega_positive_for_put(self, atm_put):
        result = compute(atm_put, 300.0)
        assert result.vega is not None
        assert result.vega > 0

    def test_theta_negative_for_call(self, atm_call):
        # options lose value as time passes
        result = compute(atm_call, 300.0)
        assert result.theta is not None
        assert result.theta < 0

    def test_theta_negative_for_put(self, atm_put):
        result = compute(atm_put, 300.0)
        assert result.theta is not None
        assert result.theta < 0

    def test_call_rho_positive(self, atm_call):
        # call value increases with rate
        result = compute(atm_call, 300.0)
        assert result.rho is not None
        assert result.rho > 0

    def test_put_rho_negative(self, atm_put):
        # put value decreases with rate
        result = compute(atm_put, 300.0)
        assert result.rho is not None
        assert result.rho < 0


# Greeks Magnitude Tests

class TestGreeksMagnitude:

    def test_atm_call_delta_near_half(self, atm_call):
        # ATM call delta should be close to 0.5
        result = compute(atm_call, 300.0)
        assert result.delta is not None
        assert 0.40 < result.delta < 0.65

    def test_atm_put_delta_near_minus_half(self, atm_put):
        result = compute(atm_put, 300.0)
        assert result.delta is not None
        assert -0.65 < result.delta < -0.40

    def test_deep_itm_call_delta_near_one(self):
        inputs = BSMInputs(
            spot=22000.0, strike=18000.0, dte=30,
            rate=0.065, div_yield=0.0123, option_type="CE"
        )
        result = compute(inputs, 4010.0)
        if result.delta is not None:
            assert result.delta > 0.90

    def test_deep_otm_call_delta_near_zero(self):
        inputs = BSMInputs(
            spot=22000.0, strike=28000.0, dte=30,
            rate=0.065, div_yield=0.0123, option_type="CE"
        )
        result = compute(inputs, 5.0)
        if result.delta is not None:
            assert result.delta < 0.10

    def test_gamma_call_equals_gamma_put(self, atm_call, atm_put):
        # gamma is identical for calls and puts at same strike
        known_vol = 0.18
        T = 30 / 365.0
        call_price = _bs_price(
            atm_call.spot, atm_call.strike, T,
            atm_call.rate, atm_call.div_yield, known_vol, "CE"
        )
        put_price = _bs_price(
            atm_put.spot, atm_put.strike, T,
            atm_put.rate, atm_put.div_yield, known_vol, "PE"
        )
        call_result = compute(atm_call, call_price)
        put_result  = compute(atm_put, put_price)
        if call_result.gamma is not None and put_result.gamma is not None:
            assert call_result.gamma == pytest.approx(put_result.gamma, abs=1e-8)

    def test_vega_call_equals_vega_put(self, atm_call, atm_put):
        # vega is identical for calls and puts at same strike
        known_vol = 0.18
        T = 30 / 365.0
        call_price = _bs_price(
            atm_call.spot, atm_call.strike, T,
            atm_call.rate, atm_call.div_yield, known_vol, "CE"
        )
        put_price = _bs_price(
            atm_put.spot, atm_put.strike, T,
            atm_put.rate, atm_put.div_yield, known_vol, "PE"
        )
        call_result = compute(atm_call, call_price)
        put_result  = compute(atm_put, put_price)
        if call_result.vega is not None and put_result.vega is not None:
            assert call_result.vega == pytest.approx(put_result.vega, abs=1e-8)


# Greeks Sensitivity Direction Tests

class TestGreeksSensitivity:

    def test_higher_vol_increases_call_price(self):
        S, K, r, q = 22000.0, 22000.0, 0.065, 0.0123
        T = 30 / 365.0
        price_low_vol  = _bs_price(S, K, T, r, q, 0.10, "CE")
        price_high_vol = _bs_price(S, K, T, r, q, 0.30, "CE")
        assert price_high_vol > price_low_vol

    def test_higher_vol_increases_put_price(self):
        S, K, r, q = 22000.0, 22000.0, 0.065, 0.0123
        T = 30 / 365.0
        price_low_vol  = _bs_price(S, K, T, r, q, 0.10, "PE")
        price_high_vol = _bs_price(S, K, T, r, q, 0.30, "PE")
        assert price_high_vol > price_low_vol

    def test_longer_dte_increases_call_price(self):
        S, K, r, q, vol = 22000.0, 22000.0, 0.065, 0.0123, 0.18
        price_short = _bs_price(S, K, 30/365.0,  r, q, vol, "CE")
        price_long  = _bs_price(S, K, 180/365.0, r, q, vol, "CE")
        assert price_long > price_short

    def test_longer_dte_increases_put_price(self):
        S, K, r, q, vol = 22000.0, 22000.0, 0.065, 0.0123, 0.18
        price_short = _bs_price(S, K, 30/365.0,  r, q, vol, "PE")
        price_long  = _bs_price(S, K, 180/365.0, r, q, vol, "PE")
        assert price_long > price_short

    def test_higher_spot_increases_call_price(self):
        K, T, r, q, vol = 22000.0, 30/365.0, 0.065, 0.0123, 0.18
        price_low_spot  = _bs_price(21000.0, K, T, r, q, vol, "CE")
        price_high_spot = _bs_price(23000.0, K, T, r, q, vol, "CE")
        assert price_high_spot > price_low_spot

    def test_higher_spot_decreases_put_price(self):
        K, T, r, q, vol = 22000.0, 30/365.0, 0.065, 0.0123, 0.18
        price_low_spot  = _bs_price(21000.0, K, T, r, q, vol, "PE")
        price_high_spot = _bs_price(23000.0, K, T, r, q, vol, "PE")
        assert price_high_spot < price_low_spot


# Norm CDF Tests

class TestNormCDF:

    def test_norm_cdf_at_zero(self):
        assert _norm_cdf(0.0) == pytest.approx(0.5, abs=1e-6)

    def test_norm_cdf_large_positive(self):
        assert _norm_cdf(10.0) == pytest.approx(1.0, abs=1e-6)

    def test_norm_cdf_large_negative(self):
        assert _norm_cdf(-10.0) == pytest.approx(0.0, abs=1e-6)

    def test_norm_cdf_symmetry(self):
        assert _norm_cdf(1.0) + _norm_cdf(-1.0) == pytest.approx(1.0, abs=1e-6)

    def test_norm_cdf_known_value(self):
        # N(1.96) ≈ 0.975 — standard stats result
        assert _norm_cdf(1.96) == pytest.approx(0.975, abs=0.001)


# BSMResult Null Completeness

class TestNullResultCompleteness:

    def test_null_result_all_fields_none(self, atm_call):
        # when any guard fires all fields must be None
        atm_call.dte = 0
        result = compute(atm_call, 300.0)
        assert result.iv    is None
        assert result.delta is None
        assert result.gamma is None
        assert result.vega  is None
        assert result.theta is None
        assert result.rho   is None

    def test_valid_result_all_fields_not_none(self, atm_call):
        known_vol = 0.18
        T = 30 / 365.0
        price = _bs_price(
            atm_call.spot, atm_call.strike, T,
            atm_call.rate, atm_call.div_yield, known_vol, "CE"
        )
        result = compute(atm_call, price)
        assert result.iv    is not None
        assert result.delta is not None
        assert result.gamma is not None
        assert result.vega  is not None
        assert result.theta is not None
        assert result.rho   is not None

#run
"""
pytest tests/quant/test_black_scholes.py -v
"""
