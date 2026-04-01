import pytest
import numpy as np
import pandas as pd
from src.quant.bs_vectorized import (
    compute_batch,
    _bs_price_vec,
    _vega_vec,
    _norm_cdf,
    _norm_pdf,
    IV_LOWER,
    IV_UPPER,
    TOLERANCE,
)

def make_df(**kwargs) -> pd.DataFrame:
    """Build minimal DataFrame for compute_batch."""
    base = {
        "spot":        22000.0,
        "strike":      22000.0,
        "dte":         30,
        "rate":        0.065,
        "div_yield":   1.23,      # percentage form — builder divides by 100
        "settle":      300.0,
        "option_type": "CE",
    }
    base.update(kwargs)
    return pd.DataFrame([base])


def make_batch_df(n: int, **kwargs) -> pd.DataFrame:
    """Build a batch DataFrame with n identical rows."""
    base = {
        "spot":        22000.0,
        "strike":      22000.0,
        "dte":         30,
        "rate":        0.065,
        "div_yield":   1.23,
        "settle":      300.0,
        "option_type": "CE",
    }
    base.update(kwargs)
    return pd.DataFrame([base] * n)


def arr(*values) -> np.ndarray:
    return np.array(values, dtype=np.float64)


# norm_cdf and norm_pdf

class TestNormFunctions:

    def test_norm_cdf_at_zero(self):
        result = _norm_cdf(arr(0.0))
        assert result[0] == pytest.approx(0.5, abs=1e-6)

    def test_norm_cdf_large_positive(self):
        result = _norm_cdf(arr(10.0))
        assert result[0] == pytest.approx(1.0, abs=1e-6)

    def test_norm_cdf_large_negative(self):
        result = _norm_cdf(arr(-10.0))
        assert result[0] == pytest.approx(0.0, abs=1e-6)

    def test_norm_cdf_symmetry(self):
        x = arr(1.5)
        assert (_norm_cdf(x) + _norm_cdf(-x))[0] == pytest.approx(1.0, abs=1e-6)

    def test_norm_cdf_known_value(self):
        result = _norm_cdf(arr(1.96))
        assert result[0] == pytest.approx(0.975, abs=0.001)

    def test_norm_pdf_at_zero(self):
        result = _norm_pdf(arr(0.0))
        assert result[0] == pytest.approx(1.0 / np.sqrt(2 * np.pi), abs=1e-8)

    def test_norm_pdf_symmetry(self):
        assert _norm_pdf(arr(1.0))[0] == pytest.approx(_norm_pdf(arr(-1.0))[0])

    def test_norm_pdf_positive_everywhere(self):
        x = arr(-5.0, -2.0, 0.0, 2.0, 5.0)
        assert ((_norm_pdf(x)) > 0).all()

    def test_norm_pdf_tails_near_zero(self):
        assert _norm_pdf(arr(10.0))[0] == pytest.approx(0.0, abs=1e-6)


# BS Price Vectorized

class TestBSPriceVec:

    def _arrays(self, S, K, T, r, q, sigma, is_call):
        return (
            arr(S), arr(K), arr(T), arr(r), arr(q),
            arr(sigma), np.array([is_call])
        )

    def test_call_price_positive(self):
        S, K, T, r, q, sigma = 22000, 22000, 30/365, 0.065, 0.0123, 0.18
        price = _bs_price_vec(*self._arrays(S, K, T, r, q, sigma, True))
        assert price[0] > 0

    def test_put_price_positive(self):
        S, K, T, r, q, sigma = 22000, 22000, 30/365, 0.065, 0.0123, 0.18
        price = _bs_price_vec(*self._arrays(S, K, T, r, q, sigma, False))
        assert price[0] > 0

    def test_put_call_parity(self):
        S, K, T, r, q, sigma = 22000, 22000, 30/365, 0.065, 0.0123, 0.18
        call = _bs_price_vec(*self._arrays(S, K, T, r, q, sigma, True))[0]
        put  = _bs_price_vec(*self._arrays(S, K, T, r, q, sigma, False))[0]
        lhs = call - put
        rhs = S * np.exp(-q * T) - K * np.exp(-r * T)
        assert lhs == pytest.approx(rhs, abs=1e-6)

    def test_higher_vol_increases_call_price(self):
        S, K, T, r, q = 22000, 22000, 30/365, 0.065, 0.0123
        low  = _bs_price_vec(*self._arrays(S, K, T, r, q, 0.10, True))[0]
        high = _bs_price_vec(*self._arrays(S, K, T, r, q, 0.30, True))[0]
        assert high > low

    def test_higher_vol_increases_put_price(self):
        S, K, T, r, q = 22000, 22000, 30/365, 0.065, 0.0123
        low  = _bs_price_vec(*self._arrays(S, K, T, r, q, 0.10, False))[0]
        high = _bs_price_vec(*self._arrays(S, K, T, r, q, 0.30, False))[0]
        assert high > low

    def test_longer_dte_increases_call_price(self):
        S, K, r, q, sigma = 22000, 22000, 0.065, 0.0123, 0.18
        short = _bs_price_vec(*self._arrays(S, K, 30/365,  r, q, sigma, True))[0]
        long  = _bs_price_vec(*self._arrays(S, K, 180/365, r, q, sigma, True))[0]
        assert long > short

    def test_safe_t_prevents_zero_division(self):
        S, K, T, r, q, sigma = 22000, 22000, 0.0, 0.065, 0.0123, 0.18
        price = _bs_price_vec(*self._arrays(S, K, T, r, q, sigma, True))
        assert not np.isnan(price[0])
        assert not np.isinf(price[0])

    def test_safe_sigma_prevents_zero_division(self):
        S, K, T, r, q, sigma = 22000, 22000, 30/365, 0.065, 0.0123, 0.0
        price = _bs_price_vec(*self._arrays(S, K, T, r, q, sigma, True))
        assert not np.isnan(price[0])
        assert not np.isinf(price[0])

    def test_batch_vectorized(self):
        n = 100
        S     = np.full(n, 22000.0)
        K     = np.linspace(20000, 24000, n)
        T     = np.full(n, 30/365)
        r     = np.full(n, 0.065)
        q     = np.full(n, 0.0123)
        sigma = np.full(n, 0.18)
        is_call = np.ones(n, dtype=bool)
        prices = _bs_price_vec(S, K, T, r, q, sigma, is_call)
        assert prices.shape == (n,)
        assert (prices >= 0).all()


# Input Guard Tests

class TestInputGuards:

    def test_zero_dte_returns_nan_iv(self):
        df = make_df(dte=0)
        result = compute_batch(df)
        assert np.isnan(result["iv"][0])

    def test_negative_dte_returns_nan_iv(self):
        df = make_df(dte=-5)
        result = compute_batch(df)
        assert np.isnan(result["iv"][0])

    def test_zero_settle_returns_nan_iv(self):
        df = make_df(settle=0.0)
        result = compute_batch(df)
        assert np.isnan(result["iv"][0])

    def test_negative_settle_returns_nan_iv(self):
        df = make_df(settle=-10.0)
        result = compute_batch(df)
        assert np.isnan(result["iv"][0])

    def test_zero_spot_returns_nan_iv(self):
        df = make_df(spot=0.0)
        result = compute_batch(df)
        assert np.isnan(result["iv"][0])

    def test_negative_spot_returns_nan_iv(self):
        df = make_df(spot=-100.0)
        result = compute_batch(df)
        assert np.isnan(result["iv"][0])

    def test_zero_strike_returns_nan_iv(self):
        df = make_df(strike=0.0)
        result = compute_batch(df)
        assert np.isnan(result["iv"][0])

    def test_negative_strike_returns_nan_iv(self):
        df = make_df(strike=-100.0)
        result = compute_batch(df)
        assert np.isnan(result["iv"][0])

    def test_invalid_option_type_returns_nan_iv(self):
        df = make_df(option_type="XX")
        result = compute_batch(df)
        assert np.isnan(result["iv"][0])

    def test_futures_option_type_returns_nan_iv(self):
        df = make_df(option_type="FUT")
        result = compute_batch(df)
        assert np.isnan(result["iv"][0])

    def test_below_intrinsic_call_returns_nan_iv(self):
        # deep ITM call intrinsic ≈ 1000, settle=1 violates no-arbitrage
        df = make_df(spot=22000.0, strike=21000.0, settle=1.0, option_type="CE")
        result = compute_batch(df)
        assert np.isnan(result["iv"][0])

    def test_below_intrinsic_put_returns_nan_iv(self):
        df = make_df(spot=22000.0, strike=23000.0, settle=1.0, option_type="PE")
        result = compute_batch(df)
        assert np.isnan(result["iv"][0])

    def test_extremely_high_settle_returns_nan_iv(self):
        df = make_df(settle=99999.0)
        result = compute_batch(df)
        assert np.isnan(result["iv"][0])


# NaN Completeness

class TestNaNCompleteness:

    def test_invalid_row_all_greeks_nan(self):
        df = make_df(dte=0)
        result = compute_batch(df)
        for key in ["iv", "delta", "gamma", "vega", "theta", "rho"]:
            assert np.isnan(result[key][0]), f"{key} should be NaN"

    def test_valid_row_all_greeks_not_nan(self):
        known_vol = 0.18
        T = 30 / 365
        S, K, r, q = 22000.0, 22000.0, 0.065, 0.0123
        from src.quant.bs_vectorized import _bs_price_vec
        price = _bs_price_vec(arr(S), arr(K), arr(T), arr(r), arr(q), arr(known_vol), np.array([True]))[0]
        df = make_df(settle=float(price))
        result = compute_batch(df)
        for key in ["iv", "delta", "gamma", "vega", "theta", "rho"]:
            assert not np.isnan(result[key][0]), f"{key} should not be NaN"

    def test_mixed_valid_invalid_batch(self):
        rows = [
            {"spot": 22000, "strike": 22000, "dte": 30,  "rate": 0.065, "div_yield": 1.23, "settle": 300.0, "option_type": "CE"},
            {"spot": 22000, "strike": 22000, "dte": 0,   "rate": 0.065, "div_yield": 1.23, "settle": 300.0, "option_type": "CE"},
            {"spot": 22000, "strike": 21000, "dte": 30, "rate": 0.065, "div_yield": 1.23, "settle": 1150.0, "option_type": "CE"},
            {"spot": 22000, "strike": 22000, "dte": -1,  "rate": 0.065, "div_yield": 1.23, "settle": 300.0, "option_type": "CE"},
        ]
        df = pd.DataFrame(rows)
        result = compute_batch(df)
        assert not np.isnan(result["iv"][0])   # valid
        assert np.isnan(result["iv"][1])        # dte=0
        assert not np.isnan(result["iv"][2])   # valid ITM
        assert np.isnan(result["iv"][3])        # negative dte


# IV Inversion Accuracy

class TestIVInversion:

    def _round_trip(self, spot, strike, dte, vol, option_type):
        T = dte / 365.0
        r, q = 0.065, 0.0123
        is_call = np.array([option_type == "CE"])
        price = _bs_price_vec(arr(spot), arr(strike), arr(T), arr(r), arr(q), arr(vol), is_call)[0]
        df = make_df(spot=spot, strike=strike, dte=dte, settle=float(price), option_type=option_type)
        result = compute_batch(df)
        return result["iv"][0]

    def test_atm_call_round_trip(self):
        iv = self._round_trip(22000, 22000, 30, 0.18, "CE")
        assert iv == pytest.approx(0.18, abs=1e-4)

    def test_atm_put_round_trip(self):
        iv = self._round_trip(22000, 22000, 30, 0.18, "PE")
        assert iv == pytest.approx(0.18, abs=1e-4)

    def test_itm_call_round_trip(self):
        iv = self._round_trip(22000, 21000, 30, 0.15, "CE")
        assert iv == pytest.approx(0.15, abs=1e-4)

    def test_otm_call_round_trip(self):
        iv = self._round_trip(22000, 23000, 30, 0.20, "CE")
        assert iv == pytest.approx(0.20, abs=1e-4)

    def test_itm_put_round_trip(self):
        iv = self._round_trip(22000, 23000, 30, 0.20, "PE")
        assert iv == pytest.approx(0.20, abs=1e-4)

    def test_long_dte_round_trip(self):
        iv = self._round_trip(22000, 22000, 180, 0.22, "CE")
        assert iv == pytest.approx(0.22, abs=1e-4)

    def test_high_vol_round_trip(self):
        iv = self._round_trip(22000, 22000, 30, 0.80, "CE")
        assert iv == pytest.approx(0.80, abs=1e-3)

    def test_low_vol_round_trip(self):
        iv = self._round_trip(22000, 22000, 30, 0.05, "CE")
        assert iv == pytest.approx(0.05, abs=1e-4)

    def test_iv_within_bounds(self):
        iv = self._round_trip(22000, 22000, 30, 0.18, "CE")
        assert IV_LOWER <= iv <= IV_UPPER

    def test_recovered_price_matches_market(self):
        T = 30 / 365
        r, q, vol = 0.065, 0.0123, 0.18
        price = _bs_price_vec(arr(22000), arr(22000), arr(T), arr(r), arr(q), arr(vol), np.array([True]))[0]
        df = make_df(settle=float(price))
        result = compute_batch(df)
        recovered = _bs_price_vec(arr(22000), arr(22000), arr(T), arr(r), arr(q), arr(result["iv"][0]), np.array([True]))[0]
        assert abs(recovered - float(price)) < 0.01

    def test_div_yield_divided_by_100(self):
        # div_yield=1.23 in df means q=0.0123 in BS
        # round trip should still recover vol correctly
        iv = self._round_trip(22000, 22000, 30, 0.18, "CE")
        assert iv == pytest.approx(0.18, abs=1e-4)

    def test_batch_round_trip_multiple_strikes(self):
        strikes = [20000, 21000, 22000, 23000, 24000]
        vol = 0.18
        T, r, q = 30/365, 0.065, 0.0123
        rows = []
        for K in strikes:
            price = _bs_price_vec(arr(22000), arr(float(K)), arr(T), arr(r), arr(q), arr(vol), np.array([True]))[0]
            rows.append({
                "spot": 22000, "strike": float(K), "dte": 30,
                "rate": r, "div_yield": q * 100, "settle": float(price),
                "option_type": "CE"
            })
        df = pd.DataFrame(rows)
        result = compute_batch(df)
        for i, iv in enumerate(result["iv"]):
            assert not np.isnan(iv), f"Strike {strikes[i]} returned NaN"
            assert iv == pytest.approx(vol, abs=1e-3)


# Greeks Direction

class TestGreeksDirection:

    def _compute(self, **kwargs):
        df = make_df(**kwargs)
        return compute_batch(df)

    def test_call_delta_positive(self):
        r = self._compute()
        assert r["delta"][0] > 0

    def test_put_delta_negative(self):
        r = self._compute(option_type="PE")
        assert r["delta"][0] < 0

    def test_call_delta_between_0_and_1(self):
        r = self._compute()
        assert 0 < r["delta"][0] < 1

    def test_put_delta_between_minus1_and_0(self):
        r = self._compute(option_type="PE")
        assert -1 < r["delta"][0] < 0

    def test_gamma_positive_call(self):
        r = self._compute()
        assert r["gamma"][0] > 0

    def test_gamma_positive_put(self):
        r = self._compute(option_type="PE")
        assert r["gamma"][0] > 0

    def test_vega_positive_call(self):
        r = self._compute()
        assert r["vega"][0] > 0

    def test_vega_positive_put(self):
        r = self._compute(option_type="PE")
        assert r["vega"][0] > 0

    def test_theta_negative_call(self):
        r = self._compute()
        assert r["theta"][0] < 0

    def test_theta_negative_put(self):
        r = self._compute(option_type="PE")
        assert r["theta"][0] < 0

    def test_call_rho_positive(self):
        r = self._compute()
        assert r["rho"][0] > 0

    def test_put_rho_negative(self):
        r = self._compute(option_type="PE")
        assert r["rho"][0] < 0

# Greeks Magnitude

class TestGreeksMagnitude:

    def test_atm_call_delta_near_half(self):
        r = compute_batch(make_df())
        assert 0.40 < r["delta"][0] < 0.65

    def test_atm_put_delta_near_minus_half(self):
        r = compute_batch(make_df(option_type="PE"))
        assert -0.65 < r["delta"][0] < -0.40

    def test_deep_itm_call_delta_near_one(self):
        T, r_rate, q, vol = 30/365, 0.065, 0.0123, 0.18
        price = _bs_price_vec(arr(22000), arr(18000), arr(T), arr(r_rate), arr(q), arr(vol), np.array([True]))[0]
        df = make_df(strike=18000.0, settle=float(price))
        result = compute_batch(df)
        if not np.isnan(result["delta"][0]):
            assert result["delta"][0] > 0.85

    def test_deep_otm_call_delta_near_zero(self):
        T, r_rate, q, vol = 30/365, 0.065, 0.0123, 0.18
        price = _bs_price_vec(arr(22000), arr(28000), arr(T), arr(r_rate), arr(q), arr(vol), np.array([True]))[0]
        df = make_df(strike=28000.0, settle=float(price))
        result = compute_batch(df)
        if not np.isnan(result["delta"][0]):
            assert result["delta"][0] < 0.15

    def test_gamma_vega_equal_call_put_same_strike(self):
        T, r_rate, q, vol = 30/365, 0.065, 0.0123, 0.18
        call_p = _bs_price_vec(arr(22000), arr(22000), arr(T), arr(r_rate), arr(q), arr(vol), np.array([True]))[0]
        put_p  = _bs_price_vec(arr(22000), arr(22000), arr(T), arr(r_rate), arr(q), arr(vol), np.array([False]))[0]
        call_r = compute_batch(make_df(settle=float(call_p), option_type="CE"))
        put_r  = compute_batch(make_df(settle=float(put_p),  option_type="PE"))
        assert call_r["gamma"][0] == pytest.approx(put_r["gamma"][0], abs=1e-8)
        assert call_r["vega"][0]  == pytest.approx(put_r["vega"][0],  abs=1e-8)


# Batch Size and Performance

class TestBatchBehavior:

    def test_single_row_works(self):
        df = make_df()
        result = compute_batch(df)
        assert result["iv"].shape == (1,)

    def test_large_batch_works(self):
        df = make_batch_df(1000)
        result = compute_batch(df)
        assert result["iv"].shape == (1000,)
        assert (~np.isnan(result["iv"])).sum() > 900

    def test_all_invalid_batch(self):
        df = make_batch_df(10, dte=0)
        result = compute_batch(df)
        assert np.isnan(result["iv"]).all()
        assert np.isnan(result["delta"]).all()

    def test_output_keys_complete(self):
        df = make_df()
        result = compute_batch(df)
        assert set(result.keys()) == {"iv", "delta", "gamma", "vega", "theta", "rho"}

    def test_output_arrays_same_length(self):
        df = make_batch_df(50)
        result = compute_batch(df)
        lengths = {len(v) for v in result.values()}
        assert len(lengths) == 1
        assert lengths.pop() == 50

    def test_mixed_ce_pe_batch(self):
        rows = [
            {"spot": 22000, "strike": 22000, "dte": 30, "rate": 0.065, "div_yield": 1.23, "settle": 300.0, "option_type": "CE"},
            {"spot": 22000, "strike": 22000, "dte": 30, "rate": 0.065, "div_yield": 1.23, "settle": 280.0, "option_type": "PE"},
            {"spot": 22000, "strike": 21000, "dte": 30, "rate": 0.065, "div_yield": 1.23, "settle": 1150.0, "option_type": "CE"},
            {"spot": 22000, "strike": 23000, "dte": 30, "rate": 0.065, "div_yield": 1.23, "settle": 1000.0, "option_type": "PE"},
        ]
        df = pd.DataFrame(rows)
        result = compute_batch(df)
        assert result["delta"][0] > 0    # CE delta positive
        assert result["delta"][1] < 0    # PE delta negative
        assert result["delta"][2] > 0    # ITM CE delta positive
        assert result["delta"][3] < 0    # ITM PE delta negative

# Scalar vs Vectorized Agreement BS Model

class TestScalarVectorizedAgreement:

    def test_atm_call_agreement(self):
        from src.quant.black_scholes import BSMInputs, compute as scalar_compute

        known_vol = 0.18
        T = 30 / 365
        r, q = 0.065, 0.0123
        S, K = 22000.0, 22000.0

        price = _bs_price_vec(
            arr(S), arr(K), arr(T), arr(r), arr(q),
            arr(known_vol), np.array([True])
        )[0]

        scalar_result = scalar_compute(
            BSMInputs(spot=S, strike=K, dte=30, rate=r, div_yield=q, option_type="CE"),
            float(price)
        )

        df = make_df(settle=float(price))
        vec_result = compute_batch(df)

        assert scalar_result.iv    == pytest.approx(vec_result["iv"][0],    abs=1e-4)
        assert scalar_result.delta == pytest.approx(vec_result["delta"][0], abs=1e-4)
        assert scalar_result.gamma == pytest.approx(vec_result["gamma"][0], abs=1e-4)
        assert scalar_result.vega  == pytest.approx(vec_result["vega"][0],  abs=1e-4)
        assert scalar_result.theta == pytest.approx(vec_result["theta"][0], abs=1e-4)
        assert scalar_result.rho   == pytest.approx(vec_result["rho"][0],   abs=1e-4)

    def test_itm_call_agreement(self):
        from src.quant.black_scholes import BSMInputs, compute as scalar_compute

        known_vol = 0.15
        T = 30 / 365
        r, q = 0.065, 0.0123
        S, K = 22000.0, 21000.0

        price = _bs_price_vec(
            arr(S), arr(K), arr(T), arr(r), arr(q),
            arr(known_vol), np.array([True])
        )[0]

        scalar_result = scalar_compute(
            BSMInputs(spot=S, strike=K, dte=30, rate=r, div_yield=q, option_type="CE"),
            float(price)
        )

        df = make_df(spot=S, strike=K, settle=float(price))
        vec_result = compute_batch(df)

        assert scalar_result.iv    == pytest.approx(vec_result["iv"][0],    abs=1e-4)
        assert scalar_result.delta == pytest.approx(vec_result["delta"][0], abs=1e-4)
        assert scalar_result.gamma == pytest.approx(vec_result["gamma"][0], abs=1e-4)

    def test_atm_put_agreement(self):
        from src.quant.black_scholes import BSMInputs, compute as scalar_compute

        known_vol = 0.18
        T = 30 / 365
        r, q = 0.065, 0.0123
        S, K = 22000.0, 22000.0

        price = _bs_price_vec(
            arr(S), arr(K), arr(T), arr(r), arr(q),
            arr(known_vol), np.array([False])
        )[0]

        scalar_result = scalar_compute(
            BSMInputs(spot=S, strike=K, dte=30, rate=r, div_yield=q, option_type="PE"),
            float(price)
        )

        df = make_df(settle=float(price), option_type="PE")
        vec_result = compute_batch(df)

        assert scalar_result.iv    == pytest.approx(vec_result["iv"][0],    abs=1e-4)
        assert scalar_result.delta == pytest.approx(vec_result["delta"][0], abs=1e-4)
        assert scalar_result.gamma == pytest.approx(vec_result["gamma"][0], abs=1e-4)
        assert scalar_result.vega  == pytest.approx(vec_result["vega"][0],  abs=1e-4)
        assert scalar_result.theta == pytest.approx(vec_result["theta"][0], abs=1e-4)
        assert scalar_result.rho   == pytest.approx(vec_result["rho"][0],   abs=1e-4)
