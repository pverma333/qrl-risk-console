import math
import pytest
from src.quant.futures_pricing import FuturesPricer

class TestAnnualizedDte:

    def test_standard_dte(self):
        T = FuturesPricer.annualized_dte(30)
        assert abs(T - 30 / 365) < 1e-10

    def test_zero_dte(self):
        T = FuturesPricer.annualized_dte(0)
        assert T == 0.0

    def test_one_year_dte(self):
        T = FuturesPricer.annualized_dte(365)
        assert abs(T - 1.0) < 1e-10

    def test_custom_day_count(self):
        T = FuturesPricer.annualized_dte(30, day_count=360)
        assert abs(T - 30 / 360) < 1e-10

    def test_negative_dte_raises(self):
        with pytest.raises(ValueError, match="DTE cannot be negative"):
            FuturesPricer.annualized_dte(-1)

class TestTheoreticalPrice:

    def test_zero_carry(self):
        # r == q → F == S
        price = FuturesPricer.theoretical_price(
            spot=22000.0, rate=0.065, div_yield=0.065, T=30 / 365
        )
        assert abs(price - 22000.0) < 1e-6

    def test_positive_carry(self):
        # r > q → F > S
        price = FuturesPricer.theoretical_price(
            spot=22000.0, rate=0.065, div_yield=0.01, T=30 / 365
        )
        assert price > 22000.0

    def test_negative_carry(self):
        # r < q → F < S
        price = FuturesPricer.theoretical_price(
            spot=22000.0, rate=0.01, div_yield=0.065, T=30 / 365
        )
        assert price < 22000.0

    def test_at_expiry(self):
        # T == 0 → F == S
        price = FuturesPricer.theoretical_price(
            spot=22000.0, rate=0.065, div_yield=0.01, T=0.0
        )
        assert abs(price - 22000.0) < 1e-6

    def test_known_value(self):
        # Manual: 22000 * e^((0.065 - 0.012) * 30/365)
        expected = 22000.0 * math.exp((0.065 - 0.012) * 30 / 365)
        price = FuturesPricer.theoretical_price(
            spot=22000.0, rate=0.065, div_yield=0.012, T=30 / 365
        )
        assert abs(price - expected) < 1e-6

    def test_non_positive_spot_raises(self):
        with pytest.raises(ValueError, match="Spot must be positive"):
            FuturesPricer.theoretical_price(
                spot=0.0, rate=0.065, div_yield=0.01, T=30 / 365
            )

    def test_negative_T_raises(self):
        with pytest.raises(ValueError, match="T cannot be negative"):
            FuturesPricer.theoretical_price(
                spot=22000.0, rate=0.065, div_yield=0.01, T=-0.01
            )

class TestBasis:

    def test_positive_basis(self):
        # Futures trading rich
        b = FuturesPricer.basis(actual_price=22100.0, theoretical_price=22050.0)
        assert abs(b - 50.0) < 1e-6

    def test_negative_basis(self):
        # Futures trading cheap
        b = FuturesPricer.basis(actual_price=21950.0, theoretical_price=22050.0)
        assert abs(b - (-100.0)) < 1e-6

    def test_zero_basis(self):
        b = FuturesPricer.basis(actual_price=22050.0, theoretical_price=22050.0)
        assert abs(b) < 1e-6

class TestDelta:

    def test_delta_is_one(self):
        assert FuturesPricer.delta() == 1.0

    def test_delta_is_float(self):
        assert isinstance(FuturesPricer.delta(), float)
