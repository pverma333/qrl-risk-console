# tests/quant/test_yield_curve.py
import pytest
from src.quant.yield_curve import TenorRates, interpolate_rate

RATES = TenorRates(rate_3m=6.440, rate_6m=6.560, rate_1y=6.566)

def test_below_3m_flat():
    assert interpolate_rate(RATES, 0) == pytest.approx(0.06440)
    assert interpolate_rate(RATES, 45) == pytest.approx(0.06440)

def test_exactly_3m():
    assert interpolate_rate(RATES, 91) == pytest.approx(0.06440)

def test_between_3m_6m():
    r = interpolate_rate(RATES, 120)
    assert 0.06440 < r < 0.06560

def test_exactly_6m():
    assert interpolate_rate(RATES, 182) == pytest.approx(0.06560)

def test_between_6m_1y():
    r = interpolate_rate(RATES, 270)
    assert 0.06560 < r < 0.06566

def test_exactly_1y():
    assert interpolate_rate(RATES, 365) == pytest.approx(0.06566)

def test_above_1y_flat():
    assert interpolate_rate(RATES, 400) == pytest.approx(0.06566)

def test_negative_dte_raises():
    with pytest.raises(ValueError):
        interpolate_rate(RATES, -1)

#run
"""
pytest tests/quant/test_yield_curve.py -v
"""
