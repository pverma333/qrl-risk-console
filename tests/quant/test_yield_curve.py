import pytest
from src.quant.yield_curve import TenorRates, interpolate_rate, TENOR_DAYS



@pytest.fixture
def rates():
    return TenorRates(rate_3m=6.440, rate_6m=6.560, rate_1y=6.566)

@pytest.fixture
def flat_rates():
    # all tenors equal — interpolation should return same value everywhere
    return TenorRates(rate_3m=6.500, rate_6m=6.500, rate_1y=6.500)

@pytest.fixture
def steep_rates():
    # steep curve — large difference between tenors
    return TenorRates(rate_3m=4.000, rate_6m=6.000, rate_1y=8.000)

class TestInputGuards:

    def test_negative_dte_raises(self, rates):
        with pytest.raises(ValueError):
            interpolate_rate(rates, -1)

    def test_large_negative_dte_raises(self, rates):
        with pytest.raises(ValueError):
            interpolate_rate(rates, -365)

class TestFlatExtrapolation:

    def test_dte_zero_returns_3m_rate(self, rates):
        # below 3M — flat at 3M rate
        r = interpolate_rate(rates, 0)
        assert r == pytest.approx(0.06440)

    def test_dte_below_3m_returns_3m_rate(self, rates):
        r = interpolate_rate(rates, 45)
        assert r == pytest.approx(0.06440)

    def test_dte_above_1y_returns_1y_rate(self, rates):
        # above 1Y — flat at 1Y rate
        r = interpolate_rate(rates, 400)
        assert r == pytest.approx(0.06566)

    def test_dte_far_above_1y_returns_1y_rate(self, rates):
        r = interpolate_rate(rates, 730)
        assert r == pytest.approx(0.06566)


class TestBoundaryValues:

    def test_exactly_3m_boundary(self, rates):
        r = interpolate_rate(rates, TENOR_DAYS["3m"])
        assert r == pytest.approx(0.06440)

    def test_exactly_6m_boundary(self, rates):
        r = interpolate_rate(rates, TENOR_DAYS["6m"])
        assert r == pytest.approx(0.06560)

    def test_exactly_1y_boundary(self, rates):
        r = interpolate_rate(rates, TENOR_DAYS["1y"])
        assert r == pytest.approx(0.06566)


class TestInterpolationAccuracy:

    def test_midpoint_3m_6m(self, rates):
        # midpoint between 3M (91d) and 6M (182d) is 136 days
        # rate should be exactly halfway between 3M and 6M rates
        mid_dte = (TENOR_DAYS["3m"] + TENOR_DAYS["6m"]) // 2
        r = interpolate_rate(rates, mid_dte)
        expected = (0.06440 + 0.06560) / 2
        assert r == pytest.approx(expected, abs=1e-4)

    def test_midpoint_6m_1y(self, rates):
        mid_dte = (TENOR_DAYS["6m"] + TENOR_DAYS["1y"]) // 2
        r = interpolate_rate(rates, mid_dte)
        expected = (0.06560 + 0.06566) / 2
        assert r == pytest.approx(expected, abs=1e-4)

    def test_interpolated_value_between_3m_6m(self, rates):
        r = interpolate_rate(rates, 120)
        assert 0.06440 < r < 0.06560

    def test_interpolated_value_between_6m_1y(self, rates):
        r = interpolate_rate(rates, 270)
        assert 0.06560 < r < 0.06566

    def test_steep_curve_midpoint_3m_6m(self, steep_rates):
        mid_dte = (TENOR_DAYS["3m"] + TENOR_DAYS["6m"]) // 2  # 136
        r = interpolate_rate(steep_rates, mid_dte)
        # compute expected from actual linear interpolation formula
        expected = 0.04 + (136 - 91) / (182 - 91) * (0.06 - 0.04)
        assert r == pytest.approx(expected, abs=1e-6)

    def test_steep_curve_midpoint_6m_1y(self, steep_rates):
        mid_dte = (TENOR_DAYS["6m"] + TENOR_DAYS["1y"]) // 2
        r = interpolate_rate(steep_rates, mid_dte)
        expected = (0.06000 + 0.08000) / 2
        assert r == pytest.approx(expected, abs=1e-4)


class TestMonotonicity:

    def test_rate_increases_with_dte_normal_curve(self, rates):
        # for a normal upward sloping curve rates should increase with DTE
        r_3m = interpolate_rate(rates, 91)
        r_6m = interpolate_rate(rates, 182)
        r_1y = interpolate_rate(rates, 365)
        assert r_3m <= r_6m <= r_1y

    def test_rate_decreases_with_dte_inverted_curve(self):
        inverted = TenorRates(rate_3m=7.000, rate_6m=6.500, rate_1y=6.000)
        r_3m = interpolate_rate(inverted, 91)
        r_6m = interpolate_rate(inverted, 182)
        r_1y = interpolate_rate(inverted, 365)
        assert r_3m >= r_6m >= r_1y

    def test_flat_curve_returns_same_rate_everywhere(self, flat_rates):
        r_short = interpolate_rate(flat_rates, 30)
        r_mid   = interpolate_rate(flat_rates, 150)
        r_long  = interpolate_rate(flat_rates, 300)
        assert r_short == pytest.approx(r_mid)
        assert r_mid   == pytest.approx(r_long)

class TestOutputConvention:

    def test_output_is_decimal_not_percentage(self, rates):
        # input is percentage (6.44), output must be decimal (0.0644)
        r = interpolate_rate(rates, 45)
        assert r < 1.0

    def test_output_is_positive(self, rates):
        for dte in [0, 45, 91, 120, 182, 270, 365, 400]:
            r = interpolate_rate(rates, dte)
            assert r > 0

    def test_output_is_reasonable_range(self, rates):
        # Indian government bond yields have historically been 3% to 12%
        # in decimal form: 0.03 to 0.12
        for dte in [0, 45, 91, 120, 182, 270, 365, 400]:
            r = interpolate_rate(rates, dte)
            assert 0.03 < r < 0.12
