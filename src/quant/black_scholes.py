import math
from dataclasses import dataclass
from typing import Optional


# IV search bounds
IV_LOWER_BOUND = 0.01
IV_UPPER_BOUND = 5.00
MAX_ITERATIONS = 100
TOLERANCE      = 1e-6


@dataclass
class BSMInputs:
    spot:       float
    strike:     float
    dte:        int
    rate:       float
    div_yield:  float
    option_type: str


@dataclass
class BSMResult:
    iv:     Optional[float]
    delta:  Optional[float]
    gamma:  Optional[float]
    vega:   Optional[float]
    theta:  Optional[float]
    rho:    Optional[float]


def _time_to_expiry(dte: int) -> float:
    return dte / 365.0

def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))

def _norm_pdf(x: float) -> float:
    return math.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)

def _bs_price(
    S: float, K: float, T: float, r: float, q: float, sigma: float, option_type: str
) -> float:
    sqrt_T = math.sqrt(T)
    d1 = (math.log(S / K) + (r - q + 0.5 * sigma ** 2) * T) / (sigma * sqrt_T)
    d2 = d1 - sigma * sqrt_T

    if option_type == "CE":
        return S * math.exp(-q * T) * _norm_cdf(d1) - K * math.exp(-r * T) * _norm_cdf(d2)
    else:
        return K * math.exp(-r * T) * _norm_cdf(-d2) - S * math.exp(-q * T) * _norm_cdf(-d1)

def _bs_greeks(
    S: float, K: float, T: float, r: float, q: float, sigma: float, option_type: str
) -> dict:
    sqrt_T = math.sqrt(T)
    d1 = (math.log(S / K) + (r - q + 0.5 * sigma ** 2) * T) / (sigma * sqrt_T)
    d2 = d1 - sigma * sqrt_T

    pdf_d1 = _norm_pdf(d1)
    cdf_d1 = _norm_cdf(d1)
    cdf_d2 = _norm_cdf(d2)

    # gamma and vega are same for calls and puts
    gamma = (math.exp(-q * T) * pdf_d1) / (S * sigma * sqrt_T)
    vega  = S * math.exp(-q * T) * pdf_d1 * sqrt_T / 100  # per 1% vol move

    if option_type == "CE":
        delta = math.exp(-q * T) * cdf_d1
        theta = (
            -(S * math.exp(-q * T) * pdf_d1 * sigma) / (2 * sqrt_T)
            - r * K * math.exp(-r * T) * cdf_d2
            + q * S * math.exp(-q * T) * cdf_d1
        ) / 365  # per calendar day
        rho = K * T * math.exp(-r * T) * cdf_d2 / 100  # per 1% rate move
    else:
        delta = math.exp(-q * T) * (cdf_d1 - 1)
        theta = (
            -(S * math.exp(-q * T) * pdf_d1 * sigma) / (2 * sqrt_T)
            + r * K * math.exp(-r * T) * _norm_cdf(-d2)
            - q * S * math.exp(-q * T) * _norm_cdf(-d1)
        ) / 365
        rho = -K * T * math.exp(-r * T) * _norm_cdf(-d2) / 100

    return {
        "delta": delta,
        "gamma": gamma,
        "vega":  vega,
        "theta": theta,
        "rho":   rho,
    }

def _invert_iv(
    market_price: float,
    S: float, K: float, T: float, r: float, q: float, option_type: str
) -> Optional[float]:
    def objective(sigma: float) -> float:
        return _bs_price(S, K, T, r, q, sigma, option_type) - market_price

    low  = IV_LOWER_BOUND
    high = IV_UPPER_BOUND

    f_low  = objective(low)
    f_high = objective(high)

    # market price outside the BS price range for any reasonable vol
    if f_low * f_high > 0:
        return None

    for _ in range(MAX_ITERATIONS):
        mid    = (low + high) / 2.0
        f_mid  = objective(mid)

        if abs(f_mid) < TOLERANCE or (high - low) / 2.0 < TOLERANCE:
            return mid

        if f_low * f_mid < 0:
            high   = mid
            f_high = f_mid
        else:
            low   = mid
            f_low = f_mid

    return None


def compute(inputs: BSMInputs, market_price: float) -> BSMResult:
    null_result = BSMResult(
        iv=None, delta=None, gamma=None,
        vega=None, theta=None, rho=None
    )
    # expired or expiry day with no time value
    if inputs.dte <= 0:
        return null_result

    # zero or negative market price
    if market_price <= 0:
        return null_result

    # zero or negative spot
    if inputs.spot <= 0:
        return null_result

    # zero or negative strike
    if inputs.strike <= 0:
        return null_result

    # unknown option type
    if inputs.option_type not in ("CE", "PE"):
        return null_result

    T = _time_to_expiry(inputs.dte)
    S = inputs.spot
    K = inputs.strike
    r = inputs.rate
    q = inputs.div_yield

    # intrinsic value check — market price must exceed intrinsic or price violates no-arbitrage and IV inversion fails
    if inputs.option_type == "CE":
        intrinsic = max(S * math.exp(-q * T) - K * math.exp(-r * T), 0.0)
    else:
        intrinsic = max(K * math.exp(-r * T) - S * math.exp(-q * T), 0.0)

    if market_price < intrinsic - TOLERANCE:
        return null_result

    # IV inversion
    iv = _invert_iv(market_price, S, K, T, r, q, inputs.option_type)

    if iv is None:
        return null_result

    # Greeks
    greeks = _bs_greeks(S, K, T, r, q, iv, inputs.option_type)

    return BSMResult(
        iv=iv,
        delta=greeks["delta"],
        gamma=greeks["gamma"],
        vega=greeks["vega"],
        theta=greeks["theta"],
        rho=greeks["rho"],
    )
