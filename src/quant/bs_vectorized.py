import numpy as np
from typing import Tuple


IV_INIT      = 0.20
IV_LOWER     = 0.001
IV_UPPER     = 5.00
MAX_ITER_NR  = 50
MAX_ITER_BIS = 100
TOLERANCE    = 1e-6
VEGA_FLOOR   = 1e-10


def _norm_cdf(x: np.ndarray) -> np.ndarray:
    from math import erf
    return 0.5 * (1.0 + np.array(
        [erf(float(v) / np.sqrt(2.0)) for v in x.flat],
        dtype=np.float64
    ).reshape(x.shape))


def _norm_pdf(x: np.ndarray) -> np.ndarray:
    return np.exp(-0.5 * x * x) / np.sqrt(2.0 * np.pi)


def _bs_price_vec(
    S: np.ndarray, K: np.ndarray, T: np.ndarray,
    r: np.ndarray, q: np.ndarray, sigma: np.ndarray,
    is_call: np.ndarray
) -> np.ndarray:
    safe_T     = np.maximum(T,     1e-10)
    safe_sigma = np.maximum(sigma, 1e-10)
    sqrt_T = np.sqrt(safe_T)

    d1 = (np.log(S / K) + (r - q + 0.5 * safe_sigma ** 2) * safe_T) / (safe_sigma * sqrt_T)
    d2 = d1 - safe_sigma * sqrt_T

    call_price = (
        S * np.exp(-q * safe_T) * _norm_cdf(d1)
        - K * np.exp(-r * safe_T) * _norm_cdf(d2)
    )
    put_price = (
        K * np.exp(-r * safe_T) * _norm_cdf(-d2)
        - S * np.exp(-q * safe_T) * _norm_cdf(-d1)
    )
    return np.where(is_call, call_price, put_price)


def _vega_vec(
    S: np.ndarray, K: np.ndarray, T: np.ndarray,
    r: np.ndarray, q: np.ndarray, sigma: np.ndarray
) -> np.ndarray:
    safe_T     = np.maximum(T,     1e-10)
    safe_sigma = np.maximum(sigma, 1e-10)
    sqrt_T = np.sqrt(safe_T)

    d1 = (np.log(S / K) + (r - q + 0.5 * safe_sigma ** 2) * safe_T) / (safe_sigma * sqrt_T)
    return S * np.exp(-q * safe_T) * _norm_pdf(d1) * sqrt_T


def _greeks_vec(
    S: np.ndarray, K: np.ndarray, T: np.ndarray,
    r: np.ndarray, q: np.ndarray, sigma: np.ndarray,
    is_call: np.ndarray
) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    safe_T     = np.maximum(T,     1e-10)
    safe_sigma = np.maximum(sigma, 1e-10)
    sqrt_T = np.sqrt(safe_T)

    d1 = (np.log(S / K) + (r - q + 0.5 * safe_sigma ** 2) * safe_T) / (safe_sigma * sqrt_T)
    d2 = d1 - safe_sigma * sqrt_T

    pdf_d1 = _norm_pdf(d1)
    cdf_d1 = _norm_cdf(d1)
    cdf_d2 = _norm_cdf(d2)

    gamma = (np.exp(-q * safe_T) * pdf_d1) / (S * safe_sigma * sqrt_T)
    vega  = S * np.exp(-q * safe_T) * pdf_d1 * sqrt_T / 100

    call_delta = np.exp(-q * safe_T) * cdf_d1
    put_delta  = np.exp(-q * safe_T) * (cdf_d1 - 1)
    delta = np.where(is_call, call_delta, put_delta)

    call_theta = (
        -(S * np.exp(-q * safe_T) * pdf_d1 * safe_sigma) / (2 * sqrt_T)
        - r * K * np.exp(-r * safe_T) * cdf_d2
        + q * S * np.exp(-q * safe_T) * cdf_d1
    ) / 365
    put_theta = (
        -(S * np.exp(-q * safe_T) * pdf_d1 * safe_sigma) / (2 * sqrt_T)
        + r * K * np.exp(-r * safe_T) * _norm_cdf(-d2)
        - q * S * np.exp(-q * safe_T) * _norm_cdf(-d1)
    ) / 365
    theta = np.where(is_call, call_theta, put_theta)

    call_rho =  K * safe_T * np.exp(-r * safe_T) * cdf_d2         / 100
    put_rho  = -K * safe_T * np.exp(-r * safe_T) * _norm_cdf(-d2) / 100
    rho = np.where(is_call, call_rho, put_rho)

    return delta, gamma, vega, theta, rho


def _invert_iv_vec(
    market_price: np.ndarray,
    S: np.ndarray, K: np.ndarray, T: np.ndarray,
    r: np.ndarray, q: np.ndarray,
    is_call: np.ndarray,
    valid_mask: np.ndarray
) -> np.ndarray:
    n = len(market_price)
    iv = np.full(n, IV_INIT)
    converged = np.zeros(n, dtype=bool)
    active = valid_mask.copy()

    # --- Phase 1: Newton-Raphson ---
    for _ in range(MAX_ITER_NR):
        if not active.any():
            break

        price = _bs_price_vec(S, K, T, r, q, iv, is_call)
        vega  = _vega_vec(S, K, T, r, q, iv)

        diff = price - market_price
        step = diff / np.maximum(vega, VEGA_FLOOR)

        iv = np.where(active, iv - step, iv)
        iv = np.clip(iv, IV_LOWER, IV_UPPER)

        newly_converged = active & (np.abs(diff) < TOLERANCE)
        converged |= newly_converged
        active &= ~newly_converged

    # --- Phase 2: Bisection fallback ---
    needs_bisection = valid_mask & ~converged

    if needs_bisection.any():
        low  = np.full(n, IV_LOWER)
        high = np.full(n, IV_UPPER)

        f_low  = _bs_price_vec(S, K, T, r, q, low,  is_call) - market_price
        f_high = _bs_price_vec(S, K, T, r, q, high, is_call) - market_price

        no_bracket = needs_bisection & (f_low * f_high > 0)
        needs_bisection &= ~no_bracket

        for _ in range(MAX_ITER_BIS):
            if not needs_bisection.any():
                break

            mid   = (low + high) / 2.0
            f_mid = _bs_price_vec(S, K, T, r, q, mid, is_call) - market_price

            go_left  = needs_bisection & (f_low * f_mid < 0)
            go_right = needs_bisection & ~go_left

            high   = np.where(go_left,  mid,   high)
            f_high = np.where(go_left,  f_mid, f_high)
            low    = np.where(go_right, mid,   low)
            f_low  = np.where(go_right, f_mid, f_low)

            newly_converged = needs_bisection & (np.abs(f_mid) < TOLERANCE)
            iv = np.where(newly_converged, mid, iv)
            converged |= newly_converged
            needs_bisection &= ~newly_converged

    iv = np.where(valid_mask & converged, iv, np.nan)
    return iv


def compute_batch(df) -> dict:
    n = len(df)

    S        = df["spot"].to_numpy(dtype=np.float64)
    K        = df["strike"].to_numpy(dtype=np.float64)
    dte      = df["dte"].to_numpy(dtype=np.float64)
    r        = df["rate"].to_numpy(dtype=np.float64)
    q        = df["div_yield"].to_numpy(dtype=np.float64) / 100
    settle   = df["settle"].to_numpy(dtype=np.float64)
    opt_type = df["option_type"].to_numpy()

    T       = dte / 365.0
    is_call = opt_type == "CE"

    fwd_S        = S * np.exp(-q * np.maximum(T, 1e-10))
    fwd_K        = K * np.exp(-r * np.maximum(T, 1e-10))
    intrinsic_ce = np.maximum(fwd_S - fwd_K, 0.0)
    intrinsic_pe = np.maximum(fwd_K - fwd_S, 0.0)
    intrinsic    = np.where(is_call, intrinsic_ce, intrinsic_pe)

    valid = (
        (dte > 0) &
        (settle > 0) &
        (S > 0) &
        (K > 0) &
        ((opt_type == "CE") | (opt_type == "PE")) &
        (settle >= intrinsic - TOLERANCE)
    )

    iv = _invert_iv_vec(settle, S, K, T, r, q, is_call, valid)

    iv_valid = valid & ~np.isnan(iv)

    delta = np.full(n, np.nan)
    gamma = np.full(n, np.nan)
    vega  = np.full(n, np.nan)
    theta = np.full(n, np.nan)
    rho   = np.full(n, np.nan)

    if iv_valid.any():
        safe_sigma = np.where(iv_valid, iv, IV_LOWER)
        d, g, ve, th, ro = _greeks_vec(S, K, T, r, q, safe_sigma, is_call)
        delta = np.where(iv_valid, d,  np.nan)
        gamma = np.where(iv_valid, g,  np.nan)
        vega  = np.where(iv_valid, ve, np.nan)
        theta = np.where(iv_valid, th, np.nan)
        rho   = np.where(iv_valid, ro, np.nan)

    return {
        "iv":    iv,
        "delta": delta,
        "gamma": gamma,
        "vega":  vega,
        "theta": theta,
        "rho":   rho,
    }
