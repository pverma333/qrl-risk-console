## Yield Interpolation - Linear Interpolation
dte < 91   → use 3m rate directly
91 ≤ dte < 182  → interpolate between 3m and 6m
182 ≤ dte < 365 → interpolate between 6m and 1y
dte ≥ 365  → use 1y rate directly

### Why?
**Choice: Linear Interpolation**

**Why not Cubic Spline:**
Cubic spline earns its value when you have 5+ tenor points spread across a full curve. With only 3 points (3M, 6M, 1Y), a spline reduces to nearly the same result as a straight line. Added complexity, negligible gain.

**Why not Nelson-Siegel:**
Nelson-Siegel is a parametric model designed to capture level, slope, and curvature of a full yield curve. It requires non-linear fitting and becomes unstable with fewer than 4-5 tenor points. Applying it to 3 points is statistically under-determined — it would overfit noise, not capture structure.

**Why Linear is correct here:**
Three tenors clustered at the short end (3M to 1Y). Most Indian index option expiries fall below 91 days — meaning the interpolation almost always operates on a single short segment. The rate difference across this segment is typically under 15 basis points. A straight line through that range introduces negligible pricing error.

**Upgrade path:**
If 2Y, 5Y, 10Y bond data is added later, swap to cubic spline in one function change. The `TenorRates` dataclass and `interpolate_rate` interface are designed to accommodate this without touching the BS engine.

**Auditability:**
The dashboard assumption panel will display interpolation method as linear so any reviewer knows the exact convention used.

--> Formula
r = r_low + (dte - dte_low) / (dte_high - dte_low) * (r_high - r_low)

--> Example
  trade_date tenor  yield_pct
0 2024-10-15    1y      6.566
1 2024-10-15    3m      6.440
2 2024-10-15    6m      6.560

below 91 → use 3m directly → r = 6.440

if dte = 120
between 91 and 182 → interpolate 3m and 6m
r = 6.440 + (120 - 91) / (182 - 91) * (6.560 - 6.440)
r = 6.440 + (29/91) * 0.120
r = 6.440 + 0.038
r = 6.478


# Black-Scholes Engine — Logic Notes
## src/quant/black_scholes.py

---

## What Black-Scholes Solves

Given observable market inputs, BS computes a theoretical option price.
IV inversion reverses this — given the market price, find the volatility
that makes BS produce that price.

---

## Inputs (BSMInputs)

| Field | Symbol | Source |
|---|---|---|
| spot | S | processed index spot |
| strike | K | processed options |
| dte | — | computed in processed layer |
| rate | r | yield interpolation engine (decimal) |
| div_yield | q | processed index yield (decimal) |
| option_type | — | CE or PE |

Market price (settle) passed separately — not part of BSMInputs
because inputs define the contract, price defines the observation.

---

## Core Formula
```
T  = dte / 365

d1 = [ln(S/K) + (r - q + 0.5σ²)T] / (σ√T)
d2 = d1 - σ√T

Call = S·e^(-qT)·N(d1) - K·e^(-rT)·N(d2)
Put  = K·e^(-rT)·N(-d2) - S·e^(-qT)·N(-d1)
```

N() = cumulative standard normal distribution
Implemented via math.erf() — no scipy dependency.

---

## IV Inversion — Brent's Method

Brent's method is a bracketed root-finding algorithm.

**Intuition:** You know the market price. You know BS produces a price
for any given σ. You want the σ where BS price = market price.
Define: objective(σ) = BS_price(σ) - market_price
You need to find σ where objective(σ) = 0.

**How bracketing works:**
1. Set low = 1% vol, high = 500% vol
2. Compute objective(low) and objective(high)
3. If both have the same sign → no root exists in this interval → return None
4. If opposite signs → a root exists somewhere between low and high
5. Bisect the interval, evaluate midpoint, narrow the bracket toward the root
6. Stop when |objective(mid)| < 1e-6 or interval width < 1e-6

**Why Brent's over Newton-Raphson:**
Newton-Raphson requires the derivative (vega) at each step and can
diverge if the initial guess is poor. Brent's is guaranteed to converge
within the bracket — no divergence possible. Industry standard for IV
inversion.

**Convergence:** Typically under 20 iterations for clean inputs.
Max iterations capped at 100 as a safety ceiling.

---

## Greeks

All Greeks are analytical — computed directly from d1, d2, not numerical
differentiation.

| Greek | Formula | Convention |
|---|---|---|
| Delta | ∂Price/∂S | raw — e.g. 0.52 means 52% of spot move |
| Gamma | ∂²Price/∂S² | raw — rate of change of delta per 1 point spot move |
| Vega | ∂Price/∂σ | divided by 100 — price change per 1% vol move |
| Theta | ∂Price/∂T | divided by 365 — price decay per calendar day |
| Rho | ∂Price/∂r | divided by 100 — price change per 1% rate move |

Gamma and Vega are identical for calls and puts (same formula).
Delta, Theta, Rho differ by sign and formula between CE and PE.

Put delta is always negative (between -1 and 0).
Call delta is always positive (between 0 and 1).

---

## Input Guards — What Gets Rejected

| Condition | Reason |
|---|---|
| dte <= 0 | T=0 causes division by zero in d1/d2 |
| market_price <= 0 | Cannot invert zero price — any σ produces price > 0 |
| spot <= 0 | log(S/K) undefined |
| strike <= 0 | log(S/K) undefined |
| option_type not CE/PE | Unknown instrument — futures rows or bad data |
| market_price < intrinsic | Violates no-arbitrage — NSE settle occasionally produces these for illiquid strikes |
| f_low * f_high > 0 | No root in [1%, 500%] bracket — IV does not exist for this price |

All rejections return BSMResult with all fields as None.
Downstream curated layer handles None rows by excluding from analytics.

---

## What Black-Scholes Does NOT Handle

- American options (early exercise) — NSE index options are European, so BS is correct
- Discrete dividends — continuous dividend yield q is used, standard for index options
- Stochastic volatility — BS assumes constant vol (vol smile exists in reality, IV surface captures this empirically)
- Jump processes — BS assumes continuous price paths

---

## Why Options Only — Not Futures

Futures have linear payoff — no optionality. Priced via Cost of Carry:
```
F = S × e^((r - q) × T)
```

No volatility, no inversion, no BS needed.
Futures Greeks: Delta ≈ 1, Gamma = 0, Vega = 0.
Separate futures_pricing.py module handles basis and futures delta later.

---

## Scaling Conventions (Industry Standard)

- Vega: per 1% move in vol (divide raw vega by 100)
- Theta: per calendar day (divide raw theta by 365)
- Rho: per 1% move in rate (divide raw rho by 100)

These match what you will see on any institutional risk report or
FRM exam question on Greeks.

---

## IV Bounds

| Bound | Value | Reason |
|---|---|---|
| Lower | 1% | Below this d1/d2 numerically unstable. India VIX never below ~10% historically |
| Upper | 500% | Above this no rational pricing exists. VIX peaked ~85% in COVID crash |
| Tolerance | 1e-6 | Price match to 0.000001 rupees — effectively zero error |
| Max iterations | 100 | Safety ceiling. Brent's converges in <20 for clean inputs |



## Black-Scholes Test Suite — What Each Section Tests and Why

---

### Section 1 — Input Guards

These tests verify the engine fails fast and cleanly on bad input rather than producing garbage output silently.

**DTE zero or negative** — Black-Scholes requires T > 0. At expiry there is no time value left — the option is worth only its intrinsic value. Dividing by zero in d1/d2 would crash the engine. Returning null is the correct behavior.

**Zero or negative market price** — Brent's method searches for a vol that makes BS price equal market price. If market price is zero, any positive vol produces a price above zero — no root exists. The search would fail anyway so we reject early.

**Zero or negative spot/strike** — `log(S/K)` is undefined for non-positive values. Mathematically and economically meaningless.

**Invalid option type** — Futures rows (`XX`) and bad data will appear in real datasets. The engine must not crash or produce nonsense Greeks for them.

**Below intrinsic value** — This is a core no-arbitrage concept from FRM. An option can never trade below its intrinsic value because of the following arbitrage: buy the option, immediately exercise, pocket the difference. If market price < intrinsic, IV inversion either fails to bracket or produces a negative IV. We reject before attempting.

---

### Section 2 — IV Inversion (Round Trip)

These tests verify the core of the engine — that if you start with a known volatility, compute a theoretical price, then invert back, you recover the original volatility.

**Round trip for ATM, ITM, OTM, long DTE** — Each moneyness regime has different numerical characteristics. ATM options are most sensitive to vol and easiest to invert. Deep ITM and OTM options have flatter vega — the price changes very little with vol — making inversion numerically harder. Testing all regimes confirms the engine is robust.

**IV within bounds** — Any recovered IV must fall between 1% and 500%. Confirms the search bracket is enforced.

**Extremely high price returns null** — A price higher than any BS model can produce within the vol bounds has no corresponding IV. Confirms the bracketing check works.

**Price match within tolerance** — The recovered IV, when plugged back into BS, must reproduce the original market price within 0.01 rupees. This is the practical accuracy standard for IV inversion.

---

### Section 3 — Put-Call Parity

This is a fundamental FRM concept. Put-call parity states:

```
C - P = S·e^(-qT) - K·e^(-rT)
```

This relationship holds for any European option under any model — not just Black-Scholes. It is a pure no-arbitrage result. If it is violated, a riskless profit exists.

**Parity for ATM and ITM** — Tests that our BS implementation satisfies this identity exactly. If the formula for calls and puts were inconsistent, parity would break. This is one of the most powerful consistency checks in options pricing.

**Same IV for call and put at same strike** — A direct consequence of put-call parity. If call and put at the same strike are priced consistently, they must imply the same volatility. This is how traders check for mispricing in live markets — if call IV and put IV diverge at the same strike, something is wrong.

---

### Section 4 — Greeks Direction

These tests verify that Greeks have the correct sign — they point in the right direction economically.

**Call delta positive, put delta negative** — Delta measures how much the option price moves per unit move in the underlying. A call gains value when spot rises — positive delta. A put gains value when spot falls — negative delta. FRM core concept.

**Delta bounded between 0 and 1 for calls, -1 and 0 for puts** — Delta can never exceed these bounds. A delta above 1 would mean the option gains more than the underlying per point move — impossible without leverage. Deep ITM options approach the bounds (delta → 1 for calls, -1 for puts) but never cross them.

**ITM call delta greater than OTM call delta** — ITM options move almost dollar-for-dollar with the underlying. OTM options are less sensitive. This is the moneyness effect on delta.

**Gamma positive for both calls and puts** — Gamma measures the rate of change of delta. It is always positive because as spot moves in your favor, delta increases (you become more right). Negative gamma would mean you become less right as spot moves in your favor — economically impossible for a long option.

**Vega positive for both** — Higher volatility always increases option value regardless of direction. More uncertainty = more chance of a favorable large move = higher option price. FRM core.

**Theta negative for both** — Options lose time value as expiry approaches. All else equal, an option today is worth more than the same option tomorrow. Theta quantifies this daily decay. Always negative for long options.

**Call rho positive, put rho negative** — A higher risk-free rate increases the present value advantage of owning the call (you delay paying the strike) and decreases the present value benefit of owning the put (the strike you receive is worth less in PV terms). FRM interest rate sensitivity concept.

---

### Section 5 — Greeks Magnitude

Direction alone is not enough. These tests verify Greeks land in sensible ranges.

**ATM delta near 0.5** — At-the-money options have roughly equal probability of expiring in or out of the money. Delta approximates this probability (loosely) — hence ATM delta is near 0.5 for calls and -0.5 for puts. This is one of the most cited practical rules in options trading.

**Deep ITM call delta near 1** — Almost certain to be exercised. Behaves like owning the underlying. Delta → 1.

**Deep OTM call delta near 0** — Almost certain to expire worthless. Barely responds to spot moves. Delta → 0.

**Gamma and vega equal for calls and puts at same strike** — Mathematically provable from the BS formula. Gamma and vega depend only on `|d1|` and `pdf(d1)` — symmetric for call and put at the same strike. A critical cross-check that the implementation is internally consistent.

---

### Section 6 — Greeks Sensitivity Direction

These tests verify the engine responds correctly when inputs change — the second-order behavior.

**Higher vol increases both call and put prices** — Vega is always positive. More uncertainty benefits both buyers. If raising vol decreased a price, vega would be negative — impossible.

**Longer DTE increases both call and put prices** — More time = more chance of a favorable large move. Theta decay is the mirror of this — you pay for time, and it erodes daily.

**Higher spot increases call, decreases put** — Directly tests delta direction at the price level. As spot rises, calls move into the money (more valuable), puts move out of the money (less valuable).

---

### Section 7 — Norm CDF

These tests verify the mathematical building block underlying all BS calculations.

**N(0) = 0.5** — The standard normal distribution is symmetric around zero. Probability of being below zero is exactly 50%.

**N(10) ≈ 1, N(-10) ≈ 0** — Extreme values converge to the distribution bounds. Confirms no numerical overflow or underflow at the tails.

**Symmetry: N(x) + N(-x) = 1** — Core property of any symmetric distribution. If violated, every BS price and Greek would be wrong.

**N(1.96) ≈ 0.975** — The classic 95% confidence interval result from statistics. 1.96 standard deviations covers 97.5% of one tail. Every FRM candidate knows this number.

---

### Section 8 — Null Result Completeness

**All fields null when any guard fires** — If any input guard rejects the computation, every output field must be null — not a mix of some nulls and some computed values. A partial result (e.g. delta computed but IV null) would corrupt any downstream aggregation that sums Greeks across a portfolio.

**All fields non-null for valid input** — The mirror test. A clean input with a known theoretical price must produce a complete result. No silent partial failures allowed.
