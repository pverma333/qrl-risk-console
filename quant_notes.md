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

FuturesPricer
=============

Model: Cost of Carry
Formula: F = S * e^((r - q) * T)

Parameters:
- S (spot):      Current index spot price. Must be positive.
- r (rate):      Risk-free rate. Annualized, continuously compounded, decimal form.
                 e.g. 6.5% → pass 0.065
- q (div_yield): Continuous dividend yield. Annualized, decimal form.
                 e.g. 1.2% → pass 0.012
- T:             Time to expiry in years. Derived from DTE / day_count.
- F:             Theoretical futures price.

Basis:
  basis = actual_futures_price - theoretical_price
  Positive basis → futures trading rich relative to fair value.
  Negative basis → futures trading cheap relative to fair value.
  At expiry, basis converges to zero (no-arbitrage condition).

Delta:
  Index futures delta = 1.0 with respect to spot.
  Used in portfolio Greeks aggregation as a uniform interface
  alongside options delta.

Day Count Convention:
  Default day_count = 365. Consistent with Black-Scholes engine.

FRM Reference:
  Cost of Carry model — FRM Part 1, Futures and Forwards.
  No-arbitrage condition: if F > S*e^((r-q)*T), cash-and-carry
  arbitrage is possible. Transaction costs create an arbitrage band
  in practice — basis within that band is not exploitable.

Scalar vs Vectorized:
  FuturesPricer is scalar by design. Used in scenario engine and
  portfolio module where per-position calls are appropriate.
  Batch computation in CuratedFuturesBuilder uses numpy directly
  on DataFrame columns — no FuturesPricer call overhead per row.


CuratedFuturesBuilder
=====================

Purpose:
  Joins processed futures with spot, dividend yield, and government
  bond data. Computes theoretical price and basis. Writes
  year-partitioned curated parquets to data/curated/futures/{year}/.

Joins Performed:
  1. v_processed_futures       ← base table
  2. v_processed_index_spot    ← join on (trade_date, symbol) → spot price
  3. v_processed_index_yield   ← join on (trade_date, symbol) → div_yield
  4. v_processed_gbond         ← pivot on trade_date, interpolate via YieldCurve → rate

Unique Key:
  (trade_date, symbol, expiry_date)

div_yield Convention:
  Processed layer stores div_yield as percentage (e.g. 1.23).
  Builder divides by 100 in SQL before any computation.
  Cost of carry model requires decimal form (e.g. 0.0123).

Rate Interpolation:
  Uses existing YieldCurve class (linear interpolation, 3M/6M/1Y tenors).
  Operates row-by-row because DTE varies per row.
  Try/except per row — interpolation failure produces null rate,
  downstream columns (theoretical_price, basis) also null.

Vectorized Computation:
  _compute_quant_columns uses numpy array operations.
  No Python loop. No FuturesPricer call.
  T = dte / 365.0 applied as array operation.
  theoretical_price = spot * exp((rate - div_yield) * T)
  basis = settle - theoretical_price
  delta = 1.0 (scalar broadcast to entire column)

Null Handling:
  Missing spot → null theoretical_price, null basis. Preserved, not filled.
  Missing gbond tenors → null rate. Preserved, not filled.
  BANKNIFTY div_yield nulls (March–June 2021) → known data quirk.
  Preserved as null. Portfolio module flags these positions.

Incremental Mode:
  Reads max trade_date from existing curated partition per year.
  Filters processed futures to rows strictly after that date.
  Concats with existing partition, deduplicates on unique key, rewrites.

Full Mode:
  Rebuilds all year partitions from scratch.
  Does not modify processed or ingest layers.

ScenarioEngine
==============

Two repricing modes per option position:

1. full_reprice (preferred)
   Requires: iv > 0
   PnL = BS(shocked params) - BS(base params)
   Accurate for any shock size.

2. greeks_approx (fallback)
   Requires: delta, gamma, vega, rho all non-null
   PnL ≈ delta*ΔS + 0.5*gamma*ΔS² + vega*Δσ_pts + rho*Δr_pts
   Breaks down for large shocks (>5% spot move).

3. no_data
   IV null and Greeks null. PnL = 0. Portfolio flags position.

Shock conventions:
  spot_shock_pct : percentage. -1.5 → spot drops 1.5%.
  vol_shock_abs  : vol points. +2.0 → IV rises 2 percentage points.
  rate_shock_bps : basis points. +20 → rate rises 20bps.

Unit alignment for greeks_approx:
  vega in BS engine is per 1 vol point (already /100 in _bs_greeks).
  Δσ_pts = vol_shock_abs passed directly in vol points.
  rho in BS engine is per 1% rate move (already /100 in _bs_greeks).
  Δr_pts = rate_shock_bps / 100 converts bps to percentage points.

Futures PnL:
  Linear. Delta = 1. No vol or rate sensitivity.
  PnL = ΔS * quantity * lot_size.

Short positions:
  quantity negative → pnl_total sign flips automatically.

Portfolio Module
================

Input: CSV with columns:
  symbol, expiry_date, strike, option_type,
  quantity, entry_date, entry_price

option_type values: CE, PE, XX (futures)
quantity: positive = long, negative = short
entry_price: price per unit at time of entry
strike: 0 for futures positions

MtM PnL (options):
  current_price = BS(today's snapshot)
  mtm_pnl = (current_price - entry_price) * quantity * lot_size

MtM PnL (futures):
  mtm_pnl = (current_spot - entry_price) * quantity * lot_size

Scenario PnL:
  Delegated to scenario_engine.py per position.
  Full reprice if IV available. Greeks approx fallback.

Total PnL per position:
  total_pnl = mtm_pnl + scenario_pnl

Net Greeks:
  Weighted by quantity * lot_size across all positions.
  Null Greeks contribute 0.0 to net — not excluded, not errored.

Lot size:
  Point-in-time lookup from processed lot_size table.
  Handles NSE lot size changes across years correctly.

no_data positions:
  Returned in results with all PnL fields = 0.0.
  Not silently dropped. UI must flag these to user.

# QuantNotes — Phase 4 Risk Engine

---

## Futures Pricing

Model: Cost of Carry
Formula: F = S * e^((r - q) * T)

Parameters:
- S (spot):      Current index spot price. Must be positive.
- r (rate):      Risk-free rate. Annualized, continuously compounded, decimal.
                 e.g. 6.5% → pass 0.065
- q (div_yield): Continuous dividend yield. Annualized, decimal.
                 e.g. 1.2% → pass 0.012
- T:             Time to expiry in years. Derived from DTE / day_count.
- F:             Theoretical futures price.

Basis:
  basis = actual_futures_price - theoretical_price
  Positive basis → futures trading rich relative to fair value.
  Negative basis → futures trading cheap relative to fair value.
  At expiry, basis converges to zero (no-arbitrage condition).

Delta:
  Index futures delta = 1.0 with respect to spot.
  Used in portfolio Greeks aggregation as a uniform interface
  alongside options delta.

Day Count Convention:
  Default day_count = 365. Consistent with Black-Scholes engine.

FRM Reference:
  Cost of Carry model — FRM Part 1, Futures and Forwards.
  No-arbitrage condition: if F > S*e^((r-q)*T), cash-and-carry
  arbitrage is possible. Transaction costs create an arbitrage band
  in practice — basis within that band is not exploitable.

Scalar vs Vectorized:
  FuturesPricer is scalar by design. Used in scenario engine and
  portfolio module where per-position calls are appropriate.
  Batch computation in CuratedFuturesBuilder uses numpy directly
  on DataFrame columns — no FuturesPricer call overhead per row.

---

## Curated Futures Builder

Purpose:
  Joins processed futures with spot, dividend yield, and government
  bond data. Computes theoretical price and basis. Writes
  year-partitioned curated parquets to data/curated/futures/{year}/.

Joins Performed:
  1. v_processed_futures       ← base table
  2. v_processed_index_spot    ← join on (trade_date, symbol) → spot price
  3. v_processed_index_yield   ← join on (trade_date, symbol) → div_yield
  4. v_processed_gbond         ← pivot on trade_date → rate_3m, rate_6m, rate_1y

Unique Key:
  (trade_date, symbol, expiry_date)

div_yield Convention:
  Processed layer stores div_yield as percentage (e.g. 1.23).
  Builder divides by 100 in SQL before any computation.
  Cost of carry model requires decimal form (e.g. 0.0123).

Rate Interpolation:
  np.where chain — identical to CuratedOptionChainBuilder.
  Breakpoints: 91 days (3M), 182 days (6M), 365 days (1Y).
  DTE < 91  → use r3m flat
  DTE < 182 → linear interpolate between r3m and r6m
  DTE < 365 → linear interpolate between r6m and r1y
  DTE >= 365 → use r1y flat

Vectorized Computation:
  T = dte / 365.0 applied as numpy array operation.
  theoretical_price = spot * exp((rate - div_yield) * T)
  basis = settle - theoretical_price
  delta = 1.0 broadcast to entire column.
  No Python loop. No FuturesPricer call.

Null Handling:
  Missing spot → null theoretical_price, null basis. Preserved, not filled.
  Missing gbond tenors → null rate. Preserved, not filled.
  BANKNIFTY div_yield nulls (March–June 2021) → known data quirk.
  Preserved as null. Portfolio module flags these positions.

Incremental Mode:
  Reads max trade_date from existing curated partition per year.
  Filters processed futures to rows strictly after that date.
  Concats with existing partition, deduplicates on unique key, rewrites.

Full Mode:
  Rebuilds all year partitions from scratch.
  Does not modify processed or ingest layers.

Daily Pipeline Integration:
  CuratedFuturesBuilder runs after CuratedOptionChainBuilder in
  run_daily_fetch.py in incremental mode. No manual trigger needed.

---

## Scenario Engine

Two repricing modes per option position:

1. full_reprice (preferred)
   Requires: iv > 0
   PnL = BS(shocked params) - BS(base params)
   Accurate for any shock size.

2. greeks_approx (fallback)
   Requires: delta, gamma, vega, rho all non-null
   PnL ≈ delta*ΔS + 0.5*gamma*ΔS² + vega*Δσ_pts + rho*Δr_pts
   Breaks down for large shocks (>5% spot move).

3. no_data
   IV null and Greeks null. PnL = 0. Portfolio flags position.

Shock Conventions:
  spot_shock_pct : percentage. -1.5 → spot drops 1.5%.
  vol_shock_abs  : vol points. +2.0 → IV rises 2 percentage points.
  rate_shock_bps : basis points. +20 → rate rises 20bps.

Shock Application:
  S_shocked = S * (1 + spot_shock_pct / 100)
  r_shocked = r + rate_shock_bps / 10000
  σ_shocked = iv + vol_shock_abs / 100
  σ_shocked floored at 1e-4 to prevent BS domain errors.

Unit Alignment for greeks_approx:
  vega in BS engine is per 1 vol point (already /100 in _bs_greeks).
  Δσ_pts = vol_shock_abs passed directly in vol points.
  rho in BS engine is per 1% rate move (already /100 in _bs_greeks).
  Δr_pts = rate_shock_bps / 100 converts bps to percentage points.

Futures PnL:
  Linear. Delta = 1. No vol or rate sensitivity.
  PnL = ΔS * quantity * lot_size.

Short Positions:
  quantity negative → pnl_total sign flips automatically.
  No special handling needed anywhere in the engine.

ScenarioPnL Fields:
  base_price    : BS price at current market parameters.
  shocked_price : BS price at shocked parameters.
  mtm_pnl       : set to 0.0 by engine. Portfolio module overwrites.
  pnl_per_lot   : PnL per single unit (before lot size and quantity).
  pnl_total     : pnl_per_lot * quantity * lot_size.
  method        : full_reprice | greeks_approx | futures_linear |
                  expired | no_data | base_price_only.

---

## Portfolio Module

Input CSV Contract:
  symbol, expiry_date, strike, option_type,
  quantity, entry_date, entry_price

  option_type values : CE, PE, XX (futures)
  strike             : 0 for futures positions
  quantity           : positive = long, negative = short
  entry_price        : price per unit at time of entry
  entry_date         : YYYY-MM-DD, stored for audit trail

MtM PnL (options):
  current_price = BS(today's snapshot using current IV)
  mtm_pnl = (current_price - entry_price) * quantity * lot_size
  Returns null current_price and 0.0 mtm_pnl if IV null or DTE <= 0.

MtM PnL (futures):
  mtm_pnl = (current_spot - entry_price) * quantity * lot_size
  Always computable — no IV dependency.

Scenario PnL:
  Delegated to scenario_engine.py per position.
  Full reprice if IV available. Greeks approx fallback.
  Futures use linear payoff.

Total PnL per position:
  total_pnl = mtm_pnl + scenario_pnl

Net Greeks:
  Weighted by quantity * lot_size across all positions.
  Null Greeks contribute 0.0 to net — not excluded, not errored.
  Formula: sum(greek * quantity * lot_size) for all positions.

Lot Size:
  Point-in-time lookup from processed lot_size table.
  Handles NSE lot size changes across years correctly.
  Open-ended periods have end_date = None — treated as
  valid for any trade_date after start_date.
  Fallback: lot_size = 1 if no match found (logged separately).

no_data Positions:
  Returned in results with all PnL fields = 0.0.
  Not silently dropped. UI must flag these to user.
  Causes: expiry not in curated layer, symbol mismatch,
  strike not found for that trade_date.

CSV Validation Rules:
  - All seven required columns must be present
  - symbol must be in {NIFTY, BANKNIFTY, FINNIFTY, MIDCPNIFTY}
  - option_type must be in {CE, PE, XX}
  - quantity cannot be zero
  - entry_price must be positive
  Fails fast on first violation with clear error message.

---

## Portfolio API Endpoint

Endpoint: POST /portfolio/analyze
Content-Type: multipart/form-data

Form Fields:
  trade_date      : date string YYYY-MM-DD — as-of date for curated data
  spot_shock_pct  : float — percentage spot shock
  vol_shock_abs   : float — absolute vol shock in vol points
  rate_shock_bps  : float — rate shock in basis points
  file            : CSV file upload

Data Flow:
  CSV bytes parsed via pandas read_csv from BytesIO
  Curated options queried for trade_date — all symbols and expiries
  Curated futures queried for trade_date — all symbols and expiries
  Lot size table loaded in full — point-in-time join done in portfolio module
  run_portfolio() called with all DataFrames and shock
  PortfolioResult converted to PortfolioResponse Pydantic model

Date Type Handling:
  DuckDB returns TIMESTAMP columns as datetime64[us] in pandas.
  All date columns explicitly cast via .dt.date after query.
  Lot size start_date and end_date stored as object strings in parquet.
  Converted via pd.to_datetime(..., errors="coerce").dt.date
  errors="coerce" converts None to NaT safely.

Error Responses:
  400 — non-CSV file uploaded
  400 — empty CSV file
  400 — missing required columns
  400 — unknown symbols or option types
  400 — no curated data for requested trade_date
  500 — unexpected errors propagate to uvicorn log

Response Structure:
  PortfolioResponse
    trade_date       : date
    positions        : list[PositionResult]
    summary          : PortfolioSummary

  PositionResult
    All position fields + current_price, mtm_pnl,
    scenario_pnl, total_pnl, method, Greeks (all optional)

  PortfolioSummary
    total_mtm_pnl, total_scenario_pnl, total_pnl
    net_delta, net_gamma, net_vega, net_theta, net_rho
