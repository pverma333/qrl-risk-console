### For QRL:
1. Storage: Parquet (partitioned by year + symbol)
2. Query: DuckDB
3. Processing: Year-wise batch jobs
4. Feature layer: Separate curated folder
5. Memory: Use categorical + float32

### Recuriter guide - If they open your repo and see:
1. Parquet partitioning
2. Schema versioning
3. Config-driven ingestion
4. SQL query layer
5. Dtype optimization
6. Batch processing

### Raw Layer (Immutable)
Source files
No transformations
Stored partitioned by date
Format: CSV or Parquet

Example -
data/raw/fo/2019/
data/raw/fo/2020/
data/raw/spot/
data/raw/yield/

### Processed Layer
- Cleaned
- Normalized schema
- Dtypes optimized
- No analytics yet
- Partitioned by year/month
- Format: Parquet only

Why Parquet?
- Columnar (fast)
- Compressed
- Reads only needed columns
- Industry standard
- Perfect for 8M rows

Example:
data/processed/derivatives/year=2024/
data/processed/spot/
data/processed/yield/

### Curated Layer
- Feature engineered
- IV computed
- Greeks computed
- DTE
- Moneyness
- Risk buckets
- Regime labels
- Also stored as partitioned Parquet

### Best Data Structure and DB - Parquet + Partioning + DuckDB
Partition by:
- year
- optionally symbol

Why?
Because in production:
You never query 2019 if you want 2025
You never query all symbols if you want NIFTY
You never query all columns

Parquet allows:
- Column pruning
- Predicate pushdown
- Huge performance gain.

Advantages:
- No server
- SQL support
- Blazing fast
- Used by hedge funds
- Recruiter impressive

### Convert Datatypes - reduces storage by 40-60%
df['symbol'] = df['symbol'].astype('category')
df['instrument'] = df['instrument'].astype('category')
df['option_typ'] = df['option_typ'].astype('category')

df['open'] = df['open'].astype('float32')
df['strike_pr'] = df['strike_pr'].astype('float32')
df['open_int'] = df['open_int'].astype('int32')

### Processing
Pattern 1: Year-wise Processing
- Process 1 year at a time.
- Append to Parquet partition.

Pattern 2: Symbol-wise Processing
- Process NIFTY separately from BANKNIFTY.

Pattern 3: Feature Computation as Batch Jobs
- Compute IV in batches
- Compute Greeks in batches
- Save results incrementally
- Production systems never recalc entire history


## Design Principle
Raw = Data Lake
Processed = Warehouse
Curated = Risk Engine Ready



""""
assumption - yes right now it only contains one parquet file but it depends on how the daily fetch will work -

1. have a script that fetches all the details daily at eod or start of the day
2. there will be different paruet files files for daily fetch and after rows are appended in the respective raw data files i will delete these files -
- derivatives
- index spot and india vix
- gov bond 1 year/ 6months/3months
- index yeild

it appends respective rows from adjacent daily detch file in their respective parquet so for isntance
daily fetch of 19th feb 2026
- index spot goes in index_spot_prices.parquet
- india vix does into india_VIX_historical.parquet
- bond file goes into gbond_combined.parquet
- index yeild goes into index_dividend_yeild.parquet
- derivaties goes and appends into ingest/derivatives/2026/derivatives_2026.parquet file


the values should be deduplicated and appended once it is successful the daily fetch will be deleted

now based on this trade calendar should run
- the moment derivatives folder any file is updated (only append new values)
- when we run the pipeline in rebuild mode (delete and recreate )
- when we run indivdual tradeclenaderwriter (delete adn recreate)


Daily Fetch → temporary daily files →
Append into consolidated historical parquet →
Deduplicate →
Delete daily temp file →
Repeat next day.

Derivatives:
data/ingest/derivatives/2026/derivatives_2026.parquet
gets appended.

Then TradeCalendar:

• If derivatives updated → append only new trade dates
• If rebuild → delete + full regenerate
• If standalone → delete + full regenerate

This is correct layering.
"""



gov bond daily fetch run this on terminal --> pip install git+https://github.com/rongardF/tvdatafeed.git pandas


result of audit_derivatives.py

INFO | src.db.ingest_registry | Registered view: v_lotsize → data/ingest/LotSize
INFO | src.db.ingest_registry | Registered view: v_tradecalendar → data/ingest/TradeCalendar
INFO | src.db.ingest_registry | Registered view: v_derivatives → data/ingest/derivatives
INFO | src.db.ingest_registry | Registered view: v_gbond → data/ingest/gbond
INFO | src.db.ingest_registry | Registered view: v_index_spot → data/ingest/index_spot
INFO | src.db.ingest_registry | Registered view: v_index_yield → data/ingest/index_yield
INFO | src.db.ingest_registry | Registered view: v_vix → data/ingest/vix
INFO | src.db.ingest_registry | Total views registered: 7

--- 1. SCHEMA ---
INFO | numexpr.utils | Note: NumExpr detected 10 cores but "NUMEXPR_MAX_THREADS" not set, so enforcing safe limit of 8.
INFO | numexpr.utils | NumExpr defaulting to 8 threads.
   column_name column_type null   key default extra
0   INSTRUMENT     VARCHAR  YES  None    None  None
1       SYMBOL     VARCHAR  YES  None    None  None
2    EXPIRY_DT     VARCHAR  YES  None    None  None
3    STRIKE_PR      DOUBLE  YES  None    None  None
4   OPTION_TYP     VARCHAR  YES  None    None  None
5         OPEN      DOUBLE  YES  None    None  None
6         HIGH      DOUBLE  YES  None    None  None
7          LOW      DOUBLE  YES  None    None  None
8        CLOSE      DOUBLE  YES  None    None  None
9    SETTLE_PR      DOUBLE  YES  None    None  None
10   CONTRACTS      DOUBLE  YES  None    None  None
11    OPEN_INT      DOUBLE  YES  None    None  None
12   CHG_IN_OI      DOUBLE  YES  None    None  None
13   TIMESTAMP     VARCHAR  YES  None    None  None

--- 2. NULL COUNTS ---
   total_rows  null_close  null_settle  null_expiry  null_strike  null_option_type
0     8949004           0            0            0            0              4956

--- 3. ZERO VALUE COUNTS ---
   zero_close  zero_settle  zero_strike
0           0       161176        20712

--- 4. SYMBOL DISTRIBUTION ---
       SYMBOL  row_count
0       NIFTY    3750005
1   BANKNIFTY    2639914
2    FINNIFTY    1405555
3  MIDCPNIFTY    1153530

--- 5. OPTION TYPE DISTRIBUTION ---
  OPTION_TYP  row_count
0       None       4956
1         PE    4488068
2         CE    4440224
3         XX      15756


## Index Yield - Nifty bank was 0 from March 2021 till july 2021 because
Throughout the March–July 2021 period, Nifty Bank price data remained fully available due to standard trading, but RBI-mandated dividend restrictions effectively zeroed out reported yields, while P/E ratios were only intermittently published based on the volatility of constituent bank earnings.

## Gbond value - mixed for May 2021 as percentage and price --> logic is if value > 15 then it is recalulated as price otherwise copied as it is. Logic being indian gbond has never crossed 15%

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
