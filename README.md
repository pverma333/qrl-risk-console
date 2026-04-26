# QRL Risk Console

Institutional-grade derivatives risk platform for Indian index options.
Built end-to-end — from raw NSE data ingestion to live portfolio risk analytics.

**Live demo:** https://qrl-index-console.streamlit.app
**API docs (Swagger):** https://qrl-api.onrender.com/docs

---

## What It Does

| Page | Function |
|---|---|
| Market Explorer | Option chain with IV and Greeks for any index, date, expiry |
| Portfolio Risk | MtM PnL, scenario PnL, and net Greeks for uploaded positions |
| Scenario Lab | Stress test: apply spot / vol / rate shocks across a portfolio |
| VaR / CVaR | Historical simulation with 252 scenarios and worst-day attribution |

A risk analyst or trader can:
- Load any NIFTY / BANKNIFTY / FINNIFTY / MIDCPNIFTY chain from 2019 to present
- Upload a position CSV and see net delta, gamma, vega, theta instantly
- Apply "Spot -3%, Vol +5, Rate +25bps" and see per-leg and total PnL
- Run VaR 95% / 99% and CVaR with full scenario distribution and worst-day identification

---

## Architecture

```
Browser
  |
Streamlit Cloud  (dashboard/)
  |  HTTPS
Render — FastAPI  (app/)
  |  downloads on startup via download_from_r2.py
Cloudflare R2 — Parquet files  (data/)
  |
DuckDB — 17 views across 3 registries
  |
Quant Engine  (src/quant/)
  IV solver · Greeks · Yield interpolation · Scenario engine · VaR/CVaR
```

### Four-Layer Data Architecture

```
Raw         NSE derivatives CSV files — immutable, never modified [Example Gbond Raw data csv file]
  |
Ingest      7 namespaces — append-safe, dedup-enforced, trading-day validated
  |
Processed   7 processors — schema normalised, idempotent, type-coerced
  |
Curated     8.8M option chain rows · 20,499 futures rows
            IV computed via BS inversion · Greeks computed analytically
```

Each layer has a single responsibility. No cross-layer dependencies.
Re-running any layer on the same input produces identical output.

---

## Tech Stack

| Component | Technology |
|---|---|
| Language | Python 3.12 |
| API framework | FastAPI + uvicorn |
| Query engine | DuckDB |
| Data format | Parquet (year-partitioned) |
| Object storage | Cloudflare R2 |
| Quant math | NumPy, SciPy, pandas |
| Charting | Plotly |
| Parquet I/O | PyArrow |
| R2 client | boto3 |
| Frontend | Streamlit |
| Backend host | Render (free tier, Singapore) |
| Frontend host | Streamlit Cloud |
| Uptime monitoring | Better Uptime (3-minute health pings) |
| Testing | pytest — 257 tests, 85% coverage |

---

## Key Technical Decisions

**IV computation via bisection method, not Newton-Raphson.**
Newton-Raphson requires the vega derivative at each iteration and diverges when
vega approaches zero — which occurs on deep OTM strikes with low time value.
Bisection is a bracketed root-finding algorithm guaranteed to converge
within [1%, 500%] vol bounds without requiring a derivative. For a production
system where any strike can appear in a portfolio, numerical robustness takes
priority over marginal speed gains.

**Linear yield interpolation across three tenors, not cubic spline or Nelson-Siegel.**
The yield curve uses three government bond tenors: 3M, 6M, 1Y. Cubic spline
earns its complexity when five or more tenor points are spread across a full
curve — with three points it produces results nearly identical to a straight
line. Nelson-Siegel is a parametric model requiring non-linear fitting that
becomes statistically under-determined with fewer than four tenor points.
Linear interpolation introduces negligible pricing error across a segment
where the rate difference is typically under 15 basis points.

```
DTE < 91             use 3M rate directly
91  <= DTE < 182     interpolate between 3M and 6M
182 <= DTE < 365     interpolate between 6M and 1Y
DTE >= 365           use 1Y rate directly

Formula: r = r_low + (dte - dte_low) / (dte_high - dte_low) * (r_high - r_low)
```

Upgrade path: if 2Y, 5Y, 10Y bond data is added later, the `interpolate_rate`
interface accommodates a swap to cubic spline without touching the BS engine.

**DuckDB over PostgreSQL.**
The analytical workload — filtering 8.8M rows by date, symbol, and expiry,
then joining spot, yield, and Greeks — is columnar and read-heavy. DuckDB
executes these queries in milliseconds without a server process. PostgreSQL
would require an always-on managed instance with no performance advantage
for this access pattern.

**Parquet with year-partitioned folder structure, read via DuckDB glob views.**
Parquet provides columnar compression and predicate pushdown — queries never
scan columns or years they do not need. DuckDB views registered at startup
abstract file paths from query logic. Adding a new data year requires no
schema migration and no code change.

**Append-only ingest with explicit deduplication keys.**
The pipeline is idempotent — re-running it on the same data produces the
same output. Deduplication is enforced at write time using explicit unique
keys per dataset type. No silent overwrites, no rebuild-on-failure patterns.

**Settlement price as Black-Scholes input, not last traded price.**
NSE's settlement price is a volume-weighted average of the last 30 minutes
of trading. It is more stable than last traded price for EOD risk systems
and is the standard input for margin computation at NSE itself.

**Arithmetic returns for VaR historical simulation.**
For single-day PnL simulation on an options portfolio, arithmetic returns
are computationally correct and consistent with FRM-standard historical
simulation methodology. Log returns add complexity with negligible benefit
at a one-day horizon and would create unit inconsistency with the scenario
engine shock convention.

**Greeks computed analytically, not via numerical differentiation.**
All five Greeks are derived directly from d1 and d2 using closed-form BS
partial derivatives. Numerical differentiation via finite differences
introduces approximation error and doubles the number of BS evaluations
per position. Analytical Greeks are exact and faster.

**Scenario engine: full reprice preferred, Greeks approximation as fallback.**
Full reprice calls BS at shocked parameters and computes exact PnL. Greeks
approximation uses the Taylor expansion (delta-gamma-vega) and breaks down
for spot shocks above 5%. The `method` field in every response tells the
caller which path was taken.

---

## API Endpoints

| Endpoint | Method | Description |
|---|---|---|
| `/health` | GET | Uptime check — pinged every 3 minutes by Better Uptime |
| `/chain/latest-date` | GET | Latest available trade date from curated layer |
| `/chain/expiries/{symbol}/{trade_date}` | GET | Available expiry dates for symbol on date |
| `/chain/{symbol}/{trade_date}/{expiry_date}` | GET | Full option chain with IV and Greeks |
| `/vix/{trade_date}` | GET | India VIX for given date |
| `/scenario/` | POST | Single contract scenario PnL under shock |
| `/portfolio/analyze` | POST | CSV upload — MtM PnL, scenario PnL, net Greeks |
| `/var/analyze` | POST | Historical simulation VaR/CVaR with 252 scenarios |
| `/market/summary/{trade_date}` | GET | Index spot, VIX, and yield snapshot for a date |

Full interactive documentation: https://qrl-api.onrender.com/docs

---

## Project Structure

```
qrl-risk-console/
├── app/                         FastAPI application layer
│   ├── main.py
│   ├── dependencies.py          DuckDB connection — reads QRL_BASE_DIR env var
│   ├── routers/                 chain, vix, scenario, portfolio, var
│   ├── schemas/                 Pydantic models per endpoint
│   └── services/                DuckDB query logic per endpoint
├── dashboard/
│   ├── Home.py                   Home page with 4 navigation tiles
│   ├── config.py                API base URL, valid symbols, shock defaults
│   └── pages/
│       ├── 1_Market_Explorer.py
│       ├── 2_Portfolio_Risk.py
│       ├── 3_Scenario_Lab.py
│       └── 4_VaR_CVaR.py
├── src/
│   ├── core/fetch_config.py     FetchConfig — env-driven base paths
│   ├── db/                      DuckDB connection + 3 registries
│   ├── data/                    Builder classes — one per data type per layer
│   └── quant/
│       ├── black_scholes.py     BS pricing + IV inversion (bisection) + Greeks
│       ├── bs_vectorized.py     Vectorised BS for curated layer batch computation
│       ├── yield_curve.py       Linear interpolation across 3M/6M/1Y tenors
│       ├── futures_pricing.py   Cost of carry — F = S * e^((r-q)*T)
│       ├── scenario_engine.py   Full reprice + Greeks approximation fallback
│       ├── portfolio.py         MtM PnL + scenario aggregation + net Greeks
│       └── var.py               Historical simulation VaR/CVaR
├── scripts/
│   ├── run_daily_fetch.py       Pipeline orchestrator
│   ├── upload_to_r2.py          Uploads Parquet to Cloudflare R2
│   ├── download_from_r2.py      Downloads Parquet from R2 to Render /tmp/qrl/
│   └── daily_sync.sh            Master daily job: fetch → upload → redeploy → notify
└── tests/                       257 tests, 85% coverage
```

---
## Running Locally

**Prerequisites:** Python 3.12

```bash
git clone https://github.com/pverma333/qrl-risk-console.git
cd qrl-risk-console
pip install -r requirements.txt
```

Set the base directory environment variable:

```bash
export QRL_BASE_DIR="/path/to/qrl-risk-console"
```

**Step 1 — Download government bond data manually.**

This is the only manual download required. All other data is fetched automatically by the pipeline.

Download historical CSV exports for your target date range from the three links below.
On each page, select your date range and export as CSV.

- 3M: https://in.investing.com/rates-bonds/india-3-month-bond-yield-historical-data
- 6M: https://in.investing.com/rates-bonds/india-6-month-bond-yield-historical-data
- 1Y: https://in.investing.com/rates-bonds/india-1-year-bond-yield-historical-data

Rename the downloaded files exactly as shown and place them here:

```
data/ingest/gbond/3monthbond.csv
data/ingest/gbond/6monthbond.csv
data/ingest/gbond/1yearbond.csv
```

**Step 2 — Run the full data pipeline (first time only).**

Everything else — NSE derivatives, index spot, India VIX, dividend yields — is fetched and built automatically.

```bash
python -m scripts.run_data_pipeline --start 2019-01-01 --end 2026-02-23 --rebuild
python scripts/run_processed_builder.py --mode full
python scripts/run_curated_option_chain.py --mode full
python scripts/run_curated_futures.py --mode full
```

Monitor progress in a separate terminal:

```bash
tail -f logs/data_pipeline_fetch.log
```

For a 7-8 year data span this takes approximately 1.5 to 2 hours. On subsequent runs the pipeline appends only new data incrementally.

**Step 3 — Start the application.**

```bash
uvicorn app.main:app --reload        # API at http://localhost:8000
streamlit run dashboard/Home.py      # UI  at http://localhost:8501
```

Open http://localhost:8501 in your browser.

---

**Daily data updates (after initial setup).**

The DuckDB lock must be released before running the fetch. Stop the API process, fetch new data, then restart.

**1 — Find and stop the running API process.**

```bash
lsof -i :8000
```

Note the PID values in the output, then kill them:

```bash
kill -9 <pid>
```

**2 — Run the daily fetch.**

```bash
python -m scripts.run_daily_fetch
```

**3 — Restart the application.**

```bash
uvicorn app.main:app --reload        # API at http://localhost:8000
streamlit run dashboard/Home.py      # UI  at http://localhost:8501
```

---

## Testing

```bash
pytest tests/ -v --cov=src --cov=app
```

257 tests, 85% coverage across:
- IV inversion round-trip (known vol → BS price → invert → recover vol)
- Put-call parity verification at multiple strikes and maturities
- Greeks sign, magnitude, and moneyness ordering checks
- Input guard rejection (DTE zero, below-intrinsic price, invalid option type)
- API endpoint contract tests (status codes, response schema, edge dates)
- Data processor idempotency (re-run produces identical output)

---

## Daily Data Pipeline

Runs automatically at 9:30 PM IST on weekdays via macOS launchd:

```
run_daily_fetch.py      Fetches NSE EOD data for all 7 namespaces
                        Appends to year-partitioned Parquet
                        Deduplicates on explicit unique keys
                        Deletes temporary daily fetch files on success

upload_to_r2.py         Uploads updated Parquet to Cloudflare R2

Render deploy hook      Triggers Render service redeploy

download_from_r2.py     Render downloads fresh data to /tmp/qrl/ on startup

Email notification      Pipeline summary + last 100 lines of log
```

Data ingested daily:
- NSE derivatives — options and futures OHLC, settlement price, open interest
- Index spot prices — NIFTY, BANKNIFTY, FINNIFTY, MIDCPNIFTY
- India VIX
- Government bond yields — 3M, 6M, 1Y
- Index dividend yields

---

## Validated Risk Metrics

Tested against a NIFTY portfolio on 2026-03-13 with a 252-day lookback:

| Metric | Value |
|---|---|
| VaR 95% | Rs 40,266 |
| VaR 99% | Rs 53,819 |
| CVaR 95% | Rs 52,298 |
| CVaR 99% | Rs 74,291 |
| Worst day | April 7, 2025 — NIFTY -3.24% (US tariff crash) |
| Best day | May 12, 2025 — NIFTY +3.82% |

---

## Data Coverage

| Symbol | Rows |
|---|---|
| NIFTY | 3,750,005 |
| BANKNIFTY | 2,639,914 |
| FINNIFTY | 1,405,555 |
| MIDCPNIFTY | 1,153,530 |
| **Total** | **8,949,004** |

Coverage: 2019 to present. Updated daily on weekdays.

---

## Known Data Characteristics

**BANKNIFTY dividend yield nulls (March–July 2021).** RBI-mandated dividend
restrictions during this period resulted in zero reported yields. Rows are
preserved as null — not filled, not dropped. Portfolio module flags affected
positions.

**Government bond data (May 2021).** Source data mixed percentage and price
conventions. Resolution: if value > 15, treat as price and convert to yield;
otherwise treat as percentage directly. Indian government bond yields have
never exceeded 15% historically.

**Zero-contract option rows.** NSE publishes settlement prices for strikes
with no trades on a given day. Settlement for these rows is computed by NSE
using their own theoretical model. IV inversion on these rows will produce
a result but Greeks should be interpreted with awareness of zero liquidity.

---

## Design Principles

**Idempotent pipeline.** Re-running any script on the same data produces
identical output. Safe to re-run after any failure.

**Fail-fast on schema violations.** Invalid data is rejected at ingest with
an explicit error message. Nothing propagates silently to downstream layers.

**No business logic in the UI layer.** Streamlit makes HTTP calls only.
All computation happens in FastAPI services and the quant engine.

**Auditability.** Every API response exposes the pricing model (Black-Scholes),
day count convention (365), rate interpolation method (linear), yield tenors
used, scenario shocks applied, and data as-of date. Export bundles include
a run manifest JSON with full inputs and assumptions.

**Separation of concerns.** Quant engines in `src/quant/` accept clean numeric
inputs and return deterministic outputs. No file I/O, no database calls, no
side effects. Independently testable and replaceable.

---

## What This Is Not

This is an EOD risk console, not an intraday trading system.
It does not execute trades, manage live positions, or stream real-time data.
It does not run systematic strategy backtests or optimise parameters.
Scenario analysis is hypothetical repricing, not path-dependent simulation.

---

## Deployment Architecture

```
User browser
  |  HTTPS
Streamlit Cloud  (qrl-risk-console.streamlit.app)
  |  HTTP
Render free tier  (qrl-api.onrender.com)
  |  reads Parquet on startup via download_from_r2.py
Cloudflare R2  (qrl-risk-data bucket, 734MB used of 10GB)

Better Uptime — pings /health every 3 minutes to keep Render warm

Daily sync (Mac launchd, 9:30 PM IST weekdays):
  run_daily_fetch.py → upload_to_r2.py → Render deploy hook → email notification
```

---

## Author

Priyam Verma
[LinkedIn](https://linkedin.com/in/priyamverma) | [GitHub](https://github.com/pverma333)
