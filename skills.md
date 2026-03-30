# QRL Risk Console — Product Description

## Overview

**QRL Risk Console** is an end-of-day (EOD) risk analytics platform for NSE index derivatives. It ingests, cleans, and stores market data across multiple asset classes, then computes risk metrics — MTM/P&L, implied volatility, Greeks, scenario stress tests, and historical VaR/CVaR — to produce decision-ready risk reports for the next trading day.

---

## Core Capabilities

### 1. Multi-Asset Data Ingestion
Automated daily fetch pipeline covering:
- **Index Derivatives** — NIFTY, BANKNIFTY, FINNIFTY, MIDCPNIFTY (futures & options)
- **Index Spot Prices** — Nifty 50, Nifty Bank, Nifty Financial Services, Nifty Midcap Select
- **India VIX** — implied volatility benchmark
- **Government Bond Yields** — 3-month, 6-month, 1-year tenors
- **Index Dividend Yields** — per index symbol

### 2. Config-Driven Pipeline
Central `FetchConfig` dataclass controls:
- Symbol universe for derivatives, index, and yield feeds
- Historical lot size schedules per symbol (period-accurate contract sizing)
- Strike price intervals per symbol
- Directory layout: `ingest → processed → curated`
- Optional year partitioning for high-volume namespaces

### 3. Parquet Storage with DuckDB Query Layer
- Raw data stored as immutable Parquet, partitioned by year and symbol
- Columnar format enables column pruning and predicate pushdown over 8M+ rows
- DuckDB registered views (`v_derivatives`, `v_lotsize`, `v_tradecalendar`, etc.) provide SQL access without a server
- Dtype optimization (categorical + float32/int32) reduces storage by 40–60%

### 4. Trade Calendar Management
- Derived from derivatives data; tracks every valid NSE trading date
- Supports append-only mode (triggered when derivatives data is updated) and full rebuild mode
- Used downstream for expiry-date alignment, DTE computation, and roll logic

### 5. Daily Fetch → Deduplication → Consolidation
```
Daily temp file → append to consolidated historical Parquet → deduplicate → delete temp file
```
- Rerun-safe: idempotent appends prevent duplicate rows
- Batch uploads in groups of 30 to manage memory
- Automatic switch between NSE Jugaad-style and Archive-style zip sources (mid-2024 cutover)

### 6. Derivatives Data Quality
- Null and zero-value auditing on close, settle price, strike, and option type
- Vectorized re-classification of OPTIDX / FUTIDX rows based on strike price
- Unknown symbol tracking for audit purposes
- Schema: INSTRUMENT, SYMBOL, EXPIRY_DT, STRIKE_PR, OPTION_TYP, OHLC, SETTLE_PR, CONTRACTS, OPEN_INT, CHG_IN_OI, TIMESTAMP

### 7. Risk Engine (Planned / In Progress)
- Position upload and join to EOD market snapshots
- MTM and daily P&L computation
- Implied volatility surface construction
- Options Greeks (Delta, Gamma, Vega, Theta, Rho)
- Scenario stress tests
- Historical VaR and CVaR

---

## Data Architecture

| Layer | Purpose | Format |
|-------|---------|--------|
| **Ingest (Raw)** | Immutable source data, no transforms | Parquet (partitioned by year) |
| **Processed** | Cleaned, normalized schema, dtype-optimized | Parquet |
| **Curated** | Feature-engineered: IV, Greeks, DTE, moneyness, risk buckets, regime labels | Parquet |

---

## Technology Stack

| Component | Choice |
|-----------|--------|
| Storage | Apache Parquet |
| Query Engine | DuckDB |
| Processing | Python (pandas, batch jobs) |
| Data Sources | NSE archives, TradingView data feed |
| Scheduling | CLI scripts + cron |

---

## Current Data Coverage

| Symbol | Rows |
|--------|------|
| NIFTY | 3,750,005 |
| BANKNIFTY | 2,639,914 |
| FINNIFTY | 1,405,555 |
| MIDCPNIFTY | 1,153,530 |
| **Total** | **~8.95M** |
