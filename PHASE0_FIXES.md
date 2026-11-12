# QRL Phase 0 — bug fixes (from the 12 Oct–11 Nov 2026 simulation)

## Status

| # | Bug | Status | Test | What changed |
|---|-----|--------|------|--------------|
| 1 | div_yield passed as percent instead of decimal | ✅ Done | `tests/quant/test_div_yield_units.py` | `portfolio._build_snapshot` and `scenario_service._build_snapshot_from_option/_from_futures` now divide `div_yield` by 100; option prices, `/scenario` P&L and VaR all match the oracle |
| 2 | Expiry-day positions show zero P&L | ✅ Done | `tests/quant/test_expiry_day.py` | `portfolio._compute_mtm_option`, `scenario_engine.scenario_option` and `price_option` split `dte <= 0` into `dte < 0` (expired → zero) and `dte == 0` (intrinsic repricing; scenario uses shocked intrinsic, vol/rate shocks ignored); seeded MTM 10,676.25 matches oracle |
| 3 | Futures MTM uses index spot, not settle | ⬜ Not started | — | — |
| 4 | Blank NSE div_yield filled with 0.0 | ⬜ Not started | — | — |
| 5 | Curated builders drop rows via INNER JOIN | ⬜ Not started | — | — |
| 6 | Fetch errors swallowed at INFO level | ⬜ Not started | — | — |
| 7 | SyncChecker blind to history holes | ⬜ Not started | — | — |
| 8 | Incremental builders can't backfill older dates | ⬜ Not started | — | — |
| 9 | `jugaad-data` missing from requirements.txt | ⬜ Not started | — | — |
| 10 | Mixed-symbol VaR (needs decision) | ⬜ Not started | — | — |

Context for Claude Code: a month of NSE-style data (synthetic, deterministic) was replayed through this repo's
real daily pipeline and every output was compared with an independent oracle (own Black-Scholes / IV / Greeks /
rate interpolation / VaR). Spot, DTE, rate, IV and Greeks all matched. The items below did not.

Working rules
- One bug at a time. Write the failing test FIRST, show it failing, then fix, then run the full suite (257 tests at baseline).
- Ask before each edit. Do not touch data/ or Render/R2 config. Do not commit; I review diffs.
- After items 1-3 and again after 4-9, re-run the simulation (see "Verification" at the bottom).

## Part A — wrong money numbers (do these first)

### 1. div_yield passed as percent instead of decimal  [CRITICAL]
- Where: `src/quant/portfolio.py::_build_snapshot`, `app/services/scenario_service.py::_build_snapshot_from_option` and `_build_snapshot_from_futures`.
  `src/quant/bs_vectorized.py::compute_batch` correctly uses `div_yield/100`; the repricing paths use the raw curated percent (1.30 -> q = 130%).
- Evidence (11 Nov 2026, synthetic): NIFTY 30000 CE current_price 21.60, correct 360.25. NIFTY 29800 PE 1,115.98 vs 232.05.
  POST /scenario/ -1% spot shock: total_pnl -823.68, correct -8,906.86. VaR_95 72,097 vs correct 67,539 (+6.7%).
  Minimal repro: `python -I ~/qrl-sim/check_units.py <repo_root>`.
- Expected: repricing a contract at its curated iv with zero shock returns its settle price (within one 0.05 tick-rounding effect).
- Test: for several curated rows, `_compute_mtm_option(snapshot, ...)` price ~= curated settle (tolerance ~0.5% of price), AND scenario_pnl == 0 at zero shock.
- Fix: convert once, at snapshot construction (`div_yield / 100.0`). Do not change the curated parquet schema.

### 2. Expiry-day positions show zero P&L
- Where: `portfolio.py::_compute_mtm_option` (`dte <= 0` returns None, 0.0); `scenario_engine.py::price_option` / `scenario_option` (`dte <= 0` -> method "expired", all zeros).
- Evidence: NIFTY weekly call expiring 9 Nov, entry 137.45, intrinsic 301.70, lot 65 -> true MTM +10,676.25; API returns 0.0, method "expired".
- Expected: at dte==0, price = intrinsic (max(S-K,0) for CE, max(K-S,0) for PE); MTM = (intrinsic - entry) * qty * lot; scenario P&L uses intrinsic under the shocked spot.
- Note: negative dte is a different case (contract already expired/gone) — keep it explicit, not silently zero.

### 3. Futures MTM uses index spot, not the futures settle
- Where: `portfolio.py::_compute_mtm_futures` (`snapshot.spot - entry_price`). `_query_curated_futures` in the services already selects `settle`; `run_portfolio` ignores it.
- Evidence: NIFTY future, spot 30,001.26 vs settle 30,045.45; MTM off by 2,872 on one 65-lot.
- Expected: mark to the futures settle. Scenario P&L stays delta-1 on the spot move.

## Part B — silent data loss in the daily pipeline

### 4. Blank NSE div_yield is filled with 0.0
- Where: `src/data/processed_index_yield_builder.py` (~line 77, `fillna(0.0)`). On 5 Nov 2026 (BANKNIFTY blank) 848 option rows were priced with q=0, shifting IV by up to 2.4 vol points.
- Expected: forward-fill from the last valid value for that symbol, flag the row (e.g. a `div_yield_imputed` column or a log line naming symbol/date), never 0.0. Decide with me whether it should instead fail the build.

### 5. Curated builders drop rows through INNER JOINs
- Where: `src/data/curated_option_chain_builder.py` and `curated_futures_builder.py` (JOIN spot, yield, 3 gbond tenors).
- Evidence: 27 Oct (one G-bond tenor missing) lost ALL option/futures rows; 29 Oct (Midcap missing in yield CSV) lost all MIDCPNIFTY rows. Total 8,632 of 81,706 option rows (10.6%) lost in the month.
- Expected: build produces a rejection report (date, symbol, missing input) and the run exits non-zero or warns loudly. Consider carrying forward the previous tenor yield for a missing G-bond row (log it).

### 6. Fetch errors are swallowed at INFO level
- Where: `src/data/master_derivatives_fetch.py` ~line 76-77 (`except Exception: logger.info("Error on ...")`). A 3 Nov HTTP 500 made the pipeline exit OK.
- Expected: log at ERROR, collect failed dates, and make `scripts/run_daily_fetch.py` return a non-zero exit code / skip the upload+deploy steps in `scripts/daily_sync.sh` when a trading day failed.
- Also check `master_index_yield_fetch.py::fetch_snapshot` (returns None on retries exhausted) for the same pattern.

### 7. SyncChecker is blind to holes in history
- Where: `src/data/sync_checker.py::_compute_expected_date` (max across datasets). After the 3 Nov failure it later reported "All layers in sync".
- Expected: compare each dataset's trade dates against the trade calendar for a window (e.g. the last 30 trading days) and report every missing date per layer.

### 8. Incremental builders cannot backfill an older date
- Where: `processed_derivatives_builder.py` (+ other processed/curated builders) use `trade_date > latest`. Re-fetching 3 Nov reached ingest only.
- Expected: incremental mode processes "dates in ingest that are missing downstream", not "dates after latest".

### 9. `jugaad-data` missing from requirements.txt
- `src/data/master_derivatives_fetch.py` imports `jugaad_data`; a clean `pip install -r requirements.txt` fails on import. Add the pinned dependency.

## Part C — needs a decision from me (do not implement without asking)

### 10. Mixed-symbol VaR
- `src/quant/var.py::compute_var` applies ONE symbol's historical return to every position in the file. For a NIFTY+BANKNIFTY book, 95% VaR was 67.5k (single-return) vs 66.1k (each leg on its own index) on synthetic data; real-world gap depends on correlation.
- Options: (a) keep, document and reject multi-symbol files; (b) per-symbol returns on the same historical date (recommended); (c) beta-map to the chosen symbol.

## Verification
The simulation scripts live OUTSIDE the repo, in `~/qrl-sim/` (sim.py, compare.py, truth.py, check_units.py). They never write into the repo or its data/ folder; all output goes to `~/qrl-sim/`.
Use the repo's own virtualenv Python (it has pandas, numpy, scipy, pyarrow, duckdb, fastapi) and `pip install httpx` once if missing.
1. `python -I ~/qrl-sim/sim.py <repo_root> ~/qrl-sim` — replays 12 Oct–11 Nov through the real pipeline with the network mocked (about 40 s). Writes `sim_base/`, `asrun_base/`, `sim_report.json`.
2. `python -I ~/qrl-sim/compare.py <repo_root> ~/qrl-sim` — compares repo outputs with the independent oracle (several minutes). Writes `compare_report.json`.
Run both BEFORE any fix (baseline) and after the fixes.
Targets after the fixes: Part A rows in the compare output show repo price == oracle price, repo scenario P&L == oracle, expiry-day MTM == +10,676.25 (seeded case), futures MTM diff == 0; Part B: row counts "expected/got" equal (or every rejection reported), and the 3 Nov repair reaches the curated layer.
