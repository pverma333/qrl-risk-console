"""Bug 8: Incremental builders cannot backfill an older date.

All six processed builders use `trade_date > latest_in_output` to decide what
to process.  After a 3 Nov failure followed by a 4 Nov success:
  - latest_in_output = 4 Nov
  - Re-fetched 3 Nov data arrives in ingest
  - `df[df["trade_date"] > Nov4]` silently drops Nov3 — it never reaches processed

Expected fix: replace the `> latest` filter with `~isin(existing_dates)` where
`existing_dates` is the full set of trade_dates already in the processed output.

Tests 1, 2, 5 fail today (the backfill dates are silently dropped).
Tests 3, 4 already pass (they test forward-only and no-op cases that work today).
"""
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from src.data.processed_derivatives_builder import ProcessedDerivativesBuilder
from src.data.processed_vix_builder import ProcessedVIXBuilder
from src.core.fetch_config import FetchConfig

_NOV3 = date(2026, 11, 3)
_NOV4 = date(2026, 11, 4)
_NOV5 = date(2026, 11, 5)
_EXPIRY = date(2026, 11, 27)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _deriv_ingest_row(trade_date: date, instrument: str = "OPTIDX") -> dict:
    return {
        "TIMESTAMP":  trade_date,
        "SYMBOL":     "NIFTY",
        "EXPIRY_DT":  _EXPIRY,
        "STRIKE_PR":  21000.0,
        "OPTION_TYP": "CE" if instrument == "OPTIDX" else "-",
        "INSTRUMENT": instrument,
        "OPEN":       100.0, "HIGH": 110.0, "LOW": 90.0,
        "CLOSE":      105.0, "SETTLE_PR": 105.0,
        "CONTRACTS":  10, "OPEN_INT": 100, "CHG_IN_OI": 5,
    }


def _write_ingest_deriv(path: Path, dates: list[date]):
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    for d in dates:
        rows.append(_deriv_ingest_row(d, "OPTIDX"))
        rows.append(_deriv_ingest_row(d, "FUTIDX"))
    pd.DataFrame(rows).to_parquet(path, index=False)


def _write_processed_deriv_options(path: Path, dates: list[date]):
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = [{
        "trade_date": d, "symbol": "NIFTY", "expiry_date": _EXPIRY,
        "strike": 21000.0, "option_type": "CE",
        "open": 100.0, "high": 110.0, "low": 90.0,
        "close": 105.0, "settle": 105.0,
        "contracts": 10, "open_interest": 100, "chg_in_oi": 5, "dte": 10,
    } for d in dates]
    pd.DataFrame(rows).to_parquet(path, index=False)


def _write_processed_deriv_futures(path: Path, dates: list[date]):
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = [{
        "trade_date": d, "symbol": "NIFTY", "expiry_date": _EXPIRY,
        "open": 100.0, "high": 110.0, "low": 90.0,
        "close": 105.0, "settle": 105.0,
        "contracts": 10, "open_interest": 100, "chg_in_oi": 5, "dte": 10,
    } for d in dates]
    pd.DataFrame(rows).to_parquet(path, index=False)


def _read_dates_from_parquet(path: Path) -> set[date]:
    if not path.exists():
        return set()
    df = pd.read_parquet(path, columns=["trade_date"])
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    return set(df["trade_date"])


# ---------------------------------------------------------------------------
# 1. Derivatives builder — backfill scenario (FAILS with old > latest code)
# ---------------------------------------------------------------------------

class TestDerivativesBuilderBackfill:

    def test_backfill_date_added_when_missing_from_processed(self, tmp_path):
        """Nov3 was never processed; it arrives in ingest after Nov4 succeeded.
        Incremental build must add Nov3 to processed output."""
        config = FetchConfig(base_dir=tmp_path, use_year_partition=True)
        ingest = tmp_path / "data/ingest/derivatives/2026/Derivatives_2026.parquet"
        opts   = tmp_path / "data/processed/options/2026/processed_options_2026.parquet"
        futs   = tmp_path / "data/processed/futures/2026/processed_futures_2026.parquet"

        _write_ingest_deriv(ingest, [_NOV3, _NOV4])      # both dates now in ingest
        _write_processed_deriv_options(opts, [_NOV4])     # only Nov4 in processed
        _write_processed_deriv_futures(futs, [_NOV4])

        ProcessedDerivativesBuilder(config).run("incremental")

        opts_dates = _read_dates_from_parquet(opts)
        futs_dates = _read_dates_from_parquet(futs)
        assert _NOV3 in opts_dates, "Nov3 missing from processed/options after backfill"
        assert _NOV3 in futs_dates, "Nov3 missing from processed/futures after backfill"

    def test_forward_date_still_processed_normally(self, tmp_path):
        """Normal case: new date Nov5 arrives; it must be processed."""
        config = FetchConfig(base_dir=tmp_path, use_year_partition=True)
        ingest = tmp_path / "data/ingest/derivatives/2026/Derivatives_2026.parquet"
        opts   = tmp_path / "data/processed/options/2026/processed_options_2026.parquet"
        futs   = tmp_path / "data/processed/futures/2026/processed_futures_2026.parquet"

        _write_ingest_deriv(ingest, [_NOV3, _NOV4, _NOV5])
        _write_processed_deriv_options(opts, [_NOV3, _NOV4])
        _write_processed_deriv_futures(futs, [_NOV3, _NOV4])

        ProcessedDerivativesBuilder(config).run("incremental")

        assert _NOV5 in _read_dates_from_parquet(opts)
        assert _NOV5 in _read_dates_from_parquet(futs)

    def test_already_present_dates_not_duplicated(self, tmp_path):
        """Dates already in processed must not be duplicated on incremental re-run."""
        config = FetchConfig(base_dir=tmp_path, use_year_partition=True)
        ingest = tmp_path / "data/ingest/derivatives/2026/Derivatives_2026.parquet"
        opts   = tmp_path / "data/processed/options/2026/processed_options_2026.parquet"
        futs   = tmp_path / "data/processed/futures/2026/processed_futures_2026.parquet"

        _write_ingest_deriv(ingest, [_NOV3, _NOV4])
        _write_processed_deriv_options(opts, [_NOV3, _NOV4])
        _write_processed_deriv_futures(futs, [_NOV3, _NOV4])

        before_opts = len(pd.read_parquet(opts))
        before_futs = len(pd.read_parquet(futs))

        ProcessedDerivativesBuilder(config).run("incremental")

        assert len(pd.read_parquet(opts)) == before_opts
        assert len(pd.read_parquet(futs)) == before_futs

    def test_bug_demonstration_latest_filter_drops_backfill(self, tmp_path):
        """Documents the bug: _get_latest_trade_date returns Nov4, so a
        naive `> Nov4` filter silently drops the backfilled Nov3 row."""
        config = FetchConfig(base_dir=tmp_path, use_year_partition=True)
        opts = tmp_path / "data/processed/options/2026/processed_options_2026.parquet"
        _write_processed_deriv_options(opts, [_NOV4])

        b = ProcessedDerivativesBuilder(config)
        latest = b._get_latest_trade_date(2026, "options")
        assert latest == _NOV4   # confirms what the old code would use as the cutoff

        # With the old filter, Nov3 < Nov4 — it would be dropped
        ingest_dates = pd.Series([_NOV3, _NOV4])
        old_filter_result = ingest_dates[ingest_dates > latest]
        assert _NOV3 not in old_filter_result.values, (
            "Confirms the bug: > latest drops the backfill date"
        )


# ---------------------------------------------------------------------------
# 2. VIX builder — backfill scenario (FAILS with old > latest code)
# ---------------------------------------------------------------------------

def _write_ingest_vix(path: Path, dates: list[date]):
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = [{"Date": d, "VIX_Close": 15.0 + i} for i, d in enumerate(dates)]
    pd.DataFrame(rows).to_parquet(path, index=False)


def _write_processed_vix(path: Path, dates: list[date]):
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = [{"trade_date": d, "close": 15.0 + i} for i, d in enumerate(dates)]
    pd.DataFrame(rows).to_parquet(path, index=False)


class TestVIXBuilderBackfill:

    def test_vix_backfill_date_processed_when_missing(self, tmp_path):
        """Nov3 missing from VIX processed output; re-fetched ingest has it.
        Incremental must include it."""
        config = FetchConfig(base_dir=tmp_path, use_year_partition=True)
        ingest = tmp_path / "data/ingest/vix/India_VIX_Historical.parquet"
        out    = tmp_path / "data/processed/vix/2026/processed_vix_2026.parquet"

        _write_ingest_vix(ingest, [_NOV3, _NOV4])
        _write_processed_vix(out, [_NOV4])

        ProcessedVIXBuilder(config).run("incremental")

        dates = _read_dates_from_parquet(out)
        assert _NOV3 in dates, "Nov3 missing from processed/vix after backfill"
        assert _NOV4 in dates
