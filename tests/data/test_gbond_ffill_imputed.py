"""Option B: G-bond forward-fill and imputed-aware curated builder.

ProcessedGBondBuilder:
  - Missing trading-day data is forward-filled from previous day per tenor
  - rate_imputed=True for ffill'd rows, False for real rows

CuratedOptionChainBuilder:
  - rate_imputed column propagated from G-bond join
  - Incremental skips dates with rate_imputed=False (confirmed real data)
  - Incremental re-processes dates with rate_imputed=True when processed G-bond
    now has rate_imputed=False for those dates

Failing before fix
------------------
TestProcessedGBondFfill.test_missing_trading_day_gets_ffilled
  → no ffill logic: Nov3 absent from output → len(df_nov3) == 0, not 3
TestProcessedGBondFfill.test_present_data_not_imputed
  → no rate_imputed column → KeyError
TestCuratedImputedAware.test_reprocesses_when_real_gbond_arrives
  → no imputed-aware logic: Nov3 stays rate_imputed=True → assertion fails
TestCuratedImputedAware.test_stable_when_gbond_still_imputed
  → passes before fix (expected stable behaviour; added as regression guard)
TestCuratedImputedAware.test_new_date_marked_not_imputed_when_gbond_real
  → no rate_imputed in SQL: column absent in output → KeyError
"""
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from src.core.fetch_config import FetchConfig
from src.data.processed_gbond_builder import ProcessedGBondBuilder
from src.data.curated_option_chain_builder import CuratedOptionChainBuilder

_NOV2  = date(2026, 11, 2)
_NOV3  = date(2026, 11, 3)
_NOV4  = date(2026, 11, 4)
_EXPIRY = date(2026, 11, 27)

TENORS = ("3m", "6m", "1y")
NOV2_YIELDS = {"3m": 6.50, "6m": 6.80, "1y": 7.10}
NOV4_YIELDS = {"3m": 6.60, "6m": 6.90, "1y": 7.20}

# ---------------------------------------------------------------------------
# shared helpers
# ---------------------------------------------------------------------------

def _w(path: Path, rows: list[dict]):
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(path, index=False)


def _gbond_ingest_rows(d: date, yields: dict) -> list[dict]:
    return [
        {"date": d, "price": yields[t], "open": yields[t],
         "high": yields[t], "low": yields[t], "change %": 0.0, "tenor": t}
        for t in TENORS
    ]


def _processed_gbond_rows(d: date, yields: dict, rate_imputed: bool) -> list[dict]:
    return [
        {"trade_date": d, "tenor": t, "yield_pct": yields[t],
         "open": yields[t], "high": yields[t], "low": yields[t],
         "change_pct": 0.0, "rate_imputed": rate_imputed}
        for t in TENORS
    ]


def _trade_calendar(path: Path, dates: list[date]):
    _w(path, [{"trade_date": d} for d in dates])


def _setup_full_processed(config: FetchConfig, dates: list[date],
                          gbond_rate_imputed_map: dict = None):
    """Write all processed sources required by CuratedOptionChainBuilder."""
    if gbond_rate_imputed_map is None:
        gbond_rate_imputed_map = {d: False for d in dates}

    opt_path   = config.processed_dir / "options"     / "2026" / "processed_options_2026.parquet"
    spot_path  = config.processed_dir / "index_spot"  / "2026" / "processed_index_spot_2026.parquet"
    yld_path   = config.processed_dir / "index_yield" / "2026" / "processed_index_yield_2026.parquet"
    gbond_path = config.processed_dir / "gbond"       / "2026" / "processed_gbond_2026.parquet"
    tc_path    = config.processed_dir / "trade_calendar" / "trade_calendar.parquet"

    _w(opt_path, [
        dict(trade_date=d, symbol="NIFTY", expiry_date=_EXPIRY,
             strike=20000.0, option_type="CE",
             open=150.0, high=160.0, low=140.0, close=150.0, settle=150.0,
             contracts=10, open_interest=100, chg_in_oi=0, dte=24)
        for d in dates
    ])
    _w(spot_path, [dict(trade_date=d, symbol="NIFTY", close=20200.0) for d in dates])
    _w(yld_path,  [dict(trade_date=d, symbol="NIFTY", div_yield=1.2)  for d in dates])
    _w(tc_path,   [{"trade_date": d} for d in dates])

    gbond_rows = []
    for d in dates:
        gbond_rows.extend(
            _processed_gbond_rows(d, NOV2_YIELDS, gbond_rate_imputed_map.get(d, False))
        )
    _w(gbond_path, gbond_rows)


def _curated_rows(dates: list[date], rate_imputed: bool = False) -> list[dict]:
    return [
        dict(trade_date=d, symbol="NIFTY", expiry_date=_EXPIRY,
             strike=20000.0, option_type="CE",
             open=150.0, high=160.0, low=140.0, close=150.0, settle=150.0,
             contracts=10, open_interest=100, chg_in_oi=0, dte=24,
             spot=20200.0, div_yield=1.2, rate=0.065,
             iv=0.15, delta=0.5, gamma=0.001, vega=10.0, theta=-1.5, rho=0.5,
             rate_imputed=rate_imputed)
        for d in dates
    ]


# ---------------------------------------------------------------------------
# 1. ProcessedGBondBuilder — ffill
# ---------------------------------------------------------------------------

class TestProcessedGBondFfill:

    def test_missing_trading_day_gets_ffilled(self, tmp_path):
        """Nov3 is a trading day but absent from ingest.
        Builder must ffill Nov3 from Nov2 and mark rate_imputed=True."""
        config = FetchConfig(base_dir=tmp_path, use_year_partition=True)

        ingest_path = config.ingest_dir / "gbond" / "gbond_combined.parquet"
        _w(ingest_path, _gbond_ingest_rows(_NOV2, NOV2_YIELDS))

        tc_path = config.processed_dir / "trade_calendar" / "trade_calendar.parquet"
        _trade_calendar(tc_path, [_NOV2, _NOV3])

        ProcessedGBondBuilder(config).run("full")

        path = config.processed_dir / "gbond" / "2026" / "processed_gbond_2026.parquet"
        df = pd.read_parquet(path)
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
        df_nov3 = df[df["trade_date"] == _NOV3]

        assert len(df_nov3) == 3, "Nov3 must have one row per tenor (ffill'd)"
        assert df_nov3["rate_imputed"].all(), "Nov3 must be marked rate_imputed=True"
        for tenor in TENORS:
            row = df_nov3[df_nov3["tenor"] == tenor].iloc[0]
            assert row["yield_pct"] == NOV2_YIELDS[tenor], \
                f"Nov3 {tenor} yield must carry forward from Nov2"

    def test_present_data_not_imputed(self, tmp_path):
        """Both Nov2 and Nov3 in ingest → all rows must have rate_imputed=False."""
        config = FetchConfig(base_dir=tmp_path, use_year_partition=True)

        ingest_path = config.ingest_dir / "gbond" / "gbond_combined.parquet"
        _w(ingest_path,
           _gbond_ingest_rows(_NOV2, NOV2_YIELDS) + _gbond_ingest_rows(_NOV3, NOV4_YIELDS))

        tc_path = config.processed_dir / "trade_calendar" / "trade_calendar.parquet"
        _trade_calendar(tc_path, [_NOV2, _NOV3])

        ProcessedGBondBuilder(config).run("full")

        path = config.processed_dir / "gbond" / "2026" / "processed_gbond_2026.parquet"
        df = pd.read_parquet(path)

        assert "rate_imputed" in df.columns, "rate_imputed column must exist"
        assert not df["rate_imputed"].any(), \
            "All rows from real ingest must have rate_imputed=False"


# ---------------------------------------------------------------------------
# 2. CuratedOptionChainBuilder — imputed-aware incremental
# ---------------------------------------------------------------------------

class TestCuratedImputedAware:

    def test_reprocesses_when_real_gbond_arrives(self, tmp_path):
        """Nov3 was curated with imputed G-bond rates (rate_imputed=True).
        Processed G-bond is then corrected (rate_imputed=False for Nov3).
        Incremental run must re-process Nov3 and update rate_imputed to False."""
        config = FetchConfig(base_dir=tmp_path, use_year_partition=True)
        out = config.curated_dir / "option_chain" / "2026" / "curated_options_2026.parquet"

        _setup_full_processed(
            config, [_NOV3, _NOV4],
            gbond_rate_imputed_map={_NOV3: False, _NOV4: False},
        )
        # Pre-existing curated: Nov3 with imputed rates, Nov4 with real rates
        _w(out, _curated_rows([_NOV3], rate_imputed=True)
               + _curated_rows([_NOV4], rate_imputed=False))

        CuratedOptionChainBuilder(config).run("incremental")

        df = pd.read_parquet(out)
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
        nov3 = df[df["trade_date"] == _NOV3]

        assert len(nov3) == 1, "Exactly one Nov3 row — no duplicate"
        assert not nov3["rate_imputed"].iloc[0], \
            "Nov3 must be re-processed: rate_imputed must now be False"

    def test_stable_when_gbond_still_imputed(self, tmp_path):
        """Nov3 curated with imputed rates; G-bond in processed is still imputed.
        Incremental must NOT re-process — no changes, no duplicates."""
        config = FetchConfig(base_dir=tmp_path, use_year_partition=True)
        out = config.curated_dir / "option_chain" / "2026" / "curated_options_2026.parquet"

        _setup_full_processed(
            config, [_NOV3],
            gbond_rate_imputed_map={_NOV3: True},
        )
        _w(out, _curated_rows([_NOV3], rate_imputed=True))

        before = len(pd.read_parquet(out))
        CuratedOptionChainBuilder(config).run("incremental")
        after = len(pd.read_parquet(out))

        assert after == before, "Row count must not change — G-bond still imputed"
        df = pd.read_parquet(out)
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
        assert df[df["trade_date"] == _NOV3]["rate_imputed"].all(), \
            "Nov3 must still be rate_imputed=True"

    def test_new_date_marked_not_imputed_when_gbond_real(self, tmp_path):
        """Brand-new date with real G-bond in processed.
        Curated output for that date must carry rate_imputed=False."""
        config = FetchConfig(base_dir=tmp_path, use_year_partition=True)
        out = config.curated_dir / "option_chain" / "2026" / "curated_options_2026.parquet"

        _setup_full_processed(
            config, [_NOV3],
            gbond_rate_imputed_map={_NOV3: False},
        )

        CuratedOptionChainBuilder(config).run("incremental")

        df = pd.read_parquet(out)
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
        assert _NOV3 in set(df["trade_date"]), "Nov3 must appear in curated"
        assert "rate_imputed" in df.columns, "rate_imputed column must exist in curated"
        assert not df[df["trade_date"] == _NOV3]["rate_imputed"].any(), \
            "Nov3 must have rate_imputed=False when G-bond data is real"
