"""Bug 7: SyncChecker is blind to history holes.

_compute_expected_date returns max(latest_dates). After a 3 Nov failure followed
by a 4 Nov success, every dataset's latest date is 4 Nov, expected = 4 Nov, so
_log_sync_report prints "All layers in sync" — the 3 Nov hole is invisible.

Expected fix:
  - Add _find_holes(dataset_dates: set[date], calendar_dates: set[date]) -> list[date]
    that returns sorted calendar dates absent from the dataset.
  - In run_daily_check, also load trade calendar dates for a window, compare each
    dataset's full date set against it, and log WARNING for every missing date.

Tests 1-5 and 7-8 fail today with AttributeError (_find_holes doesn't exist).
Test 6 passes today — it demonstrates that the existing "latest date" check
silently reports "All layers in sync" even when a hole exists (the bug itself).
"""
import logging
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from src.data.sync_checker import SyncChecker
from src.core.fetch_config import FetchConfig


# ---------------------------------------------------------------------------
# fixtures (mirror the existing test_sync_checker.py style)
# ---------------------------------------------------------------------------

@pytest.fixture
def config(tmp_path):
    return FetchConfig(base_dir=tmp_path, use_year_partition=True)


@pytest.fixture
def checker(config):
    return SyncChecker(config)


def _write(path: Path, col: str, dates: list[date]):
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({col: dates}).to_parquet(path, index=False)


_NOV3 = date(2026, 11, 3)
_NOV4 = date(2026, 11, 4)
_NOV5 = date(2026, 11, 5)


# ---------------------------------------------------------------------------
# 1. _find_holes — pure logic, no I/O
# ---------------------------------------------------------------------------

class TestFindHoles:

    def test_missing_date_in_dataset_returned(self, checker):
        calendar = {_NOV3, _NOV4}
        dataset  = {_NOV4}
        holes = checker._find_holes(dataset, calendar)
        assert _NOV3 in holes

    def test_no_holes_when_dataset_complete(self, checker):
        calendar = {_NOV3, _NOV4}
        dataset  = {_NOV3, _NOV4}
        assert checker._find_holes(dataset, calendar) == []

    def test_all_missing_when_dataset_empty(self, checker):
        calendar = {_NOV3, _NOV4}
        holes = checker._find_holes(set(), calendar)
        assert sorted(holes) == [_NOV3, _NOV4]

    def test_multiple_holes_returned_sorted(self, checker):
        calendar = {_NOV3, _NOV4, _NOV5}
        dataset  = {_NOV4}           # Nov 3 and Nov 5 missing
        holes = checker._find_holes(dataset, calendar)
        assert holes == sorted(holes)
        assert _NOV3 in holes
        assert _NOV5 in holes

    def test_dataset_dates_outside_calendar_window_ignored(self, checker):
        # Nov 1 is in the dataset but NOT in the calendar window; that's fine
        # Nov 3 is in the calendar but not in the dataset — still a hole
        nov1 = date(2026, 11, 1)
        calendar = {_NOV3, _NOV4}
        dataset  = {nov1, _NOV4}     # has Nov 1 (outside window), missing Nov 3
        holes = checker._find_holes(dataset, calendar)
        assert _NOV3 in holes
        assert nov1 not in holes


# ---------------------------------------------------------------------------
# 6. Existing "latest date" check is blind to the hole — demonstrates the bug
#    This test PASSES today (confirming the bug exists in the current code).
# ---------------------------------------------------------------------------

class TestCurrentCheckIsBlind:

    def test_latest_date_check_reports_in_sync_despite_hole(self, checker, caplog):
        # Scenario: Nov 3 failed, Nov 4 succeeded.
        # Both ingest/derivatives and processed/options show latest = Nov 4.
        # The current _compute_expected_date + _log_sync_report says "All layers in sync".
        all_dates = {
            "ingest/derivatives":   _NOV4,
            "processed/options":    _NOV4,
        }
        expected = checker._compute_expected_date(all_dates)
        assert expected == _NOV4

        with caplog.at_level(logging.INFO, logger="SyncChecker"):
            checker._log_sync_report(all_dates, expected)

        # Bug confirmed: reports "All layers in sync" even though Nov 3 is missing
        assert any("All layers in sync" in r.message for r in caplog.records)
        assert not any("OUT OF SYNC" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# 7. _find_holes detects what the latest-date check misses
# ---------------------------------------------------------------------------

class TestFindHolesDetectsBug:

    def test_nov3_hole_found_even_when_nov4_present(self, checker):
        calendar = {_NOV3, _NOV4}   # trade calendar says both days traded
        dataset  = {_NOV4}          # dataset only has Nov 4 (Nov 3 fetch failed)
        holes = checker._find_holes(dataset, calendar)
        assert _NOV3 in holes, (
            "_find_holes must surface the Nov 3 gap that the latest-date check misses"
        )


# ---------------------------------------------------------------------------
# 8. run_daily_check warns about missing dates (integration with tmp_path)
# ---------------------------------------------------------------------------

class TestRunDailyCheckHoleWarning:

    def test_missing_date_logged_as_warning(self, checker, config, caplog):
        # Write a processed trade calendar with Nov 3 and Nov 4
        _write(
            config.processed_dir / "trade_calendar" / "trade_calendar.parquet",
            "trade_date", [_NOV3, _NOV4],
        )
        # Write processed/options with ONLY Nov 4 (Nov 3 is the hole)
        _write(
            config.processed_dir / "options" / "2026" / "options_2026.parquet",
            "trade_date", [_NOV4],
        )

        with caplog.at_level(logging.WARNING, logger="SyncChecker"):
            checker.run_daily_check()

        hole_warnings = [
            r for r in caplog.records
            if r.levelno >= logging.WARNING and "2026-11-03" in r.message
        ]
        assert len(hole_warnings) >= 1, (
            "run_daily_check must log a WARNING naming the missing date 2026-11-03"
        )

    def test_no_hole_warning_when_all_dates_present(self, checker, config, caplog):
        _write(
            config.processed_dir / "trade_calendar" / "trade_calendar.parquet",
            "trade_date", [_NOV3, _NOV4],
        )
        _write(
            config.processed_dir / "options" / "2026" / "options_2026.parquet",
            "trade_date", [_NOV3, _NOV4],
        )

        with caplog.at_level(logging.WARNING, logger="SyncChecker"):
            checker.run_daily_check()

        hole_warnings = [
            r for r in caplog.records
            if r.levelno >= logging.WARNING and "HOLE" in r.message
        ]
        assert hole_warnings == [], (
            "No HOLE warnings expected when all calendar dates are present"
        )
