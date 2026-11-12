"""Bug 6: Fetch errors swallowed at INFO level.

DerivativesFetcher.run() catches exceptions and logs at INFO — the error
disappears from any ERROR-only alert. Neither fetcher returns failed dates,
so run_daily_fetch.main() cannot exit non-zero.

Expected:
  - DerivativesFetcher.run(): log at ERROR, return list of failed dates.
  - MasterIndexYieldFetcher.fetch_snapshot(): append to self._failed_dates
    when ALL retries are exhausted (not on a 404 holiday).
  - run_daily_fetch.main(): sys.exit(1) when any fetcher reports failures
    (daily_sync.sh already checks $? at line 66).

Tests fail today because:
  - DerivativesFetcher uses logger.info (not logger.error) on the exception.
  - DerivativesFetcher.run() returns None (no return statement).
  - _failed_dates attribute doesn't exist on MasterIndexYieldFetcher.
"""
import logging
from datetime import date
from unittest.mock import patch, MagicMock

import pandas as pd
import pytest

from src.data.master_derivatives_fetch import DerivativesFetcher
from src.data.master_index_yield_fetch import MasterIndexYieldFetcher


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _deriv_fetcher() -> DerivativesFetcher:
    f = DerivativesFetcher.__new__(DerivativesFetcher)
    f.logger = logging.getLogger("test_deriv_fetch")
    f.batch_size = 30
    f.rebuild = False
    f._yearly_unknown_symbols = {}
    return f


def _yield_fetcher() -> MasterIndexYieldFetcher:
    f = MasterIndexYieldFetcher.__new__(MasterIndexYieldFetcher)
    f.logger = logging.getLogger("test_yield_fetch2")
    f.max_retries = 1  # fast: single attempt before giving up
    f.HEADERS = {}
    f.CSV_URL = "https://example.invalid/{ddmmyyyy}"
    f.indices = ["Nifty 50"]
    f.CSV_TO_LEGACY_INDEX = {"Nifty 50": "NIFTY"}
    f._failed_dates = []  # will be initialised by __init__ in the fixed code
    return f


# Nov 3, 2026 is a Monday (weekday 0); Nov 4 is Tuesday — both weekdays
_D3  = date(2026, 11, 3)
_D4  = date(2026, 11, 4)


# ---------------------------------------------------------------------------
# 1. DerivativesFetcher — exception must be logged at ERROR
# ---------------------------------------------------------------------------

class TestDerivativesErrorLevel:

    def test_exception_logged_at_error(self, caplog):
        f = _deriv_fetcher()
        with patch.object(f, "_fetch_by_date", side_effect=Exception("HTTP 500")), \
             patch("time.sleep"):
            with caplog.at_level(logging.ERROR, logger="test_deriv_fetch"):
                f.run(_D3, _D3)
        assert any(r.levelno == logging.ERROR for r in caplog.records), \
            "Expected at least one ERROR-level log entry for the fetch exception"

    def test_exception_not_silenced_as_info_only(self, caplog):
        f = _deriv_fetcher()
        with patch.object(f, "_fetch_by_date", side_effect=Exception("HTTP 500")), \
             patch("time.sleep"):
            with caplog.at_level(logging.DEBUG, logger="test_deriv_fetch"):
                f.run(_D3, _D3)
        error_records = [r for r in caplog.records if r.levelno == logging.ERROR]
        assert len(error_records) >= 1, \
            "Exception must appear at ERROR level (old code used INFO)"


# ---------------------------------------------------------------------------
# 2. DerivativesFetcher — run() returns failed dates
# ---------------------------------------------------------------------------

class TestDerivativesFailedDates:

    def test_failed_date_in_returned_list(self):
        f = _deriv_fetcher()
        with patch.object(f, "_fetch_by_date", side_effect=Exception("timeout")), \
             patch("time.sleep"):
            result = f.run(_D3, _D3)
        assert result is not None, "run() must return a list (got None)"
        assert _D3 in result

    def test_no_failures_returns_empty(self):
        empty_df = pd.DataFrame(columns=DerivativesFetcher.COLS)
        f = _deriv_fetcher()
        with patch.object(f, "_fetch_by_date", return_value=empty_df), \
             patch("time.sleep"):
            result = f.run(_D3, _D3)
        assert result is not None
        assert len(result) == 0

    def test_only_failed_date_returned_when_mixed(self):
        empty_df = pd.DataFrame(columns=DerivativesFetcher.COLS)

        def _fetch(d):
            if d == _D3:
                raise Exception("HTTP 500")
            return empty_df

        f = _deriv_fetcher()
        with patch.object(f, "_fetch_by_date", side_effect=_fetch), \
             patch("time.sleep"):
            result = f.run(_D3, _D4)
        assert _D3 in result
        assert _D4 not in result


# ---------------------------------------------------------------------------
# 3. MasterIndexYieldFetcher — retries-exhausted tracked, 404 is not a failure
# ---------------------------------------------------------------------------

class TestYieldFetcherFailedDates:

    def test_retries_exhausted_tracked_in_failed_dates(self):
        f = _yield_fetcher()
        with patch("requests.get", side_effect=Exception("connection refused")), \
             patch("time.sleep"):
            result = f.fetch_snapshot(_D3)
        assert result is None  # still returns None — no data
        assert _D3 in f._failed_dates, \
            "_failed_dates must contain the date when all retries are exhausted"

    def test_holiday_404_not_in_failed_dates(self):
        f = _yield_fetcher()
        mock_resp = MagicMock()
        mock_resp.status_code = 404
        with patch("requests.get", return_value=mock_resp):
            result = f.fetch_snapshot(_D3)
        assert result is None
        assert _D3 not in f._failed_dates, \
            "A 404 (holiday/non-trading day) must NOT be recorded as a failure"
