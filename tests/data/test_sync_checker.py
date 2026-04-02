import pytest
import pandas as pd
from datetime import date
from pathlib import Path
from unittest.mock import patch
from src.data.sync_checker import SyncChecker, INGEST_DATE_COLUMNS
from src.core.fetch_config import FetchConfig

@pytest.fixture
def config(tmp_path):
    return FetchConfig(base_dir=tmp_path, use_year_partition=True)


@pytest.fixture
def checker(config):
    return SyncChecker(config)


def write_parquet(path: Path, col_name: str, dates: list[date]):
    """Writes a minimal parquet with the specified date column."""
    path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame({col_name: dates})
    df.to_parquet(path, index=False)

class TestIngestDateColumnsMap:

    def test_all_expected_keys_present(self):
        expected = {
            "derivatives", "index_spot", "vix",
            "index_yield", "gbond", "trade_calendar"
        }
        assert expected == set(INGEST_DATE_COLUMNS.keys())

    def test_derivatives_uses_timestamp(self):
        assert INGEST_DATE_COLUMNS["derivatives"] == "TIMESTAMP"

    def test_index_spot_uses_date(self):
        assert INGEST_DATE_COLUMNS["index_spot"] == "Date"

    def test_vix_uses_date(self):
        assert INGEST_DATE_COLUMNS["vix"] == "Date"

    def test_index_yield_uses_uppercase_date(self):
        assert INGEST_DATE_COLUMNS["index_yield"] == "DATE"

    def test_gbond_uses_lowercase_date(self):
        assert INGEST_DATE_COLUMNS["gbond"] == "date"

    def test_trade_calendar_uses_trade_date(self):
        assert INGEST_DATE_COLUMNS["trade_calendar"] == "trade_date"

class TestGetLatestDateFromParquet:

    def test_returns_max_date_trade_date_col(self, checker, tmp_path):
        path = tmp_path / "test.parquet"
        write_parquet(path, "trade_date", [date(2024, 1, 1), date(2024, 3, 13)])
        result = checker._get_latest_date_from_parquet(path, date_col="trade_date")
        assert result == date(2024, 3, 13)

    def test_returns_max_date_raw_timestamp_col(self, checker, tmp_path):
        path = tmp_path / "raw.parquet"
        write_parquet(path, "TIMESTAMP", [date(2024, 1, 1), date(2024, 3, 13)])
        result = checker._get_latest_date_from_parquet(path, date_col="TIMESTAMP")
        assert result == date(2024, 3, 13)

    def test_returns_max_date_raw_date_col(self, checker, tmp_path):
        path = tmp_path / "raw.parquet"
        write_parquet(path, "Date", [date(2024, 1, 1), date(2024, 3, 13)])
        result = checker._get_latest_date_from_parquet(path, date_col="Date")
        assert result == date(2024, 3, 13)

    def test_returns_none_if_file_missing(self, checker, tmp_path):
        result = checker._get_latest_date_from_parquet(
            tmp_path / "ghost.parquet", date_col="trade_date"
        )
        assert result is None

    def test_returns_none_if_empty_file(self, checker, tmp_path):
        path = tmp_path / "empty.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame({"trade_date": []}).to_parquet(path, index=False)
        result = checker._get_latest_date_from_parquet(path, date_col="trade_date")
        assert result is None

    def test_returns_none_if_column_absent(self, checker, tmp_path):
        path = tmp_path / "no_col.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame({"symbol": ["NIFTY"]}).to_parquet(path, index=False)
        result = checker._get_latest_date_from_parquet(path, date_col="trade_date")
        assert result is None

    def test_handles_string_dates(self, checker, tmp_path):
        path = tmp_path / "str_dates.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame({"trade_date": ["2024-01-01", "2024-03-13"]}).to_parquet(
            path, index=False
        )
        result = checker._get_latest_date_from_parquet(path, date_col="trade_date")
        assert result == date(2024, 3, 13)

class TestGetLatestDateFromDir:

    def test_returns_max_across_multiple_files(self, checker, tmp_path):
        write_parquet(tmp_path / "2023" / "f1.parquet", "trade_date", [date(2023, 12, 29)])
        write_parquet(tmp_path / "2024" / "f2.parquet", "trade_date", [date(2024, 3, 13)])
        result = checker._get_latest_date_from_dir(tmp_path, date_col="trade_date")
        assert result == date(2024, 3, 13)

    def test_uses_correct_raw_col_for_ingest(self, checker, tmp_path):
        write_parquet(
            tmp_path / "2024" / "Derivatives_2024.parquet",
            "TIMESTAMP",
            [date(2024, 3, 13)],
        )
        result = checker._get_latest_date_from_dir(tmp_path, date_col="TIMESTAMP")
        assert result == date(2024, 3, 13)

    def test_returns_none_if_no_files(self, checker, tmp_path):
        empty = tmp_path / "empty"
        empty.mkdir()
        result = checker._get_latest_date_from_dir(empty, date_col="trade_date")
        assert result is None

    def test_returns_none_if_dir_missing(self, checker, tmp_path):
        result = checker._get_latest_date_from_dir(
            tmp_path / "ghost", date_col="trade_date"
        )
        assert result is None

class TestComputeExpectedDate:

    def test_returns_max_of_all_dates(self, checker):
        all_dates = {
            "ingest/derivatives":   date(2024, 3, 13),
            "processed/options":    date(2024, 3, 12),
            "curated/option_chain": date(2024, 3, 11),
        }
        assert checker._compute_expected_date(all_dates) == date(2024, 3, 13)

    def test_ignores_none_values(self, checker):
        all_dates = {
            "ingest/derivatives": date(2024, 3, 13),
            "processed/options":  None,
        }
        assert checker._compute_expected_date(all_dates) == date(2024, 3, 13)

    def test_returns_none_if_all_none(self, checker):
        all_dates = {"ingest/derivatives": None, "processed/options": None}
        assert checker._compute_expected_date(all_dates) is None

class TestFilterByWindow:

    def test_excludes_dates_before_from_date(self, checker):
        all_dates = {
            "ingest/derivatives": date(2023, 6, 1),
            "processed/options":  date(2024, 3, 13),
        }
        filtered = checker._filter_by_window(
            all_dates, from_date=date(2024, 1, 1), to_date=None
        )
        assert filtered["ingest/derivatives"] is None
        assert filtered["processed/options"] == date(2024, 3, 13)

    def test_passes_all_when_no_window(self, checker):
        all_dates = {
            "ingest/derivatives": date(2023, 6, 1),
            "processed/options":  date(2024, 3, 13),
        }
        filtered = checker._filter_by_window(all_dates, from_date=None, to_date=None)
        assert filtered == all_dates

    def test_none_values_pass_through(self, checker):
        all_dates = {"ingest/derivatives": None}
        filtered = checker._filter_by_window(
            all_dates, from_date=date(2024, 1, 1), to_date=None
        )
        assert filtered["ingest/derivatives"] is None

class TestLogSyncReport:

    def test_all_in_sync_logs_single_ok_line(self, checker, caplog):
        all_dates = {
            "ingest/derivatives": date(2024, 3, 13),
            "processed/options":  date(2024, 3, 13),
        }
        with caplog.at_level("INFO", logger="SyncChecker"):
            checker._log_sync_report(all_dates, expected=date(2024, 3, 13))

        assert any("All layers in sync" in r.message for r in caplog.records)
        assert not any("OUT OF SYNC" in r.message for r in caplog.records)

    def test_out_of_sync_logged_with_both_dates(self, checker, caplog):
        all_dates = {
            "ingest/index_spot": date(2024, 3, 12),
            "processed/options": date(2024, 3, 13),
        }
        with caplog.at_level("WARNING", logger="SyncChecker"):
            checker._log_sync_report(all_dates, expected=date(2024, 3, 13))

        records = [r for r in caplog.records if "OUT OF SYNC" in r.message]
        assert len(records) == 1
        assert "ingest/index_spot" in records[0].message
        assert "2024-03-12" in records[0].message

    def test_missing_dataset_shows_no_data(self, checker, caplog):
        all_dates = {
            "ingest/index_spot": None,
            "processed/options": date(2024, 3, 13),
        }
        with caplog.at_level("WARNING", logger="SyncChecker"):
            checker._log_sync_report(all_dates, expected=date(2024, 3, 13))

        records = [r for r in caplog.records if "NO DATA" in r.message]
        assert len(records) == 1

    def test_no_expected_date_logs_warning(self, checker, caplog):
        with caplog.at_level("WARNING", logger="SyncChecker"):
            checker._log_sync_report({}, expected=None)

        assert any("No valid trade dates" in r.message for r in caplog.records)

    def test_summary_includes_count_and_names(self, checker, caplog):
        all_dates = {
            "ingest/index_spot": date(2024, 3, 12),
            "ingest/vix":        date(2024, 3, 11),
            "processed/options": date(2024, 3, 13),
        }
        with caplog.at_level("WARNING", logger="SyncChecker"):
            checker._log_sync_report(all_dates, expected=date(2024, 3, 13))

        summary = [r for r in caplog.records if "Sync check complete" in r.message]
        assert len(summary) == 1
        assert "2" in summary[0].message
        assert "ingest/index_spot" in summary[0].message
        assert "ingest/vix" in summary[0].message

class TestDiscoverCuratedDates:

    def test_auto_discovers_subdirectories(self, checker, config):
        write_parquet(
            config.curated_dir / "option_chain" / "2024" / "curated_2024.parquet",
            "trade_date",
            [date(2024, 3, 13)],
        )
        write_parquet(
            config.curated_dir / "futures_chain" / "2024" / "curated_2024.parquet",
            "trade_date",
            [date(2024, 3, 12)],
        )
        result = checker._discover_curated_dates()
        assert "curated/option_chain" in result
        assert "curated/futures_chain" in result
        assert result["curated/option_chain"] == date(2024, 3, 13)
        assert result["curated/futures_chain"] == date(2024, 3, 12)

    def test_returns_empty_dict_if_curated_dir_missing(self, checker, config):
        import shutil
        if config.curated_dir.exists():
            shutil.rmtree(config.curated_dir)
        result = checker._discover_curated_dates()
        assert result == {}

class TestRunRouting:

    def test_run_daily_calls_run_daily_check(self, checker):
        with patch.object(checker, "run_daily_check") as mock:
            checker.run(mode="daily")
            mock.assert_called_once()

    def test_run_full_calls_run_full_check_with_dates(self, checker):
        with patch.object(checker, "run_full_check") as mock:
            checker.run(
                mode="full",
                from_date=date(2024, 1, 1),
                to_date=date(2024, 12, 31),
            )
            mock.assert_called_once_with(date(2024, 1, 1), date(2024, 12, 31))

    def test_run_full_calls_run_full_check_no_dates(self, checker):
        with patch.object(checker, "run_full_check") as mock:
            checker.run(mode="full")
            mock.assert_called_once_with(None, None)

    def test_invalid_mode_raises(self, checker):
        with pytest.raises(ValueError):
            checker.run(mode="live")
