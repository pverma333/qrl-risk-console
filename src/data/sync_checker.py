import logging
from datetime import date
from pathlib import Path
from typing import Optional

import pandas as pd

from src.core.fetch_config import FetchConfig


# Raw date column names per ingest dataset.
# Confirmed from each processor's _rename_columns method.
# Processed and curated layers all use trade_date uniformly.
INGEST_DATE_COLUMNS = {
    "derivatives":    "TIMESTAMP",   # confirmed: ProcessedDerivativesBuilder
    "index_spot":     "Date",        # confirmed: ProcessedIndexSpotBuilder
    "vix":            "Date",        # confirmed: ProcessedVIXBuilder
    "index_yield":    "DATE",        # confirmed: ProcessedIndexYieldBuilder
    "gbond":          "date",        # confirmed: ProcessedGBondBuilder
    "trade_calendar": "trade_date",  # trade calendar already uses this name
}


class SyncChecker:
    """
    Compares latest date across ingest, processed, and curated layers.
    Reports misalignment per dataset. Does not raise or abort on mismatch.

    Two modes:
        daily  — compares only the single latest date found across all datasets
        full   — compares across entire history, optionally filtered by date window
    """

    def __init__(self, config: FetchConfig):
        self.ingest_dir = config.ingest_dir
        self.processed_dir = config.processed_dir
        self.curated_dir = config.curated_dir

        self.logger = logging.getLogger("SyncChecker")
        self.logger.setLevel(logging.INFO)

        # attach file handler only if not already attached
        # basicConfig is a no-op if root logger already has handlers (other processors register first)
        if not self.logger.handlers:
            handler = logging.FileHandler(
                config.logs_dir / "data_pipeline_fetch.log"
            )
            handler.setFormatter(logging.Formatter(
                "%(asctime)s | %(name)s | %(levelname)s | %(message)s"
            ))
            self.logger.addHandler(handler)

    # ------------------------------------------------------------------
    # Internal: parquet readers
    # ------------------------------------------------------------------

    def _get_latest_date_from_parquet(
        self, path: Path, date_col: str = "trade_date"
    ) -> Optional[date]:
        # reads only the date column — cheap, no full load
        # returns None on any failure, never raises
        if not path.exists():
            self.logger.warning("File not found: %s", path)
            return None

        try:
            df = pd.read_parquet(path, columns=[date_col])
        except Exception as exc:
            self.logger.warning("Could not read %s: %s", path, exc)
            return None

        if date_col not in df.columns or df.empty:
            self.logger.warning("No %s column or empty file: %s", date_col, path)
            return None

        df[date_col] = pd.to_datetime(df[date_col]).dt.date
        return df[date_col].max()

    def _get_latest_date_from_dir(
        self, root: Path, date_col: str = "trade_date"
    ) -> Optional[date]:
        # recursively globs all parquets under root
        # passes date_col down to each file read
        if not root.exists():
            self.logger.warning("Directory not found: %s", root)
            return None

        files = sorted(root.rglob("*.parquet"))
        if not files:
            self.logger.warning("No parquet files under: %s", root)
            return None

        dates = []
        for f in files:
            d = self._get_latest_date_from_parquet(f, date_col=date_col)
            if d is not None:
                dates.append(d)

        return max(dates) if dates else None

    # ------------------------------------------------------------------
    # Discovery: ingest layer
    # ------------------------------------------------------------------

    def _discover_ingest_dates(self) -> dict[str, Optional[date]]:
        # each ingest dataset passes its own raw date column name
        results = {}

        datasets = {
            "ingest/derivatives": ("derivatives", INGEST_DATE_COLUMNS["derivatives"]),
            "ingest/index_spot":  ("index_spot",  INGEST_DATE_COLUMNS["index_spot"]),
            "ingest/vix":         ("vix",          INGEST_DATE_COLUMNS["vix"]),
            "ingest/index_yield": ("index_yield",  INGEST_DATE_COLUMNS["index_yield"]),
            "ingest/gbond":       ("gbond",         INGEST_DATE_COLUMNS["gbond"]),
        }
        for label, (folder, date_col) in datasets.items():
            results[label] = self._get_latest_date_from_dir(
                self.ingest_dir / folder, date_col=date_col
            )

        # trade calendar is a single flat file, already uses trade_date
        tc_path = self.ingest_dir / "TradeCalendar" / "trade_calendar.parquet"
        results["ingest/trade_calendar"] = self._get_latest_date_from_parquet(
            tc_path, date_col=INGEST_DATE_COLUMNS["trade_calendar"]
        )

        return results

    # ------------------------------------------------------------------
    # Discovery: processed layer
    # ------------------------------------------------------------------

    def _discover_processed_dates(self) -> dict[str, Optional[date]]:
        # all processed datasets use trade_date — confirmed from all processor writers
        results = {}

        year_partitioned = {
            "processed/options":     self.processed_dir / "options",
            "processed/futures":     self.processed_dir / "futures",
            "processed/index_spot":  self.processed_dir / "index_spot",
            "processed/vix":         self.processed_dir / "vix",
            "processed/index_yield": self.processed_dir / "index_yield",
            "processed/gbond":       self.processed_dir / "gbond",
        }
        for label, path in year_partitioned.items():
            results[label] = self._get_latest_date_from_dir(path, date_col="trade_date")

        # trade calendar single flat file
        tc_path = self.processed_dir / "trade_calendar" / "trade_calendar.parquet"
        results["processed/trade_calendar"] = self._get_latest_date_from_parquet(
            tc_path, date_col="trade_date"
        )

        return results

    # ------------------------------------------------------------------
    # Discovery: curated layer (auto-discover)
    # ------------------------------------------------------------------

    def _discover_curated_dates(self) -> dict[str, Optional[date]]:
        # globs all subdirs under curated/ — no hardcoded table names
        # confirmed from CuratedOptionChainBuilder: writes trade_date column
        results = {}

        if not self.curated_dir.exists():
            self.logger.warning("Curated directory not found: %s", self.curated_dir)
            return results

        for subdir in sorted(self.curated_dir.iterdir()):
            if not subdir.is_dir():
                continue
            label = f"curated/{subdir.name}"
            results[label] = self._get_latest_date_from_dir(subdir, date_col="trade_date")

        return results

    # ------------------------------------------------------------------
    # Core comparison logic
    # ------------------------------------------------------------------

    def _compute_expected_date(
        self, all_dates: dict[str, Optional[date]]
    ) -> Optional[date]:
        # expected = max across all datasets that returned a valid date
        valid = [d for d in all_dates.values() if d is not None]
        return max(valid) if valid else None

    def _filter_by_window(
        self,
        all_dates: dict[str, Optional[date]],
        from_date: Optional[date],
        to_date: Optional[date],
    ) -> dict[str, Optional[date]]:
        # datasets whose latest date is before from_date are set to None
        # signals no data in the requested window without crashing
        filtered = {}
        for name, d in all_dates.items():
            if d is None:
                filtered[name] = None
                continue
            if from_date and d < from_date:
                filtered[name] = None
            else:
                filtered[name] = d
        return filtered

    def _log_sync_report(
        self,
        all_dates: dict[str, Optional[date]],
        expected: Optional[date],
    ):
        # IN SYNC at INFO, OUT OF SYNC at WARNING
        # single summary line at end
        if expected is None:
            self.logger.warning("[SYNC] No valid trade dates found across any dataset.")
            return

        out_of_sync = []

        for name, latest in sorted(all_dates.items()):
            if latest is None:
                self.logger.warning(
                    "[OUT OF SYNC] %-40s latest: %-12s  expected: %s",
                    name, "NO DATA", expected,
                )
                out_of_sync.append(name)
            elif latest < expected:
                self.logger.warning(
                    "[OUT OF SYNC] %-40s latest: %-12s  expected: %s",
                    name, latest, expected,
                )
                out_of_sync.append(name)
            else:
                self.logger.info(
                    "[IN SYNC]     %-40s latest: %s",
                    name, latest,
                )

        if out_of_sync:
            self.logger.warning(
                "[SYNC] Sync check complete. %d dataset(s) out of sync: %s",
                len(out_of_sync),
                ", ".join(out_of_sync),
            )
        else:
            self.logger.info(
                "[SYNC] All layers in sync. Latest trade date: %s", expected
            )

    # ------------------------------------------------------------------
    # Public entry points
    # ------------------------------------------------------------------

    def run_daily_check(self):
        self.logger.info("[SYNC] Running daily sync check.")

        ingest_dates = self._discover_ingest_dates()
        processed_dates = self._discover_processed_dates()
        curated_dates = self._discover_curated_dates()

        all_dates = {**ingest_dates, **processed_dates, **curated_dates}
        expected = self._compute_expected_date(all_dates)

        self._log_sync_report(all_dates, expected)

    def run_full_check(
        self,
        from_date: Optional[date] = None,
        to_date: Optional[date] = None,
    ):
        window_desc = "full history"
        if from_date or to_date:
            window_desc = f"{from_date or 'start'} to {to_date or 'end'}"

        self.logger.info("[SYNC] Running full sync check (%s).", window_desc)

        ingest_dates = self._discover_ingest_dates()
        processed_dates = self._discover_processed_dates()
        curated_dates = self._discover_curated_dates()

        all_dates = {**ingest_dates, **processed_dates, **curated_dates}

        if from_date or to_date:
            all_dates = self._filter_by_window(all_dates, from_date, to_date)
            expected = self._compute_expected_date(all_dates)
            if to_date and expected and expected > to_date:
                expected = to_date
        else:
            expected = self._compute_expected_date(all_dates)

        self._log_sync_report(all_dates, expected)

    def run(
        self,
        mode: str,
        from_date: Optional[date] = None,
        to_date: Optional[date] = None,
    ):
        if mode == "daily":
            self.run_daily_check()
        elif mode == "full":
            self.run_full_check(from_date, to_date)
        else:
            raise ValueError(f"Invalid mode: '{mode}'. Expected 'daily' or 'full'.")
