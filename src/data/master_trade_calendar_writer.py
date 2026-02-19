import logging
import pandas as pd
from src.core.fetch_config import FetchConfig

class TradeCalendarWriter:

    def __init__(self, config: FetchConfig, rebuild: bool = False):
        self.config = config
        self.rebuild = rebuild
        self.namespace = "TradeCalendar"

        self.derivatives_base = config.ingest_dir / "derivatives"
        self.base_path = config.ingest_dir / self.namespace
        self.calendar_path = self.base_path / "trade_calendar.parquet"

        self.base_path.mkdir(parents=True, exist_ok=True)

        logging.basicConfig(
            filename=config.logs_dir / "data_pipeline_fetch.log",
            level=logging.INFO,
            format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
        )
        self.logger = logging.getLogger("TradeCalendarWriter")

    def run(self):

        if self.rebuild:
            if self.calendar_path.exists():
                self.calendar_path.unlink()
                self.logger.info("Trade calendar deleted (rebuild mode).")

            df_calendar = self._build_full_calendar()

        else:
            if not self.calendar_path.exists():
                self.logger.info("Calendar not found. Running full build.")
                df_calendar = self._build_full_calendar()
            else:
                df_calendar = self._append_incremental()

        df_calendar.to_parquet(self.calendar_path, index=False)

        self.logger.info(f"Trade calendar saved: {self.calendar_path}")

        return self.calendar_path

    def _build_full_calendar(self):

        dfs = []

        for year_dir in sorted(self.derivatives_base.iterdir()):
            if year_dir.is_dir():
                for file in year_dir.glob("*.parquet"):
                    df = pd.read_parquet(file, columns=["TIMESTAMP", "SYMBOL"])
                    dfs.append(df)

        if not dfs:
            raise ValueError("No derivatives parquet files found.")

        combined = pd.concat(dfs, ignore_index=True)

        return self._prepare_calendar(combined)

    def _append_incremental(self):

        existing = pd.read_parquet(self.calendar_path)

        dfs = []

        for year_dir in sorted(self.derivatives_base.iterdir()):
            if year_dir.is_dir():
                for file in year_dir.glob("*.parquet"):
                    df = pd.read_parquet(file, columns=["TIMESTAMP", "SYMBOL"])
                    dfs.append(df)

        combined_derivatives = pd.concat(dfs, ignore_index=True)

        new_calendar = self._prepare_calendar(combined_derivatives)

        combined = (
            pd.concat([existing, new_calendar], ignore_index=True)
            .drop_duplicates()
            .sort_values(["trade_date", "symbol"])
            .reset_index(drop=True)
        )

        return combined

    def _prepare_calendar(self, df):

        df["trade_date"] = pd.to_datetime(df["TIMESTAMP"]).dt.normalize()

        calendar = (
            df[["trade_date", "SYMBOL"]]
            .drop_duplicates()
            .rename(columns={"SYMBOL": "symbol"})
            .sort_values(["trade_date", "symbol"])
            .reset_index(drop=True)
        )

        return calendar
